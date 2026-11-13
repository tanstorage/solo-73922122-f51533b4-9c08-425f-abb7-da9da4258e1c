"""质检异常检测关联数据提取与更正回写 API（FastAPI）。

端点
----
POST /extractions        提交检测 ID 集合，后台生成 SQL，并在同快照事务写入修订依据
GET  /extractions        查看全部任务
GET  /extractions/{id}   查询任务状态
GET  /extractions/{id}/download        仅成功任务可下载（失败不提供残件）
GET  /extractions/{id}/correction-basis 查看逐检测修订依据（xmin + 原值）
POST /corrections        提交更正回写（最多 20 条，仅改 result/measured）
GET  /corrections        查看全部更正回执
GET  /corrections/{rid}  按请求 ID 查回执（断连/重启后凭此确认，不重复修改）
GET  /health             健康检查与源库连通性

提取任务在后台线程中执行；提取过程全程处于同一个 REPEATABLE READ 快照事务，
修订依据与 SQL 产物在该事务内原子提交。
"""

from __future__ import annotations

import contextlib
import json
import os
import threading
import traceback
import uuid
from datetime import datetime, timezone
from pathlib import Path

from fastapi import FastAPI, HTTPException, Response
from fastapi.responses import FileResponse
from pydantic import BaseModel, Field, field_validator
import psycopg

from .extractor import ExtractionError, run_extraction
from . import corrections as corr

SOURCE_DSN = os.environ.get(
    "SOURCE_DSN",
    "postgresql://qc:qcpass@source-db:5432/qcwarehouse",
)
OUTPUT_DIR = Path(os.environ.get("OUTPUT_DIR", "/data/output"))
STATE_FILE = OUTPUT_DIR / "jobs.json"
MAX_WORKERS = int(os.environ.get("MAX_WORKERS", "2"))
# 演示用：快照建立后保持事务打开的秒数（生产环境保持 0）
SNAPSHOT_DELAY = float(os.environ.get("SNAPSHOT_DELAY", "0"))

app = FastAPI(
    title="质检异常检测关联数据提取与更正 API",
    version="2.0.0",
    description=(
        "提交检测 ID 集合，基于源库单一快照生成可直接导入空 PostgreSQL 的 SQL，"
        "包含递归外键闭包与复测互引（含环）还原；并支持离线排错后的核准结果回写：\n\n"
        "1. 新提取在同一快照事务内逐检测保存修订依据（xmin + 原值），旧任务无依据拒绝回写；\n"
        "2. 回写仅改包内检测的 result/measured；目标行被更新/删除/同 ID 重建（含直接 SQL "
        "改回原值）整批 409 并列出冲突 ID；\n"
        "3. 行锁保证并发更正不互相覆盖，不相交的检测仍可并行写入；\n"
        "4. 更新与回执在源 PostgreSQL 同一事务提交；request_id 幂等，断连/重启重试不重复修改。"
    ),
)


@contextlib.asynccontextmanager
async def lifespan(_app: FastAPI):
    # 幂等创建修订依据 / 回执台账（compose 初始化的库也可重复执行）
    try:
        corr.ensure_schema(SOURCE_DSN)
    except Exception:
        # 源库尚未就绪时不阻塞启动，/health 会暴露降级状态
        traceback.print_exc()
    yield


app.router.lifespan_context = lifespan


class ExtractionRequest(BaseModel):
    inspection_ids: list[int] = Field(
        ..., min_length=1, description="入选的检测 ID 集合（重复值自动去重）"
    )
    note: str | None = Field(None, description="备注，可选")


class JobView(BaseModel):
    job_id: str
    status: str
    inspection_ids: list[int]
    note: str | None
    created_at: str
    started_at: str | None
    finished_at: str | None
    error: str | None
    snapshot_xmin: int | None
    rows: dict[str, int] | None
    basis_rows: int | None
    file_name: str | None
    file_size: int | None


class JobStore:
    """内存 + JSON 文件持久化的任务表，串行化写入。"""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._jobs: dict[str, dict] = {}
        OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
        if STATE_FILE.exists():
            try:
                self._jobs = json.loads(STATE_FILE.read_text(encoding="utf-8"))
            except json.JSONDecodeError:
                self._jobs = {}
        self._sem = threading.Semaphore(MAX_WORKERS)

    def _persist(self) -> None:
        tmp = STATE_FILE.with_suffix(".json.tmp")
        tmp.write_text(
            json.dumps(self._jobs, ensure_ascii=False, indent=2), encoding="utf-8"
        )
        os.replace(tmp, STATE_FILE)

    def create(self, inspection_ids: list[int], note: str | None) -> dict:
        job_id = uuid.uuid4().hex
        job = {
            "job_id": job_id,
            "status": "pending",
            "inspection_ids": sorted(set(inspection_ids)),
            "note": note,
            "created_at": datetime.now(timezone.utc).isoformat(),
            "started_at": None,
            "finished_at": None,
            "error": None,
            "snapshot_xmin": None,
            "rows": None,
            "basis_rows": None,
            "file_name": None,
            "file_size": None,
        }
        with self._lock:
            self._jobs[job_id] = job
            self._persist()
        return dict(job)

    def update(self, job_id: str, **fields) -> dict:
        with self._lock:
            self._jobs[job_id].update(fields)
            self._persist()
            return dict(self._jobs[job_id])

    def get(self, job_id: str) -> dict | None:
        with self._lock:
            job = self._jobs.get(job_id)
            return dict(job) if job else None

    def list(self) -> list[dict]:
        with self._lock:
            return [dict(j) for j in self._jobs.values()]


store = JobStore()


def _run_job(job_id: str, inspection_ids: list[int]) -> None:
    # 信号量在进入快照事务前获取，保证延迟演示期间排队任务不会提前建快照
    with store._sem:
        store.update(
            job_id,
            status="running",
            started_at=datetime.now(timezone.utc).isoformat(),
        )
        out_path = OUTPUT_DIR / f"extraction_{job_id}.sql"
        try:
            stats = run_extraction(
                SOURCE_DSN,
                inspection_ids,
                str(out_path),
                job_id=job_id,
                delay_seconds=SNAPSHOT_DELAY,
            )
            size = out_path.stat().st_size
            store.update(
                job_id,
                status="succeeded",
                finished_at=datetime.now(timezone.utc).isoformat(),
                snapshot_xmin=stats.snapshot_xmin,
                rows=stats.rows,
                basis_rows=stats.basis_rows,
                file_name=out_path.name,
                file_size=size,
            )
        except Exception as exc:  # 任何失败都不提供残件
            for p in (out_path, Path(str(out_path) + ".part")):
                try:
                    p.unlink()
                except FileNotFoundError:
                    pass
            if isinstance(exc, ExtractionError):
                detail = str(exc)
            else:
                detail = f"{type(exc).__name__}: {exc}"
                traceback.print_exc()
            store.update(
                job_id,
                status="failed",
                finished_at=datetime.now(timezone.utc).isoformat(),
                error=detail,
            )


@app.post("/extractions", response_model=JobView, status_code=201)
def create_extraction(req: ExtractionRequest):
    job = store.create(req.inspection_ids, req.note)
    thread = threading.Thread(
        target=_run_job,
        args=(job["job_id"], job["inspection_ids"]),
        daemon=True,
    )
    thread.start()
    return job


@app.get("/extractions", response_model=list[JobView])
def list_extractions():
    return store.list()


@app.get("/extractions/{job_id}", response_model=JobView)
def get_extraction(job_id: str):
    job = store.get(job_id)
    if job is None:
        raise HTTPException(status_code=404, detail="任务不存在")
    return job


@app.get("/extractions/{job_id}/download")
def download_extraction(job_id: str):
    job = store.get(job_id)
    if job is None:
        raise HTTPException(status_code=404, detail="任务不存在")
    if job["status"] != "succeeded" or not job["file_name"]:
        # 失败 / 运行中一律不提供下载，残件已在失败时删除
        raise HTTPException(
            status_code=409,
            detail=f"任务状态为 {job['status']}，仅 succeeded 状态可下载",
        )
    path = OUTPUT_DIR / job["file_name"]
    if not path.exists():
        raise HTTPException(status_code=410, detail="结果文件已不存在")
    return FileResponse(
        path,
        media_type="application/sql",
        filename=job["file_name"],
    )


@app.get("/health")
def health():
    info = {"status": "ok", "source_db": "unknown"}
    try:
        with psycopg.connect(SOURCE_DSN, connect_timeout=5) as conn:
            conn.execute("SELECT 1").fetchone()
        info["source_db"] = "reachable"
    except Exception as exc:
        info["status"] = "degraded"
        info["source_db"] = f"unreachable: {exc}"
    return info


# ==========================================================================
# 更正回写 API
# ==========================================================================
class CorrectionItemIn(BaseModel):
    inspection_id: int = Field(..., description="包内检测 ID")
    result: str = Field(
        ..., min_length=1, max_length=1000, description="核准后的 result 新值"
    )
    measured: float | None = Field(
        ..., description="核准后的 measured 新值；允许显式置 null"
    )


class CorrectionRequest(BaseModel):
    job_id: str = Field(..., min_length=1, description="成功提取任务 ID")
    request_id: str = Field(
        ...,
        min_length=1,
        max_length=128,
        description="客户端幂等键；同 ID 同内容重试返回原回执，异内容 409",
    )
    items: list[CorrectionItemIn] = Field(
        ..., min_length=1, max_length=corr.MAX_CORRECTIONS
    )

    @field_validator("items")
    @classmethod
    def _no_duplicate_inspection(cls, v: list[CorrectionItemIn]):
        ids = [it.inspection_id for it in v]
        dup = sorted({i for i in ids if ids.count(i) > 1})
        if dup:
            raise ValueError(f"同一批更正中检测 ID 不得重复: {dup}")
        return v


class ReceiptView(BaseModel):
    request_id: str
    job_id: str
    content_hash: str
    corrected_count: int
    corrected_inspection_ids: list[int]
    request_payload: dict
    applied_changes: list[dict]
    created_at: str
    replayed: bool = Field(
        False, description="True 表示本次为幂等重放，未再次修改源库"
    )


class BasisView(BaseModel):
    job_id: str
    basis_rows: int
    items: list[dict]


@app.post(
    "/corrections",
    response_model=ReceiptView,
    responses={
        409: {"description": "目标行版本冲突 / request_id 被异内容复用 / 任务未成功"},
        422: {"description": "缺少修订依据（旧任务）或请求体不合法"},
        404: {"description": "提取任务不存在"},
    },
)
def submit_correction(req: CorrectionRequest, response: Response):
    items = [
        corr.CorrectionItem(
            inspection_id=it.inspection_id, result=it.result, measured=it.measured
        )
        for it in req.items
    ]
    try:
        receipt = corr.apply_correction(
            SOURCE_DSN,
            job_id=req.job_id,
            request_id=req.request_id,
            items=items,
            job_status_provider=store.get,
        )
    except corr.CorrectionError as exc:
        raise HTTPException(
            status_code=exc.status_code,
            detail={"message": exc.message, **exc.extra},
        )
    # 首次成功提交 201；幂等重放 200，明确告知未重复修改
    response.status_code = 200 if receipt.replayed else 201
    return receipt.__dict__


@app.get("/corrections", response_model=list[ReceiptView])
def list_corrections():
    return [r.__dict__ for r in corr.list_receipts(SOURCE_DSN)]


@app.get("/corrections/{request_id}", response_model=ReceiptView)
def get_correction(request_id: str):
    receipt = corr.get_receipt(SOURCE_DSN, request_id)
    if receipt is None:
        raise HTTPException(
            status_code=404, detail=f"request_id={request_id} 的回执不存在"
        )
    return receipt.__dict__


@app.get(
    "/extractions/{job_id}/correction-basis",
    response_model=BasisView,
    responses={
        404: {"description": "任务不存在或为无修订依据的旧任务"},
        409: {"description": "任务尚未成功，无修订依据"},
    },
)
def get_correction_basis(job_id: str):
    job = store.get(job_id)
    if job is None:
        raise HTTPException(status_code=404, detail="任务不存在")
    if job["status"] != "succeeded":
        raise HTTPException(
            status_code=409,
            detail=f"任务状态为 {job['status']}，仅 succeeded 任务存在修订依据",
        )
    items = corr.get_basis(SOURCE_DSN, job_id)
    if not items:
        raise HTTPException(
            status_code=404,
            detail="该任务没有修订依据（旧任务），不允许更正回写，请重新提取",
        )
    return {"job_id": job_id, "basis_rows": len(items), "items": items}
