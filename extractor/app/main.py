"""质检异常检测关联数据提取 API（FastAPI）。

端点
----
POST /extractions        提交检测 ID 集合，后台生成 SQL
GET  /extractions        查看全部任务
GET  /extractions/{id}   查询任务状态
GET  /extractions/{id}/download   仅成功任务可下载（失败不提供残件）
GET  /health             健康检查与源库连通性

任务在后台线程中执行；提取过程全程处于同一个 REPEATABLE READ 只读快照事务。
"""

from __future__ import annotations

import json
import os
import threading
import traceback
import uuid
from datetime import datetime, timezone
from pathlib import Path

from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse
from pydantic import BaseModel, Field
import psycopg

from .extractor import ExtractionError, run_extraction

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
    title="质检异常检测关联数据提取 API",
    version="1.0.0",
    description=(
        "提交检测 ID 集合，基于源库单一快照生成可直接导入空 PostgreSQL 的 SQL，"
        "包含递归外键闭包与复测互引（含环）还原。"
    ),
)


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
                delay_seconds=SNAPSHOT_DELAY,
            )
            size = out_path.stat().st_size
            store.update(
                job_id,
                status="succeeded",
                finished_at=datetime.now(timezone.utc).isoformat(),
                snapshot_xmin=stats.snapshot_xmin,
                rows=stats.rows,
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
