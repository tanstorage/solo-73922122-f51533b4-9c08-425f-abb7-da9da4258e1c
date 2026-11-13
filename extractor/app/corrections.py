"""检测结果更正回写服务。

核心规则
========
1. 仅接受“新提取”任务：提取时服务端在与导出 SQL 同一快照事务内，逐检测保存
   修订依据（extraction_inspection_basis：快照 xmin + 当时的 result/measured）。
   旧任务没有依据行，一律拒绝回写。
2. 乐观并发控制：回写事务先对目标行 ``SELECT ... FOR UPDATE`` 加行锁，再核对当前
   xmin 是否仍等于依据中的 base_xmin。
      * 行已删除 / 同 ID 重建 / 被 UPDATE（即使改回原值，xmin 也必然变化）
        -> 记入冲突；
      * 任一目标冲突 -> 整批 409、列出全部冲突 ID，不写任何行；
      * 行锁保证“核对与写入之间”并发更正不会互相覆盖：锁不相交的两批更正互不
        影响、都可写入；锁相交时后到者重新核对版本，发现已被更新即 409。
3. 原子性：业务 UPDATE、回执 INSERT 与审计留痕在同一个源 PostgreSQL 事务内提交，
   任何失败整体回滚，全不生效。
4. 幂等：request_id 唯一。同 request_id 重试时
      * 内容一致 -> 直接返回原回执（不重复修改）；
      * 内容不一致 -> 409；
   即使首次请求在提交后断连/进程重启，回执已随事务落库，重试仍只返回原回执。
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from typing import Any

import psycopg
from psycopg.types.json import Jsonb

MAX_CORRECTIONS = 20


class CorrectionError(Exception):
    """更正被拒绝（语义错误），HTTP 层映射为对应状态码。"""

    def __init__(self, status_code: int, message: str, extra: dict | None = None):
        super().__init__(message)
        self.status_code = status_code
        self.message = message
        self.extra = extra or {}


@dataclass(frozen=True)
class CorrectionItem:
    inspection_id: int
    result: str
    measured: float | None


@dataclass
class Receipt:
    request_id: str
    job_id: str
    content_hash: str
    corrected_count: int
    corrected_inspection_ids: list[int]
    request_payload: dict[str, Any]
    applied_changes: list[dict[str, Any]]
    created_at: str
    replayed: bool = False


# --------------------------------------------------------------------------
# 台账 DDL（与 sql/03_corrections.sql 保持一致；启动时幂等执行，
# 保证即使源库不是 compose 初始化的，也能自行建表）
# --------------------------------------------------------------------------
SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS extraction_inspection_basis (
    job_id          TEXT        NOT NULL,
    inspection_id   BIGINT      NOT NULL,
    base_xmin       BIGINT      NOT NULL,
    base_result     TEXT        NOT NULL,
    base_measured   DOUBLE PRECISION,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (job_id, inspection_id)
    -- 刻意无 inspection_id 外键：依据为历史快照审计记录，源行删除/重建后仍须保留
);
CREATE INDEX IF NOT EXISTS idx_eib_inspection
    ON extraction_inspection_basis(inspection_id);
CREATE TABLE IF NOT EXISTS correction_receipt (
    request_id          TEXT PRIMARY KEY,
    job_id              TEXT        NOT NULL,
    content_hash        TEXT        NOT NULL,
    corrected_count     INTEGER     NOT NULL,
    corrected_inspection_ids BIGINT[] NOT NULL,
    request_payload     JSONB       NOT NULL,
    applied_changes     JSONB       NOT NULL,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_cr_job ON correction_receipt(job_id);
-- 幂等迁移：移除早期版本遗留的外键，保证检测行可被直接 SQL 删除
ALTER TABLE extraction_inspection_basis
    DROP CONSTRAINT IF EXISTS extraction_inspection_basis_inspection_id_fkey;
"""


def ensure_schema(dsn: str) -> None:
    with psycopg.connect(dsn, connect_timeout=10) as conn:
        conn.execute(SCHEMA_SQL)
        conn.commit()


def canonical_hash(job_id: str, items: list[CorrectionItem]) -> str:
    """规范化内容哈希：与字段顺序、条目顺序、空格无关，只取决于 job 与逐条新值。"""
    payload = {
        "job_id": job_id,
        "items": sorted(
            (
                {
                    "inspection_id": it.inspection_id,
                    "result": it.result,
                    "measured": it.measured,
                }
                for it in items
            ),
            key=lambda x: x["inspection_id"],
        ),
    }
    blob = json.dumps(
        payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")
    return hashlib.sha256(blob).hexdigest()


def _row_to_receipt(row: tuple, replayed: bool) -> Receipt:
    (
        request_id,
        job_id,
        content_hash,
        corrected_count,
        corrected_ids,
        request_payload,
        applied_changes,
        created_at,
    ) = row
    return Receipt(
        request_id=request_id,
        job_id=job_id,
        content_hash=content_hash,
        corrected_count=corrected_count,
        corrected_inspection_ids=list(corrected_ids),
        request_payload=request_payload,
        applied_changes=applied_changes,
        created_at=created_at.isoformat(),
        replayed=replayed,
    )


_SELECT_RECEIPT = (
    "SELECT request_id, job_id, content_hash, corrected_count, "
    "corrected_inspection_ids, request_payload, applied_changes, created_at "
    "FROM correction_receipt WHERE request_id = %s"
)

_LIST_RECEIPTS = (
    "SELECT request_id, job_id, content_hash, corrected_count, "
    "corrected_inspection_ids, request_payload, applied_changes, created_at "
    "FROM correction_receipt ORDER BY created_at DESC"
)


def get_receipt(dsn: str, request_id: str) -> Receipt | None:
    with psycopg.connect(dsn, connect_timeout=10) as conn:
        row = conn.execute(_SELECT_RECEIPT, (request_id,)).fetchone()
        return _row_to_receipt(row, replayed=False) if row else None


def list_receipts(dsn: str) -> list[Receipt]:
    with psycopg.connect(dsn, connect_timeout=10) as conn:
        rows = conn.execute(_LIST_RECEIPTS).fetchall()
        return [_row_to_receipt(r, replayed=False) for r in rows]


def apply_correction(
    dsn: str,
    job_id: str,
    request_id: str,
    items: list[CorrectionItem],
    job_status_provider,
) -> Receipt:
    """执行一批更正，返回回执（首次提交 replayed=False，幂等重放 replayed=True）。

    job_status_provider: 回调，入参 job_id，返回 JobStore 中的任务 dict 或 None。
    整个流程在单条连接的单事务内完成，异常即回滚。
    """
    content_hash = canonical_hash(job_id, items)
    target_ids = sorted(it.inspection_id for it in items)
    item_by_id = {it.inspection_id: it for it in items}
    request_payload = {
        "job_id": job_id,
        "request_id": request_id,
        "items": [
            {
                "inspection_id": it.inspection_id,
                "result": it.result,
                "measured": it.measured,
            }
            for it in items
        ],
    }

    conn = psycopg.connect(dsn, connect_timeout=10)
    try:
        # 事务级咨询锁：串行化同一 request_id 的并发请求（重复点击/超时重试），
        # 不同 request_id 哈希到不同锁槽，互不阻塞。
        # blake2b 8 字节结果映射到 bigint 有符号范围，避免被推断为 numeric。
        lock_key = int.from_bytes(
            hashlib.blake2b(request_id.encode(), digest_size=8).digest(),
            "big",
            signed=True,
        )
        conn.execute("SELECT pg_advisory_xact_lock(%s)", (lock_key,))

        existing = conn.execute(_SELECT_RECEIPT, (request_id,)).fetchone()
        if existing is not None:
            receipt = _row_to_receipt(existing, replayed=True)
            if receipt.content_hash != content_hash:
                raise CorrectionError(
                    409,
                    f"request_id={request_id} 已用于不同内容的更正请求，"
                    "拒绝重复使用（幂等冲突）",
                    extra={
                        "request_id": request_id,
                        "reason": "idempotency_key_reused",
                        "original_content_hash": receipt.content_hash,
                        "incoming_content_hash": content_hash,
                    },
                )
            conn.commit()
            return receipt

        # 任务必须是成功提取（JobStore 为应用侧状态；成功任务才有依据）
        job = job_status_provider(job_id)
        if job is None:
            raise CorrectionError(404, f"提取任务 {job_id} 不存在")
        if job.get("status") != "succeeded":
            raise CorrectionError(
                409,
                f"任务状态为 {job.get('status')}，仅 succeeded 任务可更正回写",
                extra={"request_id": request_id, "job_id": job_id},
            )

        # 1) 依据核对：逐检测取出该任务保存的快照依据
        basis_rows = conn.execute(
            "SELECT inspection_id, base_xmin, base_result, base_measured "
            "FROM extraction_inspection_basis "
            "WHERE job_id = %s AND inspection_id = ANY(%s)",
            (job_id, target_ids),
        ).fetchall()
        basis_by_id = {r[0]: r for r in basis_rows}
        missing_basis = [i for i in target_ids if i not in basis_by_id]
        if missing_basis:
            # 新任务不会发生；旧任务（依据功能上线前提取）在此被拒绝
            raise CorrectionError(
                422,
                "缺少提取快照修订依据，拒绝回写：请重新发起提取后再更正"
                "（旧任务在依据功能上线前生成，不满足回写前提）",
                extra={
                    "request_id": request_id,
                    "job_id": job_id,
                    "missing_basis_inspection_ids": missing_basis,
                },
            )

        # 2) 行锁 + 版本核对。FOR UPDATE 串行化并发更正；
        #    READ COMMITTED 下若行正被另一更正事务锁住，此处等待其提交后读到最新版本。
        locked_rows = conn.execute(
            "SELECT inspection_id, xmin::text::bigint, result, measured "
            "FROM inspection WHERE inspection_id = ANY(%s) "
            "ORDER BY inspection_id FOR UPDATE",
            (target_ids,),
        ).fetchall()
        current_by_id = {r[0]: r for r in locked_rows}

        conflicts: list[dict[str, Any]] = []
        for tid in target_ids:
            cur = current_by_id.get(tid)
            base = basis_by_id[tid]
            if cur is None:
                # 提取后被删除（也可能删除后同 ID 重建为不同版本，xmin 同样对不上）
                conflicts.append(
                    {
                        "inspection_id": tid,
                        "reason": "row_missing",
                        "detail": "提取后该检测已被删除或同 ID 重建为新版本",
                        "base_xmin": base[1],
                        "current_xmin": None,
                    }
                )
            elif cur[1] != base[1]:
                conflicts.append(
                    {
                        "inspection_id": tid,
                        "reason": "version_changed",
                        "detail": "提取后该检测被更新（即使改回原值，版本号也已变化）",
                        "base_xmin": base[1],
                        "current_xmin": cur[1],
                        "current_result": cur[2],
                        "current_measured": cur[3],
                    }
                )

        if conflicts:
            raise CorrectionError(
                409,
                "目标检测在提取后已发生变化，整批拒绝回写（未修改任何数据）",
                extra={
                    "request_id": request_id,
                    "job_id": job_id,
                    "conflict_inspection_ids": [c["inspection_id"] for c in conflicts],
                    "conflicts": conflicts,
                },
            )

        # 3) 全部目标仍停留在提取快照版本：仅更新 result / measured 两列，
        #    其它列（workpiece_id / retest_of_id / inspector / inspected_at）绝不触碰。
        applied_changes: list[dict[str, Any]] = []
        for tid in target_ids:
            cur = current_by_id[tid]
            base = basis_by_id[tid]
            it = item_by_id[tid]
            conn.execute(
                "UPDATE inspection SET result = %s, measured = %s "
                "WHERE inspection_id = %s AND xmin::text::bigint = %s",
                (it.result, it.measured, tid, base[1]),
            )
            applied_changes.append(
                {
                    "inspection_id": tid,
                    "old_result": cur[2],
                    "new_result": it.result,
                    "old_measured": cur[3],
                    "new_measured": it.measured,
                    "base_xmin": base[1],
                }
            )

        # 4) 回执与业务更新同一事务提交：提交即幂等生效，未提交则全不生效
        conn.execute(
            "INSERT INTO correction_receipt "
            "(request_id, job_id, content_hash, corrected_count, "
            " corrected_inspection_ids, request_payload, applied_changes) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s)",
            (
                request_id,
                job_id,
                content_hash,
                len(items),
                target_ids,
                Jsonb(request_payload),
                Jsonb(applied_changes),
            ),
        )
        conn.commit()

        row = conn.execute(_SELECT_RECEIPT, (request_id,)).fetchone()
        return _row_to_receipt(row, replayed=False)
    except CorrectionError:
        conn.rollback()
        raise
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def get_basis(dsn: str, job_id: str) -> list[dict[str, Any]]:
    """读取某任务的逐检测修订依据（供 /docs 与排错核对）。"""
    with psycopg.connect(dsn, connect_timeout=10) as conn:
        rows = conn.execute(
            "SELECT inspection_id, base_xmin, base_result, base_measured, created_at "
            "FROM extraction_inspection_basis WHERE job_id = %s "
            "ORDER BY inspection_id",
            (job_id,),
        ).fetchall()
        return [
            {
                "inspection_id": r[0],
                "base_xmin": r[1],
                "base_result": r[2],
                "base_measured": r[3],
                "created_at": r[4].isoformat(),
            }
            for r in rows
        ]
