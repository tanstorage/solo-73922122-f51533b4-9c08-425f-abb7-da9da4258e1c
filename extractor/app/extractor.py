"""异常检测单关联数据提取核心逻辑。

设计要点
========
1. 单一快照：整个任务在一条连接、一个 REPEATABLE READ 事务中完成。
   PostgreSQL 事务快照在事务首条语句时确定，之后无论源库如何增改删，
   本事务内所有查询（含分批 keyset 读取）看到的都是同一版本的数据，
   不会混用新旧状态，且不阻塞源库写入。
   事务为读写事务：在同一快照内把逐检测修订依据（xmin/result/measured）
   写入 extraction_inspection_basis，保证依据与导出的 SQL 来自完全相同的快照，
   并与任务结果在同一事务提交，失败整体回滚不留残依据。
2. 复测图递归：retest_of_id 自引用关系按“无向图”遍历——既顺着
   retest_of_id 找父检测，也反向找以当前检测为复测对象的子检测，
   visited 集合保证成环（含自引用）时可终止且完整还原。
3. 外键闭包：入选检测 -> 工件（workpiece_id）-> 批次（batch_id），
   只递归外键目标，绝不因同批次而夹带其他工件。
4. 结构还原：内省 information_schema / pg_constraint / pg_indexes，
   数据先全部导入，最后再建主键、外键、索引，因此复测互引（含环）
   不会在导入期触发外键违约，且导入后外键约束与源库一致。
"""

from __future__ import annotations

import datetime as _dt
import decimal
import os
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import psycopg

# 提取范围固定为质检三表，顺序即外键依赖顺序
TABLES = ("qc_batch", "workpiece", "inspection")
PAGE_SIZE = 1000


class ExtractionError(Exception):
    """提取失败（数据缺失或 SQL 错误等）。"""


@dataclass
class ExtractionStats:
    snapshot_xmin: int | None = None
    inspection_ids: list[int] = field(default_factory=list)
    rows: dict[str, int] = field(default_factory=dict)
    basis_rows: int = 0


# --------------------------------------------------------------------------
# 字面量渲染：不依赖源端驱动类型，全部渲染为可直接导入的 SQL 字面量
# --------------------------------------------------------------------------
def sql_literal(value: Any) -> str:
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, (int, float, decimal.Decimal)):
        return str(value)
    if isinstance(value, bytes):
        return "'\\x" + value.hex() + "'"
    if isinstance(value, uuid.UUID):
        return "'" + str(value) + "'"
    if isinstance(value, (_dt.datetime, _dt.date, _dt.time, _dt.timedelta)):
        return "'" + str(value).replace("'", "''") + "'"
    # 其它类型一律按文本转义；standard_conforming_strings=on 下反斜杠为普通字符
    text = str(value).replace("'", "''")
    return "'" + text + "'"


# --------------------------------------------------------------------------
# 结构内省
# --------------------------------------------------------------------------
def _introspect_table(conn: psycopg.Connection, table: str) -> dict[str, Any]:
    cols = conn.execute(
        """
        SELECT column_name, data_type, udt_name, is_nullable,
               column_default, ordinal_position
          FROM information_schema.columns
         WHERE table_schema = 'public' AND table_name = %s
         ORDER BY ordinal_position
        """,
        (table,),
    ).fetchall()
    if not cols:
        raise ExtractionError(f"源库缺少表 public.{table}")

    constraints = conn.execute(
        """
        SELECT conname, pg_get_constraintdef(c.oid) AS def
          FROM pg_constraint c
          JOIN pg_class t ON t.oid = c.conrelid
          JOIN pg_namespace n ON n.oid = t.relnamespace
         WHERE n.nspname = 'public' AND t.relname = %s
         ORDER BY CASE c.contype WHEN 'p' THEN 0 WHEN 'u' THEN 1
                                 WHEN 'f' THEN 2 ELSE 3 END, c.conname
        """,
        (table,),
    ).fetchall()

    indexes = conn.execute(
        """
        SELECT cl.relname AS indexname,
               pg_get_indexdef(cl.oid) AS indexdef
          FROM pg_class cl
          JOIN pg_namespace n ON n.oid = cl.relnamespace
          JOIN pg_index ix ON ix.indexrelid = cl.oid
          JOIN pg_class tbl ON tbl.oid = ix.indrelid
         WHERE n.nspname = 'public'
           AND tbl.relname = %s
           AND cl.relkind = 'i'
           AND NOT EXISTS (
                 SELECT 1 FROM pg_constraint c WHERE c.conindid = cl.oid)
         ORDER BY cl.relname
        """,
        (table,),
    ).fetchall()

    return {
        "columns": [
            {
                "name": r[0],
                "data_type": r[1],
                "udt_name": r[2],
                "nullable": r[3] == "YES",
                "default": r[4],
                "pos": r[5],
            }
            for r in cols
        ],
        "constraints": [{"name": r[0], "def": r[1]} for r in constraints],
        "indexes": [{"name": r[0], "def": r[1]} for r in indexes],
    }


def _render_create_table(table: str, meta: dict[str, Any]) -> list[str]:
    lines: list[str] = [f"CREATE TABLE {table} ("]
    col_lines = []
    for c in meta["columns"]:
        # 优先使用标准 data_type（如 bigint、timestamp with time zone）
        type_name = c["data_type"]
        if type_name == "USER-DEFINED":
            type_name = c["udt_name"]
        parts = [c["name"], type_name]
        if c["default"] is not None:
            parts.append(f"DEFAULT {c['default']}")
        if not c["nullable"]:
            parts.append("NOT NULL")
        col_lines.append("    " + " ".join(parts))
    lines.append(",\n".join(col_lines))
    lines.append(");")
    return lines


# --------------------------------------------------------------------------
# 主流程
# --------------------------------------------------------------------------
def run_extraction(
    dsn: str,
    inspection_ids: list[int],
    out_path: str,
    job_id: str,
    delay_seconds: float = 0.0,
) -> ExtractionStats:
    """生成可直接导入空 PostgreSQL 库的 SQL 脚本，并在同快照事务内写入修订依据。

    全部成功才落盘到 out_path 并提交事务；任何异常都会回滚事务、删除残件
    （由调用方兜底清理临时文件）。
    delay_seconds 仅用于演示：快照建立后、遍历开始前保持事务打开一段时间，
    期间源库的增改删对本事务不可见，以此直观验证快照一致性。
    """
    seeds = sorted(set(inspection_ids))
    if not seeds:
        raise ExtractionError("inspection_ids 不能为空")

    stats = ExtractionStats()

    # autocommit=True 后手工开启快照事务，事务持续到任务结束。
    # 读写事务：快照读取与“修订依据写入”必须在同一事务内原子提交。
    conn = psycopg.connect(dsn, autocommit=True, connect_timeout=10)
    committed = False
    try:
        conn.execute("BEGIN ISOLATION LEVEL REPEATABLE READ")
        snapshot = conn.execute("SELECT pg_current_snapshot()::text").fetchone()[0]
        stats.snapshot_xmin = int(str(snapshot).split(":")[0])

        if delay_seconds > 0:
            # 保持快照事务打开，供演示并发写入；等待期间本事务视图不变
            conn.execute("SELECT pg_sleep(%s)", (delay_seconds,))

        wanted = _discover_inspections(conn, seeds)
        stats.inspection_ids = sorted(wanted)

        workpieces, batches = _fk_closure(conn, wanted)
        stats.rows = {
            "qc_batch": len(batches),
            "workpiece": len(workpieces),
            "inspection": len(wanted),
        }

        metas = {t: _introspect_table(conn, t) for t in TABLES}
        col_names = {
            t: [c["name"] for c in metas[t]["columns"]] for t in TABLES
        }

        tmp_path = out_path + ".part"
        with open(tmp_path, "w", encoding="utf-8", newline="\n") as f:
            _write_header(f, stats, snapshot)
            _write_schema(f, metas)
            _write_data(f, conn, "qc_batch", "batch_id", batches, col_names["qc_batch"])
            _write_data(
                f, conn, "workpiece", "workpiece_id", workpieces, col_names["workpiece"]
            )
            _write_data(
                f, conn, "inspection", "inspection_id", wanted, col_names["inspection"]
            )
            _write_constraints_and_indexes(f, metas)
            f.write("\n-- 提取结束\n")

        # 修订依据：与上面的全部读取处于同一快照事务，逐检测记录版本号与原值。
        # 之后任何对 inspection 的 UPDATE（含改回原值）/DELETE/同 ID 重建都会使
        # xmin 变化或行消失，回写时据此判定过期。
        stats.basis_rows = _persist_basis(conn, job_id, wanted)

        # 整份文件写完后原子替换，外部只可能看到“完整文件”或“没有文件”
        os.replace(tmp_path, out_path)
        # SQL 产物与修订依据同一事务提交；提交失败则依据回滚、产物删除
        conn.execute("COMMIT")
        committed = True
        return stats
    finally:
        if not committed:
            try:
                conn.execute("ROLLBACK")
            except Exception:
                pass
            for p in (out_path, Path(str(out_path) + ".part")):
                try:
                    p.unlink()
                except FileNotFoundError:
                    pass
        conn.close()


def _discover_inspections(conn: psycopg.Connection, seeds: list[int]) -> set[int]:
    """无向遍历复测关系图，返回入选检测全集（含成环复测）。"""
    wanted: set[int] = set(seeds)
    frontier: set[int] = set(seeds)

    while frontier:
        chunk = list(frontier)
        rows = conn.execute(
            "SELECT inspection_id, retest_of_id "
            "FROM inspection WHERE inspection_id = ANY(%s)",
            (chunk,),
        ).fetchall()
        found = {r[0] for r in rows}
        missing = frontier - found
        if missing:
            raise ExtractionError(
                "以下检测 ID 在源库不存在: " + ", ".join(map(str, sorted(missing)))
            )

        next_frontier: set[int] = set()
        # 正向：当前检测指向的复测父节点
        for _, retest_of_id in rows:
            if retest_of_id is not None and retest_of_id not in wanted:
                wanted.add(retest_of_id)
                next_frontier.add(retest_of_id)

        # 反向：以当前检测为复测对象的子检测（保证环上其它节点也被还原）
        rev_rows = conn.execute(
            "SELECT inspection_id FROM inspection "
            "WHERE retest_of_id = ANY(%s)",
            (chunk,),
        ).fetchall()
        for (rid,) in rev_rows:
            if rid not in wanted:
                wanted.add(rid)
                next_frontier.add(rid)

        frontier = next_frontier

    return wanted


def _fk_closure(
    conn: psycopg.Connection, inspection_ids: set[int]
) -> tuple[set[int], set[int]]:
    """按外键递归收集必需的工件与批次。"""
    workpieces: set[int] = set()
    ids = sorted(inspection_ids)
    for i in range(0, len(ids), PAGE_SIZE):
        rows = conn.execute(
            "SELECT DISTINCT workpiece_id FROM inspection "
            "WHERE inspection_id = ANY(%s)",
            (ids[i : i + PAGE_SIZE],),
        ).fetchall()
        workpieces.update(r[0] for r in rows)

    batches: set[int] = set()
    wp = sorted(workpieces)
    for i in range(0, len(wp), PAGE_SIZE):
        rows = conn.execute(
            "SELECT DISTINCT batch_id FROM workpiece "
            "WHERE workpiece_id = ANY(%s)",
            (wp[i : i + PAGE_SIZE],),
        ).fetchall()
        batches.update(r[0] for r in rows)

    return workpieces, batches


def _persist_basis(
    conn: psycopg.Connection, job_id: str, inspection_ids: set[int]
) -> int:
    """在当前快照事务内逐检测写入回写修订依据，返回写入条数。

    依据包含快照版本号 xmin 与当时的 result/measured。同一 job 重跑时先清理
    旧依据（实践中任务只执行一次，这里做幂等兜底）。
    """
    conn.execute(
        "DELETE FROM extraction_inspection_basis WHERE job_id = %s", (job_id,)
    )
    ids = sorted(inspection_ids)
    total = 0
    for i in range(0, len(ids), PAGE_SIZE):
        chunk = ids[i : i + PAGE_SIZE]
        # xmin::text::bigint：xmin 为 xid 类型，参数化后转 BIGINT 存储
        rows = conn.execute(
            "SELECT inspection_id, xmin::text::bigint, result, measured "
            "FROM inspection WHERE inspection_id = ANY(%s) ORDER BY inspection_id",
            (chunk,),
        ).fetchall()
        with conn.cursor() as cur:
            cur.executemany(
                "INSERT INTO extraction_inspection_basis "
                "(job_id, inspection_id, base_xmin, base_result, base_measured) "
                "VALUES (%s, %s, %s, %s, %s)",
                [(job_id, r[0], r[1], r[2], r[3]) for r in rows],
            )
        total += len(rows)
    if total != len(inspection_ids):
        raise ExtractionError(
            f"修订依据写入不完整: 期望 {len(inspection_ids)} 条，实际 {total} 条"
        )
    return total


# --------------------------------------------------------------------------
# SQL 输出
# --------------------------------------------------------------------------
def _write_header(f, stats: ExtractionStats, snapshot: str) -> None:
    f.write("-- 质检异常检测关联数据提取结果\n")
    f.write(
        "-- 生成时间(UTC): "
        + _dt.datetime.now(_dt.timezone.utc).isoformat()
        + "\n"
    )
    f.write(f"-- 源库快照: {snapshot} (xmin={stats.snapshot_xmin})\n")
    f.write("-- 入选检测 ID: " + ", ".join(map(str, stats.inspection_ids)) + "\n")
    f.write(
        "-- 行数: "
        + ", ".join(f"{t}={stats.rows[t]}" for t in TABLES)
        + "\n\n"
    )
    f.write("SET standard_conforming_strings = on;\n")
    f.write("SET client_encoding = 'UTF8';\n\n")


def _write_schema(f, metas: dict[str, dict]) -> None:
    f.write("-- ========== 表结构（约束与索引在数据导入后创建） ==========\n")
    for table in TABLES:
        f.write(f"DROP TABLE IF EXISTS {table} CASCADE;\n")
    f.write("\n")
    for table in TABLES:
        f.write("\n".join(_render_create_table(table, metas[table])) + "\n")
    f.write("\n")


def _write_data(
    f,
    conn: psycopg.Connection,
    table: str,
    pk_col: str,
    id_set: set[int],
    col_names: list[str],
) -> None:
    """keyset 分页读取：同一快照事务内翻页，杜绝新旧状态混用。"""
    id_list = sorted(id_set)
    f.write(f"-- ========== 数据: {table} ({len(id_list)} 行) ==========\n")
    if not id_list:
        f.write("-- 无数据\n\n")
        return

    collist = ", ".join(col_names)
    remaining = set(id_list)
    last_pk = None
    total = 0
    while True:
        if last_pk is None:
            rows = conn.execute(
                f"SELECT {collist} FROM {table} "
                f"WHERE {pk_col} = ANY(%s) ORDER BY {pk_col} LIMIT %s",
                (id_list, PAGE_SIZE),
            ).fetchall()
        else:
            rows = conn.execute(
                f"SELECT {collist} FROM {table} "
                f"WHERE {pk_col} = ANY(%s) AND {pk_col} > %s "
                f"ORDER BY {pk_col} LIMIT %s",
                (id_list, last_pk, PAGE_SIZE),
            ).fetchall()
        if not rows:
            break
        for row in rows:
            values = ", ".join(sql_literal(v) for v in row)
            f.write(
                f"INSERT INTO {table} ({collist}) VALUES ({values});\n"
            )
            remaining.discard(row[0])
            total += 1
        last_pk = rows[-1][0]
        if len(rows) < PAGE_SIZE:
            break

    if remaining:
        # 快照内理论上不可能发生，发生即说明一致性被破坏，直接失败
        raise ExtractionError(
            f"{table} 有 {len(remaining)} 个主键在快照中读取失败: "
            + ", ".join(map(str, sorted(remaining)))
        )
    f.write(f"-- {table} 实际写入 {total} 行\n\n")


def _write_constraints_and_indexes(f, metas: dict[str, dict]) -> None:
    f.write("-- ========== 主键 / 外键 / 检查 / 唯一约束（还原源库定义） ==========\n")
    for table in TABLES:
        for c in metas[table]["constraints"]:
            f.write(
                f"ALTER TABLE {table} ADD CONSTRAINT {c['name']} {c['def']};\n"
            )
    f.write("\n-- ========== 独立索引（约束支撑的索引已随约束创建，此处不再重复） ==========\n")
    wrote_any = False
    for table in TABLES:
        for idx in metas[table]["indexes"]:
            f.write(idx["def"] + ";\n")
            wrote_any = True
    if not wrote_any:
        f.write("-- （无独立索引）\n")
    f.write("\nANALYZE;\n")
