-- 更正回写功能的服务端台账（与源业务库存放在同一个 PostgreSQL 实例）
-- 两张表由 API 启动时幂等创建，本文件同时保证 docker compose 全新初始化即具备。
-- 设计要点：
--   * 修订依据 extraction_inspection_basis 由“新提取”在与 SQL 完全相同的快照事务内写入，
--     逐检测保存快照版本号 xmin 与当时的 result/measured，作为后续回写的乐观锁依据；
--   * 回执 correction_receipt 与对 inspection 的 UPDATE 在同一事务提交，
--     request_id 唯一约束保证同请求重试幂等、断连/重启不重复修改；
--   * 全部状态都在源 PostgreSQL 内，不依赖应用本地文件或内存。

CREATE TABLE IF NOT EXISTS extraction_inspection_basis (
    job_id          TEXT        NOT NULL,
    inspection_id   BIGINT      NOT NULL,
    -- 提取快照中该行的系统版本号；任何 UPDATE（即使改回原值）/DELETE/同 ID 重建都会变化
    base_xmin       BIGINT      NOT NULL,
    base_result     TEXT        NOT NULL,
    base_measured   DOUBLE PRECISION,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (job_id, inspection_id)
    -- 刻意不对 inspection_id 建外键：依据是历史快照审计记录，源行被直接 SQL 删除
    -- 或同 ID 重建后依据仍须保留，回写时据此识别 row_missing 冲突（外键会阻止删除）。
);

CREATE INDEX IF NOT EXISTS idx_eib_inspection
    ON extraction_inspection_basis(inspection_id);

CREATE TABLE IF NOT EXISTS correction_receipt (
    request_id          TEXT PRIMARY KEY,
    job_id              TEXT        NOT NULL,
    -- 规范化请求体哈希：同 request_id 同内容返回原回执，异内容 409
    content_hash        TEXT        NOT NULL,
    corrected_count     INTEGER     NOT NULL,
    corrected_inspection_ids BIGINT[] NOT NULL,
    request_payload     JSONB       NOT NULL,
    -- 提交时各检测的新旧值留痕，与业务 UPDATE 同事务提交
    applied_changes     JSONB       NOT NULL,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_cr_job ON correction_receipt(job_id);

-- 幂等迁移：早期版本对 extraction_inspection_basis.inspection_id 建过外键，
-- 它会阻止 DBA 删除检测行，与“提取后删除须可检测并 409”的需求冲突，统一移除。
ALTER TABLE extraction_inspection_basis
    DROP CONSTRAINT IF EXISTS extraction_inspection_basis_inspection_id_fkey;
