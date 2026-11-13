-- 质检数仓源库结构（PostgreSQL）
-- 三表：批次 qc_batch、工件 workpiece、检测 inspection
-- 检测可通过 retest_of_id 指向另一份检测（复测），复测之间允许成环。

CREATE TABLE IF NOT EXISTS qc_batch (
    batch_id    BIGINT PRIMARY KEY,
    batch_code  TEXT NOT NULL,
    supplier    TEXT,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS workpiece (
    workpiece_id BIGINT PRIMARY KEY,
    batch_id     BIGINT NOT NULL REFERENCES qc_batch(batch_id),
    serial_no    TEXT NOT NULL,
    material     TEXT,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS inspection (
    inspection_id BIGINT PRIMARY KEY,
    workpiece_id  BIGINT NOT NULL REFERENCES workpiece(workpiece_id),
    retest_of_id  BIGINT REFERENCES inspection(inspection_id),
    result        TEXT NOT NULL,
    inspector     TEXT,
    measured      DOUBLE PRECISION,
    inspected_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_workpiece_batch ON workpiece(batch_id);
CREATE INDEX IF NOT EXISTS idx_inspection_workpiece ON inspection(workpiece_id);
CREATE INDEX IF NOT EXISTS idx_inspection_retest ON inspection(retest_of_id);
