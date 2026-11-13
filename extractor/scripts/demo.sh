#!/usr/bin/env bash
# 端到端演示：
#   1. 普通提取（复测链 + 环），验证不夹带同批其他工件
#   2. 提取期间并发修改源库，验证整份结果来自同一快照
#   3. 将成环复测数据导入空 PostgreSQL 库并验证外键约束
#
# 用法: bash scripts/demo.sh
set -euo pipefail

API="${API_URL:-http://localhost:8000}"
PSQL_SRC=(psql "postgresql://qc:qcpass@localhost:55432/qcwarehouse" -v ON_ERROR_STOP=1 -At)
PSQL_TGT=(psql "postgresql://qc:qcpass@localhost:55433/qctarget" -v ON_ERROR_STOP=1 -At)

say() { printf '\n\033[1;36m== %s ==\033[0m\n' "$*"; }
die() { printf '\033[1;31m%s\033[0m\n' "$*" >&2; exit 1; }

command -v curl >/dev/null || die "需要 curl"
command -v jq   >/dev/null || die "需要 jq"
command -v psql >/dev/null || die "需要 psql (postgresql-client)"

say "0. 健康检查"
curl -fsS "$API/health" | jq .

say "1. 提交检测 [100] —— 应递归带出复测链 101/102/105，不含 B2 批次"
JOB=$(curl -fsS -X POST "$API/extractions" \
  -H 'Content-Type: application/json' \
  -d '{"inspection_ids":[100],"note":"chain demo"}' | jq -r .job_id)
echo "job_id=$JOB"

for _ in $(seq 1 30); do
  STATUS=$(curl -fsS "$API/extractions/$JOB" | jq -r .status)
  [ "$STATUS" = "succeeded" ] || [ "$STATUS" = "failed" ] && break
  sleep 1
done
curl -fsS "$API/extractions/$JOB" | jq '{status, rows, snapshot_xmin, error}'
[ "$STATUS" = "succeeded" ] || die "提取失败"

OUT="/tmp/extraction_$JOB.sql"
curl -fsS "$API/extractions/$JOB/download" -o "$OUT"
echo "已下载: $OUT ($(wc -c < "$OUT") bytes)"

say "1a. 抽查 SQL 内容（结构 / 外键 / 数据闭包）"
grep -E 'CREATE TABLE|ADD CONSTRAINT|FOREIGN KEY' "$OUT"
echo "--- 入选工件/批次（应为 W1,W2 + B1，绝不出现 W3/B2）---"
grep -E '^INSERT INTO (qc_batch|workpiece)' "$OUT"
grep -Eq '^INSERT INTO workpiece .*VALUES \(11,' "$OUT" && echo "OK: 同批工件 W2 因被检测引用而带入" || die "缺少 W2"
! grep -Eq '^INSERT INTO workpiece .*VALUES \(12,' "$OUT" && echo "OK: 未夹带无关工件 W3" || die "夹带了同批无关工件"
! grep -Eq '^INSERT INTO qc_batch .*VALUES \(2,' "$OUT" && echo "OK: 未夹带无关批次 B2" || die "夹带了无关批次"
grep -Eq '^INSERT INTO inspection .*VALUES \(105,' "$OUT" && echo "OK: 复测链尾 105 已递归带入" || die "缺少复测 105"

say "2. 提取期间并发修改源库（同快照一致性演示）"
# SNAPSHOT_DELAY=3：快照建立后事务保持 3 秒，下列增/改/删均发生在提取窗口内
JOB2=$(curl -fsS -X POST "$API/extractions" \
  -H 'Content-Type: application/json' \
  -d '{"inspection_ids":[100,103,109]}' | jq -r .job_id)
sleep 1
"${PSQL_SRC[@]}" -c "UPDATE inspection SET result='并发修改@'||now() WHERE inspection_id=100;"
"${PSQL_SRC[@]}" -c "DELETE FROM inspection WHERE inspection_id=102;"
"${PSQL_SRC[@]}" -c "INSERT INTO workpiece(workpiece_id,batch_id,serial_no) VALUES(99,1,'SN-CONCURRENT');" \
  || echo "  (99 已存在，继续)"
"${PSQL_SRC[@]}" -c "INSERT INTO inspection(inspection_id,workpiece_id,result) VALUES(900,99,'并发新增检测');" \
  || echo "  (900 已存在，继续)"

for _ in $(seq 1 30); do
  STATUS=$(curl -fsS "$API/extractions/$JOB2" | jq -r .status)
  { [ "$STATUS" = "succeeded" ] || [ "$STATUS" = "failed" ]; } && break
  sleep 1
done
curl -fsS "$API/extractions/$JOB2" | jq '{status, rows, error}'
[ "$STATUS" = "succeeded" ] || die "并发场景提取失败"
curl -fsS "$API/extractions/$JOB2/download" -o "/tmp/extraction_$JOB2.sql"

say "2a. 快照一致性断言（提取结果停留在修改前版本）"
grep -q "^INSERT INTO inspection.*VALUES (102," "/tmp/extraction_$JOB2.sql" \
  && echo "OK: 已删除的 102 仍在提取结果中" || die "快照应包含删除前的 102"
grep -q "初检合格" "/tmp/extraction_$JOB2.sql" \
  && echo "OK: 100 为修改前旧值" || die "快照应保留 100 的旧值"
! grep -q "并发修改" "/tmp/extraction_$JOB2.sql" \
  && echo "OK: 提取结果不含并发修改后的新值" || die "快照不得包含修改后的新值"
! grep -Eq "INSERT INTO (workpiece|inspection).*VALUES \((99|900)," "/tmp/extraction_$JOB2.sql" \
  && echo "OK: 并发新增的 99/900 未混入" || die "快照不得包含并发新增行"

# 恢复被删除/修改的数据，保证后续演示可重复
"${PSQL_SRC[@]}" -c \
 "INSERT INTO inspection(inspection_id,workpiece_id,retest_of_id,result,inspector,measured,inspected_at)
  VALUES(102,11,101,'跨工件引用复测','王五',9.98,'2026-11-03 12:00:00+00') ON CONFLICT DO NOTHING;"
"${PSQL_SRC[@]}" -c "UPDATE inspection SET result='初检合格 \"A\"级' WHERE inspection_id=100;"
"${PSQL_SRC[@]}" -c "DELETE FROM inspection WHERE inspection_id=900;"
"${PSQL_SRC[@]}" -c "DELETE FROM workpiece WHERE workpiece_id=99;"

say "3. 成环复测导入空库（103<->104 环 + 109 自引用）"
JOB3=$(curl -fsS -X POST "$API/extractions" \
  -H 'Content-Type: application/json' \
  -d '{"inspection_ids":[103,109],"note":"cycle import demo"}' | jq -r .job_id)
for _ in $(seq 1 30); do
  STATUS=$(curl -fsS "$API/extractions/$JOB3" | jq -r .status)
  { [ "$STATUS" = "succeeded" ] || [ "$STATUS" = "failed" ]; } && break
  sleep 1
done
[ "$STATUS" = "succeeded" ] || { curl -fsS "$API/extractions/$JOB3" | jq .; die "环数据提取失败"; }
curl -fsS "$API/extractions/$JOB3/download" -o "/tmp/extraction_$JOB3.sql"

# 空库导入（target-db 无任何初始化数据）
"${PSQL_TGT[@]}" -f "/tmp/extraction_$JOB3.sql"

say "3a. 空库导入结果校验（外键约束真实存在且环可查询）"
"${PSQL_TGT[@]}" -c "SELECT conname, conrelid::regclass AS tbl FROM pg_constraint WHERE contype='f';"
"${PSQL_TGT[@]}" -c "SELECT inspection_id, retest_of_id, result FROM inspection ORDER BY inspection_id;"
"${PSQL_TGT[@]}" -c \
 "WITH RECURSIVE walk AS (
    SELECT inspection_id, retest_of_id, 1 AS depth FROM inspection WHERE inspection_id=103
    UNION ALL
    SELECT i.inspection_id, i.retest_of_id, w.depth+1
      FROM inspection i JOIN walk w ON i.inspection_id = w.retest_of_id
     WHERE w.depth < 5)
  SELECT * FROM walk;"
"${PSQL_TGT[@]}" -c "SELECT count(*) AS batch_rows FROM qc_batch;
                     SELECT count(*) AS workpiece_rows FROM workpiece;
                     SELECT count(*) AS inspection_rows FROM inspection;"

say "4. 失败不提供残件：提交不存在的检测 ID"
JOB4=$(curl -fsS -X POST "$API/extractions" \
  -H 'Content-Type: application/json' \
  -d '{"inspection_ids":[999999]}' | jq -r .job_id)
for _ in $(seq 1 30); do
  STATUS=$(curl -fsS "$API/extractions/$JOB4" | jq -r .status)
  { [ "$STATUS" = "succeeded" ] || [ "$STATUS" = "failed" ]; } && break
  sleep 1
done
curl -fsS "$API/extractions/$JOB4" | jq '{status, error}'
CODE=$(curl -s -o /dev/null -w '%{http_code}' "$API/extractions/$JOB4/download")
echo "失败任务下载 HTTP 状态码: $CODE（预期 409）"
[ "$CODE" = "409" ] || die "失败任务不应可下载"

say "全部演示通过"
