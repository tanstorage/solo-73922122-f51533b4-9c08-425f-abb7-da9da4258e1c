#!/usr/bin/env bash
# 更正回写功能端到端演示（docker compose 环境）
#
# 前置：
#   docker compose up -d --build
#
# 演示内容：
#   1. 新提取在同快照事务写入逐检测修订依据，原提取与下载保持可用
#   2. 正常更正：仅改 result/measured，回执可查；同 request_id 重试幂等
#   3. 直接 SQL 写入（先改再改回原值）仍被识别：整批 409 并列出冲突 ID
#   4. 并发更正：相交检测互斥不覆盖（一个成功一个 409），不相交检测都可写入
#   5. 提交后丢响应：客户端断开后事务已提交，重试只返回原回执、不重复修改
#   6. 旧任务（无修订依据）拒绝回写
#
# 用法: bash scripts/demo_corrections.sh
set -euo pipefail

API="${API_URL:-http://localhost:8000}"
PSQL_SRC=(psql "postgresql://qc:qcpass@localhost:55432/qcwarehouse" -v ON_ERROR_STOP=1 -At)

say()  { printf '\n\033[1;36m== %s ==\033[0m\n' "$*"; }
die()  { printf '\033[1;31m%s\033[0m\n' "$*" >&2; exit 1; }

command -v curl >/dev/null || die "需要 curl"
command -v jq   >/dev/null || die "需要 jq"
command -v psql >/dev/null || die "需要 psql (postgresql-client)"

wait_job() {
  local j=$1 s
  for _ in $(seq 1 30); do
    s=$(curl -fsS "$API/extractions/$j" | jq -r .status)
    { [ "$s" = succeeded ] || [ "$s" = failed ]; } && { echo "$s"; return; }
    sleep 1
  done
  echo "$s"
}

say "0. 健康检查与样例数据复位"
curl -fsS "$API/health" | jq .
# 先清空台账（依据表已无外键，不会被业务表 TRUNCATE 级联），再复位标准样例，保证可重复
"${PSQL_SRC[@]}" -c "TRUNCATE extraction_inspection_basis, correction_receipt;"
"${PSQL_SRC[@]}" -f "$(dirname "$0")/../sql/02_seed.sql" >/dev/null

say "1. 新提取：SQL 与逐检测修订依据在同一快照事务内生成"
JOB=$(curl -fsS -X POST "$API/extractions" -H 'Content-Type: application/json' \
  -d '{"inspection_ids":[100,103],"note":"correction demo"}' | jq -r .job_id)
echo "job_id=$JOB"
S=$(wait_job "$JOB"); [ "$S" = succeeded ] || die "提取失败: $S"
curl -fsS "$API/extractions/$JOB" | jq '{status, rows, basis_rows, file_name}'
echo "--- 修订依据（快照 xmin + 当时原值）---"
curl -fsS "$API/extractions/$JOB/correction-basis" | jq '.items[] | {inspection_id, base_xmin, base_result, base_measured}'
echo "--- 原提取下载保持可用 ---"
curl -fsS "$API/extractions/$JOB/download" -o "/tmp/demo_$JOB.sql"
echo "已下载 /tmp/demo_$JOB.sql ($(wc -c < "/tmp/demo_$JOB.sql") bytes)"

say "2. 正常更正 + 回执查询 + 幂等重试"
RID="demo-req-001"
curl -fsS -X POST "$API/corrections" -H 'Content-Type: application/json' \
  -d "{\"job_id\":\"$JOB\",\"request_id\":\"$RID\",\"items\":[
        {\"inspection_id\":100,\"result\":\"核准合格-离线排错结论\",\"measured\":12.88},
        {\"inspection_id\":101,\"result\":\"核准复测-尺寸偏差\",\"measured\":null}]}" \
  | jq '{request_id, corrected_count, corrected_inspection_ids, replayed, applied_changes}'
echo "--- 源库仅 result/measured 变化，inspector 等其它列保留 ---"
"${PSQL_SRC[@]}" -c "SELECT inspection_id, result, measured, inspector FROM inspection WHERE inspection_id IN (100,101) ORDER BY 1;"
echo "--- 同 request_id 同内容重试：返回原回执（replayed=true），不重复修改 ---"
curl -fsS -X POST "$API/corrections" -H 'Content-Type: application/json' \
  -d "{\"job_id\":\"$JOB\",\"request_id\":\"$RID\",\"items\":[
        {\"inspection_id\":101,\"result\":\"核准复测-尺寸偏差\",\"measured\":null},
        {\"inspection_id\":100,\"result\":\"核准合格-离线排错结论\",\"measured\":12.88}]}" \
  | jq '{request_id, replayed, content_hash}'
echo "--- 按 request_id 查回执（断连/重启后凭此确认）---"
curl -fsS "$API/corrections/$RID" | jq '{request_id, job_id, created_at, applied_changes}'
echo "--- 同 request_id 异内容：409 ---"
CODE=$(curl -s -o /tmp/demo_conflict.json -w '%{http_code}' -X POST "$API/corrections" \
  -H 'Content-Type: application/json' \
  -d "{\"job_id\":\"$JOB\",\"request_id\":\"$RID\",\"items\":[{\"inspection_id\":100,\"result\":\"偷换\",\"measured\":1.0}]}")
echo "HTTP $CODE"; jq .detail /tmp/demo_conflict.json

say "3. 直接 SQL 写入也识别：改回原值仍算过期，整批 409 列冲突 ID"
# DBA 绕过 API 直接 SQL：先改成临时值，再改回提取时的原值
"${PSQL_SRC[@]}" -c "UPDATE inspection SET result='DBA临时干预' WHERE inspection_id=102;"
"${PSQL_SRC[@]}" -c "UPDATE inspection SET result='跨工件引用复测', measured=9.98 WHERE inspection_id=102;"
echo "102 当前值已改回: $( "${PSQL_SRC[@]}" -c "SELECT result FROM inspection WHERE inspection_id=102;" )"
CODE=$(curl -s -o /tmp/demo_sql.json -w '%{http_code}' -X POST "$API/corrections" \
  -H 'Content-Type: application/json' \
  -d "{\"job_id\":\"$JOB\",\"request_id\":\"demo-req-002\",\"items\":[
        {\"inspection_id\":102,\"result\":\"离线更正102\",\"measured\":10.02},
        {\"inspection_id\":103,\"result\":\"离线更正103\",\"measured\":7.80}]}")
echo "HTTP $CODE（预期 409）"
jq '.detail | {message, conflict_inspection_ids, conflicts}' /tmp/demo_sql.json
echo "--- 整批不生效：103 仍为原值 ---"
"${PSQL_SRC[@]}" -c "SELECT inspection_id, result FROM inspection WHERE inspection_id IN (102,103) ORDER BY 1;"

say "4. 并发更正：相交检测互斥不覆盖，不相交检测都可写入"
# 干净基线：重新提取
JOB2=$(curl -fsS -X POST "$API/extractions" -H 'Content-Type: application/json' \
  -d '{"inspection_ids":[100,106],"note":"concurrent demo"}' | jq -r .job_id)
[ "$(wait_job "$JOB2")" = succeeded ] || die "基线提取失败"
echo "--- 4a. 两个并发请求同时更正检测 100：一个成功，另一个 409，无丢失更新 ---"
curl -s -o /tmp/demo_p1.json -w '请求A HTTP %{http_code}\n' -X POST "$API/corrections" \
  -H 'Content-Type: application/json' \
  -d "{\"job_id\":\"$JOB2\",\"request_id\":\"demo-par-1\",\"items\":[{\"inspection_id\":100,\"result\":\"并发结论A\",\"measured\":1.1}]}" &
curl -s -o /tmp/demo_p2.json -w '请求B HTTP %{http_code}\n' -X POST "$API/corrections" \
  -H 'Content-Type: application/json' \
  -d "{\"job_id\":\"$JOB2\",\"request_id\":\"demo-par-2\",\"items\":[{\"inspection_id\":100,\"result\":\"并发结论B\",\"measured\":2.2}]}" &
wait
echo "100 最终值: $( "${PSQL_SRC[@]}" -c "SELECT result FROM inspection WHERE inspection_id=100;" )（只可能是 A 或 B，不会互相覆盖）"
jq '.detail.conflict_inspection_ids' /tmp/demo_p2.json 2>/dev/null || true
echo "--- 4b. 不相交检测并发更正：取全新基线后两个都成功 ---"
JOB2B=$(curl -fsS -X POST "$API/extractions" -H 'Content-Type: application/json' \
  -d '{"inspection_ids":[100,106],"note":"disjoint demo"}' | jq -r .job_id)
[ "$(wait_job "$JOB2B")" = succeeded ] || die "不相交基线提取失败"
curl -s -o /dev/null -w '请求C(100) HTTP %{http_code}\n' -X POST "$API/corrections" \
  -H 'Content-Type: application/json' \
  -d "{\"job_id\":\"$JOB2B\",\"request_id\":\"demo-par-3\",\"items\":[{\"inspection_id\":100,\"result\":\"不相交结论C\",\"measured\":3.3}]}" &
curl -s -o /dev/null -w '请求D(106) HTTP %{http_code}\n' -X POST "$API/corrections" \
  -H 'Content-Type: application/json' \
  -d "{\"job_id\":\"$JOB2B\",\"request_id\":\"demo-par-4\",\"items\":[{\"inspection_id\":106,\"result\":\"不相交结论D\",\"measured\":4.4}]}" &
wait
"${PSQL_SRC[@]}" -c "SELECT inspection_id, result, measured FROM inspection WHERE inspection_id IN (100,106) ORDER BY 1;"

say "5. 提交后丢响应：断开不影响提交，重试只拿原回执"
RID3="demo-req-lost"
# 极短超时模拟网络中断/进程重启导致响应丢失；服务端事务照常提交
curl -s --max-time 0.05 -X POST "$API/corrections" -H 'Content-Type: application/json' \
  -d "{\"job_id\":\"$JOB2\",\"request_id\":\"$RID3\",\"items\":[{\"inspection_id\":107,\"result\":\"丢响应结论\",\"measured\":3.16}]}" \
  >/dev/null 2>&1 || echo "  （客户端已超时断开，符合预期）"
sleep 1
echo "源库回执已落库: $( "${PSQL_SRC[@]}" -c "SELECT count(*) FROM correction_receipt WHERE request_id='$RID3';" ) 条"
echo "源库业务值: $( "${PSQL_SRC[@]}" -c "SELECT result FROM inspection WHERE inspection_id=107;" )"
XMIN_BEFORE=$("${PSQL_SRC[@]}" -c "SELECT xmin::text FROM inspection WHERE inspection_id=107;")
echo "--- 网络恢复后同 request_id 重试：200 + replayed=true，不重复修改 ---"
curl -s -o /tmp/demo_lost_retry.json -w '重试 HTTP %{http_code}\n' -X POST "$API/corrections" \
  -H 'Content-Type: application/json' \
  -d "{\"job_id\":\"$JOB2\",\"request_id\":\"$RID3\",\"items\":[{\"inspection_id\":107,\"result\":\"丢响应结论\",\"measured\":3.16}]}"
jq '{request_id, replayed, corrected_count}' /tmp/demo_lost_retry.json
XMIN_AFTER=$("${PSQL_SRC[@]}" -c "SELECT xmin::text FROM inspection WHERE inspection_id=107;")
[ "$XMIN_BEFORE" = "$XMIN_AFTER" ] && echo "OK: 行版本号未变化，未重复修改" || die "重复修改: $XMIN_BEFORE -> $XMIN_AFTER"

say "6. 旧任务（无修订依据）拒绝回写"
# 模拟依据功能上线前的旧任务：提取成功后删除其依据行
JOB3=$(curl -fsS -X POST "$API/extractions" -H 'Content-Type: application/json' \
  -d '{"inspection_ids":[109]}' | jq -r .job_id)
[ "$(wait_job "$JOB3")" = succeeded ] || die "旧任务模拟提取失败"
"${PSQL_SRC[@]}" -c "DELETE FROM extraction_inspection_basis WHERE job_id='$JOB3';"
CODE=$(curl -s -o /tmp/demo_old.json -w '%{http_code}' -X POST "$API/corrections" \
  -H 'Content-Type: application/json' \
  -d "{\"job_id\":\"$JOB3\",\"request_id\":\"demo-req-old\",\"items\":[{\"inspection_id\":109,\"result\":\"x\",\"measured\":1.0}]}")
echo "HTTP $CODE（预期 422）"; jq .detail.message /tmp/demo_old.json

say "全部演示完成"
echo "提示：打开 http://localhost:8000/docs 可在 Swagger UI 中直接发起更正与查询回执"
