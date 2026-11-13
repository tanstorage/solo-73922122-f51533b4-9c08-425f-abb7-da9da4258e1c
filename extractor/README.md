# 质检异常检测关联数据提取与更正回写服务

面向离线排错场景，提供两阶段能力：

1. **数据提取**：提交一组**检测 ID**，服务从持续写入的源 PostgreSQL 中，
   基于**单一数据库快照**生成一份可直接导入**空 PostgreSQL 库**的 SQL，包含：

   - 入选检测，以及沿外键递归必需的全部目标行（检测 → 工件 → 批次）；
   - 复测关系（`inspection.retest_of_id` 自引用）的完整连通分量，**含互相引用的环与自引用**；
   - 三张表的表结构、主键 / 外键 / 唯一 / 检查约束与索引，定义与源库一致；
   - 不会夹带仅因“同批次”而关联、但未被入选检测引用的其他工件。

2. **更正回写**：质检数仓工程师离线排错核准后，把检测的 `result` / `measured`
   新值写回源库。服务端在提取时即于**同一快照事务**内逐检测保存修订依据
   （快照版本号 `xmin` + 当时原值），回写时做乐观并发控制，保证：

   - 只改包内检测的 `result` / `measured` 两列，其它列绝不触碰；
   - 目标在提取后被更新（**即使直接 SQL 改回原值**）、删除或同 ID 重建，
     整批拒绝并返回 409 与全部冲突 ID；
   - 行锁保证并发更正不互相覆盖；不相交的检测仍可并行写入；
   - 业务更新与成功回执在源 PostgreSQL **同一事务**提交，失败全不生效；
   - `request_id` 幂等：同 ID 同内容重试返回原回执，异内容 409；
     提交后断连或重启也不会重复修改。

## 目录

```
extractor/
├── app/
│   ├── main.py          # FastAPI：提取 / 下载 / 更正 / 回执 / 健康检查
│   ├── extractor.py     # 快照事务、复测图递归、外键闭包、DDL 内省、SQL 生成、修订依据写入
│   └── corrections.py   # 更正回写：乐观锁、行锁、冲突识别、幂等回执、台账 DDL
├── sql/
│   ├── 01_schema.sql    # 源库三表结构（含外键、索引）
│   ├── 02_seed.sql      # 关联样例：复测链、跨工件复测、两组成环、自引用
│   └── 03_corrections.sql # 修订依据表 + 回执表（API 启动时也会幂等自建/迁移）
├── scripts/
│   ├── demo.sh             # 提取端到端演示（含并发修改与空库导入）
│   └── demo_corrections.sh # 更正回写演示（直接 SQL 改回 / 并发更正 / 丢响应重试）
├── Dockerfile
└── docker-compose.yml # source-db + api + 空库 target-db
```

## 快速启动

```bash
docker compose up -d --build
# Swagger UI：发起提取、查询状态、下载
open http://localhost:8000/docs
```

服务端口 `8000`；源库映射到宿主机 `55432`；用于导入验证的空库映射到 `55433`。

## API 用法

```bash
# 1) 提交检测 ID 集合（重复值自动去重）
curl -X POST http://localhost:8000/extractions \
  -H 'Content-Type: application/json' \
  -d '{"inspection_ids":[103,109],"note":"成环复测"}'

# 2) 查询状态：pending / running / succeeded / failed
curl http://localhost:8000/extractions/<job_id>

# 3) 仅 succeeded 可下载；运行中/失败返回 409，且失败任务不产生任何残件
curl -OJ http://localhost:8000/extractions/<job_id>/download

# 4) 查看该任务的逐检测修订依据（快照 xmin + 原值）；旧任务无依据时 404
curl http://localhost:8000/extractions/<job_id>/correction-basis
```

## 更正回写 API

```bash
# 提交更正：job_id 必须是成功提取；items 最多 20 条，仅 result/measured 可改
# request_id 由客户端生成并负责唯一（建议 UUID），作为幂等键
curl -X POST http://localhost:8000/corrections \
  -H 'Content-Type: application/json' \
  -d '{
    "job_id": "<job_id>",
    "request_id": "7f3d1c9e-...-a2",
    "items": [
      {"inspection_id": 100, "result": "核准合格", "measured": 12.88},
      {"inspection_id": 101, "result": "核准复测", "measured": null}
    ]
  }'

# 成功（首次）返回 201；同 request_id 同内容重试返回 200 且 replayed=true
# 按 request_id 查回执（断连/重启后凭此确认是否已生效）
curl http://localhost:8000/corrections/7f3d1c9e-...-a2
# 列出全部回执
curl http://localhost:8000/corrections
```

### 状态码约定

| 状态码 | 含义 |
| --- | --- |
| 201 | 更正首次提交成功，业务更新与回执已同事务提交 |
| 200 | 同 `request_id` 同内容重试，返回原回执（`replayed=true`），未再次修改 |
| 404 | 提取任务不存在 / 回执不存在 |
| 409 | 目标行版本冲突（更新/删除/同 ID 重建，含改回原值）、`request_id` 被异内容复用、任务未成功 |
| 422 | 旧任务缺少修订依据、包外检测、超 20 条、重复检测 ID 等请求语义错误 |

409 响应体 `detail.conflict_inspection_ids` 给出本批全部冲突 ID，
`detail.conflicts[].reason` 区分 `version_changed` 与 `row_missing`。

## 演示

需要宿主机具备 `curl`、`jq`、`psql`：

```bash
# 提取演示（快照一致性、成环导入空库、失败无残件）
bash scripts/demo.sh
# 更正回写演示（直接 SQL 改回、并发更正、提交后丢响应、旧任务拒绝）
bash scripts/demo_corrections.sh
```

更正回写演示覆盖：

1. 新提取在同一快照事务写入逐检测修订依据，原提取 SQL 与下载保持可用；
2. 正常更正只改 `result`/`measured`，回执可查，同 `request_id` 重试幂等、异内容 409；
3. **直接 SQL 先改再改回原值**仍被识别为过期，整批 409 并列出冲突 ID，无关检测不写入；
4. **并发更正**：相交检测一个成功一个 409（无丢失更新），不相交检测两个都成功；
5. **提交后丢响应**：客户端超时断开后事务已提交，重试只返回原回执、行版本号不变；
6. 旧任务（无修订依据）回写返回 422。

提取演示覆盖：

1. 从检测 `100` 出发递归带出复测链 `101→102→105`，断言不含另一批次的工件 / 检测；
2. **提取期间**对源库执行 UPDATE / DELETE / INSERT（`SNAPSHOT_DELAY=3` 让快照事务保持
   3 秒），断言结果文件停留在修改前的快照版本：被删行仍在、被改行是旧值、新增行不混入；
3. 把含两节点环（`103 ↔ 104`）与自引用环（`109 → 109`）的提取结果导入空库 `target-db`，
   验证外键约束真实建立且环可递归查询；
4. 提交不存在的检测 ID，任务失败、下载返回 409、磁盘无残件。

手动导入空库：

```bash
docker compose exec -T target-db \
  psql "postgresql://qc:qcpass@localhost:5432/qctarget" -v ON_ERROR_STOP=1 \
  -f - < extraction_xxxxxxxx.sql
```

## 关键设计

| 需求 | 实现 |
| --- | --- |
| 整份结果同一快照 | 每个任务一条连接，开启 `BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY`；快照在首条语句固定，源库持续增改删不阻塞也不污染本事务 |
| 分批读取不混用新旧状态 | 全部读取（含 keyset 分页）在同一快照事务内完成；分页基于主键 `> last_pk` 而非 OFFSET |
| 递归外键目标 | 检测 → `workpiece_id` → 工件 `batch_id` → 批次，仅收集被引用目标 |
| 复测互引可还原（含环） | 对 `retest_of_id` 同时做正向与反向遍历（无向图 BFS），`visited` 集合保证环终止；导入时先灌全部数据、最后建外键，环不触发违约 |
| 不夹带同批工件 | 批次仅通过“入选检测引用的工件”反向收集，不做同批扩散 |
| 表结构与原外键 | 内省 `information_schema` / `pg_constraint` / `pg_indexes`，输出建表、约束（PK/UNIQUE/FK/CHECK）与索引 DDL |
| 全成功才可下载 | 结果先写 `*.part` 临时文件，完成后原子 `rename`；任何异常删除临时文件，下载端点仅认 `succeeded` |
| 源库地址固定 | 仅从环境变量 `SOURCE_DSN` 读取，请求体不接受库地址 |

### 更正回写关键设计

| 需求 | 实现 |
| --- | --- |
| 依据与 SQL 同一快照 | 提取从事务去掉 `READ ONLY` 改为读写事务，在 `REPEATABLE READ` 快照内把每个入选检测的 `xmin`/`result`/`measured` 写入 `extraction_inspection_basis`，与 SQL 产物同事务 `COMMIT`；失败整体回滚并删除残件 |
| 旧任务拒绝回写 | 回写先按 `(job_id, inspection_id)` 查依据，任一目标缺依据即 422，提示重新提取 |
| 直接 SQL 改动也识别 | 依据保存的是系统版本号 `xmin` 而非原值：任何 UPDATE（即使改回原值）都会产生新 `xmin`；DELETE 使行消失、同 ID 重建是新行新 `xmin`，三类都能检出 |
| 整批 409 列冲突 ID | 回写事务先 `SELECT ... ORDER BY ... FOR UPDATE` 锁全部目标行再逐行核对版本，收集**全部**冲突后一次性拒绝，任一冲突则不执行任何 UPDATE |
| 并发更正不覆盖 | `FOR UPDATE` 行锁串行化相交目标；后到事务在 READ COMMITTED 下等锁释放后读到新版本，核对失败即 409。锁不相交的两批更正互不阻塞、都可写入 |
| 只改两列 | UPDATE 语句显式只写 `result`/`measured`，并附带 `xmin` 条件做最后一道乐观锁防线 |
| 回执与更新原子提交 | 业务 UPDATE 与 `correction_receipt` INSERT 在同一事务，回执唯一键为 `request_id`；提交失败整体回滚，全不生效 |
| 重试/断连/重启幂等 | 同 `request_id` 先查回执：内容哈希一致直接返回原回执（`replayed=true`），不一致 409。回执持久化在源 PostgreSQL，应用重启无内存依赖 |
| 依据不阻止源行删除 | `extraction_inspection_basis` 刻意不建指向 `inspection` 的外键，保证 DBA 直接 SQL 删行可行，且删除后依据保留用于识别 `row_missing` |
| 并发同 request_id 串行 | 事务级咨询锁 `pg_advisory_xact_lock`（键由 request_id 哈希）串行化同键请求，不同键互不阻塞 |

## 配置项

| 变量 | 默认值 | 说明 |
| --- | --- | --- |
| `SOURCE_DSN` | `postgresql://qc:qcpass@source-db:5432/qcwarehouse` | 固定源库连接串 |
| `OUTPUT_DIR` | `/data/output` | SQL 产物与任务状态目录 |
| `MAX_WORKERS` | `2` | 并发提取任务上限 |
| `SNAPSHOT_DELAY` | `0` | 演示用：快照建立后保持事务打开的秒数，生产保持 `0` |
