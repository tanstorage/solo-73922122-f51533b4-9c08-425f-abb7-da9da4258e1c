# 质检异常检测关联数据提取服务

面向离线排错场景：提交一组**检测 ID**，服务从持续写入的源 PostgreSQL 中，
基于**单一数据库快照**生成一份可直接导入**空 PostgreSQL 库**的 SQL，包含：

- 入选检测，以及沿外键递归必需的全部目标行（检测 → 工件 → 批次）；
- 复测关系（`inspection.retest_of_id` 自引用）的完整连通分量，**含互相引用的环与自引用**；
- 三张表的表结构、主键 / 外键 / 唯一 / 检查约束与索引，定义与源库一致；
- 不会夹带仅因“同批次”而关联、但未被入选检测引用的其他工件。

## 目录

```
extractor/
├── app/
│   ├── main.py        # FastAPI：提交提取 / 查询状态 / 下载 / 健康检查
│   └── extractor.py   # 快照事务、复测图递归、外键闭包、DDL 内省、SQL 生成
├── sql/
│   ├── 01_schema.sql  # 源库三表结构（含外键、索引）
│   └── 02_seed.sql    # 关联样例：复测链、跨工件复测、两组成环、自引用
├── scripts/demo.sh    # 端到端演示（含并发修改与空库导入）
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
```

## 演示

需要宿主机具备 `curl`、`jq`、`psql`：

```bash
bash scripts/demo.sh
```

演示覆盖：

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

## 配置项

| 变量 | 默认值 | 说明 |
| --- | --- | --- |
| `SOURCE_DSN` | `postgresql://qc:qcpass@source-db:5432/qcwarehouse` | 固定源库连接串 |
| `OUTPUT_DIR` | `/data/output` | SQL 产物与任务状态目录 |
| `MAX_WORKERS` | `2` | 并发提取任务上限 |
| `SNAPSHOT_DELAY` | `0` | 演示用：快照建立后保持事务打开的秒数，生产保持 `0` |
