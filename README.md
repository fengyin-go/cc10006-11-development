# 数据湖仓一体管理平台

面向数据湖仓一体架构下的多源异构接入、原始文件编目、湖内分区治理、流批一体转换、版本回溯与数据编目检索的管理平台。

这是一个前后端分离的管理平台：前端 Vue 3 + Vite + TypeScript，后端 FastAPI（Python）。
两边各自独立启动，前端 dev server 已关掉自动打开页面，启动后按终端打印的地址手工打开。

## 目录结构

```text
.
├── frontend/                 Vue 3 + Vite + TypeScript 前端
│   ├── src/views/            每个业务模块一个页面
│   ├── src/api/              统一请求封装
│   ├── src/stores/           会话与筛选状态
│   └── vite.config.ts        dev server 配置（open: false）
├── backend/                  FastAPI（Python） 后端
│   ├── app/routers/          每个业务模块一组接口
│   ├── app/services/         业务规则与状态流转
│   └── app/store.py          内存数据仓库与示例数据
├── .gitignore
└── docker-compose.yml
```

## 启动

### 后端

```bash
cd backend
python3 -m venv .venv && .venv/bin/pip install -r requirements.txt
./run.sh
```

健康检查：`curl http://127.0.0.1:8000/api/health`

### 前端

```bash
cd frontend
npm install
npm run dev
```

前端默认监听 `http://127.0.0.1:5173/`，dev server 不会自动打开浏览器，
需要自己访问。`/api` 由 vite 代理到后端 `http://127.0.0.1:8000`。

## 业务模块

| 模块 | 目录 | 业务对象 | 主要字段 |
| --- | --- | --- | --- |
| 数据编目 | `catalog` | 数据目录 | 编目编号、数据集名称、存储路径 |
| 多源接入 | `ingest` | 接入通道 | 通道编号、源端类型、连接参数 |
| 原始层管理 | `bronze` | 原始表 | 表编号、来源通道、目标路径 |
| 清洗层管理 | `silver` | 清洗表 | 表编号、上游原始表、清洗脚本 |
| 聚合层管理 | `gold` | 聚合视图 | 视图编号、上游清洗表、聚合维度 |
| 湖区分区 | `lakezone` | 分区方案 | 方案编号、目标表名、分区字段 |
| 流式处理 | `stream` | 流处理作业 | 作业编号、输入Topic、处理逻辑 |
| 批量转换 | `batch` | 批处理任务 | 任务编号、输入路径、处理脚本 |
| 版本快照 | `snapshot` | 快照记录 | 快照编号、目标表名、快照时间 |
| 湖仓治理 | `govern` | 治理规则 | 规则编号、治理对象、治理类型 |
| 元数据中心 | `meta` | 元数据条目 | 条目编号、对象类型、对象名称 |
| 血缘图谱 | `lineage` | 血缘节点 | 节点编号、节点类型、节点名称 |
| 质量监控 | `quality` | 质量指标 | 指标编号、监控对象、指标类型 |
| 编排调度 | `schedule` | 编排计划 | 计划编号、任务列表、依赖图定义 |
| 历史回填 | `backfill` | 回填作业 | 作业编号、目标表名、回填起始 |
| 数据检索 | `search` | 检索索引 | 索引编号、索引名称、覆盖表名 |
| 资源计量 | `resource` | 计量记录 | 记录编号、计算引擎、作业名称 |
| 操作审计 | `audit` | 审计日志 | 日志编号、操作类型、操作对象 |

## 约定

- 每个模块的前端页面在 `frontend/src/views/<模块>/index.vue`，后端接口在
  `backend/app/routers/<模块>.py`，业务规则在 `backend/app/services/<模块>.py`。
- 列表接口统一返回 `{ items, total, page, size }`，动作接口统一返回 `{ ok, message }`。
- 状态流转只允许在 `app/services` 里改，路由层不做业务判断。

## 流处理检查点与恢复

流处理作业的检查点事实由 `backend/app/services/checkpoint.py` 统一管理，按**输入 Topic**
持久化到 `backend/var/checkpoints.json`（原子写入，进程重启后自动加载）。核心约束：

- 每个输入 Topic 记录已确认水位、检查点间隔、最后成功检查点与输出回执；
- 检查点写入、输出回执、流作业状态、下游回填台账与审计日志在同一次提交里一起推进，
  输出写入或落盘失败时整体回滚到可恢复状态，水位停留在最后成功检查点；
- 恢复只从最后成功检查点继续，重复提交或并发到达的恢复请求只保留一个实例生效；
- 非整数、负数、低于或等于最后成功水位、超出输入 Topic 范围的恢复水位一律拒绝；
- 有未完成检查点时不会显示成已恢复。

接口：

| 接口 | 说明 |
| --- | --- |
| `POST /api/stream/{id}/actions` | `提交检查点`（`watermark`、`checkpoint_interval`）与 `恢复作业` |
| `GET /api/stream/recovery/{id}` | 恢复面板：最后成功水位、检查点间隔、剩余待处理事件、本次/上一次检查点、输出回执、回填台账 |
| `GET /api/stream/recovery?job_no=...` | 值班排查：按作业编号定位本次检查点与上一次成功检查点 |

示例：`orders-input` 每秒 2000 条订单事件，作业在检查点后故障重启，页面只会从最后成功
水位继续消费，已写入 `orders-output` 的编号不会再次出现。回归验证：

```bash
make verify-checkpoint   # 需先起后端
```

领域：数据工程
子方向：数仓与数据湖
