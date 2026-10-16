# Reactor

具备**持久化任务管理、故障恢复、自适应并发控制和可解释调度能力**的 Java 异步任务执行引擎。

Reactor 关注的不是"任务能不能跑起来"，而是任务在异常条件下的行为：执行器崩溃后任务能否恢复、下游过载时能否主动降压、每一次调度决策能否被解释和验证。

- 需求文档（唯一权威来源）：[`docs/PRD.md`](docs/PRD.md)
- 开发准则与硬约束：[`AGENTS.md`](AGENTS.md)
- 当前进度：**第一阶段（任务执行基础）已完成**

> 本文件在每阶段结束时更新。第一阶段的"能做什么 / 不能做什么"在文末如实列出。

## 第一阶段：能做什么

| 能力 | 状态 |
|---|---|
| REST 提交任务、持久化、状态查询 | ✅ |
| 幂等键（数据库唯一约束）防重复创建 | ✅ |
| 多 Worker 竞争领取（条件更新，无重复领取） | ✅ |
| 单个 Worker 内可配置并发执行 | ✅ |
| 任务优先级调度、按状态与时间范围分页查询 | ✅ |
| 执行记录（每次尝试；领取时即落库，崩溃也留痕） | ✅ |
| fencing token 保护：拒绝过期执行者覆盖任务状态 | ✅ |
| 取消未开始的任务、管理员重放失败任务（强制记录 operator/reason） | ✅ |
| 状态机校验 + 条件更新双重保护 | ✅ |
| 执行器崩溃恢复（租约续约 / 超时回收） | ❌ 第二阶段 |
| 失败分类、指数退避、最大重试次数 | ❌ 第二阶段 |
| 自适应并发控制（AIMD） | ❌ 第三阶段 |
| 调度决策记录、任务时间线、监控面板 | ❌ 第四阶段 |
| 管理接口认证与鉴权 | ❌ 第五阶段 |

未实现的能力不会在代码里留空壳或未使用的抽象；对应模块在各自阶段才创建。

## 架构

```
task-core    领域模型、状态机、MyBatis 映射、任务服务与领取/上报操作（含 Flyway 迁移）
task-api     REST 接口与异常到 HTTP 语义的映射
task-worker  领取-执行-续约的执行引擎（SmartLifecycle），可多进程独立运行
app          Spring Boot 装配、配置、演示任务类型
```

依赖方向：`task-api`、`task-worker` 依赖 `task-core`；`task-api` 与 `task-worker` 互不依赖；`app` 只做装配。**MySQL 是唯一任务状态来源**，未引入 Redis / 消息队列。

## 快速开始

前置：JDK 21、Docker（用于 MySQL）、Maven Wrapper 已随仓库提供（`mvnw` / `mvnw.cmd`）。

```powershell
# 1. 启动 MySQL 8（库 reactor，用户 reactor / reactor123）
docker compose up -d mysql

# 2. 启动应用（PowerShell 需先指定 JDK 21）
$env:JAVA_HOME="$env:USERPROFILE\.jdks\jdk-21.0.12.1+1"
.\mvnw.cmd -pl app -am spring-boot:run
```

提交一个任务并观察执行结果：

```powershell
# 提交（200ms 后成功）
curl.exe -s -X POST http://localhost:8080/api/v1/tasks `
  -H "Content-Type: application/json" `
  -d '{"taskType":"echo","payload":{"sleepMs":200},"idempotencyScope":"demo","idempotencyKey":"order-1"}'

# 查询状态（提交成功只代表任务被接受并持久化，不代表执行成功）
curl.exe -s http://localhost:8080/api/v1/tasks/1

# 查看每次执行的记录
curl.exe -s http://localhost:8080/api/v1/tasks/1/executions
```

## REST API

所有路径以 `/api/v1` 开头，错误响应统一为 `{"code": "...", "message": "..."}`。

| 方法 | 路径 | 说明 |
|---|---|---|
| POST | `/api/v1/tasks` | 提交任务。新建返回 `201`，命中幂等键返回 `200`（`idempotentReplay=true`） |
| GET | `/api/v1/tasks/{id}` | 任务详情（状态、执行次数、最近错误、下次允许执行时间） |
| GET | `/api/v1/tasks` | 分页查询：`status`、`from`/`to`（ISO-8601，按创建时间）、`page`（从 0 开始）、`size`（1–200，默认 20） |
| GET | `/api/v1/tasks/{id}/executions` | 执行历史（每次尝试的开始/结束时间、执行器、结果、错误码） |
| POST | `/api/v1/tasks/{id}/cancel` | 取消尚未开始的任务，body：`{"operator": "...", "reason": "..."}` |
| POST | `/api/v1/tasks/{id}/replay` | 管理员重放失败任务，body 同上 |

提交请求字段：

```json
{
  "taskType": "echo",
  "payload": { "sleepMs": 0, "fail": false },
  "priority": 100,
  "maxAttempts": 3,
  "timeoutMs": 60000,
  "idempotencyScope": "demo",
  "idempotencyKey": "order-1"
}
```

- `priority`：数值越小越优先（默认 100，范围 0–1000）。
- `idempotencyScope` + `idempotencyKey`：**同一调用方、同一幂等键只能创建一个逻辑任务**，由数据库唯一键 `uk_task_idempotency` 保证；重复提交返回已有任务标识。不同业务范围中的相同字符串不视为同一任务。不传幂等键时允许重复创建。
- `payload` 只能是 JSON 对象，且**仅被当作数据**：不会被反射成类名、SQL 或脚本执行。

错误码：`TASK_NOT_FOUND`(404)、`UNKNOWN_TASK_TYPE`(400)、`INVALID_REQUEST`(400)、`ILLEGAL_TASK_TRANSITION`(409)、`INVALID_TASK_STATE`(409)、`IDEMPOTENCY_CONFLICT`(409)、`INTERNAL_ERROR`(500)。

## 任务状态机

```
PENDING → RUNNING → SUCCEEDED
   │         ├─────→ RETRY_WAIT → RUNNING   （第二阶段启用）
   │         └─────→ FAILED → PENDING       （仅管理员重放）
   └─────→ CANCELLED                        （仅限尚未开始的任务）
```

- 所有状态变更**先经状态机校验，再执行带条件的 SQL 更新**（`WHERE status = ... AND fencing_token = ...`）。
- 领取任务使用条件更新，受影响行数必须为 1；多个 Worker 竞争同一任务时只有一方成功。
- 结果上报必须匹配当前 `fencing_token`，旧执行者/迟到写入被拒绝并记为 `ABANDONED` + `STALE_WRITER`，**不会覆盖已确认的状态**。
- 执行器失联不会被推导为成功：只有执行者提交成功结果才会进入 `SUCCEEDED`。
- 本轮**不校验**执行中取消：`RUNNING → CANCELLED` 不是合法转换，对执行中任务的取消请求会被拒绝。

## 新增任务类型

实现 `TaskHandler` 并注册为 Spring Bean 即可，**不需要修改调度核心代码**：

```java
@Component
public class ReportTaskHandler implements TaskHandler {

    @Override
    public String taskType() {
        return "report";           // 提交时校验，未注册的类型直接 400
    }

    @Override
    public void execute(TaskContext context) throws Exception {
        JsonNode payload = context.payload();
        // context: taskId / attempt / taskType / payload
        // 失败直接抛异常，由执行引擎记录为 EXECUTION_ERROR
    }
}
```

内置演示任务类型 `echo`：`{"sleepMs": 200}` 睡眠指定毫秒；`{"fail": true}` 抛出异常用于演示失败终态。它只用于验证引擎行为，第三阶段的 `fault-lab` 会提供真正可控的故障注入。

## 配置

| 配置项 | 默认值 | 说明 |
|---|---|---|
| `reactor.worker.enabled` | `true` | 是否启动执行引擎；设为 `false` 时进程只提供 API |
| `reactor.worker.id` | 自动 | 执行器标识，默认 `<host>-<pid>-<随机后缀>` |
| `reactor.worker.concurrency` | `4` | 单个 Worker 的最大并发执行数 |
| `reactor.worker.poll-interval` | `500ms` | 领取轮询间隔 |
| `reactor.worker.batch-size` | `10` | 每次轮询最多尝试领取的任务数 |
| `reactor.worker.lease-ttl` | `60s` | 领取时写入的租约时长 |

数据库连接可用环境变量覆盖：`MYSQL_HOST`、`MYSQL_PORT`、`MYSQL_DATABASE`、`MYSQL_USER`、`MYSQL_PASSWORD`。时间统一使用 UTC，通过注入的 `Clock` 获取。

> 说明：`lease-ttl` 已经写入数据库，但第一阶段的引擎**不做续约与过期回收**，因此它目前只用于标识"当前持有者"。超时回收属于第二阶段。

## 测试

```powershell
$env:JAVA_HOME="$env:USERPROFILE\.jdks\jdk-21.0.12.1+1"
.\mvnw.cmd -B verify
```

- 单元测试（`task-core`，不需要 Docker）：状态机全部合法/非法转换、任务类型注册表。
- 集成测试（`app`，**Testcontainers + MySQL 8，不使用 H2**）：执行成功/失败流程、16 线程并发幂等提交、4 个竞争者的"恰好领取一次"、过期 fencing token 被拒、取消与重放语义、REST 契约。
- 集成测试需要 Docker 可用。

### 访问 Docker Hub

Testcontainers 需要 `testcontainers/ryuk` 与 `mysql:8.4`。本机 `registry-1.docker.io` 直连超时，已在 Docker Desktop 配置手动代理 `http://127.0.0.1:7890`（`%APPDATA%\Docker\settings-store.json` 中的 `ProxyHTTPMode=manual` 与 `OverrideProxyHTTP/OverrideProxyHTTPS`），两个镜像均可正常拉取。改动代理设置后需要重启 Docker Desktop 才生效：`docker desktop restart`。

代理未运行时可回退镜像站：

```powershell
docker pull docker.m.daocloud.io/testcontainers/ryuk:0.12.0
docker pull docker.m.daocloud.io/library/mysql:8.4
docker tag  docker.m.daocloud.io/testcontainers/ryuk:0.12.0 testcontainers/ryuk:0.12.0
docker tag  docker.m.daocloud.io/library/mysql:8.4 mysql:8.4
```

也可以设 `TESTCONTAINERS_HUB_IMAGE_NAME_PREFIX=docker.m.daocloud.io/` 让 Testcontainers 直接走镜像站。镜像与网络细节只影响本机运行，不写入项目配置和测试代码。

## 文档

- `docs/PRD.md`：产品需求与验收标准（V1.0）
- `AGENTS.md`：开发纪律、模块边界、测试要求与本机环境
