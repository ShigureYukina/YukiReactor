# Reactor

具备**持久化任务管理、故障恢复、自适应并发控制和可解释调度能力**的 Java 异步任务执行引擎。

Reactor 关注的不是"任务能不能跑起来"，而是任务在异常条件下的行为：执行器崩溃后任务能否恢复、下游过载时能否主动降压、每一次调度决策能否被解释和验证。

- 需求文档（唯一权威来源）：[`docs/PRD.md`](docs/PRD.md)
- 开发准则与硬约束：[`AGENTS.md`](AGENTS.md)
- 当前进度：**第四阶段（调度解释与故障实验）已完成**
- 实验报告：[第二阶段故障恢复](docs/reports/phase-2-recovery.md) · [第三阶段自适应并发控制](docs/reports/phase-3-adaptive-control.md) · [第四阶段固定并发与自适应并发对照](docs/reports/phase-4-comparison-experiment.md)

> 本文件在每阶段结束时更新。已实现与未实现的能力在下表如实列出。

## 能力现状

| 能力 | 状态 |
|---|---|
| REST 提交任务、持久化、状态查询 | ✅ |
| 幂等键（数据库唯一约束）防重复创建 | ✅ |
| 多 Worker 竞争领取（条件更新，无重复领取） | ✅ |
| 可配置并发执行、按任务类型限制最大并发 | ✅ |
| 任务优先级调度、按状态与时间范围分页查询 | ✅ |
| 执行记录（每次尝试；领取时即落库，崩溃也留痕） | ✅ |
| fencing token 保护：拒绝过期执行者覆盖任务状态 | ✅ |
| 取消未开始的任务、管理员重放失败任务（强制记录 operator/reason） | ✅ |
| 状态机校验 + 条件更新双重保护 | ✅ |
| 租约心跳续约、执行超时（协作式中断） | ✅ 第二阶段 |
| 过期租约回收与孤儿执行记录清理（执行器崩溃后恢复） | ✅ 第二阶段 |
| 失败分类、指数退避 + 抖动、最大重试次数、重试派发限速 | ✅ 第二阶段 |
| 运行时观测信号（下游延迟/错误率、超时率、队列等待） | ✅ 第三阶段 |
| 自适应并发控制（AIMD，默认关闭） | ✅ 第三阶段 |
| 模拟下游与运行时故障注入（延迟、错误率、失败类型、限流） | ✅ 第三阶段 |
| 并发度调整的决策记录（`scheduler_decision`，只写） | ✅ 第三阶段 |
| 任务级调度决策记录（延迟、重试退避、重试耗尽、租约回收的原因码） | ✅ 第四阶段 |
| 任务时间线、决策查询、指标聚合、实验记录与静态面板 | ✅ 第四阶段 |
| 管理接口认证与权限校验（HTTP Basic + ADMIN 角色） | ✅ 第五阶段 |
| Docker 镜像与 Compose 一键部署 | ✅ 第五阶段 |

未实现的能力不会在代码里留空壳或未使用的抽象；对应模块在各自阶段才创建。

## 架构

```
task-core     领域模型、状态机、失败分类与重试策略、MyBatis 映射、任务服务与领取/上报操作、共享并发度与运行时信号（含 Flyway 迁移）
task-api      REST 接口与异常到 HTTP 语义的映射
task-worker   领取-执行-续约-上报的执行引擎（SmartLifecycle），可多进程独立运行
task-recovery 过期租约回收与孤儿执行记录清理（定时扫描器）
fault-lab     可运行时配置的模拟下游与 `downstream` 任务类型（故障注入）
adaptive-controller AIMD 并发度控制器与决策记录写入（读信号 → 调整并发度）
observability 任务时间线、决策查询、指标聚合、实验记录（含 Flyway V5 迁移）与静态面板
app           Spring Boot 装配、配置、演示任务类型
```

依赖方向：`task-api`、`task-worker`、`task-recovery`、`fault-lab`、`adaptive-controller`、`observability` 依赖 `task-core`，业务模块之间互不依赖；`app` 只做装配。worker 与自适应控制器不直接依赖：控制器写、worker 读同一个共享并发度（`ConcurrencyLimit`），观测信号经 `RuntimeSignals` 传递。决策记录的**写入**由 `task-core` 的 `SchedulerDecisionRecorder` 与 `adaptive-controller` 的 `ConcurrencyDecisionRecorder` 在真实决策代码路径完成，`observability` 只负责**读取**：决策分页查询、任务时间线、指标聚合与 `experiment_run` 记录读取所需的 SQL 放在 `task-core`，领域对象与服务放在 `observability`。**MySQL 是唯一任务状态来源**，未引入 Redis / 消息队列。

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
| GET | `/api/v1/tasks/{id}/executions` | 执行历史（每次尝试的开始/结束时间、执行器、结果、错误码与错误分类、所属 fencing token） |
| POST | `/api/v1/tasks/{id}/cancel` | 取消尚未开始的任务，body：`{"operator": "...", "reason": "..."}` |
| POST | `/api/v1/tasks/{id}/replay` | 管理员重放失败任务，body 同上 |
| GET | `/api/v1/tasks/{id}/timeline` | 任务时间线：把任务状态变更、每次执行尝试与调度决策按时间合并成一条可读序列 |
| GET | `/api/v1/decisions` | 决策分页查询：`taskId`、`type`、`reasonCode`、`from`/`to`（ISO-8601，按决策时间）、`page`、`size` |
| GET | `/api/v1/metrics/summary` | 指标聚合：`from`/`to` 为半开区间 `[from, to)`，缺省为最近 5 分钟；返回完成数、失败率、吞吐、队列/执行延迟均值与查询时刻的积压/在飞 |
| POST | `/api/v1/experiments` | 记录一轮对照实验（配置、随机种子、起止时间、指标快照、结果汇总），新建返回 `201`；同一实验名 + 策略 + 轮次重复返回 `409` |
| GET | `/api/v1/experiments` | 实验轮次分页查询：`name`、`strategy`、`page`、`size` |
| GET | `/api/v1/experiments/{name}` | 某个实验名的全部轮次（按策略与轮次排序） |

静态面板在 `http://localhost:8080/panel/index.html`：无构建步骤的单个 HTML，直接调用上面的接口展示任务时间线、决策查询与指标摘要。

### 管理接口的认证与权限

取消、重放与故障注入是运维写操作（PRD §8.4），需要 HTTP Basic 认证且用户具有 `ADMIN` 角色；读取接口（任务提交/查询、执行历史、时间线、决策、指标、实验记录、面板与 actuator）保持开放，实验脚本与监控不需要凭证：

```powershell
# 无凭证 → 401 {"code":"UNAUTHORIZED", ...}
curl.exe -s -X POST http://localhost:8080/api/v1/tasks/1/cancel -H "Content-Type: application/json" -d '{"operator":"alice","reason":"duplicate order"}'

# 有凭证但无 ADMIN 角色 → 403 {"code":"FORBIDDEN", ...}
# 有凭证且有 ADMIN 角色 → 正常执行
curl.exe -s -u admin:reactor-dev -X POST http://localhost:8080/api/v1/tasks/1/cancel -H "Content-Type: application/json" -d '{"operator":"alice","reason":"duplicate order"}'
```

用户与角色在配置中维护，密码经环境变量注入（`reactor-dev` 只是本机开发默认值，任何非本机环境必须覆盖）：

| 配置项 | 默认值 | 说明 |
|---|---|---|
| `reactor.security.users.<用户名>.password` | `${REACTOR_ADMIN_PASSWORD:reactor-dev}` | 该用户的密码；留空则启动失败 |
| `reactor.security.users.<用户名>.roles` | 无 | 角色列表；`ADMIN` 放行全部管理写操作，其他角色只能认证通过 |

realm 为 `reactor`，错误响应与 REST 错误同形（`{code, message}`），不会返回 HTML 登录页。会话策略为无状态（不创建 cookie），CSRF 已按纯 API 场景关闭。

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

错误码：`TASK_NOT_FOUND`(404)、`UNKNOWN_TASK_TYPE`(400)、`INVALID_REQUEST`(400)、`ILLEGAL_TASK_TRANSITION`(409)、`INVALID_TASK_STATE`(409)、`IDEMPOTENCY_CONFLICT`(409)、`DUPLICATE_EXPERIMENT_ROUND`(409)、`UNAUTHORIZED`(401)、`FORBIDDEN`(403)、`INTERNAL_ERROR`(500)。

## 任务状态机

```
PENDING → RUNNING → SUCCEEDED
   │         ├─────→ RETRY_WAIT → RUNNING   （到达下次执行时间后重新领取）
   │         └─────→ FAILED → PENDING       （仅管理员重放）
   └─────→ CANCELLED                        （仅限尚未开始的任务）
```

- 所有状态变更**先经状态机校验，再执行带条件的 SQL 更新**（`WHERE status = ... AND fencing_token = ...`）。
- 领取任务使用条件更新（`PENDING` 或已到期的 `RETRY_WAIT`），受影响行数必须为 1；多个 Worker 竞争同一任务时只有一方成功。
- 结果上报必须匹配当前 `fencing_token`，旧执行者/迟到写入被拒绝并记为 `ABANDONED` + `STALE_WRITER`，**不会覆盖已确认的状态**。
- 执行器失联不会被推导为成功：只有执行者提交成功结果才会进入 `SUCCEEDED`。
- **租约**：领取时写入 `lease_owner` + `lease_until`，执行期间按 `lease-renew-interval` 心跳续约；续约失败（已失去所有权）会中断本地执行并丢弃结果。
- **回收**：`task-recovery` 扫描 `status='RUNNING' AND lease_until <= now` 的任务，在确认状态与 `fencing_token` 未变、租约确实过期后才回收为 `RETRY_WAIT`（尝试次数用尽则为 `FAILED`），并把崩溃者遗留的执行记录关闭为 `ABANDONED` + `LEASE_EXPIRED`。
- **超时**：`timeoutMs` 通过中断执行线程实现（协作式）。响应中断的处理器记为 `TIMEOUT` 并按策略重试；不响应中断的处理器会继续运行，其迟到结果被上述守卫拒绝。
- 「执行中取消」只表示请求取消：`RUNNING → CANCELLED` 不是合法自动转换，对执行中任务的取消请求会被拒绝。

## 失败分类与重试

失败的处理方式由错误分类决定（PRD §5.4）：

| 触发方式 | 错误分类 | 是否重试 |
|---|---|---|
| `InvalidTaskParameterException` | `INVALID_PARAMETER` | 否，直接 `FAILED` |
| `PermanentTaskException` | `BUSINESS_REJECTION` | 否，直接 `FAILED` |
| `TransientTaskException` | `TRANSIENT` | 是 |
| `ThrottledTaskException`（带 `retryAfter`） | `THROTTLED` | 是，且等待时间不小于下游给出的 `retryAfter` |
| `OverloadedTaskException` | `OVERLOADED` | 是 |
| 执行超时（引擎判定） | `TIMEOUT` | 是 |
| 租约过期回收（引擎判定） | `EXECUTOR_LOST` | 是（尝试次数用尽则 `FAILED`） |
| 其他未分类异常 | `UNKNOWN` | **是**（受 `maxAttempts` 上限约束，默认按此处理） |

- 退避 = `base-delay × multiplier^(已失败次数−1)`，上限 `max-delay`，再叠加 ±`jitter` 比例的抖动。
- `attempt_count >= maxAttempts` 时不再重试，任务进入 `FAILED`，执行记录的原因码为 `MAX_RETRIES_EXCEEDED`。
- `RETRY_WAIT` 的派发受 `reactor.retry.max-dispatches-per-second` 限速，避免重试任务挤占正常任务的执行资源。
- `task.last_error` 的前缀取「最具体的事实」：有具体原因码（如 `MAX_RETRIES_EXCEEDED`、`LEASE_EXPIRED`）时用原因码，只有通用的 `EXECUTION_ERROR`（即 handler 抛异常）时才用错误分类。

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
        // 失败时按可重试性抛出对应异常（都在 com.yuki.reactor.core.error 下）：
        //   throw new TransientTaskException("downstream timeout");                       // 可重试
        //   throw new ThrottledTaskException("rate limited", Duration.ofSeconds(30));      // 可重试，且遵守下游的等待指示
        //   throw new PermanentTaskException("order already archived");                    // 不可重试
        //   throw new InvalidTaskParameterException("amount must be positive");            // 不可重试
        // 其他异常视为未分类（UNKNOWN），按可重试处理，仍受 maxAttempts 上限约束
    }
}
```

内置演示任务类型 `echo`：`{"sleepMs": 200}` 睡眠指定毫秒；`{"fail": true}` 抛出未分类异常，用于演示「重试到上限后进入 `FAILED`」。它只用于验证引擎行为，第三阶段的 `fault-lab` 会提供真正可控的故障注入。

## 配置

| 配置项 | 默认值 | 说明 |
|---|---|---|
| `reactor.worker.enabled` | `true` | 是否启动执行引擎；设为 `false` 时进程只提供 API |
| `reactor.worker.id` | 自动 | 执行器标识，默认 `<host>-<pid>-<随机后缀>` |
| `reactor.worker.concurrency` | `4` | 单个 Worker 的最大并发执行数 |
| `reactor.worker.poll-interval` | `500ms` | 领取轮询间隔 |
| `reactor.worker.batch-size` | `10` | 每次轮询最多尝试领取的任务数 |
| `reactor.worker.lease-ttl` | `60s` | 领取时写入的租约时长（同时决定崩溃恢复延时的上界） |
| `reactor.worker.lease-renew-interval` | `lease-ttl / 3` | 心跳续约周期，必须小于 `lease-ttl` |
| `reactor.worker.task-type-limits.<type>` | 不限制 | 按任务类型的最大并发执行数，例如 `task-type-limits.echo: 1` |

| 配置项 | 默认值 | 说明 |
|---|---|---|
| `reactor.retry.base-delay` | `1s` | 首次重试的退避基准 |
| `reactor.retry.multiplier` | `2.0` | 退避倍数 |
| `reactor.retry.max-delay` | `5m` | 退避上限（下游 `retryAfter` 指示不受此上限约束） |
| `reactor.retry.jitter` | `0.2` | 抖动比例（0–1），用于打散同时重试的任务 |
| `reactor.retry.max-dispatches-per-second` | `20` | 每秒最多派发多少个重试任务；`0` 表示不限速 |

| 配置项 | 默认值 | 说明 |
|---|---|---|
| `reactor.recovery.enabled` | `true` | 是否启动过期租约恢复扫描器 |
| `reactor.recovery.scan-interval` | `5s` | 扫描周期，与 `lease-ttl` 共同决定恢复延时 |
| `reactor.recovery.batch-size` | `50` | 每轮最多处理的过期租约数与孤儿执行记录数 |
| `reactor.recovery.orphan-sweep-enabled` | `true` | 是否清理「任务已不在执行中却仍开放」的执行记录 |

| 配置项 | 默认值 | 说明 |
|---|---|---|
| `reactor.adaptive.enabled` | `false` | 是否启用 AIMD 自适应并发控制；关闭时保持固定并发语义（`worker.concurrency` 即上限） |
| `reactor.adaptive.min-concurrency` | `1` | 并发度下界 |
| `reactor.adaptive.max-concurrency` | `8` | 并发度上界，同时决定线程池大小 |
| `reactor.adaptive.initial-concurrency` | `4` | 启用时的起始并发度 |
| `reactor.adaptive.sample-interval` | `2s` | 采样周期（采样点不写入决策表） |
| `reactor.adaptive.latency-threshold` | `500ms` | 下游/执行延迟越过该值即视为越界 |
| `reactor.adaptive.error-rate-threshold` | `0.3` | 可重试类错误（TRANSIENT / THROTTLED / OVERLOADED / TIMEOUT）的比例上限 |
| `reactor.adaptive.timeout-rate-threshold` | `0.2` | 执行超时比例上限 |
| `reactor.adaptive.increase-step` | `1` | 每次上调的步长 |
| `reactor.adaptive.decrease-factor` | `0.5` | 下调的比例因子 |
| `reactor.adaptive.max-decrease-step` | `4` | 单次下调的绝对幅度上限 |
| `reactor.adaptive.cooldown` | `10s` | 两次调整之间的最短间隔 |
| `reactor.adaptive.consecutive-breaches-before-decrease` | `2` | 连续多少次越界才下调 |
| `reactor.adaptive.recovery-observation` | `15s` | 上调前必须持续健康的观察窗口；每次上调后重新计时 |
| `reactor.adaptive.conservative-concurrency` | `1` | 指标缺失或不可读时的回退并发度 |

| 配置项 | 默认值 | 说明 |
|---|---|---|
| `reactor.fault-lab.latency-ms` | `0` | 模拟下游的基础延迟 |
| `reactor.fault-lab.jitter-ms` | `0` | 延迟抖动上限（0 表示无抖动） |
| `reactor.fault-lab.error-rate` | `0.0` | 失败概率 |
| `reactor.fault-lab.failure-kind` | `TRANSIENT` | 失败映射到哪一类任务错误：`TRANSIENT`、`OVERLOADED`、`THROTTLED` |
| `reactor.fault-lab.retry-after-ms` | `2000` | `THROTTLED` 时下发给调度器的等待指示 |
| `reactor.fault-lab.seed` | `0` | 随机种子（0 表示随机）；固定种子让故障注入可复现 |

| 配置项 | 默认值 | 说明 |
|---|---|---|
| `reactor.security.users.<用户名>.password` | `${REACTOR_ADMIN_PASSWORD:reactor-dev}` | 管理接口用户的密码，仅本机开发用默认值 |
| `reactor.security.users.<用户名>.roles` | 无 | 该用户的角色；`ADMIN` 才能调用取消/重放/故障注入 |

以上参数都可在运行时通过 `PUT /api/v1/fault-lab/downstream` 覆盖（必须带 `operator`，变更写日志）。Actuator 暴露 `health,metrics,info`；控制器注册的指标：`reactor.adaptive.concurrency.limit`（当前并发度，gauge）、`reactor.adaptive.concurrency.max`（上限，gauge）、`reactor.adaptive.concurrency.adjustments`（按 `reason` 标签累计的调整次数，counter）。

数据库连接可用环境变量覆盖：`MYSQL_HOST`、`MYSQL_PORT`、`MYSQL_DATABASE`、`MYSQL_USER`、`MYSQL_PASSWORD`。时间统一使用 UTC，通过注入的 `Clock` 获取。

## 测试

```powershell
$env:JAVA_HOME="$env:USERPROFILE\.jdks\jdk-21.0.12.1+1"
.\mvnw.cmd -B verify
```

- 单元测试（`task-core`、`observability`、`adaptive-controller`，不需要 Docker）：状态机全部合法/非法转换、任务类型注册表、退避计算（含抖动边界与上限）、重试派发限速器、失败分类；共享并发度的钳制语义、观测信号滚动窗口的聚合与淘汰；决策分页与按原因码过滤、任务时间线的事件顺序与类型、指标窗口口径（半开区间、失败率与吞吐分母）、实验记录的去重与按名称汇总；AIMD 控制律（递增步长与上限钳制、连续越界才下调、单次降幅上限、下界钳制、冷却、上调前的健康观察窗口、指标缺失与计数不可读时回退保守并发度、参数校验）。
- 集成测试（`app`，**Testcontainers + MySQL 8，不使用 H2**）：执行成功/失败流程、16 线程并发幂等提交、多引擎竞争的"恰好领取一次"、过期 fencing token 被拒、取消与重放、REST 契约、心跳续约阻止误回收、过期租约回收后由其它实例完成、孤儿执行记录清理、退避窗口内不派发、下游 `retryAfter` 指示优先、不可重试错误立即失败、尝试次数用尽后停止重试、超时中断、按类型并发上限、重试派发限速、运行中由外部改变并发度后派发被限制而不重启引擎、过载下调与恢复上调各写一条带前后值的决策记录、定时采样循环能自行完成调整、模拟下游的失败映射与 `retryAfter` 传递、控制接口的 `operator` 审计、任务时间线/决策查询/指标聚合/实验记录的 HTTP 与服务层契约（含重复轮次 409 与静态面板资源）、管理端点认证与权限（无凭证 401、错误密码 401、无 ADMIN 角色 403、ADMIN 放行、读取接口匿名可访问），以及「副作用已产生但成功状态未写入」故障窗口的专项验证。
- 集成测试需要 Docker 可用。当前规模：**158 个测试** —— `task-core` 50 + `observability` 20 + `adaptive-controller` 11（单元测试）+ `app` 77（Testcontainers 集成测试），`.\mvnw.cmd -B verify` → `BUILD SUCCESS`。

## 故障实验

本机可复现的强制终止执行器实验（真实多进程，非模拟）：

```powershell
docker compose up -d mysql
pwsh -NoProfile -File scripts/phase-2-recovery-experiment.ps1              # 先构建再运行
pwsh -NoProfile -File scripts/phase-2-recovery-experiment.ps1 -SkipBuild   # 复用已构建的 jar
```

脚本会启动两个实例（8080/8081）、跑一轮多 Worker 竞争（校验无重复领取）、再对持有长任务租约的实例执行 `Stop-Process -Force`，最后把环境、配置、注入方式、校验结果、测量值与执行记录写入 `docs/reports/phase-2/<UTC 时间戳>/raw-result.json`，实例日志同目录保存。实测结果与局限见 [`docs/reports/phase-2-recovery.md`](docs/reports/phase-2-recovery.md)。

本机可复现的自适应并发调整实验（真实下游调用与真实故障注入，非模拟）：

```powershell
docker compose up -d mysql
pwsh -NoProfile -File scripts/phase-3-adaptive-experiment.ps1              # 先构建再运行
pwsh -NoProfile -File scripts/phase-3-adaptive-experiment.ps1 -SkipBuild   # 复用已构建的 jar
```

故障注入同样需要 `ADMIN` 凭证（`-AdminUser` / `-AdminPassword` 或 `REACTOR_ADMIN_PASSWORD`，默认 `admin` + 开发默认密码）。

脚本启用自适应控制（初始并发 4、上限 8、冷却与恢复观察各 3 s）并跑四段：健康下游下把并发度升到上限 → 通过 `PUT /api/v1/fault-lab/downstream` 注入 1500 ms 延迟与 100% 失败，观察并发度降到 1 → 恢复下游并提交 100 个任务，观察并发度重新升到上限 → 关闭自适应、固定并发 4 重跑同一段过载作为对照。全程记录并发度时间线、`scheduler_decision` 决策行（原因码与调整前后值）与固定并发对照的峰值在飞任务数，写入 `docs/reports/phase-3/<UTC 时间戳>/raw-result.json`，实例日志与构建日志同目录保存。实测结果与局限见 [`docs/reports/phase-3-adaptive-control.md`](docs/reports/phase-3-adaptive-control.md)。

本机可复现的固定并发与自适应并发对照实验（两种策略各跑 3 轮，同样的任务分布与故障窗口）：

```powershell
docker compose up -d mysql
pwsh -NoProfile -File scripts/phase-4-comparison-experiment.ps1              # 先构建再运行
pwsh -NoProfile -File scripts/phase-4-comparison-experiment.ps1 -SkipBuild   # 复用已构建的 jar
pwsh -NoProfile -File scripts/phase-4-comparison-experiment.ps1 -Rounds 1 -TaskCount 120 -SkipBuild  # 冒烟（单轮）
```

实验脚本的故障注入走管理接口，需要 `ADMIN` 凭证：默认使用 `admin` + 开发默认密码，可用 `-AdminUser` / `-AdminPassword` 或环境变量 `REACTOR_ADMIN_PASSWORD` 覆盖（凭证不会写入证据文件）。

每轮都重启一个干净实例，空转 3 s 后提交 150 个 `downstream` 任务（`maxAttempts=4`，重试用内置指数退避），提交后 3 s 通过 `PUT /api/v1/fault-lab/downstream` 注入 1500 ms 延迟与 100% 失败并持续 20 s，再恢复下游并等待全部任务到达终态。两种策略各跑 3 轮：固定并发用 `reactor.adaptive.enabled=false` + `concurrency=8`，自适应用初始 4 / 下限 1 / 上限 8、冷却与恢复观察各 3 s。每秒采样并发度、在飞、积压、队列与执行延迟、吞吐，并读取 `scheduler_decision` 全量决策行；每轮通过 `POST /api/v1/experiments` 落一行 `experiment_run`，全部证据（采样、决策行、实例日志、构建日志）写入 `docs/reports/phase-4/<UTC 时间戳>/raw-result.json`。实测结果与局限见 [`docs/reports/phase-4-comparison-experiment.md`](docs/reports/phase-4-comparison-experiment.md)。

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

- `docs/PRD.md`：产品需求与验收标准（V1.1）
- `AGENTS.md`：开发纪律、模块边界、测试要求与本机环境
- [`docs/reports/phase-2-recovery.md`](docs/reports/phase-2-recovery.md)：第二阶段故障恢复实验报告
- [`docs/reports/phase-3-adaptive-control.md`](docs/reports/phase-3-adaptive-control.md)：第三阶段自适应并发控制实验报告
- [`docs/reports/phase-4-comparison-experiment.md`](docs/reports/phase-4-comparison-experiment.md)：第四阶段固定并发与自适应并发对照实验报告
