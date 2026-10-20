# ChatChat

ChatChat 是一个 IM（即时通讯）后端服务：Go + Gin + WebSocket + Kafka，目标版本 **0.1.0**（当前处于第 1 周工程骨架阶段）。

## 环境要求

- Go ≥ 1.22（本仓库 go.mod 声明 1.25.0，依赖生态锁定所致，符合「1.22+」规范口径）；
- Git Bash + make（Windows 下经 Git Bash 执行 Makefile 目标）；
- Docker Desktop（W1-M04 起使用：MySQL / Redis / Kafka 编排经 `make compose-up` 拉起，见下文[基础设施](#基础设施docker-compose)章节）。

## 目录结构

对照 spec §4 全量骨架（W1-M01 建立，占位包仅 doc.go 可编译；括号内为填实周次/责任模块）：

```text
chatchat/
├── api/                    # REST API 定义/接口文档（后续模块）
├── assets/                 # 静态资源（后续模块）
├── cmd/
│   ├── api/                # HTTPS API 服务入口（W1-M01 最小 main，W1-M06 完整装配）
│   ├── gateway/            # WS 网关服务（W2-M02 建立连接管理，W2-M03~M05 挂接 Logic/投递/拉取）
│   ├── logic/              # 业务逻辑服务（W2-M03 骨架，W3 接 Kafka 消费）
│   └── pushpull/           # 推拉投递服务（W2-M06 空闲骨架，W3 接 MQ）
├── config/                 # 配置加载 viper（W1-M02）
├── deployments/            # docker-compose 编排（W1-M04）
├── docs/adr/               # 架构决策记录
├── internal/
│   ├── dao/                # 数据访问（W1-M05）
│   ├── model/              # GORM 数据模型（W1-M05）
│   ├── router/             # 路由注册（W1-M06）
│   ├── server/             # WS 连接管理（Week 2）
│   ├── service/
│   │   ├── user/           # 用户注册/登录/JWT（W1-M06）
│   │   ├── group/          # 群组，500 上限（Week 3）
│   │   ├── message/        # 消息/信箱/推拉（Week 2 起）
│   │   ├── friend/         # 好友关系（Week 3）
│   │   └── extend/         # 扩展模块（Week 4）
│   ├── mq/                 # Kafka 生产/消费（Week 3）
│   └── hotdetect/          # 热点群动态识别（Week 4）
├── pkg/
│   ├── common/             # 通用工具（W1-M03）
│   ├── errors/             # 错误码（W1-M03）
│   ├── global/             # 全局常量（W1-M01 版本常量，W1-M03 扩充）
│   └── protocol/           # protobuf 协议（Week 2）
├── test/                   # 跨包集成测试（build tag `integration`，夹具放此）
├── web/                    # 前端/静态托管（后续模块）
├── go.mod / go.sum         # 模块与依赖锁定
├── Makefile
└── README.md
```

## Makefile 目标

| 目标 | 语义 |
|---|---|
| `make build` | 编译，产物入 `bin/{api,gateway,logic,pushpull}` |
| `make test` | 离线单元测试（`go test ./... -race`） |
| `make test-integration` | 集成测试（`-tags integration`，依赖 compose 环境，用例 W1-M05/M06 填） |
| `make lint` | golangci-lint（v2.13.2，配置 `.golangci.yml`：W1-M07 落档，四规则 errcheck/govet/staticcheck/revive + gofmt） |
| `make fmt-check` | gofmt 格式检查（辅助目标） |
| `make vet` | go vet（辅助目标） |
| `make cover` | 覆盖率断言：白名单 15 包 `go test -cover` 逐包解析，任一 < `COVERAGE_MIN`（默认 70，可覆盖）即 exit 1 |
| `make check` | 聚合门禁：build + fmt-check + vet + lint + test + cover |
| `make compose-up` | 拉起 MySQL/Redis/Kafka 编排并轮询等待全部 healthy（超时上限 `COMPOSE_WAIT_TIMEOUT` 秒，默认 300，W1-M04 已实装） |
| `make compose-down` | 停止编排环境（**不带 -v**，保留数据卷，可重复拉起） |
| `make compose-logs` | 跟随查看三服务日志（Ctrl-C 退出，不影响容器） |
| `make compose-reset` | 停止并**删除全部数据卷**（改 initdb SQL 后的重置入口，危险操作） |
| `make topics` | Kafka topic 分区治理（W3-M05-C2，ADR-0007 §8；W4-M02 增 hot topic + 数值断言）：幂等确保 `chatchat-message`/`chatchat-push`/`chatchat-hot-group` 存在且 `KAFKA_PARTITIONS`（默认 6，可覆盖）分区——不存在则建（RF=1）、分区不足则 `--alter` 扩、已满足（==N 或 >N）则 no-op，末尾 `--describe` 数值断言（PartitionCount 逐 topic ≥N，sed 单行提取，ADR-0010 D9） |
| `make run` | 一键启动四服务（W2-M06）：compose-up（幂等）→ build → 串行起 api→gateway→logic→pushpull，逐个 `/healthz` 就绪轮询（2s×60s 上限）；PID/日志/注入 env 落 `logs/run/`（gitignored）；**自动注入** `CHATCHAT_JWT_SECRET`（首次生成 urandom 48B base64，此后复用同值）与 `CHATCHAT_MYSQL_PASSWORD`（每次自根 `.env` 解析 `MYSQL_ROOT_PASSWORD`） |
| `make stop` | 读 `logs/run/<svc>.pid` 逐个停止四服务（含 `ps -W`+`taskkill` 兜底），幂等 |
| `make restart-{api,gateway,logic,pushpull}` | 独立重启单服务（stop 单个 → start 单个，复用 `logs/run/env` 的 JWT secret，重启语义连续） |
| `make build-probe` | 编译两个验收工具到 `bin/`（W2-M07）：`bin/wsprobe`（六场景断言探针）+ `bin/loadgen`（性能压测客户端；test/ 域工具，非 cmd 入口） |
| `make perf` | W2 性能基线自包含目标（W2-M07）：stop 清残留 → compose-up → build+build-probe → 起四服务 → `bin/loadgen`（100 连接 × 1msg/s × 60s，日志 `logs/run/perf.log`）→ 解析 JSON 断言 p99_ms<100 且 lost==0 → stop；失败也 stop 后 exit 1；**不进 make check**（时长独立验收） |
| `make clean` | 清理 `bin/` |
| `make full-build` | 构建本地镜像 `chatchat:0.1.0`（W4-M05，多阶段：golang:1.25-alpine 构建层四二进制 + web/ → alpine:3.20 非 root 运行层；`DOCKER_GOPROXY` 可覆盖构建代理，缺省 goproxy.cn；`IMAGE` 可覆盖 tag） |
| `make full-up` | 拉起容器全栈（W4-M05）：双文件叠加（基础设施 compose + 服务 overlay）→ 三依赖 healthy + topics-init 一次性建题（6 分区）+ 四服务 readyz/healthz healthcheck；幂等轮询七容器 healthy + init exited(0)，上限 `FULL_WAIT_TIMEOUT`（默认 180s）；复跑 no-op |
| `make full-down` | 停全栈（保留数据卷与镜像） |
| `make full-logs` | 跟随查看全栈日志 |
| `make full-reset` | 停全栈并**删除全部数据卷**（镜像保留；从零重验入口，危险操作） |

## 本地开发

```bash
# 方式一（推荐，W2-M06 起）：make run 一键四服务——自动注入 JWT secret
# （首次生成、此后复用）与 MySQL 密码（自 .env 解析），逐服务 healthz 就绪
make run                      # 起 api/gateway/logic/pushpull 四服务
make restart-gateway          # 独立重启单服务（secret 连续）
make stop                     # 全部停止（幂等）
tail -f logs/run/gateway.log  # 查看服务日志（logs/run/ 为运行时产物，gitignored）
# 注意：make run 的输出勿经 `| tail` / `| grep` 管道读取——服务存活期间
# MSYS pid-mapping stub 持有管道写端，管道读者会阻塞至服务停止；用文件
# 重定向（make run > /tmp/run.log 2>&1）或直接执行即可（W2-M06 实测登记）。

# 方式二：手动运行——jwt.secret 必须经环境变量注入（非空且 ≥32 字节），
# 否则启动失败（字段级错误 + 退出码 1）
export CHATCHAT_JWT_SECRET='please-generate-a-random-32+byte-secret!!'

make build      # 编译到 bin/{api,gateway,logic,pushpull}
./bin/api       # 输出 "chatchat version 0.1.0"（连资源、监听 :8080）
make test       # 单元测试（-race，需 gcc + CGO_ENABLED=1）
make check      # 全量门禁
```

- 四服务探针统一为 `GET /healthz` → 200 `ok`（api 经 router 免鉴权注册；gateway/logic/pushpull 各自 mux；make run 就绪轮询即打该端点）；
- `logs/run/` 含 pid/日志/注入 env（**内含 JWT secret 与 MySQL 密码**，已 gitignored；Windows 下 chmod 语义弱化，请勿提交或外发该目录）；

- **缺 `config.toml`**：使用内置默认值继续（warn 日志提示）；
- **配置覆盖优先级**：环境变量 `CHATCHAT_*` > `config.toml` > 内置默认值；
- **校验失败**：启动前输出字段级错误（`<段>.<字段>: <原因>`，逐行）并以非零码退出；
- 非法示例：`log.level="trace"`、`CHATCHAT_JWT_SECRET` 少于 32 字节、`config.toml` 中填写 `jwt.secret`（仅认环境变量）。

## config.toml 与 config.toml.example 的关系

- 仓库**只提交 `config.toml.example`**（模板，W1-M02 创建），真实 `config.toml` **不入库**；
- `config.toml` 已被 `.gitignore` 忽略，本地 `cp config.toml.example config.toml` 后按需修改；敏感项（mysql/redis 密码、jwt secret）一律走环境变量，`config.toml` 中留空；
- `config.toml` 缺失属预期：程序使用内置默认值 + warn 日志继续（但 `CHATCHAT_JWT_SECRET` 仍必须由环境变量提供）；
- 环境变量命名规则：`CHATCHAT_` 前缀 + 段.键大写（`.`→`_`），如 `CHATCHAT_SERVER_ADDR`、`CHATCHAT_MYSQL_PASSWORD`；全量对照见 `config.toml.example` 各行注释。

## 基础设施（docker-compose）

W1-M04 起 `deployments/docker-compose.yml` 一键编排 MySQL 8.0 / Redis 7 / Kafka 3.9.0（KRaft 单节点），三服务全部带 healthcheck；`make compose-up` 拉起后轮询等待全部 healthy（间隔 5s，上限经 `COMPOSE_WAIT_TIMEOUT` 配置，默认 300s；超时列出未 healthy 的服务并 exit 1，不静默降级）。

### 端口与凭据对齐表

单一来源为 W1-M00 §6.4（config.toml 默认值）。应用连接容器环境时按下表注入（compose 侧键即 `.env` 变量）：

| 连接项 | compose / .env 变量 | config.toml 键 | CHATCHAT_ 环境变量 | 默认值 |
|---|---|---|---|---|
| MySQL 地址/端口 | `MYSQL_PORT`（宿主侧） | `mysql.host` / `mysql.port` | `CHATCHAT_MYSQL_HOST` / `CHATCHAT_MYSQL_PORT` | 127.0.0.1:3306 |
| MySQL 用户 | —（容器内固定 root） | `mysql.user` | `CHATCHAT_MYSQL_USER` | root |
| MySQL 密码 | `MYSQL_ROOT_PASSWORD`（**必填**） | `mysql.password` | `CHATCHAT_MYSQL_PASSWORD` | 无默认：.env 必填注入；应用侧须传同一值 |
| MySQL 库名 | `MYSQL_DATABASE` | `mysql.database` | `CHATCHAT_MYSQL_DATABASE` | chatchat |
| Redis 地址 | `REDIS_PORT`（宿主侧） | `redis.addr` | `CHATCHAT_REDIS_ADDR` | 127.0.0.1:6379 |
| Redis 密码 | —（compose 不设 requirepass） | `redis.password` | `CHATCHAT_REDIS_PASSWORD` | 空（无密码，与容器一致） |
| Kafka brokers | `KAFKA_PORT`（宿主侧） | `kafka.brokers` | `CHATCHAT_KAFKA_BROKERS` | 127.0.0.1:9092 |
| api 服务监听 | —（应用自身端口） | `server.addr` | `CHATCHAT_SERVER_ADDR` | :8080 |
| gateway WS 监听 | —（应用自身端口） | `ws.addr` | `CHATCHAT_WS_ADDR` | :8081 |
| logic 健康端点 | —（应用自身端口，W2-M06） | `logic.addr` | `CHATCHAT_LOGIC_ADDR` | :8082 |
| pushpull 健康端点 | —（应用自身端口，W2-M06） | `pushpull.addr` | `CHATCHAT_PUSHPULL_ADDR` | :8083 |
| hot 专属 topic | — | `kafka.hot_topic` | `CHATCHAT_KAFKA_HOT_TOPIC` | chatchat-hot-group（W4-M02/ADR-0010） |
| hot 消费组 | — | `kafka.hot_consumer_group` | `CHATCHAT_KAFKA_HOT_CONSUMER_GROUP` | chatchat-pushpull-hot（W4-M02） |
| hot 旁路池 worker | — | `pushpull.hot_notify_workers` | `CHATCHAT_PUSHPULL_HOT_NOTIFY_WORKERS` | 4（[1,64]，W4-M02 D5） |
| hot 旁路池队列 | — | `pushpull.hot_notify_queue` | `CHATCHAT_PUSHPULL_HOT_NOTIFY_QUEUE` | 1024（[1,65536]，满即丢+计数） |
| 信令 gRPC 契约面监听 | —（应用自身端口，W4-M04） | `signal.addr` | `CHATCHAT_SIGNAL_ADDR` | 127.0.0.1:8084（强制 loopback，ADR-0011） |
| 信令会话 TTL | — | `signal.session_ttl_sec` | `CHATCHAT_SIGNAL_SESSION_TTL_SEC` | 60（[10,3600]） |

### 使用

```bash
cp .env.example .env      # 生成环境变量文件（.env 不入库，已被 .gitignore 忽略）
# 编辑 .env：MYSQL_ROOT_PASSWORD 必填（缺失或为空时 compose up 直接报错）
make compose-up           # 拉起并等待三服务全部 healthy
make compose-logs         # 跟随查看日志（Ctrl-C 退出）
make compose-down         # 停止（保留数据卷，可重复拉起）
make compose-reset        # 停止并删除全部数据卷（危险，见下）
```

- **compose-down 不删卷**：数据保留，二次 `compose-up` 幂等拉起；等待上限可配：`COMPOSE_WAIT_TIMEOUT=60 make compose-up`（秒）。
- **compose-reset（down -v）**：删除 mysql/redis/kafka 全部数据卷。`deployments/initdb/*.sql`（W1-M05 放入）仅在数据卷为空的**首次初始化**时执行——修改 SQL 后必须 `make compose-reset && make compose-up` 才会重新执行。
- **镜像预拉取**：首次 `compose-up` 前建议预拉取镜像，避免首次下载耗时挤占健康等待窗口：`docker pull mysql:8.0 & docker pull redis:7 & docker pull apache/kafka:3.9.0 & wait`。tag 策略：MySQL/Redis 系列锚定（防 latest 漂移），Kafka 精确锚定（KRaft env 配置对版本敏感）。
- **端口冲突**：宿主机 3306/6379/9092 被占用时（如本地已装 MySQL/Redis），只改 `.env` 的 `MYSQL_PORT` / `REDIS_PORT` / `KAFKA_PORT`（仅宿主侧映射，容器内端口固定），应用侧同步改 `CHATCHAT_MYSQL_PORT` 等。**Kafka 特例**：`KAFKA_ADVERTISED_LISTENERS` 固定广播 `localhost:9092`，改 `KAFKA_PORT` 后客户端引导发现仍指向 9092——W1 场景建议保持 9092 不变，确需改端口时须同步修改 compose 中 advertised 配置（W4 容器化时统一参数化）。
- **Windows / WSL 注意**：
  - 全部命令在 Git Bash 执行（Makefile 为 Git Bash 兼容写法，不依赖 jq 等）；
  - Kafka 内存占用较高，若 Docker Desktop（WSL2 后端）资源不足导致容器被杀，请在 Docker Desktop Settings → Resources 调大内存（建议 ≥ 4GB）；
  - Kafka 双 listener（W4-M05）：`EXTERNAL` 广播 `localhost:9092`（宿主形态不变），`INTERNAL` 广播 `kafka:29092`（服务容器寻址，不发布宿主）；容器内 healthcheck 与 `make topics` 的 localhost:9092 路径继续命中 EXTERNAL listener；
  - 数据卷存于 Docker Desktop 的 WSL 虚拟机内，仅 `make compose-reset` 会删除。

## 容器全栈（W4-M05 起，部署编排与可观测性）

W4-M05 起提供容器化全栈形态（ADR-0012）：`deployments/Dockerfile`（多阶段单 Dockerfile 四入口）+ `deployments/docker-compose.services.yml`（服务 overlay，叠加既有基础设施 compose 使用）。与本地进程形态（`make run`）**并存**（D12）：宿主端口 8080-8083 双形态一致，W2/W3 全部验收资产零改动；M07 压测「真部署形态复验」走本形态。

```bash
# 前置：.env 除 MYSQL_ROOT_PASSWORD 外还需 CHATCHAT_JWT_SECRET（>=32 字节，
# 部署密钥应显式，full-up 不自动生成）：
#   head -c 48 /dev/urandom | base64   # 生成示例，写入 .env 的 CHATCHAT_JWT_SECRET=
make full-build               # 构建镜像 chatchat:0.1.0（GOPROXY 困境网络：DOCKER_GOPROXY 可覆盖）
make full-up && make full-up  # 从零拉起 + 幂等复跑（CASE-I51；七容器 healthy + topics-init exited 0）
make full-logs                # 跟随日志（json-file 轮转：max-size 10m × max-file 3）
./scripts/full_graceful.sh    # 优雅退出补验（CASE-I53：四服务逐一 stop/start + 退出链断言 + 位移不回退）
make full-reset && make full-up  # 从零重验（删卷；镜像保留）
make full-down                # 降容器栈

- **双形态端口互斥（重要）**：容器全栈发布宿主 8080-8083，与本地进程形态（`make run`/`make perf`/`w3_acceptance`）**同端口互斥**——切换形态前必须先 `make full-down`（或 `make stop` 反向），否则本地进程绑定失败表现为「pid file missing or process dead / make run broken」。`make compose-up`（仅三依赖，不发布 8080-8083）不受此约束。
```

- **拓扑**：mysql/redis/kafka（healthy 门控）→ `topics-init`（一次性建题：三 topic × `KAFKA_PARTITIONS`（默认 6）分区 + PartitionCount 数值断言，`service_completed_successfully` 门控——先于一切生产者，规避自动建题 1 分区退化）→ api/gateway/logic/pushpull（`restart: unless-stopped`；依赖 mysql/redis/kafka healthy + topics-init 完成；logic 仅依赖 mysql/redis）。
- **镜像**：构建层 golang:1.25-alpine（CGO_ENABLED=0 纯静态、`-ldflags "-s -w -X main.buildVersion=0.1.0"` 版本注入）；运行层 alpine:3.20（ca-certificates+tzdata、非 root `app` 用户、四二进制 + web/）；compose 以 `entrypoint: ["/app/<svc>]` 差异化四服务。
- **Kafka 双 listener（D3）**：服务容器走 `kafka:29092`（INTERNAL 广播，容器间地址可解析）；宿主客户端/集成测试/`make topics`/healthcheck 继续走 `localhost:9092`（EXTERNAL，宿主端口映射不变）。本形态与 dev 形态的 broker 为**同一容器**（三依赖 compose 只改 listener 三键，卷/网络/CLUSTER_ID 不动）。
- **健康分级（D8）**：`/healthz` 维持静态存活语义；api/gateway/pushpull 增 `/readyz`（MySQL+Redis Ping，单项 2s 探针超时，任一失败 503+失败项名）——compose healthcheck 即 readyz（「通过健康检查」=依赖就绪门槛语义）；logic 无依赖面仅 healthz。探针路径无访问日志（不刷屏）。
- **指标端点（D5，CAP-504）**：三服务各挂 `GET /debug/vars`（expvar，宿主经既有发布端口访问——无独立 debug 端口）。指标集（前缀 `chatchat_`）：gateway `online_connections`/`msg_received_chat_total`/`msg_received_group_total`；pushpull `msg_consumed_total`/`push_total`/`push_fail_total`/`notify_dropped_total`/`consumer_lag_{message,hot_group,push}`（ClusterAdmin 30s 轮询，三消费组维度总 lag）；api `hot_group_count`/`reconcile_rounds`/`overlimit_groups`。**预留**：message_p99 / ack_timeout_rate 归 M07 与下月 Prometheus 面（spec §11 剩余两项）。**已知边界**：/debug/vars 无鉴权（0.1.0 登记下月 token 化）。
- **leader 门控（D7，CAP-506）**：api 进程内两后台循环（群对账 Reconciler、热点聚合 Detector.Run）经 Redis 每轮咨询锁（`chatchat:leader:reconcile` / `chatchat:leader:hotdetect`，TTL=间隔×2，SET NX EX 单命令原子）多实例同轮至多一执行者；Redis 不可达=跳轮（宁缺勿重，正确性由任务幂等兜底）。
- **memberCount 观测（移交 2）**：Reconciler 每轮 Info 摘要（`groups_scanned`/`overlimit`）+ api expvar 两计数。
- **资源限制档（D2，保守值；压测级校准归 M07）**：mysql 1g / redis 256m / kafka 2g / api 192m / gateway 256m / logic 64m / pushpull 256m / topics-init 64m（overlay 仅作用于 full-* 形态，dev 流程不限额）。与池参数对齐登记见 `.env.example` 第三段。
- **优雅退出（D9）**：四服务 `stop_grace_period: 15s`（Shutdown 预算 10s + 余量）；dispatcher Close 3s 排空上界（超时放弃在途旁路任务并 Error 登记）；`scripts/full_graceful.sh` 断言退出链日志（shutting down→shutdown complete）、退出码 0、恢复后 readyz healthy、push 消费组名稳定与位移不回退（消息不丢演练归 M07）。



后续模块的开发与测试派发均以下表为准（agent.md §4 要求由主 Agent 注入，Subagent 不应自行摸索）；各行的详细语义与背景见上方「基础设施（docker-compose）」章节，两处不一致时以本表 + Makefile/代码实际行为为准。

| # | 事实 | 命令/说明 |
|---|---|---|
| 1 | 集成测试前置 | `make compose-up`（三服务 healthy）→ `make test-integration`（= `go test -tags integration ./... -race -count=1`）；MySQL root 密码读根目录 `.env` 的 `MYSQL_ROOT_PASSWORD`（密码值禁入任何输出） |
| 2 | DDL/初始化重放 | `chatchat.sql` 变更后 initdb **不会自动重放**（仅空卷首启执行）：`make compose-reset && make compose-up`（⚠️ 删卷，本地开发数据会清空） |
| 3 | 运行期必需变量 | `CHATCHAT_JWT_SECRET`（非空且 ≥32 字节），缺失则服务/测试启动即 exit 1（M00 冻结的 fail-fast 契约）；`make run` **已内建注入**（首次自动生成 `head -c 48 /dev/urandom \| base64` 存 `logs/run/env` 复用 + 从根 `.env` 解析 `MYSQL_ROOT_PASSWORD` 注入 `CHATCHAT_MYSQL_PASSWORD`），无需手工 export；手动运行 `bin/api` / `bin/gateway` / `bin/logic` / `bin/pushpull` 时须自行注入（示例：`export CHATCHAT_JWT_SECRET="$(head -c 48 /dev/urandom \| base64)"`，另需 MySQL 密码：`config.toml` 或 `CHATCHAT_MYSQL_PASSWORD`，取与 `.env` 相同的值） |
| 4 | 工具链 | `-race` 需 mingw gcc 在 PATH（`G:\compile\x86_64-16.2.0-release-win32-seh-ucrt-rt_v14-rev1\mingw64\bin`，新终端自动生效）+ `CGO_ENABLED=1`（已 `go env -w` 持久化）；golangci-lint v2.13.2 位于 `$(go env GOPATH)/bin`（不在系统 PATH 时需 `export PATH="$(go env GOPATH)/bin:$PATH"`） |
| 5 | Kafka topic 分区 | `make topics`（幂等确保 `chatchat-message`/`chatchat-push`/`chatchat-hot-group` 各 6 分区 + 逐 topic PartitionCount 数值断言，W4-M02/ADR-0010 D1/D9；W3-M05-C2 前两 topic 为 broker 自动建题默认 1 分区——pushpull 投递延迟根因①，ADR-0007 §8）。**改分区对既有数据的影响**：扩分区只新增分区、不迁移既有数据，但同键（conversationId）消息扩容后可能换分区（murmur2 对分区数敏感），跨分区无 broker 级到达序——正确性不受影响：msg_seq 权威序与分区无关（Redis INCR 落库前分配），接收侧按 (conv, seq) 排序/去重收敛（客户端幂等 + 拉取路径排序，ADR-0005/0006），写扩散幂等键 `uk_user_conv_seq` 与分区无关 |
| 6 | 热点打标注入（W4-M01） | 热点聚合宿主=api 进程（`[hotdetect]` 段，五键 `CHATCHAT_HOTDETECT_THRESHOLD_MSG_PER_MIN` / `_AGGREGATE_INTERVAL_SEC` / `_WINDOW_MIN` / `_BUCKET_SEC` / `_LOW_PERIODS_TO_UNMARK`）；验收/集成注入组合：threshold=5、interval=10、window=1、bucket=1、N=2。**注意**：W4-M01 起新增 `hot_group_stat` 表——旧数据卷须先 `make compose-reset && make compose-up` 重放 DDL（同第 2 行口径），否则集成测试自举重放全表（测试库数据可弃） |
| 7 | 热点专属通道（W4-M02） | 写路径全链：gateway 行快照路由（GetByID 行 HotLevel>0 且 hotDeliver 注入 → `chatchat-hot-group`，信封 members 空）→ pushpull 第二消费者（组 `chatchat-pushpull-hot`）写 `group_timeline`（uk `(group_id,msg_seq)` 幂等，写放大 500→1）；摘标切换 P1 补齐→P2 摘标→P3 回写扩散（状态键 `group:{id}:hot:backfill`，ADR-0010 D6）。**注意**：W4-M02 起新增 `group_timeline` 表 + 新增 `chatchat-hot-group` topic——旧数据卷须 `make compose-reset && make compose-up` 重放 DDL 后再 `make topics`（同第 2/5 行口径） |
| 8 | 服务二进制构建口径（W4-M04-C2 教训） | Makefile build/start 对**无扩展名** `bin/<svc>` 自洽（`go build -o bin/gateway` 写无扩展名文件，MSYS bash 的 `exec ./bin/gateway` 优先解析无扩展名文件）；向 `bin/<svc>.exe` 手工构建会产生**不被运行的平行二进制**（曾致三轮「修复无效」假象）。重编译服务二进制必须 `make build` 或 `go build -o bin/<svc> ./cmd/<svc>`（无扩展名） |
| 9 | 容器全栈形态（W4-M05） | `make full-build && make full-up`（前置：`.env` 含 `MYSQL_ROOT_PASSWORD` **与** `CHATCHAT_JWT_SECRET`，后者缺失 compose 即报错——部署密钥显式注入，full-up 不自动生成；生成命令 `head -c 48 /dev/urandom \| base64`）；broker 双 listener 后 dev 流程照旧（宿主仍 localhost:9092）；优雅退出补验 `./scripts/full_graceful.sh`；指标 `curl -s http://127.0.0.1:8081/debug/vars`。详见「容器全栈」章节 |
| 10 | WebRTC 联调页（W4-M04-C2） | `make run` 后 gateway 同源托管 `http://127.0.0.1:8081/`（web/index.html，单文件零依赖）；凭据：`bin/wsprobe -scenario=webcall` 输出两行 `WEBCALL uid=<n> token=<t>`（REST 预验证后粘贴入页）；WS 鉴权支持 `?token=` query 回退（D2，header 优先）；媒体面人工补验义务见 docs/w4-acceptance-report.md §4 |

## 周验收（W1-M07 起）

`scripts/w1_acceptance.sh` 为第 1 周收口黑盒验收脚本：读 `.env` 的 `MYSQL_ROOT_PASSWORD`、随机生成 JWT secret → `make compose-up` + `make build` → 后台起 `bin/api` → 就绪轮询（`/api/user/me` 返回 401 即装配完成）→ 六断言（注册 / 重复注册 409/2001 / 登录双 token / me 手机号回读 / 篡改 token 401/2004 / refresh 换发轮换）→ 停服清理并输出 `PASS X/6`。

```bash
./scripts/w1_acceptance.sh && ./scripts/w1_acceptance.sh   # 连跑两次，均应 PASS 6/6
```

- **随机手机号**：每次执行生成 `138` + 8 位随机数字，连跑两次互不冲突，脚本可重复；
- **退出码**：0 = 6/6 全过；1 = 存在失败断言；2 = 环境中断（.env 缺密码 / compose / build / 就绪超时 / 端口被占）；
- **篡改 token 规则**：改 payload 首字符而非签名尾字符（HS256 尾字符低 4 位解码丢弃，1/16 概率等价类——与 `pkg/common/jwt` 单测同源规则）；
- 验收证据归档于 `docs/w1-acceptance-report.md`（覆盖率口径与豁免理由见该报告）。

## 周验收与性能基线（W2-M07 起）

W2 半月验收两件套（均自包含：自动 compose-up + 起停四服务，可重复执行，每次随机手机号）：

```bash
./scripts/w2_acceptance.sh    # 12 段 18 断言黑盒验收（约 4~5 分钟）
make perf                     # 性能基线：100 连接 × 1msg/s × 60s（P99<100ms 零丢失）
```

- **`scripts/w2_acceptance.sh`**：前置（`.env` 密码→compose-up→build+build-probe）→ `make run`（文件重定向 `logs/run/acceptance-run.log`）→ REST 注册登录 → wsprobe 六场景（握手 401 / 单聊收发 ACK / 默认档重推时序 ~17s / 断线补齐 / 历史分页 55 条）→ traceId 日志级断言（`grep logs/run/gateway.log w2-acc-tr`）→ restart-gateway+healthz → 心跳真实时长（连接静默 80s，Redis 在线键 5s 存在→75s 内删除）→ `make stop` ×2 幂等 → `PASS n/18` 汇总；探针每场景对自身会话做三重清理（message/inbox_cursor 行 + Redis conv seq 键），对集成测试遗留的共享 Redis 残键免疫、连跑可重复；
- **`make perf`**：`bin/loadgen` 注册 100 用户 50 会话 100 WS 连接，双向各 1msg/s × 60s（共 6000 条），逐条 ACK；stdout 单行 JSON 统计（count/lost/p50/p90/p95/p99/max_ms/ack_recv，日志 `logs/run/perf.log`），Makefile 内建断言 p99_ms<100 且 lost==0，另直查 MySQL 断言各接收者 `max_recv_seq` 完备；发送相位带 [0,1s) 随机抖动（100 端同相位同步突发会让 MySQL 池排队、P99 失真——W2-M07 实测登记）；
- **退出码**（两件套同语义）：0=全过；1=存在失败断言/性能不达标；2=环境中断（w2 脚本）；
- 冒烟参数：`bin/loadgen -conns=4 -dur=5s`（工具直跑，需四服务在线）；
- 验收证据归档于 `docs/w2-acceptance-report.md`（§7.4 九行对照、覆盖率口径、lint 清理记录）。

## 周验收（W3-M07 起）

`scripts/w3_acceptance.sh` 为第 3 周收口黑盒验收脚本（自包含：自动 compose-up + 起停四服务，可重复执行，每次随机手机号），对 plan.md §8.4 全部六行周门槛做 42 断言验收：

```bash
./scripts/w3_acceptance.sh && ./scripts/w3_acceptance.sh   # 连跑两次，均应 PASS 42/42
```

- **时长**：本机实测约 74~76 秒/次（连跑 73s/75s + 审查复跑 74s；与验收报告 §10 一致）（bulkjoin 500 注册+入群 ~15s、kill 30s 窗口、对账 10s 周期 30s 轮询窗均已计入）；plan 时长预算上限 8~12 分钟/次（慢机裕量）；
- **十段编排（S1~S10）**：S1 前置（`.env` 密码→compose-up→build+build-probe→export `CHATCHAT_GROUP_RECONCILE_INTERVAL_SEC=10` 对账周期注入，`make run` 透传）→ S2 `make run`（文件重定向 `logs/run/w3-acceptance-run.log`）+ 四 healthz → S3 群组域 REST（注册/建群/入群/重复入群 409/4005/非管理员踢人 403/4003/禁言/退群→再入群/成员数核对 + wsprobe `bulkjoin` 并发注册 500 用户入群至满 + 第 501 人 curl 入群 409/4004）→ S4 好友域生命周期（apply→approve→wsprobe `fchat` 单聊闭环→delete→`fdeny` 双向 3005→REST 历史仍可读→再申请→reject→`fdeny` 拒发 3005）→ S5 wsprobe `gchat` 群聊闭环（在线双成员推送帧+traceId 回显+ACK+游标）→ S6 wsprobe `gmute`（禁言发言 CmdError 5010+零落库+解禁恢复）→ S8 kill pushpull 30s 演练（读 `logs/run/pushpull.pid` 硬 kill→`killdur -phase=killwin` 窗口内发 30 条群消息→30s→`make restart-pushpull`→`killdur -phase=verify` 拉全去重恰 30+seq 连续+信箱 30×2 行完备，90s 轮询上限吸收 rebalance）→ S7' traceId 三断言（重启后新发消息：gateway.log `group message saved` + 推送帧回显 + pushpull.log `投递信封处理完成` 同一 traceId——pushpull.log 随 restart 截断故顺序敏感，S8 先于 S7' 执行、S10 首断言嵌于 S8 窗口，段编号不变）→ S9 对账告警（wsprobe `gdirty` gorm 直连 `UPDATE group_info SET max_member=1`→api.log grep `group members exceed max_member`）→ S10 漫游抽样（REST `/api/message/inbox` 未同步清单含演练会话且 lastSeq=30→拉取后清单空→二次调用幂等空）+ `make stop` ×2 幂等 → `PASS n/42` 汇总；
- **执行顺序与段编号差异**：S8（kill 演练）先于 S7'（traceId）执行、S10 首断言在 S8 的 restart 与 kverify 之间执行（清单证据要求未同步态）——脚本头注释与 `docs/w3-acceptance-report.md` 已登记；
- **退出码**：0=42/42 全过；1=存在失败断言；2=环境中断（`.env` 缺密码/compose/build/run 就绪超时/pushpull 未运行）；
- **与 perf 的隔离**：`make perf`（W2 单聊基线）与 `go test -tags integration ./test/ -count=1 -run 'TestPushPullPerf'`（pushpull 消费分位数）不进本脚本——重负载与验收断言隔离调度（W3-M05-C2 §9 依据，plan 裁决 D8），作为独立命令证据归档于 `docs/w3-acceptance-report.md`；
- wsprobe 同步新增 W3 场景（`gchat`/`gmute`/`killdur` 两阶段/`bulkjoin`/`fchat`/`fdeny`/`gdirty`，复用 W2 封装并新增 CmdError 帧读取），探针日志落 `/tmp/w3_acc/probe-*.log` 供排障；
- 验收证据归档于 `docs/w3-acceptance-report.md`（门槛六行对照、ADR 状态、微循环记录、移交 W4 清单）。

### W4-M03 wsprobe 场景速查

`bin/wsprobe -scenario hotnotify`（W4-M03 CASE-I25，四服务运行态）：注册对建群 → gorm 直连 `UPDATE group_info SET hot_level=1` 直标（`gdirty` 先例；打标闭环归 hotdetect 集成用例）→ A 发 5 条群消息 → 断言 B **仅收 CmdNotify**（无 CmdChat/CmdGroupChat 内容帧、payload=`PullResponse{conv,maxSeq}` 集 1..5、traceId 回显）→ B 拉取 5 条完整覆盖（去重接收集恰一次呈现）→ 写放大面（`group_timeline`=5 行、`inbox_delivery`=0 行）。输出 `PROBE_GROUP/PROBE_CONV/PROBE_PHONE_B` 供脚本复核。

### W4-M04 wsprobe 场景速查

`bin/wsprobe -scenario signal`（W4-M04 CASE-I34，四服务运行态；`make run` 后执行）：注册对双 WS 连接（信号无好友门槛——ChatGate 不适用，W4-M04 冻结）走五型闭环 invite→offer→answer→交叉 ICE→hangup（12 断言：每帧 traceId 回显、from 覆写后对端见鉴权 uid、SDP/ICE 原样透传、ICE 无序容忍、hangup 转发）→ 迟到 offer 收 CmdError 7102 + traceId 回显 → 离线 invite（第三用户注册不连接）收 CmdError 7101（显式回执，ADR-0011 D3/D5）。gateway 同进程在 `127.0.0.1:8084` 起 gRPC Exchange 契约面（loopback 强制）。输出 `PROBE_SIGNAL_CALL` 供复核。

`bin/wsprobe -scenario=webcall`（W4-M04-C2 CASE-405，浏览器联调前置件；api 运行态即可）：注册两用户（随机手机号，`registerOne` 复用——2001 冲突自动换号，连跑可重复）并打印两行 `WEBCALL uid=<n> token=<t>`（access token；密码不入输出）——粘贴到 web 联调页两栏驱动真实浏览器双端通话。

**web 联调页**（W4-M04-C2）：`make run` 后 gateway 同源托管 `web/index.html`（`http://127.0.0.1:8081/`；单文件零依赖 vanilla JS：手写最小 proto 编解码含页面自检 round-trip 断言、五型时序（offer/ICE 早到缓存、远程 video 不 muted、二次呼叫重建 pc）、20s 心跳、CmdError 7101 toast/7102 收尾静默、自检断言面板 id=`chk-<name>-value|ok` 供自动化读取；WS 鉴权经 `?token=` query 回退（安全登记 ADR-0011 §6 C2-D2）；媒体回退链 D8：无媒体模式以 dataChannel 证明信令+P2P 连通）。

## 周验收与月末发布包（W4-M07 起）

W4 收口三件套（**容器全栈形态主跑**——脚本自含 `make full-reset/full-build/full-up`，与本地形态端口互斥须分批串行；均自包含可重复/随机手机号/退出码 0|1|2，w3 三代范式）：

```bash
./scripts/w4_acceptance.sh && ./scripts/w4_acceptance.sh   # E2E 十段 26 断言，连跑两次均应 PASS 26/26
./scripts/w4_stress.sh    # 500 人群两态压测（约 20 分钟）：四件套断言（零丢失/lag 有界/goroutine 无泄漏/写放大）
./scripts/w4_drills.sh    # 六目标 30s 故障演练：gateway/logic/pushpull/mysql/redis/kafka 逐一
```

- **`scripts/w4_acceptance.sh`**（约 5~8 分钟）：S1 full-reset+full-build+full-up（热点参数经 overlay env 透传注入：threshold=5/interval=10s/window=1m/bucket=1s/N=2 + 对账 10s）→ S2 四 healthz + 三服务 `chatchat_goroutines` expvar 键（CASE-701）→ S3 注册+好友+`fchat` 单聊 → S4 建群+`bulkjoin` 500 满员+第 501 人 409/4004 → S5 `gchat` 群聊+traceId 双容器日志（`docker logs` grep）→ S6 断线补齐+漫游（`killdur` 两阶段+REST inbox 清单 lastSeq=30）→ S7 **真流量热点打标**（`hotmark`：发送 6 条超阈值流量→聚合器打标→timeline 增量==N2/inbox 增量==0 写放大=1）→ S8 `hotnotify` 轻通知直标面 → S9 `signal` 信令五型 → S10 full-down；
- **`scripts/w4_stress.sh`**（约 18~22 分钟）：两态对比——态一默认档基线（5min 250 条 50msg/min，inbox 增量==250×500=125,000）→ 态间 full-down/full-up 注入热点 env（N=100 粘性防摘标回写）→ 态二 `-premark` 打标边界固化后同负载（timeline 增量==250 且 inbox 增量==0）；每态断言四件套：零丢失（在线端 (msg,receiver) 对去重集——冷态推送/热态 CmdNotify→拉取两来源统一计量）/ lag 有界（120s 上限，末 3 采样非递增且终值≤50）/ goroutine 无泄漏（三服务前后差≤10）/ 写放大断言；读数全部打印供报告归档；
- **`scripts/w4_drills.sh`**（约 10~14 分钟）：六目标逐一 30s 窗——gateway（窗口内拨号失败=可感知不算丢）/ logic（形态 B 空进程：窗口内消息面无影响）/ pushpull（窗口内发送 kafka 堆积，恢复后 lag 归零+inbox 补齐）/ mysql/redis（docker stop+start，恢复后 readyz+数据完备）/ kafka（docker restart，120s lag 吸收重平衡+恢复后管道复活证明）；判据口径冻结：「已受理必达」（落库为受理锚点），可感知失败不算丢；
- **`bin/wsprobe -scenario hotmark`**（W4-M07）：真流量打标场景——不 UPDATE hot_level，发送超阈值流量由 api 聚合器打标（env 注入前提），打标边界固化后增量断言（timeline/inbox）+ 仅收 CmdNotify + 热分支拉取补齐；输出 `PROBE_GROUP/PROBE_CONV/PROBE_MARKED`；
- **loadgen 群拓扑档**：`./bin/loadgen -scenario=group -members=500 -senders=50 -extra-online=50 -dur=5m -rate=1`（rate=每发送者每分钟条数；默认抖动开，`-jitter=false` 同相位容量档——`-p99-gate=false` 配套记录口径；`-premark` 打标前置；冒烟档 `-members=6 -senders=2 -extra-online=2 -dur=10s -rate=6`）；
- 验收证据归档于 `docs/w4-acceptance-report.md`（全周 §9.4/§13 双证据表 + 移交终表），发布记录见 `docs/CHANGELOG.md`（0.1.0）。

## 扩展模块契约预留（W4-M06 起）

spec §9 四扩展模块的契约预留已归档（**纯文档，零业务实现、零 proto 实文件、零 DDL 实建**，ADR-0013）：

| 模块 | 契约面 | 挂接点 | 文档 |
|---|---|---|---|
| media（图片处理） | REST：上传 `POST /api/media/image` + 状态查询；异步管道（解码→缩略图→WebP→质量分级） | 消息 `url` + contentType 3/4/5 | docs/extend-contracts.md §3 |
| bot（AI 机器人） | gRPC（proto 草案内嵌，子包 `protocol.bot`）+ topic 候选 `chatchat-bot` | Logic HandleChat/HandleGroupChat 行快照路由同构插入点；`ext.bot="true"`；下行信封 kind=2 扩值 | §4 |
| feed（朋友圈/空间） | gRPC（Publish/Feed 流/Like/Comment/可见性校验/鉴权换验）+ 四表草案 | 仅共享用户体系（鉴权经 loopback 换验） | §5 |
| pay（支付） | REST 五端点（绑卡/解绑/脱敏回读/下单幂等/流水）+ 四表草案（bank_card 卡号 AES 注记） | 错误码 8xxx 段预留；**不实现真实扣款** | §6 |

- **统一规约**：插件位八条（独立服务+MQ/RPC、能力自报、宿主窄接口白名单、降级、消息必经宿主管道、ext 命名空间、extend 子包三段式）见 docs/extend-contracts.md §1；`ChatMessage.ext` 字段规范（wire-only，落库为 ADR 级预留）见 §2；8xxx 与 cmd>=100 段预留声明（只登记不注册）见 §7；
- **裁决依据**：ADR-0013（接入形态取舍、ext wire-only、bot 信封 kind 扩值、段位预留、Draft 状态约束）；
- 契约文档状态 **Draft**——各模块实现启动时须先复核对应章节，偏差经 ADR 级变更登记。
