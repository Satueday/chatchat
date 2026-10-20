# ChatChat Makefile（W1-M01 建立目标语义见 W1-M00-plan.md §6.2；W1-M07 增强 cover/check）
# 约束：Git Bash 兼容（无 jq / find -delete 等 POSIX 重依赖）。
# 依赖说明：W1-M01 的 tidy 禁令已于 W1-M07 解除（8 冻结依赖全部 import 实测
# 核实后 go mod tidy 收口，见 W1-M07-plan §2 裁决 #5）；此后依赖变更须走
# tidy + go mod verify + diff 审查。

BIN := bin/api
GATEWAY := bin/gateway
LOGIC := bin/logic
PUSHPULL := bin/pushpull

# compose 收口（W1-M04）：env-file 固定取仓库根 .env（cp .env.example .env 生成）。
COMPOSE := docker compose -f deployments/docker-compose.yml --env-file .env
# compose-up 等待全部 healthy 的轮询上限（秒），可经环境变量覆盖。
COMPOSE_WAIT_TIMEOUT ?= 300

# ---- 容器全栈形态（W4-M05，plan D1/D2/D12，ADR-0012）----
# FULL_COMPOSE：双文件叠加（基础设施 compose + 服务 overlay）；与 make
# compose-*（dev 依赖形态）互不干扰（run/perf/w3 验收资产零改动，D12）。
# MSYS_NO_PATHCONV=1：Git Bash 防 -f 路径改写（make topics 先例）。
FULL_COMPOSE := MSYS_NO_PATHCONV=1 docker compose -f deployments/docker-compose.yml -f deployments/docker-compose.services.yml --env-file .env
# 本地镜像 tag（plan D1：chatchat:0.1.0，与 global.Version 对齐）。
IMAGE ?= chatchat:0.1.0
# 镜像构建 GOPROXY 注入（plan D1 build-arg；容器内 proxy.golang.org 不可达
# 网络的缺省走 goproxy.cn——make proto 的 W3-M03 偏差①先例，可覆盖）。
DOCKER_GOPROXY ?= https://goproxy.cn,direct
# full-up 幂等轮询上限（秒）：七容器 healthy + topics-init exited(0)。
FULL_WAIT_TIMEOUT ?= 180

# 四服务运行时产物目录（W2-M06）：pid/日志/注入 env（含 secret，gitignored，
# README 已注明敏感性；chmod 语义在 Windows 弱化）。
RUN_DIR := logs/run
# 服务→健康端口映射（与 config 默认值/README 端口对齐表同步）。
api_PORT := 8080
gateway_PORT := 8081
logic_PORT := 8082
pushpull_PORT := 8083


.PHONY: build lint fmt-check vet check compose-up compose-down compose-logs compose-reset topics clean proto
.PHONY: run stop ensure-env start-api start-gateway start-logic start-pushpull
.PHONY: stop-api stop-gateway stop-logic stop-pushpull
.PHONY: restart-api restart-gateway restart-logic restart-pushpull
.PHONY: full-build full-up full-down full-logs full-reset

build:
	@mkdir -p bin && go build -o $(BIN) ./cmd/api && go build -o $(GATEWAY) ./cmd/gateway && go build -o $(LOGIC) ./cmd/logic && go build -o $(PUSHPULL) ./cmd/pushpull



lint:
	golangci-lint run ./...

fmt-check:
	@test -z "$$(gofmt -l .)" || (gofmt -l . && exit 1)

vet:
	go vet ./...

# 聚合门禁：build + fmt-check + vet + lint（发布分支口径：无测试代码）。
check: build fmt-check vet lint


# 拉起三件套并轮询等待全部 healthy（间隔 5s，上限 COMPOSE_WAIT_TIMEOUT 秒，默认 300）。
# 超时则列出未 healthy 的服务并给出日志指引，exit 1（失败立即暴露，不静默降级）。
compose-up:
	@$(COMPOSE) up -d
	@echo "compose-up: waiting for services to become healthy (timeout $(COMPOSE_WAIT_TIMEOUT)s)..."
	@elapsed=0; \
	while :; do \
		failed=''; \
		for svc in mysql redis kafka; do \
			state=`docker inspect --format '{{.State.Status}}/{{if .State.Health}}{{.State.Health.Status}}{{else}}none{{end}}' chatchat-$$svc 2>/dev/null || echo missing/missing`; \
			if [ "$$state" != "running/healthy" ]; then failed="$$failed $$svc($$state)"; fi; \
		done; \
		if [ -z "$$failed" ]; then \
			echo ""; \
			echo "compose-up: all services healthy (waited $${elapsed}s)."; \
			exit 0; \
		fi; \
		if [ $$elapsed -ge $$(( $(COMPOSE_WAIT_TIMEOUT) )) ]; then \
			echo ""; \
			echo "compose-up: TIMEOUT after $$(($(COMPOSE_WAIT_TIMEOUT)))s, not healthy:$$failed"; \
			echo "Hint: check logs with 'make compose-logs' or '$(COMPOSE) logs <service>'"; \
			exit 1; \
		fi; \
		printf '.'; \
		sleep 5; \
		elapsed=$$((elapsed + 5)); \
	done

# 停止编排环境（不带 -v：保留数据卷，数据与初始化幂等，可重复拉起）。
compose-down:
	$(COMPOSE) down

# 跟随查看三件套日志（Ctrl-C 退出，不影响容器）。
compose-logs:
	$(COMPOSE) logs -f

# 危险操作：停止并删除全部数据卷（mysql/redis/kafka 数据全丢）。
# initdb/*.sql 仅在数据卷为空的首次初始化时执行——修改 SQL 后须用本目标重置再 compose-up。
compose-reset:
	@echo "WARNING: compose-reset deletes ALL data volumes (mysql/redis/kafka data will be lost)."
	$(COMPOSE) down -v

# ---- W3-M05-C2：Kafka topic 分区治理（ADR-0007 §8；W4-M02 增 hot topic + 数值断言）----
# topics：幂等确保 chatchat-message/chatchat-push/chatchat-hot-group 存在且
# KAFKA_PARTITIONS（默认 6，可覆盖如 KAFKA_PARTITIONS=12 make topics）分区：
# 不存在则建、分区数不足则 --alter 扩、已满足（==N 或 >N）则 no-op；末尾
# --describe 数值断言（PartitionCount >= N 逐 topic，W4-M02 D9——head 管道
# 改为 sed 单行提取，消除管道 stderr 噪音）。
# 幂等实现（Kafka 3.9 实测行为，2026-09-20 探针验证）：先试 --alter
# --partitions N——成功即扩容完成；失败按错误串分流：「does not exist」
# → --create --if-not-exists（建题，RF=1 单节点不变量）；「already has N」
# （同数 alter 为 InvalidPartitionsException exit 1，非 no-op）或
# 「would not be an increase」（当前 >N）→ 已满足 no-op；其余（broker
# 不可达等）→ 失败 exit 1 不静默降级。
# Git Bash 兼容：MSYS_NO_PATHCONV=1 阻止容器路径 /opt/kafka/... 被 MSYS
# 改写为 Windows 路径（make proto 同坑先例）。recipe 正文纯 ASCII（W2-M06
# 引号坑登记）。
KAFKA_PARTITIONS ?= 6
KAFKA_TOPICS := chatchat-message chatchat-push chatchat-hot-group

topics:  ## idempotently ensure all three topics exist with $(KAFKA_PARTITIONS) partitions, then --describe with numeric assertion
	@set -e; \
	for topic in $(KAFKA_TOPICS); do \
		if out=`MSYS_NO_PATHCONV=1 docker exec chatchat-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --alter --topic $$topic --partitions $(KAFKA_PARTITIONS) 2>&1`; then \
			echo "topics: $$topic partitions increased to $(KAFKA_PARTITIONS)"; \
		elif printf '%s' "$$out" | grep -q 'does not exist'; then \
			MSYS_NO_PATHCONV=1 docker exec chatchat-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --if-not-exists --topic $$topic --partitions $(KAFKA_PARTITIONS) --replication-factor 1; \
			echo "topics: $$topic created with $(KAFKA_PARTITIONS) partitions"; \
		elif printf '%s' "$$out" | grep -q 'already has\|would not be an increase'; then \
			echo "topics: $$topic already has >= $(KAFKA_PARTITIONS) partitions, no-op"; \
		else \
			echo "topics: FAIL on $$topic: $$out (is chatchat-kafka running? try: make compose-up)" >&2; \
			exit 1; \
		fi; \
	done; \
	echo "topics: describe numeric assertion (PartitionCount >= $(KAFKA_PARTITIONS) per topic):"; \
	for topic in $(KAFKA_TOPICS); do \
		pcount=`MSYS_NO_PATHCONV=1 docker exec chatchat-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic $$topic | sed -n 's/.*PartitionCount: *\([0-9][0-9]*\).*/\1/p' | sed -n '1p'`; \
		if [ -z "$$pcount" ]; then \
			echo "topics: FAIL $$topic: describe produced no PartitionCount (topic missing or broker unreachable)" >&2; \
			exit 1; \
		fi; \
		if [ "$$pcount" -lt $(KAFKA_PARTITIONS) ]; then \
			echo "topics: FAIL $$topic: PartitionCount=$$pcount < $(KAFKA_PARTITIONS)" >&2; \
			exit 1; \
		fi; \
		echo "topics: $$topic PartitionCount=$$pcount OK"; \
	done

clean:
	rm -rf bin/

# ---- 容器全栈目标（W4-M05 plan D1/D2；recipe 正文纯 ASCII——W2-M06 引号坑）----
# full-build：多阶段镜像构建（deployments/Dockerfile，上下文=仓库根）。
#   四二进制一次构建 + web/ 拷入 + 非 root 运行层；GOPROXY 经 build-arg
#   （DOCKER_GOPROXY 覆盖，缺省 goproxy.cn——本机容器网络实测路径）。
full-build:
	docker build -f deployments/Dockerfile --build-arg GOPROXY=$(DOCKER_GOPROXY) -t $(IMAGE) .

# full-up：双文件叠加拉起全栈（三依赖 + topics-init + 四服务）并幂等轮询
#   七容器 running/healthy + topics-init exited(0)，上限 FULL_WAIT_TIMEOUT
#   （180s）；复跑 no-op（CASE-I51 双跑幂等）。前置：.env 含
#   MYSQL_ROOT_PASSWORD 与 CHATCHAT_JWT_SECRET（缺失 compose 即报错——
#   部署密钥应显式，plan §6）。
full-up:
	@$(FULL_COMPOSE) up -d
	@echo "full-up: waiting for all containers (timeout $(FULL_WAIT_TIMEOUT)s)..."
	@elapsed=0; \
	while :; do \
		failed=''; \
		for svc in mysql redis kafka api gateway logic pushpull; do \
			state=`docker inspect --format '{{.State.Status}}/{{if .State.Health}}{{.State.Health.Status}}{{else}}none{{end}}' chatchat-$$svc 2>/dev/null || echo missing/missing`; \
			if [ "$$state" != "running/healthy" ]; then failed="$$failed $$svc($$state)"; fi; \
		done; \
		init=`docker inspect --format '{{.State.Status}}:{{.State.ExitCode}}' chatchat-topics-init 2>/dev/null || echo missing`; \
		if [ "$$init" != "exited:0" ]; then failed="$$failed topics-init($$init)"; fi; \
		if [ -z "$$failed" ]; then \
			echo ""; \
			echo "full-up: all services healthy + topics-init exited 0 (waited $${elapsed}s)."; \
			echo "full-up: host ports 8080(api)/8081(gateway+web)/8082(logic)/8083(pushpull); metrics: curl -s http://127.0.0.1:8081/debug/vars"; \
			exit 0; \
		fi; \
		if [ $$elapsed -ge $(FULL_WAIT_TIMEOUT) ]; then \
			echo ""; \
			echo "full-up: TIMEOUT after $(FULL_WAIT_TIMEOUT)s, not ready:$$failed"; \
			echo "Hint: make full-logs; or $(FULL_COMPOSE) logs <service>"; \
			exit 1; \
		fi; \
		printf '.'; \
		sleep 5; \
		elapsed=$$((elapsed + 5)); \
	done

# full-down：停全栈（保留数据卷与镜像；依赖数据与 init 幂等可重复拉起）。
full-down:
	$(FULL_COMPOSE) down

# full-logs：跟随全栈日志（Ctrl-C 退出，不影响容器）。
full-logs:
	$(FULL_COMPOSE) logs -f

# full-reset：危险操作——停全栈并删全部数据卷（mysql/redis/kafka 数据全丢；
#   镜像保留，须再 full-build 才有镜像）。从零重验入口（plan §8 验收命令）。
full-reset:
	@echo "WARNING: full-reset deletes ALL data volumes (mysql/redis/kafka data will be lost)."
	$(FULL_COMPOSE) down -v

# protobuf 生成（W2-M01）：docker 化 protoc（本机无 protoc，plan.md §12 方案）。
# 插件 pin v1.36.9 = go.mod runtime 版本；golang:1.25 官方镜像 + apt
# protobuf-compiler（protoc 次版本随 debian 源浮动，确定性口径见
# docs/adr/0004）。paths=source_relative 使 go_package 保持 spec 字面
# "pkg/protocol"（module= 前缀剥离对相对形态不适用）。
# Git Bash 兼容：MSYS_NO_PATHCONV=1 阻止 MSYS 把容器路径 /wd 改写为
# Windows 路径；pwd -W 取 Windows 形态工作目录；容器内 shell 的 $ 转义 $$。
# 生成物提交仓库（离线构建/单测不依赖 docker）；重跑幂等（CASE-009 以
# git diff 空证明）。
# W3-M03 起双文件编译：deliver.proto 引用 protocol.proto 的冻结 ChatMessage
# （同一 protoc 调用，import 路径按 proto_path 根写全路径 pkg/protocol/...）。
# M 标志（W3-M03）：deliver.proto 的 go_package 为完整模块路径
# chatchat/pkg/protocol（plan §6.2 冻结字面），而 protocol.proto 为 spec 字面
# 相对形态 pkg/protocol——两文件映射到不同 Go import path 时 protoc-gen-go
# 会在 deliver.pb.go 里对同目录包发出跨包 import（编译期断裂）。M 标志把
# protocol.proto 统一映射到 chatchat/pkg/protocol，两文件同包直引、零 import；
# M 只影响 import path 解析，不改写 descriptor，protocol.pb.go 字节稳定
# （make proto 后 git diff 核验）。
# W4-M04 起三文件 + grpc 插件（ADR-0011 D8）：signal.proto 增 service Signal
# + SignalFrame（Exchange 双向流，spec §8.4）；protoc-gen-go-grpc pin v1.5.1
# 与 go.mod 锁定的 grpc v1.76.0 配对（SupportPackageIsVersion9，>=1.62 兼容；
# v1.6.x 插件生成面要求 grpc >=1.81/protobuf >=1.36.11，会强制越过本仓
# protobuf v1.36.9 冻结锚点——版本矩阵核对见 ADR-0011）；--go-grpc_out 与
# --go_out 同参（source_relative + M 标志同值），产物 signal.pb.go +
# signal_grpc.pb.go 提交仓库。容器内 proxy.golang.org 不可达时注入
# GOPROXY=https://goproxy.cn,direct（W3-M03 偏差①先例）。
proto:
	@MSYS_NO_PATHCONV=1 docker run --rm -v "$$(pwd -W)":/wd -w /wd golang:1.25 \
		sh -ec 'apt-get update -qq >/dev/null && apt-get install -y -qq protobuf-compiler >/dev/null && \
		GOBIN=/tmp/bin go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.36.9 && \
		GOBIN=/tmp/bin go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.5.1 && \
		PATH=$$PATH:/tmp/bin protoc --go_out=. --go_opt=paths=source_relative \
		--go_opt=Mpkg/protocol/protocol.proto=chatchat/pkg/protocol \
		--go-grpc_out=. --go-grpc_opt=paths=source_relative \
		--go-grpc_opt=Mpkg/protocol/protocol.proto=chatchat/pkg/protocol \
		pkg/protocol/protocol.proto pkg/protocol/deliver.proto pkg/protocol/signal.proto' && \
		echo "proto: pkg/protocol/{protocol,deliver,signal}.pb.go + signal_grpc.pb.go regenerated"

# ---- 四服务一键启动/停止/独立重启（W2-M06，plan §6.2 冻结骨架）----
# run：compose-up（幂等）→ build → 串行起 api→gateway→logic→pushpull，
#       逐个 /healthz 就绪轮询（curl -sf，2s 间隔 60s 上限）；
# stop：读 $(RUN_DIR)/<svc>.pid 逐个 kill（含 taskkill 兜底），幂等；
# restart-<svc>：独立重启单服务，复用 $(RUN_DIR)/env 的 JWT secret（语义连续）。
# Git Bash 兼容：$$ 转义、ps -W 取 winpid、taskkill //F //PID、无 jq。

run:
	@$(MAKE) --no-print-directory compose-up
	@$(MAKE) --no-print-directory build
	@$(MAKE) --no-print-directory start-api
	@$(MAKE) --no-print-directory start-gateway
	@$(MAKE) --no-print-directory start-logic
	@$(MAKE) --no-print-directory start-pushpull
	@echo "run: all services ready (api :$(api_PORT) / gateway :$(gateway_PORT) / logic :$(logic_PORT) / pushpull :$(pushpull_PORT))"

stop:
	@$(MAKE) --no-print-directory stop-pushpull
	@$(MAKE) --no-print-directory stop-logic
	@$(MAKE) --no-print-directory stop-gateway
	@$(MAKE) --no-print-directory stop-api

restart-api restart-gateway restart-logic restart-pushpull:
	@set -e; \
	svc=`echo "$(@)" | sed 's/^restart-//'`; \
	echo "restart: $$svc"; \
	$(MAKE) --no-print-directory stop-$$svc; \
	$(MAKE) --no-print-directory start-$$svc

# ensure-env 幂等生成 $(RUN_DIR)/env：JWT secret 首次生成（urandom 48B
# base64，此后 run/restart 复用同值——重启语义连续性）；MySQL 密码每次
# 自根 .env 解析 MYSQL_ROOT_PASSWORD 刷新注入。env 文件含 secret，属
# gitignored 运行时产物，禁止 cat/提交（README 已注明敏感性）。
# 注意：recipe 正文保持纯 ASCII——native Win32 make 向 sh 传含多字节字符
# 的多行命令会破坏引号配对（W2-M06 实测坑，注释不受影响）。
ensure-env:
	@mkdir -p $(RUN_DIR)
	@set -e; \
	envf=$(RUN_DIR)/env; \
	secret=`sed -n "s/^export CHATCHAT_JWT_SECRET='\(.*\)'\$$/\1/p" $$envf 2>/dev/null | head -n 1`; \
	if [ -z "$$secret" ]; then secret=`head -c 48 /dev/urandom | base64`; fi; \
	mysql_pw=`grep -E '^[[:space:]]*MYSQL_ROOT_PASSWORD[[:space:]]*=' .env 2>/dev/null | head -n 1 | cut -d= -f2- | tr -d '\r\n"' | sed -e "s/^[[:space:]]*//" -e "s/^'//" -e "s/'\$$//"`; \
	if [ -z "$$mysql_pw" ]; then \
		echo "ensure-env: MYSQL_ROOT_PASSWORD missing in .env (cp .env.example .env, fill it, retry)" >&2; \
		exit 1; \
	fi; \
	{ echo "export CHATCHAT_JWT_SECRET='$$secret'"; echo "export CHATCHAT_MYSQL_PASSWORD='$$mysql_pw'"; } > $$envf

# start-<svc> 起单服务（幂等：pid 存活即跳过）：后台 exec 注入 env、日志落
# $(RUN_DIR)/<svc>.log、就绪轮询 /healthz（2s×60s），失败打印日志尾部并
# exit 1（不静默降级）。
start-api start-gateway start-logic start-pushpull: ensure-env
	@set -e; \
	name=`echo "$(@)" | sed 's/^start-//'`; \
	port=; \
	case "$$name" in \
		api) port=$(api_PORT);; gateway) port=$(gateway_PORT);; logic) port=$(logic_PORT);; pushpull) port=$(pushpull_PORT);; \
	esac; \
	pidf=$(RUN_DIR)/$$name.pid; \
	if [ -f "$$pidf" ] && kill -0 `cat "$$pidf"` 2>/dev/null; then \
		echo "run: $$name already running (pid `cat $$pidf`), skip"; exit 0; \
	fi; \
	( . $(RUN_DIR)/env; exec ./bin/$$name ) > $(RUN_DIR)/$$name.log 2>&1 & \
	pid=$$!; echo $$pid > "$$pidf"; \
	elapsed=0; ok=0; \
	while [ $$elapsed -lt 60 ]; do \
		if curl -sf "http://127.0.0.1:$$port/healthz" >/dev/null 2>&1; then ok=1; break; fi; \
		if ! kill -0 $$pid 2>/dev/null; then break; fi; \
		sleep 2; elapsed=$$((elapsed + 2)); \
	done; \
	if [ "$$ok" != 1 ]; then \
		echo "run: $$name NOT ready on port :$$port (last 5 lines of $(RUN_DIR)/$$name.log:)" >&2; \
		tail -n 5 $(RUN_DIR)/$$name.log 2>/dev/null || true; \
		rm -f "$$pidf"; \
		exit 1; \
	fi; \
	echo "run: $$name ready on :$$port (pid $$pid)"

# stop-<svc> 停单服务（幂等）：kill 后等待退出（10s 上限），超时兜底
# ps -W 取 winpid + taskkill //F（MSYS kill 对原生进程的力度兜底），清理 pid。
stop-api stop-gateway stop-logic stop-pushpull:
	@set -e; \
	name=`echo "$(@)" | sed 's/^stop-//'`; \
	pidf=$(RUN_DIR)/$$name.pid; \
	if [ ! -f "$$pidf" ]; then echo "stop: $$name not running (no pid file)"; exit 0; fi; \
	pid=`cat "$$pidf"`; \
	if kill -0 $$pid 2>/dev/null; then \
		kill $$pid 2>/dev/null || true; \
		n=0; while [ $$n -lt 10 ] && kill -0 $$pid 2>/dev/null; do sleep 1; n=$$((n + 1)); done; \
		if kill -0 $$pid 2>/dev/null; then \
			winpid=`ps -W | awk -v p=$$pid '$$1 == p { print $$4; exit }'`; \
			if [ -n "$$winpid" ]; then taskkill //F //PID $$winpid >/dev/null 2>&1 || true; fi; \
			kill -9 $$pid 2>/dev/null || true; \
			sleep 1; \
		fi; \
		echo "stop: $$name stopped (pid $$pid)"; \
	else \
		echo "stop: $$name already stopped (stale pid file cleaned)"; \
	fi; \
	rm -f "$$pidf"

