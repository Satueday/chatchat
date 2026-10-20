# ChatChat 多阶段镜像（W4-M05 plan D1，ADR-0012）：单 Dockerfile 四入口
# （api/gateway/logic/pushpull 一次构建四产物——层缓存最大化，gotify 范式）；
# compose 以 entrypoint 差异化四服务。HEALTHCHECK 不进本文件（端口随服务
# 不同，归 compose，plan D1）。
#
# 构建上下文=仓库根：docker build -f deployments/Dockerfile -t chatchat:0.1.0 .
# GOPROXY 经 build-arg 注入（ADR-0011 D8 先例；容器内 proxy.golang.org
# 不可达网络注入 --build-arg GOPROXY=https://goproxy.cn,direct——make
# full-build 的 DOCKER_GOPROXY 已参数化）。

# ---- 构建层：golang:1.25-alpine（CGO_ENABLED=0 纯静态，go.mod 锚 1.25）----
FROM golang:1.25-alpine AS build

ARG GOPROXY=https://goproxy.cn,direct
ARG VERSION=0.1.0
ENV CGO_ENABLED=0 GOOS=linux

WORKDIR /src
# 先拷 go.mod/go.sum 单独下载依赖（层缓存：源码变动不重下依赖）。
COPY go.mod go.sum ./
RUN GOPROXY=${GOPROXY} go mod download

COPY . .
# 四二进制一次构建；-trimpath 去宿主路径、-s -w 去符号表缩体积、
# -X main.buildVersion 版本注入（四入口统一 ldflags 注入点，W4-M05 §6）。
RUN go build -trimpath -ldflags "-s -w -X main.buildVersion=${VERSION}" -o /out/api ./cmd/api \
 && go build -trimpath -ldflags "-s -w -X main.buildVersion=${VERSION}" -o /out/gateway ./cmd/gateway \
 && go build -trimpath -ldflags "-s -w -X main.buildVersion=${VERSION}" -o /out/logic ./cmd/logic \
 && go build -trimpath -ldflags "-s -w -X main.buildVersion=${VERSION}" -o /out/pushpull ./cmd/pushpull

# ---- 运行层：alpine:3.20（纯静态二进制，无运行时 Go 依赖）----
FROM alpine:3.20

# ca-certificates（出站 TLS）+ tzdata（时区数据）；非 root 用户（gotify 缺口
# 补齐，plan D1）：固定 uid 系统用户 app。
RUN apk add --no-cache ca-certificates tzdata \
 && addgroup -g 10001 -S app \
 && adduser -u 10001 -S -G app -H -h /app app

WORKDIR /app
COPY --from=build /out/ /app/
# web/ 静态联调页（gateway 同源供给，W4-M04-C2 D3）。
COPY web/ /app/web/

USER app
# 默认入口 api；compose overlay 以 entrypoint: ["/app/gateway"] 等差异化。
ENTRYPOINT ["/app/api"]
