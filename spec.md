# ChatChat IM 后端 — 项目规格说明书

> 版本：v0.1（规划稿）
> 语言：Golang
> 参考项目：`G:\code\go\go-chat`（结构参考：Gin + WebSocket + Kafka + MySQL 分层架构）

---

## 1. 项目概述

构建一个可水平扩展的 IM 后端系统，核心特性：

- **通信协议**：对外 HTTPS（REST API）+ WebSocket（长连接实时消息），内部服务间 RPC（gRPC）
- **消息编码**：Protocol Buffers（结构化、体积小、支持二进制字段）
- **消息投递**：推拉结合（Push + Pull），通过消息队列（MQ）广播写入各用户信箱
- **实时音视频**：WebRTC，后端通过 RPC 提供信令（Signaling）服务
- **群规模**：单群成员上限 500 人
- **热门群聊**：按近期消息频率动态打标，走独立处理通道（专用消息队列 + 专属协程池）
- **账号体系**：注册绑定手机号，支持账号密码登录
- **文件传输**：大文件（图片/视频/文件）走对象存储客户端直传 + 预签名 URL（私有桶），消息只引用资源地址，字节流不经过业务服务
- **消息搜索**：基于 MySQL 全文索引（ngram 分词）的消息检索，限本人参与会话
- **可扩展性**：预留图片编解码/压缩、AI 机器人、朋友圈/空间社群、银行卡绑定支付等扩展模块

### 1.1 设计目标

| 目标 | 指标/说明 |
|------|-----------|
| 低延迟 | 在线消息端到端 P99 < 200ms（同城部署） |
| 高可靠 | 消息不丢、不重、可排序（ACK + 去重 + 递增序号） |
| 可扩展 | 网关无状态，支持水平扩容；模块间通过 MQ/RPC 解耦 |
| 低服务器压力 | 推拉结合 + 热点群降级 + 消息聚合批量推送 |
| 用户舒适度 | 断线重连快速补齐消息、多端同步、离线消息漫游 |

---

## 2. 总体架构

```
                        ┌─────────────────────────────────────────┐
                        │              客户端 (Web/App)            │
                        └───────┬─────────────────┬───────────────┘
                          HTTPS │          WSS    │
                     (REST API)│                 │(WebSocket 长连接)
                        ┌──────▼──────┐   ┌──────▼──────────────┐
                        │  API 网关   │   │  WS 网关 (Gateway)   │
                        │  (Gin/HTTPS)│  │  无状态·可水平扩展    │
                        └──────┬──────┘   └──────┬──────────────┘
                               │                 │
                 ┌─────────────▼─────────────────▼─────────────┐
                 │            Logic 业务服务 (RPC)               │
                 │  用户/群组/消息/好友/扩展模块(插件化)          │
                 └──────┬──────────┬──────────┬────────────────┘
                        │          │          │
              ┌─────────▼───┐ ┌────▼─────┐ ┌──▼──────────────┐
              │ 消息队列 MQ  │ │  存储    │ │  Push/Pull 服务  │
              │ (Kafka)     │ │ MySQL    │ │  (消费MQ·投递)   │
              │ 普通topic + │ │ Redis    │ └──┬──────────────┘
              │ 热点群topic │ │ 对象存储 │    │
              └─────────────┘ └──────────┘ ───┘
```

### 2.1 分层职责

| 层 | 职责 |
|----|------|
| WS 网关层 | 维持连接、心跳、鉴权、编解码、把消息投递到客户端；不持业务状态 |
| API 网关层 | HTTPS REST：注册登录、历史消息拉取、文件上传、管理类接口 |
| Logic 层 | 业务逻辑：消息编排、群管理、信箱写入决策、热点群打标 |
| MQ 层 | 解耦消息生产与投递；普通群 topic 与热点群 topic 分离 |
| Push/Pull 服务 | 消费 MQ → 判断在线（推）或写入信箱（留待拉取） |
| 存储层 | MySQL（持久化）+ Redis（在线状态、游标、热点统计、缓存） |

---

## 3. 技术选型

| 领域 | 选型 | 说明 |
|------|------|------|
| Web 框架 | Gin | 参考 go-chat，成熟稳定 |
| WebSocket | gorilla/websocket 或 nhooyr/websocket | 长连接 |
| RPC | gRPC | 内部服务通信；音视频信令亦走 gRPC stream |
| 协议 | protobuf v3 | 消息体定义见 §5 |
| MQ | Kafka | 参考 go-chat 的 kafka 配置；topic 划分见 §7 |
| ORM | GORM | 参考 go-chat |
| 缓存 | Redis | 在线状态、seq 游标、热点群统计、限流 |
| 数据库 | MySQL 8 | 业务数据持久化 |
| 日志 | zap | 参考 go-chat |
| 配置 | viper（config.toml） | 参考 go-chat |
| 认证 | JWT（access + refresh） | 登录态管理 |
| 密码 | bcrypt | 账号密码登录 |
| 对象存储 | 腾讯云 COS（本地磁盘实现保留为开发/测试后端） | 图片/文件/语音/视频；私有桶 + 预签名读写 |

---

## 4. 目录结构规划（参考 go-chat）

```
chatchat/
├── api/                    # REST API 定义 (swagger/接口文档)
├── assets/                 # 静态资源/截图
├── cmd/                    # 程序入口
│   ├── gateway/            #   WS 网关服务
│   ├── api/                #   HTTPS API 服务
│   ├── logic/              #   业务逻辑服务
│   └── pushpull/           #   推拉投递服务
├── config/                 # 配置加载 (viper)
│   └── config.go
├── deployments/            # docker-compose / k8s 部署
├── internal/               # 内部实现
│   ├── dao/                # 数据访问
│   ├── model/              # 数据模型
│   ├── router/             # 路由注册
│   ├── server/             # 连接管理 (client.go / server.go，参考 go-chat)
│   ├── service/            # 业务逻辑
│   │   ├── user/           #   用户（注册/登录/手机号绑定）
│   │   ├── group/          #   群组（成员上限500）
│   │   ├── message/        #   消息（发送/信箱/推拉）
│   │   ├── friend/         #   好友关系
│   │   └── extend/         #   扩展模块（见 §9）
│   ├── mq/                 # MQ 生产/消费 (kafka)
│   │   ├── producer.go
│   │   ├── consumer.go
│   │   └── hotgroup/       #   热点群专用消费者
│   └── hotdetect/          # 热点群动态识别
├── pkg/                    # 可复用公共包
│   ├── common/             # 通用工具
│   ├── errors/             # 错误码
│   ├── global/             # 全局常量
│   └── protocol/           # protobuf 定义与生成代码
├── test/                   # 测试
├── web/                    # 前端或静态托管
├── config.toml
├── Makefile                # build/proto/test 命令
├── chatchat.sql            # 建表语句（参考 go-chat.sql）
├── go.mod
└── README.md
```

---

## 5. 通信协议设计（protobuf）

在 go-chat 的 `Message` 基础上重构，去掉冗余字段（avatar、fromUsername 不随消息传输，由客户端按 userId 缓存），并增加 seq/ack 相关字段支撑推拉模式：

```protobuf
syntax = "proto3";
package protocol;
option go_package = "pkg/protocol";

// WebSocket 帧外层信封
message Frame {
  Header header = 1;      // 帧头（cmd、seq、ack 等）
  bytes payload = 2;      // 按 cmd 编解码的具体消息体
}

message Header {
  int32 cmd = 1;          // 命令字：心跳/单聊/群聊/信令/ACK/拉取请求/拉取响应…
  int64 seq = 2;          // 客户端发起请求的序号（用于响应匹配）
  int64 ack = 3;          // 确认的 seq（ACK 回执）
  string traceId = 4;     // 链路追踪
}

// 聊天消息
message ChatMessage {
  string msgId = 1;        // 服务端生成的全局唯一消息ID
  string conversationId = 2; // 会话ID（单聊: u1|u2 排序拼接；群聊: g_{groupId}）
  string from = 3;         // 发送者 userId
  string to = 4;           // 接收者 userId 或 groupId
  int64  msgSeq = 5;       // 会话内递增序号（拉取/去重/排序核心）
  int64  timestamp = 6;    // 服务端时间戳
  int32  contentType = 7;  // 1文本 2文件 3图片 4音频 5视频 6语音通话 7视频通话 8自定义
  string content = 8;      // 文本内容 / JSON 扩展
  string url = 9;          // 媒体资源地址
  map<string, string> ext = 10; // 扩展字段（AI消息标记等）
}

// 拉取请求（客户端携带本地最大 seq）
message PullRequest {
  string conversationId = 1;
  int64  afterSeq = 2;     // 本地已同步到的 seq
  int32  limit = 3;        // 单次拉取上限（默认 50）
}

// 拉取响应
message PullResponse {
  string conversationId = 1;
  repeated ChatMessage messages = 2;
  int64  maxSeq = 3;       // 服务端当前最大 seq，判断是否拉全
  bool   hasMore = 4;
}

// WebRTC 信令
message Signal {
  string callId = 1;       // 一次通话会话ID
  string from = 2;
  string to = 3;
  int32  signalType = 4;   // 1 offer 2 answer 3 ice-candidate 4 挂断 5 呼叫邀请
  string sdp = 5;          // SDP 内容
  string candidate = 6;    // ICE candidate JSON
  bool   isGroup = 7;      // 是否群组通话（预留）
}
```

**设计原则**：
- 小消息走 WebSocket 帧传输；大文件（图片/视频/文件）先经对象存储客户端直传（服务端只签发短时效预签名 PUT，文件字节流不经过业务服务），消息以 objectKey 引用资源；读取时经鉴权接口换取短时效预签名 GET（私有桶，链接可过期），避免大 payload 阻塞长连接（语音通话/视频流场景由 WebRTC 独立承担）
- `Frame` 信封 + cmd 命令字设计，便于扩展新消息类型而不破坏兼容

---

## 6. 推拉结合消息模型（核心设计）

### 6.1 基本概念

- **信箱（Inbox）**：每个用户 × 每个会话一个信箱，记录 `maxSeq` 游标。消息投递即向目标用户的信箱写入一条投递记录（含 msgId 与 seq）
- **推（Push）**：用户在线时，服务器通过 WebSocket 主动把新消息推给客户端
- **拉（Pull）**：客户端携带本地 `afterSeq` 主动向服务器拉取增量消息（登录时、重连时、切后台回前台时、收到推送通知但本地缺失时）

### 6.2 可靠投递流程（推 + ACK）

```
发送方 ──ChatMessage──▶ WS网关 ──▶ Logic
                              │ 1. 落库(MySQL) 分配会话内递增seq
                              │ 2. 写入 MQ（普通topic 或 热点群topic）
                              ▼
                     Push/Pull 服务消费
                              │ 3. 对会话成员逐个投递：
                              │    在线 → WebSocket 推送，等待客户端 ACK(seq)
                              │    离线 → 仅保留在信箱，可触发离线通知(可选)
                              ▼
                    客户端收到消息 ──ACK──▶ 服务端更新信箱游标
                              │ 客户端超时未 ACK → 服务端重推(指数退避, 最多N次)
                              │ 重推仍失败 → 视为离线，转拉取兜底
```

### 6.3 推拉策略（降服务器压力 + 提舒适度）

| 场景 | 策略 |
|------|------|
| 在线接收 | 推为主；推送失败转拉取兜底 |
| 登录/重连 | 拉为主：客户端按会话逐个 PullRequest 补齐 |
| 热点群高频消息 | 推送聚合：同会话消息在短窗口（如 50~200ms）内合并为一次批量推送，减少帧数 |
| 客户端弱网 | 检测到 ACK 延迟高 → 主动暂停推送，标记该连接只通知"有新消息"（seq 提示），由客户端拉取 |
| 离线消息 | 不推。登录后拉取，按会话 maxSeq 增量补齐；支持漫游（最近 N 天） |
| 消息去重 | 客户端按 (conversationId, msgSeq) 去重，天然幂等 |

### 6.4 与"消息队列广播到信箱"的关系

MQ 是 Logic → 投递服务之间的解耦通道；信箱写入与投递由投递服务完成：

- 普通消息（单聊/普通群聊）：MQ 消息即"写扩散指令"，消费后为每个成员生成信箱投递记录
- 热门群聊：走**读扩散**（见 §7.2），消息只写群 Timeline 一份，不逐个写信箱，成员拉取时按游标读群 Timeline

---

## 7. 热门群聊专项设计

### 7.1 动态打标（热点识别）

- 统计口径：滑动窗口（最近 1 小时）群消息速率（条/分钟）
- 实现：Redis `INCRBY group:{id}:msg:{时间桶}` + 定时任务聚合（1 分钟周期）
- 分级：
  - `< 50 msg/min`：普通群 → 写扩散 + 普通 topic
  - `≥ 50 msg/min`（阈值可配）：标记 **hot** → 读扩散 + 热点 topic，独立消费者组处理
  - 连续 N 个周期低于阈值 → 摘除 hot 标记，回退普通通道（回退需先保证群 Timeline 已补齐该群历史，避免切换丢消息）
- 打标结果写入 Redis + MySQL `group.hot_level`，Logic 发消息前查询决定路由

### 7.2 专属处理通道

- **独立 topic**：Kafka 单独 `chatchat-hot-group` topic，与普通消息 topic 物理隔离，热点流量不挤占普通消息消费能力
- **专属协程池**：热点消费者使用独立的 worker pool（协程数可配），单独限流与背压（消费 lag 超限 → 丢弃聚合通知、仅保留 Timeline 写入）
- **读扩散**：热门群消息只写一份到群 Timeline 表（`group_timeline`，按 groupId + seq 组织），500 人群一次消息从最多 500 次信箱写降为 1 次写，大幅降低写放大
- **推送降级**：热门群对在线成员仅推送"轻通知"（conversationId + maxSeq），客户端据此决定何时拉取，由客户端侧消化高频消息

---

## 8. 基础模块规划

### 8.1 用户模块

- 注册流程：手机号 + 验证码注册（验证码服务接口预留，开发期可 mock）→ 账号密码（bcrypt）→ 绑定手机号唯一约束
- 登录：账号密码登录 → 颁发 JWT（access 短期 + refresh 长期）；登录后建立 WebSocket 连接时校验 token
- 扩展预留：多端登录策略（互踢/共存）、设备管理表

### 8.2 群组模块

- 建群/入群/退群/踢人/禁言/转让群主/群公告
- 成员上限 500（服务端强校验，入群接口校验 + 定时任务对账）
- 群成员关系表 + 群角色（群主/管理员/成员）

### 8.3 好友模块

- 好友申请/同意/拒绝/删除；好友与群聊会话统一抽象为 `conversationId`，消息层不区分

### 8.4 音视频模块（WebRTC + gRPC）

- 参考 go-chat 的 WebRTC p2p 通话，信令改走 gRPC stream（网关侧转换为 WebSocket Signal 帧转发）
- 通话信令：呼叫邀请 → offer/answer → ICE candidate 交换 → 挂断
- 预留：群组通话需 SFU（如 mediasoup/Pion SFU），本期仅做 1v1，接口按 `isGroup` 预留

### 8.5 连接管理（参考 go-chat internal/server）

- 每个连接一个读协程 + 一个写协程（写协程配带缓冲 send channel，写超时断连）
- 连接注册表：本机 map（userId → conn）+ Redis 全局在线状态（userId → gateway 节点），支持分布式路由投递
- 心跳：客户端周期 ping，服务端超时（如 60s）踢出

### 8.6 文件传输（对象存储直传）

- 接入形态：服务端仅做凭证签发与元数据登记，文件字节流走客户端 ↔ 对象存储直连（业务服务无状态、不经手大流量，与 §2 分层职责一致）
- 上传流程：申请预签名 PUT（短时效，如 10 分钟）→ 客户端直传 → 完成确认（服务端 HEAD 校验后置 ready）
- objectKey 规范：`{uid}/{ULID}/{安全化文件名}`——含归属者、单调且不可猜测
- 读取策略：私有桶；消息/帖子只存 objectKey，展示时经鉴权接口换取短时效预签名 GET（如 30 分钟，客户端缓存至过期）
- 访问控制：换取预签名 GET 需登录态；key 不可猜测性由 ULID 保证，会话/好友级 ACL 为预留增强
- 配额：按内容类型（图片/音频/文件/视频）分设大小上限，服务端签发时校验
- 元数据表 `file_object`（uid、object_key 唯一、size、content_type、status pending/ready）支撑孤儿清理与审计

### 8.7 消息搜索

- 全文检索：MySQL 8 FULLTEXT + ngram 分词器（中文二元切分），不引入独立搜索引擎；查询接口形态与检索后端解耦，规模不足时可在不改对外接口的前提下更换后端
- 检索范围：仅文本类消息（content_type=1）；支持全库或指定会话
- 权限：仅可检索自身参与的会话（单聊会话归属 / 群成员关系校验）
- 查询语义：BOOLEAN MODE + 用户输入转义；相关性优先、时间序次之；游标分页
- 写路径零侵入：全文索引由存储层维护，消息发送链路不变（索引写放大纳入性能观测项）

---

## 9. 扩展模块规划（预留接口，不阻塞核心）

统一以 `internal/service/extend/` 插件位 + protobuf `ext` 扩展字段承载：

| 模块 | 设计预留 |
|------|----------|
| 图片处理 | 独立 `media` 微服务：上传回调触发异步管道（解码 → 缩略图 → WebP 压缩 → 质量分级），消息中的 url 指向处理产物；接口：`POST /api/media/image` |
| AI 机器人 | 独立 `bot` 服务；用户 @机器人 或私聊机器人 → Logic 识别路由到 bot topic → bot 消费生成回复 → 经回投 topic 由宿主代发（走标准 seq/落库/投递链），消息 `ext.bot = true` 标记；回复契约为富消息形态（contentType/url/ext），外部机器人框架以"平台适配器"形态接入（能力协商降级：流式/卡片/按钮不支持时退化为文本回复） |
| 朋友圈/空间 | 独立 `feed` 服务（gRPC 契约预留）；轻量形态：图文分享（文字 ≤200 字、图片 ≤9 张、图片走对象存储预签名上传，复用 §8.6）、时间线按时间窗展示（如最近 72 小时，窗口可配）、好友可见；轻量形态可先以 api 内置 REST 落地，功能变重（点赞评论/可见性范围分组）再拆独立服务 |
| 支付 | 独立 `pay` 服务：银行卡绑定（卡号校验 + 短信验证，敏感字段加密存储 AES）、支付渠道适配层（策略模式接入渠道）、账户流水表、幂等 + 对账任务；**本期仅做表结构与接口契约，不实现真实扣款** |

扩展模块一律"独立服务 + MQ/RPC 挂接"方式接入，核心链路零侵入。

---

## 10. 数据库设计概要

核心表（详见后续 `chatchat.sql`）：

| 表 | 关键字段 |
|----|----------|
| user | id, username, password(bcrypt), phone(唯一), avatar, created_at |
| user_device | user_id, device_id, platform, last_login_at |
| friend | user_id, friend_id, status(申请/好友/拉黑), remark |
| group_info | id, name, owner_id, max_member(500), hot_level, status |
| group_member | group_id, user_id, role, joined_at |
| message | id, msg_id(唯一), conversation_id, from, content_type, content, url, msg_seq, created_at（会话+seq 联合唯一索引；content 建 FULLTEXT ngram 索引供消息搜索） |
| inbox_cursor | user_id, conversation_id, max_synced_seq, max_recv_seq |
| group_timeline | group_id, msg_seq, msg_id（热点群读扩散，物理按群分表/分桶） |
| hot_group_stat | group_id, window_ts, msg_count（打标统计） |
| session(会话) | conversation_id, type(单聊/群聊), members |
| file_object | uid, object_key(唯一), size, content_type, status(pending/ready), created_at（文件传输元数据，§8.6） |
| feed_post | uid, text(≤200 字), images(JSON 数组，≤9 项), created_at（朋友圈图文，§9） |

Redis 结构：

| Key | 用途 |
|-----|------|
| `online:{userId}` | 在线状态与所在网关节点 |
| `conv:{conversationId}:seq` | 会话 seq 生成（原子 INCR） |
| `group:{id}:msg:{bucket}` | 热点统计时间桶 |
| `group:{id}:hot` | 热点群标记 |
| `user:{id}:conn:notify` | 弱网降级标记 |

---

## 11. 部署与可观测性

- `deployments/`：docker-compose（MySQL、Redis、Kafka、各服务），k8s manifests 预留
- 网关无状态水平扩展；投递服务按 Kafka consumer group 扩展
- 日志 zap + traceId 贯穿（Frame.header.traceId）
- 指标体系：Prometheus 采集 + Grafana 看板；核心指标：消息 P99 延迟、推送成功率、ACK 超时率、热点群数量、消费 lag、在线连接数、goroutine 数、堆内存
- 运行时治理：GOMEMLIMIT/GOGC 按容器配额调优；下行编码缓冲 sync.Pool 复用；每连接内存预算（send 缓冲 × 连接数）纳入容量规划；旁路 worker 池支持按消费 lag 自适应伸缩

---

## 12. 里程碑

| 阶段 | 内容 | 交付物 |
|------|------|--------|
| M1 骨架 | 目录结构、config、日志、MySQL/Redis/Kafka 接入、用户注册登录(手机号绑定+账号密码) | 可运行服务 + 建表 SQL |
| M2 连接与消息 | WebSocket 网关、protobuf 协议、单聊消息收发（推+ACK）、拉取接口 | 单聊可用 |
| M3 群聊与信箱 | 群管理(500上限)、MQ 广播写信箱、离线拉取、漫游 | 群聊可用 |
| M4 热点群 | 动态打标、热点 topic、读扩散、推送聚合降级 | 热点压测报告 |
| M5 音视频 | gRPC 信令、WebRTC 1v1 通话 | 音视频通话可用 |
| M6 扩展 | media(图片压缩)、bot、feed、pay 契约 | 各扩展服务骨架 |

---

## 13. 风险与取舍

1. **写扩散 vs 读扩散**：普通群写扩散（投递快、拉取简单），热点群读扩散（写压力小、拉取放大）——混合模式为业界常规做法，切换点必须保证 Timeline 与信箱数据一致（先写 Timeline 后打标）
2. **热点阈值**：初始经验值 50 msg/min，需压测校准；打标/摘除有延迟，期间消息可能双通道并存，需以去重保证幂等
3. **消息队列选型**：Kafka 起步（参考 go-chat）；若后续需要更强 TTL/死信语义可评估 NSQ/RocketMQ
4. **支付与银行卡**：涉及合规与加密（PCI-DSS 参考），本期只做契约与加密存储设计
