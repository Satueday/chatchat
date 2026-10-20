package message

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"time"

	"chatchat/internal/dao"
	"chatchat/internal/model"
	pkgerrors "chatchat/pkg/errors"
	"chatchat/pkg/protocol"

	"go.uber.org/zap"
)

// Deliverer 为消息投递契约（W2-M03 冻结；plan.md §4：位置 internal/service/message、
// 被依赖方 logic、W2 内存实现 / W3 Kafka 实现，接口不变）。
//
// 语义：fire-and-forget——在消息**已成功落库后**调用；实现方对错误自行
// 记录/重试（M04 内存实现 + ACK 重推；W3 Kafka 至少一次），调用方不因
// 投递失败回滚或报错（落库已成事实，投递失败=离线路径，由拉取兜底）。
type Deliverer interface {
	// Deliver 向会话成员投递已落库消息。
	Deliver(ctx context.Context, msg *model.Message, members []uint64) error
}

// Pusher 为网关下行通道窄接口（W2-M04 裁决 D5）：由装配层适配
// server.ConnManager（两阶段指针注入），service/message 不 import
// internal/server。
type Pusher interface {
	// Push 向 uid 投递下行帧字节：false=离线或 send 缓冲满被踢（非阻塞）。
	Push(uid uint64, data []byte) bool
	// Online 查询 uid 是否本机在线。
	Online(uid uint64) bool
}

// AckHandler 为网关 CmdAck 帧入口（W2-M04）：校验 ACK 载荷（D1：
// PullRequest{conversationId, afterSeq}）→ 取消 pending → 推进收件游标。
// uid 一律取鉴权连接（不自报）。
type AckHandler interface {
	// HandleAck 处理接收方 ACK 帧；返回 *pkgerrors.Error 由装配层回 CmdError 帧
	// （游标更新失败不回帧，仅日志——D6）。
	HandleAck(ctx context.Context, uid uint64, frame *protocol.Frame) *pkgerrors.Error
}

// DeliveryOptions 为内存投递重推参数（W2-M04 裁决 D3/D10，对齐 config [deliver] 段）
// + 群 ACK 成员判定依赖（W3-M05 裁决 D5）。
type DeliveryOptions struct {
	RetryBase time.Duration // 首次等待与退避基数（retry_base_ms）；间隔 wait(i)=base×2^(i-1)
	RetryMax  int           // 重推次数上限（retry_max）；另含首发共最多 1+RetryMax 次 Push
	// GroupMembers 群成员 repo（群聊 ACK g_ 会话成员判定，W3-M05）：nil 时
	// g_ 一律 1001 冻结口径（PullOptions.GroupMembers 先例——漏注入安全失败
	// 而非放行）；单聊 ACK 不经此依赖。装配层（cmd/gateway）必须注入。
	GroupMembers dao.GroupMemberRepo
}

// NoopDeliverer 为 W2-M03 占位实现：仅记日志（真实投递归 W2-M04 内存
// Deliverer / W3 Kafka Producer，装配处一行替换）。
type NoopDeliverer struct {
	Logger *zap.Logger
}

// Deliver 实现 Deliverer：Info 日志记录待投递消息元数据（不含 content 全文）。
func (d NoopDeliverer) Deliver(_ context.Context, msg *model.Message, members []uint64) error {
	if d.Logger != nil {
		d.Logger.Info("deliver(noop)",
			zap.String("msgId", msg.MsgID),
			zap.String("conversationId", msg.ConversationID),
			zap.Uint64("msgSeq", msg.MsgSeq),
			zap.Uint64s("members", members))
	}
	return nil
}

// ctxKey 为包内私有 context key 类型（不导出，防与其他包 key 冲突）。
type ctxKey int

// traceIDKey 携带上行帧 traceId（裁决 D9：Service 写入、Deliverer 读取）。
const traceIDKey ctxKey = 0

// withTraceID 将上行帧 traceId 注入 ctx（HandleChat 调用投递前写入）。
func withTraceID(ctx context.Context, traceID string) context.Context {
	return context.WithValue(ctx, traceIDKey, traceID)
}

// traceIDFromContext 读取注入的 traceId；缺失（或空值）时本地生成 16 hex
// （包内自备，不 import internal/server.newTraceID，D9）。
func traceIDFromContext(ctx context.Context) string {
	if v, ok := ctx.Value(traceIDKey).(string); ok && v != "" {
		return v
	}
	return newTraceID16()
}

// newTraceID16 生成 16 hex 字符 traceId；crypto/rand 失败以纳秒时间戳兜底
// （同为 16 hex，W2-M02 newTraceID 同构先例）。
func newTraceID16() string {
	b := make([]byte, 8)
	if _, err := rand.Read(b); err != nil {
		return fmt.Sprintf("%016x", time.Now().UnixNano())
	}
	return hex.EncodeToString(b)
}
