package message

import (
	"context"
	"fmt"

	"chatchat/internal/model"
	"chatchat/internal/mq"
	"chatchat/pkg/protocol"

	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
)

// deliverPush 为推送投递执行点（包级变量仅为单测注入错误路径保留的替换
// 缝——生产路径唯一实现 MemoryDeliverer.Deliver；正常链路 Deliver 的唯一
// 错误面是下行帧 proto 序列化失败，经真信封不可构造）。
var deliverPush = (*MemoryDeliverer).Deliver

// NewPushHandler 构造 gateway 推送 topic 消费 handler（W3-M05 裁决 D4，
// CAP-003）：消费 pushpull 发布的**在线列表推送信封**（members=在线成员
// 列表——W3-M05-C2 合并推送形态；移交 8 注释修正：非「单成员信封」，
// 单成员仅为 members 长度 1 的特例），复用 DeliveryEnvelope 零新 proto，
// 经 withTraceID 注入信封 traceId（包私有函数——本包为唯一可见位置）→
// MemoryDeliverer.Deliver 逐成员投递，完整复用其 pending/Push/重推/ACK
// 机制与 ADR-0005 [deliver] 参数。
//
// W4-M03 kind 分流（plan D2，CAP-201/207）：信封 kind=1（轻通知）→ 构
// **CmdNotify 帧**（payload=PullResponse{conversationId, maxSeq}——D1 冻结，
// message 携 conv/seq 锚）→ 逐成员 Pusher.Push **直推**：不入 pending、
// 无 ACK 期待、不触 MemoryDeliverer 与游标（可丢语义——通知≠接收，双游标
// 均不动，D5；kind 判别在进 pending 引擎之前，spec §7.2 对 §6.2 通用 ACK
// 流程的专项覆盖关系入 ADR-0010 增补）；kind=0（content 缺省）→ 既有
// pending/ACK/重推引擎零改动。push 为通知分支直推通道（与 md 同源装配，
// 二者职责隔离——md 仅 content 分支触达）。
//
// 错误语义冻结（D9）：解码失败=确定性毒消息 → Error 日志（topic/partition/
// offset）+ return nil 跳过（与 pushpull.Handle 口径一致，防分区停滞）；
// 推送阶段错误一律 Warn + return nil 提交——推送是优化路径、信箱为事实源
// （gotify「先落库后 Notify」同构），防推送故障停滞分区；离线语义（Push
// false/离线终态）由 MemoryDeliverer 内部 Warn/重推状态机自行消化。
//
// 广播组消费（Group=chatchat-gw-{gatewayID}，装配层派生）：每网关收全量
// push 项，Deliver 内 Pusher.Online 为本机判定——非本网关成员自然零副作用
// （多网关演进就绪，ADR-0009）；消费 goroutine 属主为装配层（cmd/gateway）。
func NewPushHandler(md *MemoryDeliverer, push Pusher, logger *zap.Logger) mq.Handler {
	if logger == nil {
		logger = zap.NewNop()
	}
	log := logger
	return func(ctx context.Context, m *mq.Message) error {
		if m == nil {
			return nil // mq 层保证非 nil；防御性跳过
		}
		d, err := mq.UnmarshalDelivery(m.Value)
		if err != nil {
			log.Error("push consumer: 推送信封解码失败，跳过（确定性毒消息）",
				zap.String("topic", m.Topic),
				zap.Int32("partition", m.Partition),
				zap.Int64("offset", m.Offset),
				zap.Error(err))
			return nil
		}
		// 轻通知分支（W4-M03 D2）：kind=1 → CmdNotify 帧直推，与 pending/
		// ACK/重推引擎完全隔离（判别在进 pending 之前）。
		if d.Kind == mq.DeliveryKindNotify {
			delivered := pushNotifyFrame(push, d, log)
			log.Info("push consumer: 轻通知已直推（不入 pending/ACK 引擎）",
				zap.String("traceId", d.TraceID),
				zap.String("msgId", d.Message.MsgID),
				zap.String("conversationId", d.Message.ConversationID),
				zap.Uint64("maxSeq", d.Message.MsgSeq),
				zap.Int("memberCount", len(d.Members)),
				zap.Int("delivered", delivered))
			return nil
		}
		// 信封 traceId 注入 ctx：Deliver → traceIDFromContext 读取并写入下行帧
		// Header（上行帧 traceId 跨进程贯穿的唯一衔接点，CAP-006）。
		ctx = withTraceID(ctx, d.TraceID)
		if err := deliverPush(md, ctx, d.Message, d.Members); err != nil {
			log.Warn("push consumer: 下行推送失败，提交（推送为优化路径，信箱为事实源）",
				zap.String("msgId", d.Message.MsgID),
				zap.String("conversationId", d.Message.ConversationID),
				zap.Uint64("msgSeq", d.Message.MsgSeq),
				zap.Uint64s("members", d.Members),
				zap.String("traceId", d.TraceID),
				zap.Error(err))
			return nil
		}
		log.Info("push consumer: 推送信封已投递推送引擎",
			zap.String("traceId", d.TraceID),
			zap.String("msgId", d.Message.MsgID),
			zap.String("conversationId", d.Message.ConversationID),
			zap.Uint64("msgSeq", d.Message.MsgSeq),
			zap.Int("memberCount", len(d.Members)))
		return nil
	}
}

// pushNotifyFrame 轻通知分支执行体（W4-M03 D1/D2，CAP-201/207）：构
// CmdNotify 帧（payload=PullResponse{conversationId, maxSeq=信封 msgSeq}，
// messages 空/hasMore=false——D1 冻结；Header.TraceId 回显信封值）→
// 逐成员 Pusher.Push 非阻塞直推（false=离线竞态/慢消费者，容忍——拉取
// 兜底，D11 同口径）。返回成功直推数（可观测）。
func pushNotifyFrame(push Pusher, d *mq.Delivery, logger *zap.Logger) int {
	frame, err := buildNotifyFrame(d.Message, d.TraceID)
	if err != nil {
		// proto 序列化失败=帧构造缺陷（正常链路不可达），Error 提交跳过。
		logger.Error("push consumer: 轻通知帧构造失败，跳过（确定性）",
			zap.String("msgId", d.Message.MsgID),
			zap.String("conversationId", d.Message.ConversationID),
			zap.Uint64("msgSeq", d.Message.MsgSeq),
			zap.String("traceId", d.TraceID),
			zap.Error(err))
		return 0
	}
	delivered := 0
	for _, uid := range d.Members {
		if push.Push(uid, frame) {
			delivered++
		} else {
			logger.Debug("push consumer: 轻通知直推未达（离线竞态，拉取兜底）",
				zap.Uint64("uid", uid),
				zap.String("conversationId", d.Message.ConversationID),
				zap.Uint64("maxSeq", d.Message.MsgSeq),
				zap.String("traceId", d.TraceID))
		}
	}
	return delivered
}

// buildNotifyFrame 组 CmdNotify 下行帧（W4-M03 D1 冻结核）：payload=
// PullResponse{conversationId, maxSeq}（messages 空、hasMore=false——轻
// 通知仅「该会话有新消息至 maxSeq」的信号，内容经拉取获得）；Header
// {Cmd: CmdNotify, TraceId 回显}（CASE-I24 traceId 串联面）。
func buildNotifyFrame(msg *model.Message, traceID string) ([]byte, error) {
	payload, err := proto.Marshal(&protocol.PullResponse{
		ConversationId: msg.ConversationID,
		MaxSeq:         int64(msg.MsgSeq),
	})
	if err != nil {
		return nil, fmt.Errorf("marshal PullResponse: %w", err)
	}
	frame := &protocol.Frame{
		Header:  &protocol.Header{Cmd: protocol.CmdNotify, TraceId: traceID},
		Payload: payload,
	}
	b, err := proto.Marshal(frame)
	if err != nil {
		return nil, fmt.Errorf("marshal Frame: %w", err)
	}
	return b, nil
}
