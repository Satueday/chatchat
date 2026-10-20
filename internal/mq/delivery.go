package mq

import (
	"errors"
	"fmt"
	"math"
	"strconv"
	"time"

	"chatchat/internal/model"
	"chatchat/pkg/protocol"

	"google.golang.org/protobuf/proto"
)

// Delivery 为投递信封 DeliveryEnvelope（deliver.proto，D1）的高层视图
// （CAP-004）：Logic→pushpull 的消息体（复用冻结 ChatMessage，与 WS 下行
// 帧同构——pushpull 侧组帧零转换）+ 写扩散目标 members + 显式透传 traceId
// （不依赖进程 ctx）。model.Message ↔ ChatMessage 的映射收敛于本文件
// （单一映射点，D6）。W4-M03 D2 增 Kind（信封类型，0=content/1=notify
// 值冻结于 deliver.proto 注释）——gateway push_consumer 分流依据。
type Delivery struct {
	Message *model.Message
	Members []uint64
	TraceID string
	Kind    int32
}

// 信封类型值（W4-M03 D2 冻结，与 deliver.proto 注释一致；W2 冻结面唯一
// 授权增量字段——ADR-0010 增补节登记）。
const (
	// DeliveryKindContent 内容信封（缺省）：既有写扩散/推送语义。
	DeliveryKindContent int32 = 0
	// DeliveryKindNotify 轻通知信封：热点群 Timeline 写成功后的可丢通知
	//（members=在线成员列表，gateway 构 CmdNotify 帧直推不入 pending）。
	DeliveryKindNotify int32 = 1
)

// MarshalDelivery 编码投递信封（CASE-001）：d/d.Message 为 nil 返回 error；
// MsgSeq 超 int64 上限返回 error 非 panic（W3-M03 P3-1 顺延修复，W3-M05
// CASE-008——chatMessageOf 的 uint64→int64 转换对 >math.MaxInt64 的值会
// 产生负数字段，下游 messageOf 按「负 seq 拒绝」回环成为不可解的信封，
// 守卫在编码口拦截）。正常链路 seq 来自 Redis INCR，实测数量级远不可达，
// 该守卫为防御纵深。
func MarshalDelivery(d *Delivery) ([]byte, error) {
	if d == nil || d.Message == nil {
		return nil, errors.New("mq: Delivery 与 Delivery.Message 不得为 nil")
	}
	if d.Message.MsgSeq > math.MaxInt64 {
		return nil, fmt.Errorf("mq: Delivery.Message.MsgSeq=%d 超 int64 上限（ChatMessage.msgSeq 为 int64 字段）", d.Message.MsgSeq)
	}
	b, err := proto.Marshal(&protocol.DeliveryEnvelope{
		Message: chatMessageOf(d.Message),
		Members: d.Members,
		TraceId: d.TraceID,
		Kind:    d.Kind,
	})
	if err != nil {
		return nil, fmt.Errorf("mq: 编码 DeliveryEnvelope 失败: %w", err)
	}
	return b, nil
}

// UnmarshalDelivery 解码投递信封；畸形/截断输入返回 error 非 panic
// （CASE-002，对齐 W2-M01 协议测试先例）：非法 proto 字节、缺 message
// 字段、from/to 非 uid 形态、负 msgSeq 均拒绝。
func UnmarshalDelivery(b []byte) (*Delivery, error) {
	env := &protocol.DeliveryEnvelope{}
	if err := proto.Unmarshal(b, env); err != nil {
		return nil, fmt.Errorf("mq: 解码 DeliveryEnvelope 失败: %w", err)
	}
	msg, err := messageOf(env.Message)
	if err != nil {
		return nil, err
	}
	return &Delivery{Message: msg, Members: env.Members, TraceID: env.TraceId, Kind: env.GetKind()}, nil
}

// chatMessageOf model.Message → ChatMessage（映射口径与
// service/message.chatMessageOf 下行帧一致：from/to uid 字符串化、
// msgSeq uint64→int64、timestamp=CreatedAt.UnixMilli 毫秒；ChatMessage.ext
// 在 model.Message 无对应字段，不映射）。
func chatMessageOf(m *model.Message) *protocol.ChatMessage {
	return &protocol.ChatMessage{
		MsgId:          m.MsgID,
		ConversationId: m.ConversationID,
		From:           strconv.FormatUint(m.FromUID, 10),
		To:             strconv.FormatUint(m.ToUID, 10),
		MsgSeq:         int64(m.MsgSeq),
		Timestamp:      m.CreatedAt.UnixMilli(),
		ContentType:    m.ContentType,
		Content:        m.Content,
		Url:            m.URL,
	}
}

// messageOf ChatMessage → model.Message（反向映射）：from/to 解析 uint64
// （畸形拒绝）、msgSeq 负值拒绝（uint64 不可表示）、timestamp 毫秒→UTC。
func messageOf(cm *protocol.ChatMessage) (*model.Message, error) {
	if cm == nil {
		return nil, fmt.Errorf("mq: 信封缺少 message 字段")
	}
	from, err := strconv.ParseUint(cm.GetFrom(), 10, 64)
	if err != nil {
		return nil, fmt.Errorf("mq: 信封 from=%q 非法 uid: %w", cm.GetFrom(), err)
	}
	to, err := strconv.ParseUint(cm.GetTo(), 10, 64)
	if err != nil {
		return nil, fmt.Errorf("mq: 信封 to=%q 非法 uid: %w", cm.GetTo(), err)
	}
	if cm.GetMsgSeq() < 0 {
		return nil, fmt.Errorf("mq: 信封 msgSeq=%d 为负，无法映射 uint64", cm.GetMsgSeq())
	}
	return &model.Message{
		MsgID:          cm.GetMsgId(),
		ConversationID: cm.GetConversationId(),
		FromUID:        from,
		ToUID:          to,
		ContentType:    cm.GetContentType(),
		Content:        cm.GetContent(),
		URL:            cm.GetUrl(),
		MsgSeq:         uint64(cm.GetMsgSeq()),
		CreatedAt:      time.UnixMilli(cm.GetTimestamp()).UTC(),
	}, nil
}
