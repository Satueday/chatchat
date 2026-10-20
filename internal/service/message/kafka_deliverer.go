package message

import (
	"context"
	"fmt"

	"chatchat/internal/model"
	"chatchat/internal/mq"

	"go.uber.org/zap"
)

// KafkaDeliverer 为 Deliverer 的 Kafka 生产实现（W3-M04，裁决 D4；接口与
// fire-and-forget 语义不变——消息已落库后调用，错误不回滚）：
// traceIDFromContext 提取（本包包内私有函数可见的唯一实现位置）→
// mq.Delivery{Message, Members, TraceID} → MarshalDelivery 信封编码 →
// producer.Send(topic=[kafka].topic, Key=convId UTF-8 字节，同会话同分区
// 保序，ADR-0007 D2）。依赖 mq.Producer 接口注入（fake 可离线单测；
// 本包不 import sarama——mq 为接口包）。同步 Send、无后台协程：句柄由
// 装配层（cmd/gateway）持有，Close 入优雅退出链。
//
// 日志脱敏（M03 约束）：仅记 msgId/conv/seq/topic 级元数据，Value（信封
// 含用户 content）不入日志。
type KafkaDeliverer struct {
	producer mq.Producer
	topic    string
	logger   *zap.Logger
}

// 编译期接口实现断言（Deliverer 契约 W2-M03 冻结签名不变）。
var _ Deliverer = (*KafkaDeliverer)(nil)

// NewKafkaDeliverer 构造 Kafka 投递器。p 为 mq.Producer（装配层经
// mq.NewProducer 构造，初始化失败在装配层上抛终止——ADR-0001）；topic
// 取 [kafka].topic（cfg.Kafka.Topic）；logger 为 nil 时以 zap.NewNop 兜底。
func NewKafkaDeliverer(p mq.Producer, topic string, logger *zap.Logger) *KafkaDeliverer {
	if logger == nil {
		logger = zap.NewNop()
	}
	return &KafkaDeliverer{producer: p, topic: topic, logger: logger}
}

// Deliver 实现 Deliverer（CAP-002）：组装投递信封并同步发送到 Kafka。
// 错误原样上抛（fire-and-forget 语义由调用方落实：HandleChat/
// HandleGroupChat 仅 Warn，不回滚落库、不影响成功返回）。重复调用并发
// 安全（producer.Send 自身线程安全，无共享可变状态）。
func (d *KafkaDeliverer) Deliver(ctx context.Context, msg *model.Message, members []uint64) error {
	if msg == nil {
		return fmt.Errorf("kafka deliverer: msg 不得为 nil")
	}
	traceID := traceIDFromContext(ctx)
	value, err := mq.MarshalDelivery(&mq.Delivery{Message: msg, Members: members, TraceID: traceID})
	if err != nil {
		return fmt.Errorf("kafka deliverer: 编码投递信封失败: %w", err)
	}
	if err := d.producer.Send(ctx, &mq.Message{
		Topic: d.topic,
		Key:   []byte(msg.ConversationID), // 分区键 conversationId（同会话保序）
		Value: value,
	}); err != nil {
		return fmt.Errorf("kafka deliverer: 发送投递信封失败: %w", err)
	}
	d.logger.Info("delivery envelope sent",
		zap.String("msgId", msg.MsgID),
		zap.String("conversationId", msg.ConversationID),
		zap.Uint64("msgSeq", msg.MsgSeq),
		zap.Int("memberCount", len(members)),
		zap.String("topic", d.topic))
	return nil
}

// Close 关闭底层 producer（具体类型方法，不入 Deliverer 接口——
// MemoryDeliverer.Close 先例；接口实现零 Close 语义约束）。幂等性由
// mq.Producer.Close 保证（重复调用返回 nil）；由装配层纳入优雅退出链。
func (d *KafkaDeliverer) Close() error {
	if d.producer == nil {
		return nil
	}
	return d.producer.Close()
}
