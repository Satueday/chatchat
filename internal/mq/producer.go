package mq

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/IBM/sarama"
	"go.uber.org/zap"
)

// producer 为 sarama sync producer 封装（CAP-001，D3）：同步发送——
// SendMessage 阻塞至 ack/错误（sync 路线天然满足 ADR-0001「producer 必须
// 消费 Errors() 通道」——该约束针对裸 async 用法，sync 内部自消化确认/
// 错误通道，ADR-0007 澄清）。并发安全：Send 可多 goroutine 并发（sarama
// SyncProducer 自身线程安全）；Close 幂等（重复调用返回 nil）。
type producer struct {
	sp     sarama.SyncProducer
	logger *zap.Logger

	mu     sync.Mutex
	closed bool
}

// 编译期接口实现断言。
var _ Producer = (*producer)(nil)

// NewProducer 构造 Kafka 同步生产者（初始化失败上抛——ADR-0001 硬约束，
// 禁止降级为只记日志继续跑）。cfg.Topic 不消费（生产侧 topic 由
// Message.Topic 逐条指定，Send 时校验非空）。
func NewProducer(cfg Config, logger *zap.Logger) (Producer, error) {
	cfg = cfg.normalize()
	if err := cfg.validateBrokers(); err != nil {
		return nil, err
	}
	sp, err := sarama.NewSyncProducer(cfg.Brokers, newSaramaConfig(cfg))
	if err != nil {
		return nil, fmt.Errorf("初始化 Kafka producer 失败: %w", err)
	}
	return newProducer(sp, logger), nil
}

// newProducer 内部构造（可测性注入：离线单测注入 fake SyncProducer）。
func newProducer(sp sarama.SyncProducer, logger *zap.Logger) *producer {
	if logger == nil {
		logger = zap.NewNop()
	}
	return &producer{sp: sp, logger: logger}
}

// Send 同步发送一条消息（CAP-001）：ctx 入口检查（发送本身由
// Producer.Timeout 超时约束，不随 ctx 中断在途请求——D3）；m.Topic 必填；
// 错误返回调用方（D8：M04 装配层记 warn）。日志只记 topic/partition/offset
// 元数据，Value 不入日志。
func (p *producer) Send(ctx context.Context, m *Message) error {
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("mq: send 入口 ctx 已结束: %w", err)
	}
	if m == nil {
		return errors.New("mq: Message 不得为 nil")
	}
	if m.Topic == "" {
		return errors.New("mq: Message.Topic 不得为空（生产侧 topic 由消息指定）")
	}
	p.mu.Lock()
	closed := p.closed
	p.mu.Unlock()
	if closed {
		return ErrClosed
	}

	pm := &sarama.ProducerMessage{
		Topic: m.Topic,
		Key:   sarama.ByteEncoder(m.Key),
		Value: sarama.ByteEncoder(m.Value),
	}
	if !m.Timestamp.IsZero() {
		pm.Timestamp = m.Timestamp
	}
	partition, offset, err := p.sp.SendMessage(pm)
	if err != nil {
		p.logger.Error("kafka 发送失败",
			zap.String("topic", m.Topic),
			zap.Error(err))
		return fmt.Errorf("mq: 发送 Kafka 消息失败: %w", err)
	}
	p.logger.Debug("kafka 发送成功",
		zap.String("topic", m.Topic),
		zap.Int32("partition", partition),
		zap.Int64("offset", offset))
	return nil
}

// Close 关闭底层 producer（幂等：重复调用返回 nil 且不重复关闭）。
// 由装配层纳入优雅退出链（ADR-0001）。
func (p *producer) Close() error {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil
	}
	p.closed = true
	p.mu.Unlock()
	if err := p.sp.Close(); err != nil {
		return fmt.Errorf("mq: 关闭 Kafka producer 失败: %w", err)
	}
	return nil
}
