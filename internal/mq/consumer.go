package mq

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/IBM/sarama"
	"go.uber.org/zap"
)

// consumerGroupAlias 收敛 sarama.ConsumerGroup 接口（可测性注入：离线单测
// fake 实现该接口覆盖外循环全部分支，plan §2 冻结设计）。
type consumerGroupAlias = sarama.ConsumerGroup

// consumer 为 ConsumerGroup 路线消费封装（CAP-002/003，D4）。goroutine
// 属主：Consume 阻塞于调用方；mq 内部不自起常驻协程（sarama 会话内部
// 协程由其自身管理并随 Close 回收）。
type consumer struct {
	cg     consumerGroupAlias
	cfg    Config // 已 normalize
	logger *zap.Logger
	// restartBackoff 为消费组错误后的重启退避间隔（默认 1s；单测注入缩短）。
	restartBackoff time.Duration

	mu     sync.Mutex
	closed bool
}

// 编译期接口实现断言。
var _ Consumer = (*consumer)(nil)

// NewConsumer 构造 Kafka 消费组成员（初始化失败上抛——ADR-0001 硬约束）。
// cfg.Topic/cfg.Group 必填。
func NewConsumer(cfg Config, logger *zap.Logger) (Consumer, error) {
	cfg = cfg.normalize()
	if err := cfg.validateBrokers(); err != nil {
		return nil, err
	}
	if cfg.Topic == "" {
		return nil, errors.New("mq: Config.Topic 不得为空")
	}
	if cfg.Group == "" {
		return nil, errors.New("mq: Config.Group 不得为空")
	}
	cg, err := sarama.NewConsumerGroup(cfg.Brokers, cfg.Group, newSaramaConfig(cfg))
	if err != nil {
		return nil, fmt.Errorf("初始化 Kafka 消费组失败: %w", err)
	}
	return newConsumer(cg, cfg, logger), nil
}

// newConsumer 内部构造（可测性注入：离线单测注入 fake ConsumerGroup）。
func newConsumer(cg consumerGroupAlias, cfg Config, logger *zap.Logger) *consumer {
	if logger == nil {
		logger = zap.NewNop()
	}
	return &consumer{
		cg:             cg,
		cfg:            cfg.normalize(),
		logger:         logger,
		restartBackoff: time.Second,
	}
}

// Consume 阻塞消费（CAP-002/003）：外循环重启会话（rebalance 透明——
// cg.Consume 正常返回即会话结束，立即重启；组错误则退避后重启，plan §6.3）。
// 退出路径：ctx 取消→返回 nil（优雅退出，已 Mark 位移随会话结束提交）；
// Close 后→ErrClosed。handler 错误不上抛 Consume（会话重启消化——重投）。
func (c *consumer) Consume(ctx context.Context, h Handler) error {
	if h == nil {
		return errors.New("mq: Handler 不得为 nil")
	}
	if err := c.checkClosed(); err != nil {
		return err
	}
	for {
		if err := c.checkClosed(); err != nil {
			return err
		}
		err := c.cg.Consume(ctx, []string{c.cfg.Topic}, &groupHandler{
			cfg:     c.cfg,
			logger:  c.logger,
			handler: h,
		})
		if err == nil {
			if ctx.Err() != nil {
				return nil // 优雅退出：位移保留（I04 语义）
			}
			continue // 会话正常结束（rebalance 等）→ 立即重启会话
		}
		if errors.Is(err, sarama.ErrClosedConsumerGroup) {
			return ErrClosed
		}
		if ctx.Err() != nil {
			return nil // ctx 取消引发的错误同样视为优雅退出
		}
		c.logger.Error("kafka 消费会话异常，退避后重启",
			zap.String("topic", c.cfg.Topic),
			zap.String("group", c.cfg.Group),
			zap.Error(err))
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(c.restartBackoff):
		}
	}
}

// Close 关闭底层消费组（幂等语义：sarama 多次 Close 返回 ErrClosedConsumerGroup，
// 归一为 nil）。由装配层纳入优雅退出链。
func (c *consumer) Close() error {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil
	}
	c.closed = true
	c.mu.Unlock()
	if err := c.cg.Close(); err != nil {
		if errors.Is(err, sarama.ErrClosedConsumerGroup) {
			return nil
		}
		return fmt.Errorf("mq: 关闭 Kafka 消费组失败: %w", err)
	}
	return nil
}

// checkClosed 读关闭标志（Close 后 Consume 拒绝，plan §6.3）。
func (c *consumer) checkClosed() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return ErrClosed
	}
	return nil
}

// groupHandler 实现 sarama.ConsumerGroupHandler 三方法（ADR-0001 硬约束：
// ConsumerGroup + ConsumerGroupHandler 路线）。
type groupHandler struct {
	cfg     Config
	logger  *zap.Logger
	handler Handler
}

// groupHandler 编译期断言。
var _ sarama.ConsumerGroupHandler = (*groupHandler)(nil)

// Setup 会话建立（rebalance 完成后）：Info 日志观测（generation/分区数，
// D4——rebalance 可观测性）。
func (h *groupHandler) Setup(s sarama.ConsumerGroupSession) error {
	partitions := 0
	for _, ps := range s.Claims() {
		partitions += len(ps)
	}
	h.logger.Info("kafka 消费会话建立（rebalance 完成）",
		zap.String("group", h.cfg.Group),
		zap.String("topic", h.cfg.Topic),
		zap.Int32("generation", s.GenerationID()),
		zap.Int("partitions", partitions))
	return nil
}

// Cleanup 会话结束（位移提交前）：Info 日志观测。
func (h *groupHandler) Cleanup(_ sarama.ConsumerGroupSession) error {
	h.logger.Info("kafka 消费会话结束",
		zap.String("group", h.cfg.Group),
		zap.String("topic", h.cfg.Topic))
	return nil
}

// ConsumeClaim 逐消息处理 + 手动 commit（plan §6.3 冻结伪代码逐行对应）：
// handler 成功 → MarkMessage（重投窗口最小）；失败 → claim 内立即重试
// ≤RetryMax → 仍失败则 return err（不提交该消息 → 会话结束 → 外循环重启
// → 从已提交 offset 重投——严格至少一次）。无隔离区/毒消息跳过：消息不丢
// 优先于分区畅通，毒消息表现为该分区 lag 停滞 + Error 日志（W4 DLQ 缓解，
// ADR-0007 记录取舍）。日志只记元数据，Value 不入日志。
func (h *groupHandler) ConsumeClaim(sess sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for msg := range claim.Messages() {
		m := toMessage(msg)
		committed := false
		for attempt := 1; attempt <= h.cfg.RetryMax; attempt++ {
			if err := h.handler(sess.Context(), m); err == nil {
				sess.MarkMessage(msg, "")
				committed = true
				break
			}
			h.logger.Error("kafka 消息处理失败，claim 内重试",
				zap.String("topic", m.Topic),
				zap.Int32("partition", m.Partition),
				zap.Int64("offset", m.Offset),
				zap.Int("attempt", attempt),
				zap.Int("retryMax", h.cfg.RetryMax))
		}
		if !committed {
			return fmt.Errorf("mq: 消息处理重试耗尽，不提交待会话重启重投: topic=%s partition=%d offset=%d retryMax=%d",
				m.Topic, m.Partition, m.Offset, h.cfg.RetryMax)
		}
	}
	return nil
}

// toMessage sarama.ConsumerMessage → mq.Message（字段一一透传）。
func toMessage(m *sarama.ConsumerMessage) *Message {
	return &Message{
		Topic:     m.Topic,
		Partition: m.Partition,
		Offset:    m.Offset,
		Key:       m.Key,
		Value:     m.Value,
		Timestamp: m.Timestamp,
	}
}
