// Package mq Kafka 通道封装（W3-M03）：Producer（sarama sync producer）与
// Consumer（ConsumerGroup + ConsumerGroupHandler 三方法路线，禁裸
// ConsumePartition）——对外接口零 sarama 类型泄漏（ADR-0001 硬约束），
// sarama 仅在本包内部出现，且全部经内部构造注入（SyncProducer/
// ConsumerGroup/ConsumerGroupSession/ConsumerGroupClaim/ClusterAdmin 均为
// 接口，离线单测 fake 覆盖全部循环/重试/提交分支）。
//
// 决策档：客户端选型与四硬约束见 docs/adr/0001；分区/命名/commit/重试/
// 幂等语义见 docs/adr/0007（W3-M03-plan §4 D1~D8 同源）。
//
// 本包为纯通道库：不接业务（Deliverer 的 Kafka 实现归 M04、pushpull 业务
// 消费归 M05）；不 import gin/gorm/internal/service。生命周期：句柄由装配层
// 持有、Close 入优雅退出链；Consume 阻塞于调用方 goroutine（属主=调用者），
// mq 内部不自起常驻协程。
//
// 安全：消息体含用户 content——日志只记 topic/partition/offset/msgId 级
// 元数据，Value 不入日志。
package mq

import (
	"context"
	"errors"
	"time"

	"github.com/IBM/sarama"
)

// 命名常量（D2 冻结，入 ADR-0007）。
const (
	// TopicMessage 普通投递 topic：分区键 conversationId（UTF-8 字节，
	// sarama 默认 hash），同会话同分区保序。
	TopicMessage = "chatchat-message"
	// TopicHotGroup 热点群 topic（spec §7.2 冻结名）：W4-M02 接入——gateway
	// 行快照路由生产（分区键 conversationId，与 TopicMessage 同口径）、
	// pushpull 第二消费者组消费写 group_timeline（ADR-0010 D1）。
	TopicHotGroup = "chatchat-hot-group"
	// GroupPushPull pushpull 投递消费组（按组水平扩展，rebalance 透明）。
	GroupPushPull = "chatchat-pushpull"
	// GroupPushPullHot pushpull hot 消费组（W4-M02，ADR-0010 D1）：与
	// GroupPushPull 分离——rebalance/lag 域互不耦合（ADR-0007 范式延续）。
	GroupPushPullHot = "chatchat-pushpull-hot"
)

// Config 零值归一默认（D 冻结：SessionTimeoutMS/RetryMax 零值→10000/3）。
const (
	defaultSessionTimeoutMS = 10000
	defaultRetryMax         = 3
)

// ErrClosed 句柄 Close 后 Send/Consume 的拒绝语义（plan §6.3）。
var ErrClosed = errors.New("mq: 句柄已关闭")

// Message 为通道层消息的统一视图：生产侧填 Topic/Key/Value（Timestamp 可选），
// 消费侧由 broker 回填全字段。
type Message struct {
	Topic     string
	Partition int32
	Offset    int64
	Key       []byte
	Value     []byte
	Timestamp time.Time
}

// Handler 消费回调：返回 nil → 提交该消息（MarkMessage）；返回非 nil →
// claim 内立即重试 ≤RetryMax，仍失败则不提交、会话重启后重投（至少一次，
// plan §6.3 冻结语义）。
type Handler func(ctx context.Context, m *Message) error

// Producer 同步生产者（CAP-001）：Send 阻塞至 ack/错误（sync 语义，错误
// 即时可见）；Close 幂等，入优雅退出链。
type Producer interface {
	Send(ctx context.Context, m *Message) error
	Close() error
}

// Consumer 消费组成员（CAP-002/003）：Consume 阻塞于调用方 goroutine，
// ctx 取消→优雅退出（已处理位移保留）；rebalance 对调用方透明（会话
// 结束自动重启）。
type Consumer interface {
	Consume(ctx context.Context, h Handler) error
	Close() error
}

// Config 为 mq 通道配置（[kafka] 段的通道视图 + 测试覆写项）。
type Config struct {
	Brokers          []string
	Topic            string // 消费的 topic（生产侧由 Message.Topic 指定，Send 时校验非空）
	Group            string
	SessionTimeoutMS int // 0→10000（测试可调低加速 rebalance）
	RetryMax         int // 0→3
}

// normalize 零值归一：返回归一后的副本（不改接收者）。
func (c Config) normalize() Config {
	if c.SessionTimeoutMS <= 0 {
		c.SessionTimeoutMS = defaultSessionTimeoutMS
	}
	if c.RetryMax <= 0 {
		c.RetryMax = defaultRetryMax
	}
	return c
}

// validateBrokers brokers 非空校验（三种构造共用）。
func (c Config) validateBrokers() error {
	if len(c.Brokers) == 0 {
		return errors.New("mq: Config.Brokers 不得为空")
	}
	return nil
}

// newSaramaConfig 构造 sarama 配置（全项目唯一构造点；cfg 须先 normalize）：
//   - Version=V2_8_0_0（D3 安全现代值，KRaft broker 3.9 实测兼容，ADR-0007）；
//   - Net 超时显式设置（外部 I/O 有上界，防启动/收发无界挂起）；
//   - Producer：sync 路线（Return.Successes 必须 true）；幂等生产下 sarama
//     强制 RequiredAcks=WaitForAll + Net.MaxOpenRequests=1（单副本集群上与
//     WaitForLocal 等价——D3 裁决的语义注记，ADR-0007）；Producer.Timeout
//     为单次发送上界；Retry 走 sarama 默认（3）；
//   - Consumer：Offsets.Initial=OffsetOldest（新组从头上消费，kill 期间
//     消息不漏，D4）；Session.Timeout 由 SessionTimeoutMS 覆写。
func newSaramaConfig(cfg Config) *sarama.Config {
	sc := sarama.NewConfig()
	sc.Version = sarama.V2_8_0_0

	sc.Net.DialTimeout = 10 * time.Second
	sc.Net.ReadTimeout = 10 * time.Second
	sc.Net.WriteTimeout = 10 * time.Second

	sc.Producer.RequiredAcks = sarama.WaitForAll
	sc.Producer.Idempotent = true
	sc.Net.MaxOpenRequests = 1
	sc.Producer.Return.Successes = true
	sc.Producer.Timeout = 5 * time.Second

	sc.Consumer.Offsets.Initial = sarama.OffsetOldest
	sc.Consumer.Group.Session.Timeout = time.Duration(cfg.SessionTimeoutMS) * time.Millisecond
	return sc
}
