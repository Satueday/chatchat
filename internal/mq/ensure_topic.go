package mq

import (
	"errors"
	"fmt"
	"strings"

	"github.com/IBM/sarama"
	"go.uber.org/zap"
)

// EnsureTopic 幂等建 topic（CAP-005，D5）：ClusterAdmin 查询——不存在→
// CreateTopic(partitions, replication=1)；已存在→nil（分区数变更/删除
// 不支持）；查询/创建失败上抛，唯「已存在」类错误（并发建冲突）归 nil。
// 测试与部署共用（生产建议 6 分区及创建命令见 ADR-0007；部署期创建归
// W4 编排）。replication=1 为单节点不变量（compose 无多副本）。
func EnsureTopic(cfg Config, partitions int32, logger *zap.Logger) error {
	cfg = cfg.normalize()
	if err := cfg.validateBrokers(); err != nil {
		return err
	}
	if cfg.Topic == "" {
		return errors.New("mq: Config.Topic 不得为空")
	}
	if partitions < 1 {
		return fmt.Errorf("mq: partitions=%d 必须为正", partitions)
	}
	ca, err := sarama.NewClusterAdmin(cfg.Brokers, newSaramaConfig(cfg))
	if err != nil {
		return fmt.Errorf("初始化 Kafka ClusterAdmin 失败: %w", err)
	}
	defer func() { _ = ca.Close() }()
	return ensureTopicWith(ca, cfg.Topic, partitions, logger)
}

// ensureTopicWith 可注入实现（fake ClusterAdmin 单测，CASE-007）。
func ensureTopicWith(ca sarama.ClusterAdmin, topic string, partitions int32, logger *zap.Logger) error {
	topics, err := ca.ListTopics()
	if err != nil {
		return fmt.Errorf("mq: 查询 topic 列表失败: %w", err)
	}
	if _, exists := topics[topic]; exists {
		return nil // 已存在幂等
	}
	if err := ca.CreateTopic(topic, &sarama.TopicDetail{
		NumPartitions:     partitions,
		ReplicationFactor: 1,
	}, false); err != nil {
		if isTopicExistsErr(err) {
			return nil // 并发建冲突：已存在类错误归幂等成功（D5）
		}
		return fmt.Errorf("mq: 创建 topic %s（%d 分区）失败: %w", topic, partitions, err)
	}
	if logger != nil {
		logger.Info("kafka topic 已创建",
			zap.String("topic", topic),
			zap.Int32("partitions", partitions))
	}
	return nil
}

// isTopicExistsErr 判「topic 已存在」类错误：*sarama.TopicError 的错误码
// 匹配 ErrTopicAlreadyExists；其余按文案兜底（broker 错误串可能不透传
// 结构化码，大小写不敏感，双路径防漏判）。
func isTopicExistsErr(err error) bool {
	var te *sarama.TopicError
	if errors.As(err, &te) {
		return te.Err == sarama.ErrTopicAlreadyExists
	}
	return strings.Contains(strings.ToLower(err.Error()), "already exists")
}
