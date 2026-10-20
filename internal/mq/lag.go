package mq

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/IBM/sarama"
	"go.uber.org/zap"
)

// defaultLagInterval lag 轮询周期缺省（plan D5：30s；调用方可注入覆盖——
// 单测缩短）。查询调用面（sarama admin/client）不接受 ctx，单调用时长由
// newSaramaConfig 的 Net 超时（10s 级）上界。
const defaultLagInterval = 30 * time.Second

// LagWatch 单个消费 lag 监控目标（plan D5：覆盖 message/hot-group 两 topic
// 的 pushpull 两组 + push topic 的 per-gateway 广播组）。
type LagWatch struct {
	Topic string // 监控 topic
	Group string // 消费组（lag 语义=组维度）
	Label string // sink 标签（expvar 变量名派生段，如 message/hot_group/push）
}

// LagAdmin lag 轮询所需的 ClusterAdmin 最小面（sarama.ClusterAdmin 子集，
// fake 可离线注入——本包惯例：sarama 类型只在内部出现）。
type LagAdmin interface {
	DescribeTopics(topics []string) ([]*sarama.TopicMetadata, error)
	ListConsumerGroupOffsets(group string, topicPartitions map[string][]int32) (*sarama.OffsetFetchResponse, error)
}

// OffsetReader 分区末位 offset 读取面（sarama.Client.GetOffset 子集）。
type OffsetReader interface {
	GetOffset(topic string, partition int32, time int64) (int64, error)
}

// LagSink 单目标总 lag 上报回调（label → lag）；实现负责落 expvar 等观测面。
type LagSink func(label string, lag int64)

// ComputeLag 单分区 lag 计算（plan §6 消歧，测试先例产品化）：committed<0
// （组在该分区无提交位移）按 0；end-committed 负值（topic 重建等罕见态）
// 截 0——lag 是观测指标，不为罕见态产出负噪音。
func ComputeLag(committed, end int64) int64 {
	if committed < 0 {
		return 0
	}
	if d := end - committed; d > 0 {
		return d
	}
	return 0
}

// LagPoller 周期轮询消费组 lag（TGT-506，CAP-504）：启动即一轮 → interval
// ticker → ctx.Done 返回。单 goroutine 同步轮询=天然单飞行（上一轮未完成
// 不叠新一轮，plan §6）；单目标查询失败仅 Warn 不中断（下轮自愈——lag 是
// 观测面非正确性数据）。Run 阻塞于调用方 goroutine（属主=调用者，本包
// 惯例）；轮间与逐目标前检查 ctx（sarama 调用无 ctx，尽快响应退出）。
type LagPoller struct {
	admin    LagAdmin
	reader   OffsetReader
	watches  []LagWatch
	interval time.Duration
	sink     LagSink
	logger   *zap.Logger
	closeFn  func() error // sarama 句柄回收（自建构造注入；fake 构造为 nil）
}

// NewLagPoller 构造真实 sarama 后端的 lag 轮询器：创建 ClusterAdmin +
// Client（Net 超时沿用 newSaramaConfig），句柄由 Close 回收（装配层入
// 优雅退出链，Run 返回后调用）；interval≤0 取 30s 缺省。初始化失败上抛
// （ADR-0001 fail-fast）。
func NewLagPoller(brokers []string, watches []LagWatch, interval time.Duration, sink LagSink, logger *zap.Logger) (*LagPoller, error) {
	if interval <= 0 {
		interval = defaultLagInterval
	}
	cfg := newSaramaConfig(Config{Brokers: brokers}.normalize())
	admin, err := sarama.NewClusterAdmin(brokers, cfg)
	if err != nil {
		return nil, fmt.Errorf("mq: 创建 lag ClusterAdmin 失败: %w", err)
	}
	client, err := sarama.NewClient(brokers, cfg)
	if err != nil {
		_ = admin.Close()
		return nil, fmt.Errorf("mq: 创建 lag Client 失败: %w", err)
	}
	return newLagPoller(admin, client, watches, interval, sink, logger, func() error {
		aerr := admin.Close()
		cerr := client.Close()
		return errors.Join(aerr, cerr)
	}), nil
}

// newLagPoller 内部构造（单测注入 fake admin/reader）。
func newLagPoller(admin LagAdmin, reader OffsetReader, watches []LagWatch, interval time.Duration,
	sink LagSink, logger *zap.Logger, closeFn func() error) *LagPoller {
	if logger == nil {
		logger = zap.NewNop()
	}
	if interval <= 0 {
		interval = defaultLagInterval
	}
	return &LagPoller{
		admin:    admin,
		reader:   reader,
		watches:  watches,
		interval: interval,
		sink:     sink,
		logger:   logger,
		closeFn:  closeFn,
	}
}

// Run 阻塞轮询循环（宿主 goroutine 属主 cmd/pushpull 装配层）。
func (p *LagPoller) Run(ctx context.Context) {
	p.pollOnce(ctx)
	ticker := time.NewTicker(p.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			p.pollOnce(ctx)
		}
	}
}

// Close 回收自建的 sarama admin/client 句柄（幂等性由 sarama Close 归一；
// fake 注入构造为 no-op）。须在 Run 返回后调用（退出链序）。
func (p *LagPoller) Close() error {
	if p.closeFn == nil {
		return nil
	}
	return p.closeFn()
}

// pollOnce 单轮：逐 watch 计算组维度总 lag 并 sink；每目标前检查 ctx
// （退出不拖完整轮）。
func (p *LagPoller) pollOnce(ctx context.Context) {
	for _, w := range p.watches {
		select {
		case <-ctx.Done():
			return
		default:
		}
		lag, err := p.watchLag(w)
		if err != nil {
			p.logger.Warn("mq: 消费 lag 查询失败（下轮重试）",
				zap.String("topic", w.Topic),
				zap.String("group", w.Group),
				zap.String("label", w.Label),
				zap.Error(err))
			continue
		}
		if p.sink != nil {
			p.sink(w.Label, lag)
		}
	}
}

// watchLag 计算单 watch 的组维度总 lag：DescribeTopics 取分区清单 →
// ListConsumerGroupOffsets 取已提交位移 → GetOffset(OffsetNewest) 取末位
// → Σ ComputeLag。topic 不存在/无分区/位移块 Err 均为该目标本轮失败
// （Warn 语义，不影响其他目标）。
func (p *LagPoller) watchLag(w LagWatch) (int64, error) {
	metas, err := p.admin.DescribeTopics([]string{w.Topic})
	if err != nil {
		return 0, fmt.Errorf("mq: describe topic %s: %w", w.Topic, err)
	}
	if len(metas) == 0 || len(metas[0].Partitions) == 0 {
		return 0, fmt.Errorf("mq: topic %s 不存在或无分区", w.Topic)
	}
	parts := make([]int32, 0, len(metas[0].Partitions))
	for _, pm := range metas[0].Partitions {
		parts = append(parts, pm.ID)
	}
	resp, err := p.admin.ListConsumerGroupOffsets(w.Group, map[string][]int32{w.Topic: parts})
	if err != nil {
		return 0, fmt.Errorf("mq: list offsets group %s: %w", w.Group, err)
	}
	var total int64
	for _, part := range parts {
		end, err := p.reader.GetOffset(w.Topic, part, sarama.OffsetNewest)
		if err != nil {
			return 0, fmt.Errorf("mq: get end offset %s/%d: %w", w.Topic, part, err)
		}
		block := resp.GetBlock(w.Topic, part)
		if block == nil {
			continue // 该分区无位移记录 → 无 lag（ComputeLag 的 -1→0 消歧同义）
		}
		if block.Err != sarama.ErrNoError {
			return 0, fmt.Errorf("mq: offset fetch block %s/%d err: %w", w.Topic, part, block.Err)
		}
		total += ComputeLag(block.Offset, end)
	}
	return total, nil
}
