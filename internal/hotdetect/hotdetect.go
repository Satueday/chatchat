package hotdetect

import (
	"context"
	"time"

	"chatchat/config"

	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
)

// HotDetector 热点状态查询契约（W4-M01 冻结，M02 路由分流的消费面）。
type HotDetector interface {
	// IsHot 判定群是否处于 hot 标记态：Redis group:{id}:hot 优先，miss/
	// 错误回源 MySQL group_info.hot_level（>0 即 hot）；双源均败返回
	// 6003 哨兵错误（供 M02 降级判定，本模块不替 M02 决定降级行为）。
	IsHot(ctx context.Context, groupID uint64) (bool, error)
}

// Recorder 群消息计数入口契约（gateway/logic 埋点消费面）。message 包
// 自定义 GroupMsgCounter 窄接口结构化满足（ChatGate/Deliverer 先例），
// 消费方不 import 本包；本接口供装配层与进程内直用。
type Recorder interface {
	// RecordGroupMsg 对一条已落库成功的群消息计数：单 pipeline
	// INCRBY 桶键 + EXPIRE 刷新 + ZADD recent（单往返，D1/D2）。
	// 失败原样上抛（基础设施错误如实传播，不打业务码），由调用方
	// Warn 不阻断发送（消息已落库即事实，下周期聚合自愈）。
	RecordGroupMsg(ctx context.Context, groupID uint64) error
}

// options 为 config.HotDetect 的内部换算形态（校验前置：范围与对齐约束
// 由 config.validate fail-fast 把关，本转换只做算术派生，D10/D1/D4）：
// windowBuckets/periodBuckets 由整除约束保证无余数；bucketTTL/hotTTL/
// streakTTL 为派生护栏（键自愈上限）。
type options struct {
	thresholdMsgPerMin int64         // 打标阈值（msg/min，窗口均值 ≥ 判 hot 含等号）
	interval           time.Duration // 聚合周期（Run ticker + stat 窗口粒度）
	window             time.Duration // 滑动窗口（rate 分母）
	bucketSec          int64         // 桶宽秒数（桶号 = unixSec / bucketSec）
	lowPeriods         int64         // 摘标所需连续低周期数 N
	windowBuckets      int64         // window/bucketSec（MGET 键数，≤3600 由校验护栏）
	periodBuckets      int64         // interval/bucketSec（stat 周期桶数）
	bucketTTL          time.Duration // 桶键 TTL = 窗口 + 2×桶宽（D1）
	hotTTL             time.Duration // hot 键 TTL = 窗口 + 2×周期（D4）
	streakTTL          time.Duration // streak 键 TTL = N×周期×3 护栏（D4）
}

// newOptions 换算内部参数（前置校验归 config.validate：window*60 与
// interval 均为 bucketSec 整数倍、窗口桶数 ≤3600——违反时派生字段失真，
// 由启动 fail-fast 拦在转换之前）。
func newOptions(cfg config.HotDetect) options {
	window := time.Duration(cfg.WindowMin) * time.Minute
	interval := time.Duration(cfg.AggregateIntervalSec) * time.Second
	bucket := time.Duration(cfg.BucketSec) * time.Second
	n := int64(cfg.LowPeriodsToUnmark)
	return options{
		thresholdMsgPerMin: int64(cfg.ThresholdMsgPerMin),
		interval:           interval,
		window:             window,
		bucketSec:          int64(cfg.BucketSec),
		lowPeriods:         n,
		windowBuckets:      int64(window / bucket),
		periodBuckets:      int64(interval / bucket),
		bucketTTL:          window + 2*bucket,
		hotTTL:             window + 2*interval,
		streakTTL:          time.Duration(n) * interval * 3,
	}
}

// newBucketStore 构造私有桶存取器并绑定时钟（server-master 三层范式：
// 构造注入 now、方法传时间参数；默认 time.Now，单测注入 fake 时钟）。
func newBucketStore(rdb redis.UniversalClient, opts options, now func() time.Time) *bucketStore {
	return &bucketStore{rdb: rdb, opts: opts, now: now}
}

// NewRecorder 构造群消息计数器（gateway/logic 埋点装配用；轻量：仅 Redis
// 依赖，D7）。返回 Recorder 接口（dao.NewRedisPendMarker 先例）；rdb 由
// 装配层创建与关闭。
func NewRecorder(rdb redis.UniversalClient, cfg config.HotDetect, logger *zap.Logger) Recorder {
	return newRecorder(newBucketStore(rdb, newOptions(cfg), time.Now), logger)
}
