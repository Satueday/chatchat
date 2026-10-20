package hotdetect

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

// 键契约（spec §10 原样，W4-M01 D1/D4）——全私有，禁导出（桶结构不泄漏）：
//
//	group:{id}:msg:{bucket}  桶计数（INCRBY，bucket=unixSec/bucketSec 全局
//	                         对齐——无 epoch 漂移；TTL=窗口+2×桶宽，每次
//	                         INCRBY 同 pipeline 刷新）
//	group:{id}:hot           hot 标记（值固定 "1" 存在性语义；TTL=窗口+2×
//	                         周期，每周期重评仍 hot 时 SET 刷新）
//	group:{id}:hot:low       摘标 streak 计数（INCR/DEL，TTL=N×周期×3 护栏
//	                         ——跨聚合实例与重启可见，为 M05 多实例预留）
//	group:msg:recent         活跃群注册表 ZSET（member=groupId，score=最后
//	                         消息 unix 秒；聚合轮按 score 窗口取候选并裁剪）
const (
	recentZSetKey = "group:msg:recent"
	hotValue      = "1"
)

// bucketKey 桶计数键 group:{id}:msg:{bucket}。
func bucketKey(groupID uint64, bucket int64) string {
	return fmt.Sprintf("group:%d:msg:%d", groupID, bucket)
}

// hotKey hot 标记键 group:{id}:hot。
func hotKey(groupID uint64) string {
	return fmt.Sprintf("group:%d:hot", groupID)
}

// lowStreakKey 摘标 streak 计数键 group:{id}:hot:low。
func lowStreakKey(groupID uint64) string {
	return fmt.Sprintf("group:%d:hot:low", groupID)
}

// bucketStore 私有桶存取器：Recorder（写）与 Detector（读/判定）共用的
// Redis 键操作集合。持有客户端句柄与派生参数，无业务状态。
type bucketStore struct {
	rdb  redis.UniversalClient
	opts options
	now  func() time.Time // 构造注入（默认 time.Now，单测 fake 时钟）
}

// bucketOf 时间 → 桶号（unixSec/bucketSec 整除全局对齐，D1）。
func (s *bucketStore) bucketOf(t time.Time) int64 {
	return t.Unix() / s.opts.bucketSec
}

// incr 对一条群消息计数（CAP-001）：单 pipeline 三命令一往返——INCRBY
// 桶键 + EXPIRE 桶键（TTL 刷新）+ ZADD recent（score=最后消息 unix 秒，
// 活跃群发现的数据源）。pipeline 命令级错误在 Exec 聚合返回。
func (s *bucketStore) incr(ctx context.Context, groupID uint64, now time.Time) error {
	b := s.bucketOf(now)
	key := bucketKey(groupID, b)
	pipe := s.rdb.Pipeline()
	pipe.IncrBy(ctx, key, 1)
	pipe.Expire(ctx, key, s.opts.bucketTTL)
	pipe.ZAdd(ctx, recentZSetKey, redis.Z{Score: float64(now.Unix()), Member: strconv.FormatUint(groupID, 10)})
	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("hotdetect incr pipeline (group %d bucket %d): %w", groupID, b, err)
	}
	return nil
}

// mgetSum 对确定桶序列 [first, last]（闭区间）MGET 求和，nil 视为 0
// （键过期/未写过=零计数）。单节点 Redis 约束：MGET 要求键同节点——
// W3-M05-C2 OnlineMany 同先例注释登记，W4 单节点满足；引入 Cluster 须
// 改 pipeline 形态或 hash-tag。
func (s *bucketStore) mgetSum(ctx context.Context, groupID uint64, first, last int64) (int64, error) {
	keys := make([]string, 0, last-first+1)
	for b := first; b <= last; b++ {
		keys = append(keys, bucketKey(groupID, b))
	}
	vals, err := s.rdb.MGet(ctx, keys...).Result()
	if err != nil {
		return 0, fmt.Errorf("hotdetect mget buckets (group %d [%d,%d]): %w", groupID, first, last, err)
	}
	var sum int64
	for i, v := range vals {
		if v == nil {
			continue // 键缺失=0
		}
		n, convErr := strconv.ParseInt(fmt.Sprint(v), 10, 64)
		if convErr != nil {
			return 0, fmt.Errorf("hotdetect bucket value %q (group %d bucket %d): %w", v, groupID, first+int64(i), convErr)
		}
		sum += n
	}
	return sum, nil
}

// windowSum 滑动窗口内计数和（CAP-002）：lastBucket=now/bucket 含当前未满
// 桶（向上偏保守——打标宁可早不可漏，D1 冻结口径）；键数=windowBuckets
// （≤3600，校验护栏）。
func (s *bucketStore) windowSum(ctx context.Context, groupID uint64, now time.Time) (int64, error) {
	last := s.bucketOf(now)
	return s.mgetSum(ctx, groupID, last-s.opts.windowBuckets+1, last)
}

// periodSum 最近一个已完成周期的桶和（CAP-006 stat 数据面）：周期
// [lastBucket-periodBuckets, lastBucket) 对齐桶边界（周期为桶宽整数倍由
// 校验保证，跨周期不相交，D9）；windowTS=该周期起点。
func (s *bucketStore) periodSum(ctx context.Context, groupID uint64, now time.Time) (int64, error) {
	end := s.bucketOf(now) // end 桶为当前进行中桶，不计入（左闭右开 [end-n, end)）
	return s.mgetSum(ctx, groupID, end-s.opts.periodBuckets, end-1)
}

// periodWindowTS 最近一个已完成周期的起点（UTC，对齐桶边界）。
func (s *bucketStore) periodWindowTS(now time.Time) time.Time {
	end := s.bucketOf(now)
	return time.Unix((end-s.opts.periodBuckets)*s.opts.bucketSec, 0).UTC()
}

// candidates 活跃群发现（CAP-002）：ZRANGEBYSCORE recent [now-窗口-2×周期,
// +inf]（min 含边界，"("开区间语法不用——D1）；非法 member（历史脏数据）
// 跳过不阻断整轮。注册表裁剪归 pruneRecent（独立调用，失败不伤本轮候选）。
func (s *bucketStore) candidates(ctx context.Context, now time.Time) ([]uint64, error) {
	minSec := now.Unix() - int64(s.opts.window/time.Second) - int64(2*s.opts.interval/time.Second)
	members, err := s.rdb.ZRangeByScore(ctx, recentZSetKey, &redis.ZRangeBy{
		Min: strconv.FormatInt(minSec, 10), Max: "+inf",
	}).Result()
	if err != nil {
		return nil, fmt.Errorf("hotdetect zrangebyscore %s: %w", recentZSetKey, err)
	}
	ids := make([]uint64, 0, len(members))
	for _, m := range members {
		id, convErr := strconv.ParseUint(m, 10, 64)
		if convErr != nil {
			continue // 非法 member（历史脏数据）跳过，不阻断整轮
		}
		ids = append(ids, id)
	}
	return ids, nil
}

// pruneRecent 裁剪注册表：删除 score < 候选下界的成员（严格小于——保留
// 边界成员，与 candidates 集合一致）。失败仅影响注册表大小（下轮重试
// 自愈），由调用方记日志，不影响本轮。
func (s *bucketStore) pruneRecent(ctx context.Context, now time.Time) error {
	minSec := now.Unix() - int64(s.opts.window/time.Second) - int64(2*s.opts.interval/time.Second)
	if err := s.rdb.ZRemRangeByScore(ctx, recentZSetKey, "-inf",
		fmt.Sprintf("(%d", minSec)).Err(); err != nil {
		return fmt.Errorf("hotdetect zremrangebyscore %s: %w", recentZSetKey, err)
	}
	return nil
}

// setHot 写 hot 标记键（值 "1" 存在性语义；EX=hotTTL，每周期重评仍 hot
// 时刷新，TTL 丢失由 IsHot 回源自愈，D4）。
func (s *bucketStore) setHot(ctx context.Context, groupID uint64) error {
	if err := s.rdb.Set(ctx, hotKey(groupID), hotValue, s.opts.hotTTL).Err(); err != nil {
		return fmt.Errorf("hotdetect set %s: %w", hotKey(groupID), err)
	}
	return nil
}

// hotExists 读 hot 标记键（不存在返回 redis.Nil，由调用方分流回源）。
func (s *bucketStore) hotExists(ctx context.Context, groupID uint64) (bool, error) {
	v, err := s.rdb.Get(ctx, hotKey(groupID)).Result()
	if err != nil {
		return false, err // 含 redis.Nil：调用方按 miss 分流
	}
	return v == hotValue, nil
}

// delHot 删除 hot 标记键（幂等：键不存在亦成功）。
func (s *bucketStore) delHot(ctx context.Context, groupID uint64) error {
	if err := s.rdb.Del(ctx, hotKey(groupID)).Err(); err != nil {
		return fmt.Errorf("hotdetect del %s: %w", hotKey(groupID), err)
	}
	return nil
}

// incrStreak streak 计数 +1 并刷新护栏 TTL（D4：跨周期累计、TTL=N×周期×3
// ——连续低周期不可能超 N（≥N 即摘标 DEL），TTL 兜底进程停摆期间的残留）。
func (s *bucketStore) incrStreak(ctx context.Context, groupID uint64) (int64, error) {
	key := lowStreakKey(groupID)
	pipe := s.rdb.Pipeline()
	incr := pipe.Incr(ctx, key)
	pipe.Expire(ctx, key, s.opts.streakTTL)
	if _, err := pipe.Exec(ctx); err != nil {
		return 0, fmt.Errorf("hotdetect incr streak %s: %w", key, err)
	}
	return incr.Val(), nil
}

// delStreak 清零 streak（高周期命中或摘标时调用；幂等）。
func (s *bucketStore) delStreak(ctx context.Context, groupID uint64) error {
	if err := s.rdb.Del(ctx, lowStreakKey(groupID)).Err(); err != nil {
		return fmt.Errorf("hotdetect del streak %s: %w", lowStreakKey(groupID), err)
	}
	return nil
}
