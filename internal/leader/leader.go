// Package leader 后台任务多实例轮门控（W4-M05 plan D7，移交 1）。
//
// 语义（冻结）：Redis `SET key 1 NX EX ttl` 单命令原子的「每轮咨询锁」——
// 无续期、无长期属主：每轮执行前竞取，TTL=任务 interval×2（吸收单轮超时），
// 进程崩溃锁自然过期；同 key 每轮至多一个执行者。正确性兜底=任务本身幂等
// （W3 对账 / W4-M01 聚合既证），门控消除的是多实例重复告警/重复聚合的
// 观测噪音与重复写放大，不是一致性机制（长期租约/fencing token 明确不做，
// plan 禁止事项）。
//
// 错误处理：Redis 不可达=Acquire 返回 false（本轮跳过，宁缺勿重——正确性
// 由任务幂等兜底，plan「错误处理」节），不 panic 不重试。
package leader

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"
)

// lockValue 锁值占位（1.0.0 不承载持有者身份，见包注「无长期属主」）。
const lockValue = "1"

// Gate 每轮咨询锁接口（plan D7）：key 为任务级锁键（如
// chatchat:leader:reconcile），ttl 为锁存活上界（调用方按 interval×2 注入）。
// 返回 true=本轮由本实例执行；false=他实例持有或 Redis 不可达（跳轮）。
// 实现必须并发安全（Acquire 可能被多 goroutine 调用）。
type Gate interface {
	Acquire(ctx context.Context, key string, ttl time.Duration) bool
}

// RedisGate Gate 的 Redis 实现（SET NX EX，单命令原子）。
type RedisGate struct {
	rdb redis.UniversalClient
}

// 编译期接口实现断言。
var _ Gate = (*RedisGate)(nil)

// NewRedisGate 构造 Redis 轮门控；rdb 由装配层创建与关闭（复用既有连接）。
func NewRedisGate(rdb redis.UniversalClient) *RedisGate {
	return &RedisGate{rdb: rdb}
}

// Acquire 竞取一轮执行权：SET key 1 NX EX ttl——键已存在（他实例持锁）
// 返回 false；Redis 错误返回 false（不可达=跳轮，不 panic）；防御参数
// （空键/非正 TTL/nil 客户端）一律 false（宁缺勿重的缺省方向）。
func (g *RedisGate) Acquire(ctx context.Context, key string, ttl time.Duration) bool {
	if g == nil || g.rdb == nil || key == "" || ttl <= 0 {
		return false
	}
	ok, err := g.rdb.SetNX(ctx, key, lockValue, ttl).Result()
	if err != nil {
		return false
	}
	return ok
}
