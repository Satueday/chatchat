package dao

import (
	"context"
	"fmt"

	"github.com/redis/go-redis/v9"
)

// SeqGen 会话 seq 生成契约（W2-M03 冻结，spec §10：conv:{conversationId}:seq
// 原子 INCR）。INCR 与 MySQL 落库间无分布式事务：落库失败接受 seq 空洞、
// 不做补偿（补偿引入竞态；拉取按 maxSeq 增量语义下空洞无害，客户端按
// (conversationId, msgSeq) 去重不校验连续——W2-M03-plan §2/§10 裁决 D3）。
// seq 键不设 TTL：会话长期不活跃后 seq 若被过期重置，将自 1 重新计数并
// 与历史落库行冲突 uk_conv_seq——恒存键的存储代价（每会话一个计数器）可忽略。
type SeqGen interface {
	// Next 返回会话内下一个递增序号（从 1 开始，跨会话独立）。
	Next(ctx context.Context, conversationID string) (uint64, error)
}

// redisSeqGen 为 SeqGen 的 go-redis 实现：单命令 INCR 原子自增。
type redisSeqGen struct {
	rdb *redis.Client
}

// NewRedisSeqGen 构造 SeqGen；rdb 由装配层创建与关闭。
func NewRedisSeqGen(rdb *redis.Client) SeqGen {
	return &redisSeqGen{rdb: rdb}
}

// Next 以 Redis INCR 生成会话内递增序号（原子性由 Redis 单命令保证）。
func (g *redisSeqGen) Next(ctx context.Context, conversationID string) (uint64, error) {
	v, err := g.rdb.Incr(ctx, fmt.Sprintf("conv:%s:seq", conversationID)).Result()
	if err != nil {
		return 0, err
	}
	return uint64(v), nil
}
