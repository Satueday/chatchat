package server

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

// RedisPresence 为 PresenceStore 的 go-redis 实现（W2-M02 CAP-005）。
// 键 online:{userId}（spec.md §10），值为网关节点标识字符串。
// 客户端创建/关闭属装配层（cmd/gateway）；本实现只使用注入的 *redis.Client。
type RedisPresence struct {
	rdb *redis.Client
}

// NewRedisPresence 构造在线状态存储。
func NewRedisPresence(rdb *redis.Client) *RedisPresence {
	return &RedisPresence{rdb: rdb}
}

func presenceKey(uid uint64) string {
	return fmt.Sprintf("online:%d", uid)
}

// SetOnline 写在线键并设置 TTL（覆盖旧值——同 uid 顶替/重连场景）。
func (p *RedisPresence) SetOnline(ctx context.Context, uid uint64, gatewayID string, ttl time.Duration) error {
	return p.rdb.Set(ctx, presenceKey(uid), gatewayID, ttl).Err()
}

// Refresh 续期在线键 TTL（心跳驱动）。
func (p *RedisPresence) Refresh(ctx context.Context, uid uint64, ttl time.Duration) error {
	return p.rdb.Expire(ctx, presenceKey(uid), ttl).Err()
}

// SetOffline 删除在线键（正常关闭/超时踢出）。
func (p *RedisPresence) SetOffline(ctx context.Context, uid uint64) error {
	return p.rdb.Del(ctx, presenceKey(uid)).Err()
}
