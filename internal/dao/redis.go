package dao

import (
	"context"
	"fmt"

	"chatchat/config"

	"github.com/redis/go-redis/v9"
)

// NewRedisClient 构造 Redis 客户端并 Ping 验证连通（构造失败即上抛，不静默降级；
// Ping 失败时关闭半开句柄后返回错误）。Close 由装配层持有调用（defer Close）。
func NewRedisClient(ctx context.Context, cfg config.Redis) (*redis.Client, error) {
	client := redis.NewClient(&redis.Options{
		Addr:     cfg.Addr,
		Password: cfg.Password.Value(),
		DB:       cfg.DB,
		PoolSize: cfg.PoolSize,
	})
	if err := client.Ping(ctx).Err(); err != nil {
		_ = client.Close()
		return nil, fmt.Errorf("redis ping 失败: %w", err)
	}
	return client, nil
}
