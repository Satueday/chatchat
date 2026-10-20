package dao

import (
	"context"
	"fmt"
	"time"

	"chatchat/pkg/global"

	"github.com/redis/go-redis/v9"
)

// PendMarker 为 seq 在途标记的读/清契约（W2-M05-C2 裁决 D8/D12）：写路径
// 分配 seq 时登记（见 PendingSeqGen），落库成败后清除；拉取侧对缺失 seq
// 逐个探测，区分「在途（迟到提交可能，须截断防丢）」与「永久空洞（insert
// 失败永不出现，跳过无害——W2-M03 D3 原语义仅对此成立）」。
type PendMarker interface {
	// HasPending 报告 (conversationID, seq) 的在途标记是否存在。
	// true=在途；键不存在=false（永久空洞或已提交清除）。
	HasPending(ctx context.Context, conversationID string, seq uint64) (bool, error)
	// ClearPending 删除 (conversationID, seq) 的在途标记（幂等：键不存在
	// 视为成功）。
	ClearPending(ctx context.Context, conversationID string, seq uint64) error
}

// PendingSeqGen 为写路径 seq 供给契约（W2-M05-C2 裁决 D8/D9）：NextPending
// 以 Lua 脚本原子完成「INCR conv:{id}:seq + SETEX conv:{id}:pend:{seq}」，
// 封闭分立 INCR→SETEX 间进程崩溃漏记在途标记的微窗；内嵌 PendMarker 使
// 写路径可在 Create 成败两分支 best-effort 清除标记（D9），单一 go-redis
// 实现同时服务读写两路径。不扩 M03 冻结的 SeqGen（独立接口，签名零涟漪）。
type PendingSeqGen interface {
	PendMarker
	// NextPending 返回会话内下一个递增序号，并原子登记在途标记
	// conv:{conversationId}:pend:{seq}（TTL=global.PendingMarkerTTL，D10）。
	NextPending(ctx context.Context, conversationID string) (uint64, error)
}

// pendingSeqScript 原子「分配 seq + 登记在途标记」（go-redis Script：EVALSHA
// 优先、miss 时回落 EVAL）。pend 键含脚本内 INCR 的结果值，无法客户端预构
// 传入 KEYS——经 ARGV[2] 前缀拼接（string.format %d 防 Lua number 科学
// 计数拼接，W2-M05-C2 审查建议）。Cluster 迁移注意（W3+，ADR 0006 R5）：
// 当前键名 conv:{id}:seq 与 conv:{id}:pend:{seq} 无字面 {...} hash tag，
// 不保证同槽；且 pend 键为脚本内构造的未声明键——迁移前须重设计键名并把
// pend 键移入 KEYS 声明。
var pendingSeqScript = redis.NewScript(`
local seq = redis.call('INCR', KEYS[1])
redis.call('SETEX', string.format('%s%d', ARGV[2], seq), ARGV[1], 1)
return seq
`)

// redisPendMarker 为 PendMarker/PendingSeqGen 的 go-redis 实现：无状态，
// 仅持客户端句柄（零新依赖，Eval 走既有 go-redis）。
type redisPendMarker struct {
	rdb *redis.Client
}

// NewRedisPendingSeqGen 构造写路径 PendingSeqGen；rdb 由装配层创建与关闭。
func NewRedisPendingSeqGen(rdb *redis.Client) PendingSeqGen {
	return &redisPendMarker{rdb: rdb}
}

// NewRedisPendMarker 构造拉取侧 PendMarker 探针；rdb 由装配层创建与关闭。
func NewRedisPendMarker(rdb *redis.Client) PendMarker {
	return &redisPendMarker{rdb: rdb}
}

// NextPending 原子生成会话 seq 并登记在途标记（TTL 秒参数化自
// global.PendingMarkerTTL）。
func (m *redisPendMarker) NextPending(ctx context.Context, conversationID string) (uint64, error) {
	v, err := pendingSeqScript.Run(ctx, m.rdb,
		[]string{fmt.Sprintf("conv:%s:seq", conversationID)},
		int64(global.PendingMarkerTTL/time.Second),
		fmt.Sprintf("conv:%s:pend:", conversationID),
	).Int64()
	if err != nil {
		return 0, err
	}
	return uint64(v), nil
}

// HasPending 以 EXISTS 探测在途标记（0/1 计数归一为 bool）。
func (m *redisPendMarker) HasPending(ctx context.Context, conversationID string, seq uint64) (bool, error) {
	n, err := m.rdb.Exists(ctx, pendKey(conversationID, seq)).Result()
	if err != nil {
		return false, err
	}
	return n > 0, nil
}

// ClearPending 以 DEL 清除在途标记（键不存在亦成功，幂等）。
func (m *redisPendMarker) ClearPending(ctx context.Context, conversationID string, seq uint64) error {
	return m.rdb.Del(ctx, pendKey(conversationID, seq)).Err()
}

// pendKey 在途标记键：conv:{conversationId}:pend:{seq}（与 pendingSeqScript
// 内拼接的键格式同源）。
func pendKey(conversationID string, seq uint64) string {
	return fmt.Sprintf("conv:%s:pend:%d", conversationID, seq)
}
