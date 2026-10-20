package server

import (
	"context"
	"sync"
	"time"
)

// PresenceStore 为在线状态抽象（W2-M02 CAP-005）：单测用内存实现
// memPresence，生产/集成用 go-redis 实现 RedisPresence（presence_redis.go）。
// 键约定 spec.md §10：online:{userId}，值为网关节点标识，TTL>0 恒成立——
// 进程崩溃残留由 TTL 自然过期兜底。
type PresenceStore interface {
	// SetOnline 写在线键（注册/顶替时调用，覆盖旧值并重置 TTL）。
	SetOnline(ctx context.Context, uid uint64, gatewayID string, ttl time.Duration) error
	// Refresh 续期在线键（心跳时调用）。
	Refresh(ctx context.Context, uid uint64, ttl time.Duration) error
	// SetOffline 删除在线键（连接正常关闭/超时踢出时调用）。
	SetOffline(ctx context.Context, uid uint64) error
}

// memPresence 为内存 fake（单测离线用）：记录调用序列供断言，
// 不模拟 TTL 过期（超时语义由连接层测试覆盖）。
type memPresence struct {
	mu     sync.Mutex
	online map[uint64]string
	calls  []string
}

func newMemPresence() *memPresence {
	return &memPresence{online: make(map[uint64]string)}
}

func (p *memPresence) SetOnline(_ context.Context, uid uint64, gatewayID string, _ time.Duration) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.online[uid] = gatewayID
	p.calls = append(p.calls, "set")
	return nil
}

func (p *memPresence) Refresh(_ context.Context, uid uint64, _ time.Duration) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if _, ok := p.online[uid]; !ok {
		return errNotOnline
	}
	p.calls = append(p.calls, "refresh")
	return nil
}

func (p *memPresence) SetOffline(_ context.Context, uid uint64) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	delete(p.online, uid)
	p.calls = append(p.calls, "del")
	return nil
}

// onlineNow 返回当前在线快照（测试断言用）。
func (p *memPresence) onlineNow() map[uint64]string {
	p.mu.Lock()
	defer p.mu.Unlock()
	snapshot := make(map[uint64]string, len(p.online))
	for k, v := range p.online {
		snapshot[k] = v
	}
	return snapshot
}

// callLog 返回调用序列快照（"set"/"refresh"/"del"）。
func (p *memPresence) callLog() []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]string(nil), p.calls...)
}

type presenceError string

func (e presenceError) Error() string { return string(e) }

const errNotOnline presenceError = "presence: uid 不在线"
