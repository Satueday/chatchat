package server

import (
	"sync"
)

// ConnManager 为本机连接注册表（W2-M02 CAP-002/004；spec.md §8.5
// 「本机 map（userId → conn）」）。并发安全：读多写少场景 RWMutex。
// 后续被依赖方：W2-M04 内存 Deliverer 与 W3 pushpull 投递（plan.md §4）。
//
// 生命周期约定：Register 顶替踢旧（返回被踢连接，由调用方触发其关闭）；
// Unregister 仅当 c 仍是该 uid 当前注册者时删除——顶替后旧连接的迟到清理
// 不会误删新连接（go-chat 按名判等误删的反例规避）。
type ConnManager struct {
	mu    sync.RWMutex
	conns map[uint64]*Client
}

// NewConnManager 构造空注册表。
func NewConnManager() *ConnManager {
	return &ConnManager{conns: make(map[uint64]*Client)}
}

// Register 注册连接；同 uid 已有连接时顶替（旧连接返回给调用方踢出）。
func (m *ConnManager) Register(c *Client) (kicked *Client) {
	m.mu.Lock()
	defer m.mu.Unlock()
	old, exists := m.conns[c.uid]
	m.conns[c.uid] = c
	if exists && old != c {
		return old
	}
	return nil
}

// Unregister 注销连接：仅当 c 是该 uid 当前注册者时生效（返回是否生效）。
func (m *ConnManager) Unregister(c *Client) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	if cur, ok := m.conns[c.uid]; ok && cur == c {
		delete(m.conns, c.uid)
		return true
	}
	return false
}

// Push 向 uid 投递下行数据（非阻塞）：不在线返回 false；send 缓冲满按
// 慢消费者策略踢连接并返回 false（绝不阻塞调用方——go-chat 事件循环
// 阻塞投递死锁的反例规避）。
func (m *ConnManager) Push(uid uint64, data []byte) bool {
	m.mu.RLock()
	c, ok := m.conns[uid]
	m.mu.RUnlock()
	if !ok {
		return false
	}
	return c.trySend(data)
}

// Online 查询 uid 是否本机在线。
func (m *ConnManager) Online(uid uint64) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	_, ok := m.conns[uid]
	return ok
}

// Count 返回当前本机连接数（日志/测试用）。
func (m *ConnManager) Count() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.conns)
}

// CloseAll 优雅关闭全部连接：逐个 shutdown（读写泵各自退出、清理路径
// 恰好一次），等待全部 finalize 完成后返回（含 presence 删除）。
// 注册表由各连接的 finalize 自行 Unregister 清空；重复调用幂等。
func (m *ConnManager) CloseAll() {
	m.mu.RLock()
	clients := make([]*Client, 0, len(m.conns))
	for _, c := range m.conns {
		clients = append(clients, c)
	}
	m.mu.RUnlock()
	for _, c := range clients {
		c.shutdown()
	}
	for _, c := range clients {
		c.waitFinished()
	}
}
