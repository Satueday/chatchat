package signal

import (
	"context"
	"sync"
	"time"

	"go.uber.org/zap"
)

// callSession 一次 1v1 通话会话（D2）：callId 由主叫客户端生成（UUID 串），
// 服务端 invite 时登记三元组；hangup 即删；超时由 sweep 兜底回收。
// lastSeen 为最近活跃锚（C2 裁决 D1/P2-1 修复：offer/answer/ICE 转发成功
// 后 touch 刷新）——TTL 判定以 lastSeen 为准，P2P 媒体期（无服务端帧）的
// 长通话不因 createdAt 龄被误删（CAP-305「不误删活跃会话」）。
type callSession struct {
	caller    uint64
	callee    uint64
	createdAt time.Time
	lastSeen  time.Time
}

// sessionStore 信令会话表（CAP-302/305）：内存 map + RWMutex（读多写少；
// 单实例边界，多网关分布式会话表归下月）。时钟经 now 注入（测试 fake clock
// /生产 time.Now），TTL 判定统一走该锚点。
type sessionStore struct {
	mu       sync.RWMutex
	sessions map[string]*callSession
	ttl      time.Duration
	now      func() time.Time
	logger   *zap.Logger
}

// newSessionStore 构造会话表；now 为 nil 时取 time.Now（生产缺省）。
func newSessionStore(ttl time.Duration, now func() time.Time, logger *zap.Logger) *sessionStore {
	if now == nil {
		now = time.Now
	}
	return &sessionStore{
		sessions: make(map[string]*callSession),
		ttl:      ttl,
		now:      now,
		logger:   logger,
	}
}

// register 登记 callId 会话（invite 路径）：callId 已活跃（含空串）返回
// false（D2：活跃重复 → 7102 由调用方组错误）；登记原子于锁内判重。
func (s *sessionStore) register(callID string, caller, callee uint64) bool {
	if callID == "" {
		return false // 空 callId 无法作为会话键，视同无效（统一 7102 面）
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.sessions[callID]; exists {
		return false
	}
	s.sessions[callID] = &callSession{caller: caller, callee: callee,
		createdAt: s.now(), lastSeen: s.now()}
	return true
}

// participant 判定 callId 会话存在且 uid ∈ {caller, callee}（D2：offer/
// answer/ICE/hangup 的发送者校验；不存在或非参与者均 false → 7102）。
func (s *sessionStore) participant(callID string, uid uint64) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	sess, ok := s.sessions[callID]
	if !ok {
		return false
	}
	return uid == sess.caller || uid == sess.callee
}

// peer 返回 callId 会话中 uid 的对端参与者（W4-M05 plan D10：offer/answer/
// ICE/hangup 在册分支的 to 收紧——toUID 必须等于对端）。会话不存在或 uid
// 非参与者返回 false（与 participant 同判据，附带对端值）。
func (s *sessionStore) peer(callID string, uid uint64) (uint64, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	sess, ok := s.sessions[callID]
	if !ok {
		return 0, false
	}
	switch uid {
	case sess.caller:
		return sess.callee, true
	case sess.callee:
		return sess.caller, true
	}
	return 0, false
}

// touch 刷新 callId 会话的 lastSeen（C2 裁决 D1/P2-1）：offer/answer/ICE
// **转发成功后**由 Service 调用（deliver 失败不刷——该路径本就附带拆会话）；
// 锁内刷新；会话不存在为幂等 no-op（touch 与 remove 的竞态下不复活会话）。
func (s *sessionStore) touch(callID string) {
	s.mu.Lock()
	if sess, ok := s.sessions[callID]; ok {
		sess.lastSeen = s.now()
	}
	s.mu.Unlock()
}

// remove 删除 callId 会话（hangup 即删 / 离线附带清理 / sweep 过期回收）。
func (s *sessionStore) remove(callID string) {
	s.mu.Lock()
	delete(s.sessions, callID)
	s.mu.Unlock()
}

// count 返回当前会话数（测试/观测用）。
func (s *sessionStore) count() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.sessions)
}

// sweepOnce 执行单轮回收：删除 lastSeen 距当前时刻满 ttl 的会话
// （C2 裁决 D1/P2-1：判定锚由 createdAt 改 lastSeen——未应答会话 TTL 兜底
// 回收，活跃通话由转发 touch 保活、hangup 主路径拆除，TTL 仅回收失联
// 残留——CAP-305「不误删活跃会话」）。返回本轮删除数（日志面）。
func (s *sessionStore) sweepOnce() int {
	now := s.now()
	var expired []string
	s.mu.Lock()
	for id, sess := range s.sessions {
		if now.Sub(sess.lastSeen) >= s.ttl {
			expired = append(expired, id)
			delete(s.sessions, id)
		}
	}
	s.mu.Unlock()
	if n := len(expired); n > 0 {
		s.logger.Info("signal sessions swept (ttl expired)",
			zap.Int("removed", n), zap.Int("remaining", s.count()))
	}
	return len(expired)
}

// RunSweep TTL 清理阻塞循环（RunReconcile 范式，CAP-305）：周期 =
// min(ttl, 10s)（D2——短 TTL 会话不留长死区，长 TTL 亦不超 10s 检查粒度）；
// ctx 取消即返回（goroutine 回收，宿主 cmd/gateway）。与 Service 共享
// 会话表锁（单表两视角，无第二把锁）。
func (s *sessionStore) RunSweep(ctx context.Context) {
	interval := s.ttl
	if interval > 10*time.Second {
		interval = 10 * time.Second
	}
	if interval <= 0 {
		interval = time.Second // 防御：ttl 非正值（config 校验已挡，零值兜底）
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		s.sweepOnce()
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}
