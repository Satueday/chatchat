package message

import (
	"context"
	stderrors "errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"chatchat/internal/dao"
	"chatchat/internal/model"
	pkgerrors "chatchat/pkg/errors"
	"chatchat/pkg/protocol"

	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
)

// pendingKey 唯一标识一项待确认投递：接收者 uid × 会话 × seq。
// (uid, conv) 前缀匹配 + seq≤afterSeq 即 ACK 累计取消面（CAP-005）。
type pendingKey struct {
	uid  uint64
	conv string
	seq  uint64
}

// pendingDelivery 为单条消息的重推状态机载体（CAP-005）。
// 生命周期 ≤ base×(2^(RetryMax+1)−1)（默认 15s）自然过期（终窗/离线/ACK
// 三条出口），内存有界；frame 复用同字节重推（客户端按 (conv,seq) 去重）。
type pendingDelivery struct {
	msgID    string
	traceID  string
	frame    []byte    // 组好的下行帧（CmdChat/CmdGroupChat，首发与重推字节相同）
	attempts int       // 已完成的重推次数（0=仅首发）
	at       time.Time // 首发时刻（ACK 延迟观测）
	timer    *time.Timer
}

// MemoryDeliverer 为 W2 内存投递实现（W2-M04；W3 Kafka Producer 同接口替换）：
// 在线判定 → 组 CmdChat 下行帧 → 经 Pusher 非阻塞投递 → 登记 pending 等 ACK；
// 超时未 ACK 指数退避重推（wait(i)=base×2^(i-1)，≤RetryMax 次）→ 终窗仍无
// ACK 或成员离线 → 转离线终态（不推不重推，游标不推进，由拉取兜底，D4）。
//
// 并发约定：mu 保护 closed/pending；timer 回调（fire）持锁判态、释锁后 Push
// （禁止持锁调用 Push——Push 可能触发慢消费者踢出路径）；Push 返回后重取锁
// 校验 pending 归属再布防下一轮 timer（ACK 已取消则不布防，防 timer 泄漏）。
type MemoryDeliverer struct {
	push    Pusher
	cursors dao.InboxCursorRepo
	opts    DeliveryOptions
	logger  *zap.Logger

	mu      sync.Mutex
	closed  bool
	pending map[pendingKey]*pendingDelivery
}

// 编译期接口实现断言（Deliverer 契约 W2-M03 冻结签名不变）。
var (
	_ Deliverer  = (*MemoryDeliverer)(nil)
	_ AckHandler = (*MemoryDeliverer)(nil)
)

// NewMemoryDeliverer 构造内存投递器。opts 零值/越界字段补默认
// （RetryBase≤0→1s、RetryMax<0→0，server.Options.normalize 先例）；
// RetryMax>10 钳制为 10——config 校验（[1,10]）为第一防线，此处为第二道
// （W2-M05 P3 回补：防绕过 config 直构导致 nextDelay 移位与重推窗口失控）；
// logger 为 nil 时以 zap.NewNop 兜底。
func NewMemoryDeliverer(push Pusher, cursors dao.InboxCursorRepo, opts DeliveryOptions, logger *zap.Logger) *MemoryDeliverer {
	if logger == nil {
		logger = zap.NewNop()
	}
	if opts.RetryBase <= 0 {
		opts.RetryBase = time.Second
	}
	if opts.RetryMax < 0 {
		opts.RetryMax = 0
	}
	if opts.RetryMax > 10 {
		opts.RetryMax = 10
	}
	return &MemoryDeliverer{
		push:    push,
		cursors: cursors,
		opts:    opts,
		logger:  logger,
		pending: make(map[pendingKey]*pendingDelivery),
	}
}

// Deliver 实现 Deliverer（CAP-004）：逐成员判定在线——离线=零 Push 零
// pending（Warn）；在线=注册 pending（先注册后 Push，防快 ACK 竞态漏取消）
// → Push#0 → 布防首轮 timer(base)。closed 后仅 Warn 返回 nil（零副作用）。
func (d *MemoryDeliverer) Deliver(ctx context.Context, msg *model.Message, members []uint64) error {
	d.mu.Lock()
	closed := d.closed
	d.mu.Unlock()
	if closed {
		d.logger.Warn("deliver ignored after close",
			zap.String("msgId", msg.MsgID),
			zap.String("conversationId", msg.ConversationID),
			zap.Uint64("msgSeq", msg.MsgSeq),
			zap.Uint64s("members", members))
		return nil
	}

	traceID := traceIDFromContext(ctx)
	for _, uid := range members {
		if !d.push.Online(uid) {
			d.logger.Warn("deliver: member offline, skip push",
				zap.Uint64("uid", uid),
				zap.String("conversationId", msg.ConversationID),
				zap.Uint64("msgSeq", msg.MsgSeq),
				zap.String("msgId", msg.MsgID),
				zap.String("traceId", traceID))
			continue
		}

		frameBytes, err := buildChatFrame(msg, traceID)
		if err != nil {
			// proto 序列化失败=帧构造缺陷（影响后续成员同理），上抛由调用方
			// 记 Warn（Deliverer 契约：不回滚落库）。
			return fmt.Errorf("组装下行帧失败: %w", err)
		}

		key := pendingKey{uid: uid, conv: msg.ConversationID, seq: msg.MsgSeq}
		p := &pendingDelivery{
			msgID:   msg.MsgID,
			traceID: traceID,
			frame:   frameBytes,
			at:      time.Now(),
		}
		d.mu.Lock()
		if d.closed {
			d.mu.Unlock()
			d.logger.Warn("deliver: closer won race, skip member",
				zap.Uint64("uid", uid), zap.String("msgId", msg.MsgID))
			continue
		}
		d.pending[key] = p
		d.mu.Unlock()

		d.pushOnce(key, p, 0)
	}
	return nil
}

// pushOnce 执行一次 Push 并按 plan §4 状态机布防下一轮 timer。
// attempt 为本次 Push 序号（0=首发）；nextDelay 为距下一次 fire 的等待
// （首发后 base，重推 i 后 base×2^i）。须在 pending 已注册且未关闭时调用。
func (d *MemoryDeliverer) pushOnce(key pendingKey, p *pendingDelivery, attempt int) {
	ok := d.push.Push(key.uid, p.frame)
	if ok {
		d.logger.Info("push delivered",
			zap.Uint64("uid", key.uid),
			zap.String("conversationId", key.conv),
			zap.Uint64("msgSeq", key.seq),
			zap.String("msgId", p.msgID),
			zap.Int("attempt", attempt),
			zap.String("traceId", p.traceID))
	} else {
		// false≠终态（慢消费者被踢/瞬时离线），仅记录，重推调度照常（CASE-006）。
		d.logger.Warn("push returned false (offline or slow consumer)",
			zap.Uint64("uid", key.uid),
			zap.String("conversationId", key.conv),
			zap.Uint64("msgSeq", key.seq),
			zap.String("msgId", p.msgID),
			zap.Int("attempt", attempt),
			zap.String("traceId", p.traceID))
	}

	d.mu.Lock()
	defer d.mu.Unlock()
	if d.closed {
		return
	}
	if cur, exist := d.pending[key]; exist && cur == p {
		p.timer = time.AfterFunc(d.nextDelay(attempt), func() { d.fire(key) })
	}
}

// nextDelay 计算本次 Push（序号 attempt）之后的等待：
// 首发后 base（wait(1)=base×2^0），重推 i 后 base×2^i（末次重推后即终窗
// base×2^RetryMax，D3）。attempt 由 config 限制下 ≤10，移位安全。
func (d *MemoryDeliverer) nextDelay(attempt int) time.Duration {
	if attempt <= 0 {
		return d.opts.RetryBase
	}
	return d.opts.RetryBase << uint(min(attempt, 32))
}

// fire 为重推定时器回调（plan §4 冻结状态机）：
// pending 已删（ACK/Close）→ no-op；attempts==RetryMax（终窗耗尽）或成员
// 离线 → 删 pending 转 Warn 离线终态；否则 attempts++ → Push → 布防。
func (d *MemoryDeliverer) fire(key pendingKey) {
	d.mu.Lock()
	if d.closed {
		d.mu.Unlock()
		return
	}
	p, ok := d.pending[key]
	if !ok {
		d.mu.Unlock()
		return
	}
	if p.attempts >= d.opts.RetryMax {
		delete(d.pending, key)
		d.mu.Unlock()
		d.logger.Warn("deliver: retry window exhausted, offline final",
			zap.Uint64("uid", key.uid),
			zap.String("conversationId", key.conv),
			zap.Uint64("msgSeq", key.seq),
			zap.String("msgId", p.msgID),
			zap.Int("attempts", p.attempts),
			zap.String("traceId", p.traceID))
		return
	}
	if !d.push.Online(key.uid) {
		delete(d.pending, key)
		d.mu.Unlock()
		d.logger.Warn("deliver: member offline during retry, offline final",
			zap.Uint64("uid", key.uid),
			zap.String("conversationId", key.conv),
			zap.Uint64("msgSeq", key.seq),
			zap.String("msgId", p.msgID),
			zap.Int("attempts", p.attempts),
			zap.String("traceId", p.traceID))
		return
	}
	p.attempts++
	attempt := p.attempts
	d.mu.Unlock()

	d.pushOnce(key, p, attempt)
}

// HandleAck 实现 AckHandler（CAP-002/003）：解码 PullRequest（失败 5001）
// → 校验 conv 非空/uid∈conv/afterSeq≥1（失败 1001）→ 取消 (uid,conv) 下
// seq≤afterSeq 的全部 pending（累计取消，停 timer）→ cursors.AdvanceRecv
// （失败仅 Error 日志，不回帧——客户端无过错，D6）。
func (d *MemoryDeliverer) HandleAck(ctx context.Context, uid uint64, frame *protocol.Frame) *pkgerrors.Error {
	pr := &protocol.PullRequest{}
	if err := proto.Unmarshal(frame.GetPayload(), pr); err != nil {
		return pkgerrors.Wrap(err, pkgerrors.ErrMessageDecode.Code, pkgerrors.ErrMessageDecode.Message)
	}

	conv := pr.GetConversationId()
	afterSeq := pr.GetAfterSeq()
	if conv == "" || !d.ackConvMember(ctx, uid, conv) {
		return pkgerrors.ErrInvalidParam
	}
	if afterSeq < 1 {
		return pkgerrors.ErrInvalidParam
	}

	var (
		cancelled int
		latency   time.Duration
	)
	d.mu.Lock()
	for key, p := range d.pending {
		if key.uid == uid && key.conv == conv && key.seq <= uint64(afterSeq) {
			if p.timer != nil {
				p.timer.Stop()
			}
			delete(d.pending, key)
			cancelled++
			latency = time.Since(p.at)
		}
	}
	d.mu.Unlock()

	if cancelled > 0 {
		d.logger.Info("ack received, pending cancelled",
			zap.Uint64("uid", uid),
			zap.String("conversationId", conv),
			zap.Int64("afterSeq", afterSeq),
			zap.Int("cancelled", cancelled),
			zap.Int64("latencyMs", latency.Milliseconds()),
			zap.String("traceId", frame.GetHeader().GetTraceId()))
	}

	if err := d.cursors.AdvanceRecv(ctx, uid, conv, uint64(afterSeq)); err != nil {
		// 游标推进失败不回 CmdError（客户端无过错）；消息已落库、pending 已
		// 取消，游标滞后由拉取侧兜底（M05）。5007 链仅入日志面；code 数值
		// 字段为 M04 P3 回补（W2-M05 D7）——失败链可能带非 5007 码（如
		// ctx 取消），仅靠文案/cause 排障会误导。
		d.logger.Error("inbox cursor advance failed",
			zap.Uint64("uid", uid),
			zap.String("conversationId", conv),
			zap.Int64("afterSeq", afterSeq),
			zap.String("traceId", frame.GetHeader().GetTraceId()),
			zap.Int32("code", errCodeOf(err, pkgerrors.ErrCursorUpdate.Code)),
			zap.Error(err))
	}
	return nil
}

// errCodeOf 提取错误的业务码数值（日志字段用）：*pkgerrors.Error 取其
// Code，非业务错误回退 fallback（调用方预期的哨兵码）。
func errCodeOf(err error, fallback int32) int32 {
	var e *pkgerrors.Error
	if stderrors.As(err, &e) {
		return e.Code
	}
	return fallback
}

// Close 幂等关闭（CAP-006）：置 closed、停全部 timer、清 pending；
// Close 前已注册的 pending 不再 fire（回调首查 closed/pending 双保险）。
func (d *MemoryDeliverer) Close() {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.closed {
		return
	}
	d.closed = true
	for key, p := range d.pending {
		if p.timer != nil {
			p.timer.Stop()
		}
		delete(d.pending, key)
	}
}

// PendingCount 返回当前待确认投递数（测试/日志用）。
func (d *MemoryDeliverer) PendingCount() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return len(d.pending)
}

// chatMessageOf 将 model.Message 转换为 ChatMessage 协议体（W2-M05 自
// buildChatFrame 提取共享：投递下行帧与拉取响应共用同一转换，字段口径
// 唯一）：from/to 为 uid 字符串、timestamp=CreatedAt.UnixMilli()、
// contentType 原值透传（0 语义归 service 写路径覆写，此处不再默认）。
func chatMessageOf(msg *model.Message) *protocol.ChatMessage {
	return &protocol.ChatMessage{
		MsgId:          msg.MsgID,
		ConversationId: msg.ConversationID,
		From:           strconv.FormatUint(msg.FromUID, 10),
		To:             strconv.FormatUint(msg.ToUID, 10),
		MsgSeq:         int64(msg.MsgSeq),
		Timestamp:      msg.CreatedAt.UnixMilli(),
		ContentType:    msg.ContentType,
		Content:        msg.Content,
		Url:            msg.URL,
	}
}

// buildChatFrame 组装聊天下行帧（CAP-004；W3-M04 裁决 D7 cmd 对称）：
// 消息体经 chatMessageOf 转换（9 字段齐，timestamp=msg.CreatedAt.UnixMilli()，
// P2 回补）、Header{Cmd, TraceId}——cmd 按 conv 前缀选择：g_ →
// CmdGroupChat、单聊（及其余形态）→ CmdChat，与上行帧 cmd 对称（客户端按
// cmd+conversationId 双重区分；M05 pushpull 下行沿用同规则）。memory 全员
// 在线推送仅 dev/测试语义，生产行为以 kafka driver + M05 pushpull 为准
// （ADR-0008 附注）。
func buildChatFrame(msg *model.Message, traceID string) ([]byte, error) {
	cmd := protocol.CmdChat
	if strings.HasPrefix(msg.ConversationID, groupConvPrefix) {
		cmd = protocol.CmdGroupChat
	}
	payload, err := proto.Marshal(chatMessageOf(msg))
	if err != nil {
		return nil, fmt.Errorf("marshal ChatMessage: %w", err)
	}
	frame := &protocol.Frame{
		Header:  &protocol.Header{Cmd: cmd, TraceId: traceID},
		Payload: payload,
	}
	b, err := proto.Marshal(frame)
	if err != nil {
		return nil, fmt.Errorf("marshal Frame: %w", err)
	}
	return b, nil
}

// isConvMember 校验 uid 是否单聊会话成员（conv 格式 "min|max"，spec §5）。
// 防伪造 ACK 写脏任意游标行。群聊 g_{groupId} 成员判定不经此函数——
// HandleAck 经 ackConvMember 查群表（W3-M05 群 ACK 启用）；PullService
// 读侧成员判定查群表（W3-M04 D6）。
func isConvMember(uid uint64, conv string) bool {
	left, right, ok := strings.Cut(conv, "|")
	if !ok {
		return false
	}
	a, errA := strconv.ParseUint(left, 10, 64)
	b, errB := strconv.ParseUint(right, 10, 64)
	if errA != nil || errB != nil {
		return false
	}
	return uid == a || uid == b
}

// ackConvMember 校验 uid 对 conv 的 ACK 资格（W3-M05 裁决 D5，CAP-004）：
// 单聊（非 g_ 前缀）→ isConvMember 本地解析（W2 冻结口径零变化）；
// 群聊 g_{groupId} → GroupMembers.GetMember 查群表判定——群成员即可 ACK
// （无角色语义）；GroupMembers 未注入（nil）或 groupId 解析失败 → 一律
// 拒绝（1001，PullOptions.GroupMembers 先例：漏注入安全失败而非放行）；
// 判定面基础设施错误同样拒绝（fail-closed，非 4002 类错误记 Warn 排障）。
func (d *MemoryDeliverer) ackConvMember(ctx context.Context, uid uint64, conv string) bool {
	if !strings.HasPrefix(conv, groupConvPrefix) {
		return isConvMember(uid, conv)
	}
	if d.opts.GroupMembers == nil {
		return false
	}
	groupID, err := strconv.ParseUint(strings.TrimPrefix(conv, groupConvPrefix), 10, 64)
	if err != nil {
		return false
	}
	_, err = d.opts.GroupMembers.GetMember(ctx, groupID, uid)
	if err == nil {
		return true
	}
	var e *pkgerrors.Error
	isNotMember := stderrors.As(err, &e) && e.Code == pkgerrors.ErrNotGroupMember.Code
	if !isNotMember {
		d.logger.Warn("group ack membership check failed, fail closed",
			zap.Uint64("uid", uid),
			zap.String("conversationId", conv),
			zap.Error(err))
	}
	return false
}
