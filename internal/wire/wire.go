// Package wire 跨服务接线装配层（W2-M06；与 google/wire DI 工具无关）：
// 网关上行帧六路分派（LogicHandler，W3-M04 增 CmdGroupChat、W4-M04 增
// CmdSignal）+ 下行推送通道适配（ConnPusher）。纯适配，零连接创建；位于
// internal/server 与 internal/service/message 之上（两者互不依赖，M04
// 裁决 D5），被 cmd/gateway 与 test/ 共用——消除 M03/M04/M05 三模块的
// adapter 双份同构 P3。
package wire

import (
	"context"

	"chatchat/internal/server"
	"chatchat/internal/service/message"
	pkgerrors "chatchat/pkg/errors"
	"chatchat/pkg/protocol"
)

// SignalHandler 信令帧处理窄接口（W4-M04，ADR-0011 D1）：internal/
// service/extend/signal.WSAdapter 结构化满足——wire 不 import signal 包
// （分派面只依赖签名形态；adapter 由 cmd/gateway 装配注入，ChatGate/
// AckHandler 先例）。错误面与其他 handler 一致（*pkgerrors.Error →
// sendErr 组 CmdError 回帧）。
type SignalHandler interface {
	// HandleSignal 处理上行 CmdSignal 帧（from=鉴权 uid、to/负载由实现
	// 解析；错误经 wire 错误面回发送方 CmdError 7101/7102/1001/5001）。
	HandleSignal(ctx context.Context, uid uint64, frame *protocol.Frame) *pkgerrors.Error
}

// LogicHandler 将单聊 Logic 服务与 ACK/拉取/信令处理适配为网关上行帧
// 处理器（自 cmd/gateway 装配层上移，W2-M06 裁决 D2；service 保持网关
// 无关）：业务错误经 sendErr（server.SendError）回 CmdError 帧
// （common.Fail 消毒后的 {code,message,data} payload）。
//
// 两阶段装配：构造 Server 时先以指针注入（sendErr=nil 语义完整），Server
// 就绪后 AttachSendError 回填——避免 handler→srv→handler 构造环，且零
// internal/server 改动。
type LogicHandler struct {
	svc     *message.Service    // CmdChat 与 default 分派目标（nil 安全）
	acker   message.AckHandler  // CmdAck 分派（W2-M04；nil 安全）
	puller  message.PullHandler // CmdPullRequest 分派（W2-M05；nil 安全）
	signal  SignalHandler       // CmdSignal 分派（W4-M04；nil 安全）
	sendErr func(uid uint64, seq int64, traceID string, e *pkgerrors.Error)
}

// 编译期接口实现断言：LogicHandler 即网关上行帧 Handler。
var _ server.Handler = (*LogicHandler)(nil)

// NewLogicHandler 构造六路分派处理器（svc/acker/puller/signal 均可为
// nil，nil 分派 no-op；sendErr 经 AttachSendError 二阶段回填）。
func NewLogicHandler(svc *message.Service, acker message.AckHandler, puller message.PullHandler, signal SignalHandler) *LogicHandler {
	return &LogicHandler{svc: svc, acker: acker, puller: puller, signal: signal}
}

// HandleFrame 实现 server.Handler 六路分派：CmdChat→单聊链路、CmdGroupChat→
// 群聊链路（W3-M04，HandleGroupChat g_{groupId} 会话）、CmdAck→ACK 链路、
// CmdPullRequest→拉取链路（心跳已被网关读泵消化）、CmdSignal→信令链路
// （W4-M04，Signal 会话校验+透传转发，错误 7101/7102/1001/5001）、
// default→HandleChat 保持 M03 忽略语义（记日志忽略）。错误统一经 sendErr
// 回 CmdError 帧（uid 取鉴权连接、seq/traceId 透传上行帧头）。
func (h *LogicHandler) HandleFrame(ctx context.Context, uid uint64, frame *protocol.Frame) {
	var e *pkgerrors.Error
	switch frame.GetHeader().GetCmd() {
	case protocol.CmdChat:
		if h.svc != nil {
			e = h.svc.HandleChat(ctx, uid, frame)
		}
	case protocol.CmdGroupChat:
		if h.svc != nil {
			e = h.svc.HandleGroupChat(ctx, uid, frame)
		}
	case protocol.CmdAck:
		if h.acker != nil {
			e = h.acker.HandleAck(ctx, uid, frame)
		}
	case protocol.CmdPullRequest:
		if h.puller != nil {
			e = h.puller.HandlePull(ctx, uid, frame)
		}
	case protocol.CmdSignal:
		if h.signal != nil {
			e = h.signal.HandleSignal(ctx, uid, frame)
		}
	default:
		if h.svc != nil {
			e = h.svc.HandleChat(ctx, uid, frame)
		}
	}
	if e != nil {
		seq, trace := int64(0), ""
		if frame.GetHeader() != nil {
			seq, trace = frame.GetHeader().GetSeq(), frame.GetHeader().GetTraceId()
		}
		if h.sendErr != nil {
			h.sendErr(uid, seq, trace, e)
		}
	}
}

// AttachSendError 回填错误回帧函数（Server 构造完成后、开始监听前调用一次）。
func (h *LogicHandler) AttachSendError(fn func(uid uint64, seq int64, traceID string, e *pkgerrors.Error)) {
	h.sendErr = fn
}

// ConnPusher 将 server.ConnManager 适配为 message.Pusher（W2-M04 裁决 D5 →
// W2-M06 上移 wire：装配层适配，service/message 不 import internal/server）。
// 两阶段：Manager 由 NewServer 创建，Server 构造后经 Attach 回填（对齐
// LogicHandler.AttachSendError 先例；Attach 在开始监听前完成，此后只读）。
type ConnPusher struct {
	m *server.ConnManager
}

// 编译期接口实现断言：ConnPusher 即 message.Pusher。
var _ message.Pusher = (*ConnPusher)(nil)

// NewConnPusher 构造未挂 Manager 的推送适配器（Push/Online 恒 false）。
func NewConnPusher() *ConnPusher { return &ConnPusher{} }

// Push 实现 message.Pusher：非阻塞下行（false=离线/send 缓冲满被踢；
// Manager 未挂同样 false）。
func (p *ConnPusher) Push(uid uint64, data []byte) bool {
	if p.m == nil {
		return false
	}
	return p.m.Push(uid, data)
}

// Online 实现 message.Pusher：查询 uid 是否本机在线（Manager 未挂 false）。
func (p *ConnPusher) Online(uid uint64) bool {
	if p.m == nil {
		return false
	}
	return p.m.Online(uid)
}

// Attach 回填 Manager（Server 构造完成后、开始监听前调用一次）。
func (p *ConnPusher) Attach(m *server.ConnManager) { p.m = m }
