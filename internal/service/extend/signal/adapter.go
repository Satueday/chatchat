package signal

import (
	"context"
	"fmt"
	"strconv"

	pkgerrors "chatchat/pkg/errors"
	"chatchat/pkg/protocol"

	"google.golang.org/protobuf/proto"
)

// ConnManagerView 连接注册表结构化窄视图（WSAdapter 的 PeerRouter 底座）：
// *server.ConnManager 结构化满足（Push/Online 签名一致）——本包不 import
// internal/server（禁止事项；message.Pusher/wire.ConnPusher 先例的倒置
// 形态：接口定义在服务包、实现对象由装配层注入）。
type ConnManagerView interface {
	// Push 向 uid 非阻塞投递下行字节（false=离线/慢消费者被踢）。
	Push(uid uint64, data []byte) bool
	// Online 查询 uid 是否本机在线（多连接任一即在线）。
	Online(uid uint64) bool
}

// WSAdapter WS 数据面适配层（CAP-301，TGT-304）：上行——CmdSignal 帧
// 解码（5001）→ to ParseUint（1001，HandleChat service.go:113 先例）→
// Service 核心；下行——实现 PeerRouter（Signal 组 CmdSignal 转发帧，
// Header.traceId 回显上行值、seq 不占用——信令非消息流，D6）经
// ConnManagerView 投递。HandleSignal 签名对齐 wire.SignalHandler
// （错误面 *pkgerrors.Error → wire 统一 sendErr 回 CmdError 帧）。
type WSAdapter struct {
	svc *Service
	mgr ConnManagerView // 两阶段：Manager 随 Server 构造后 Attach 回填
}

// NewWSAdapter 构造适配层（svc 必须先于本构造存在；mgr 经 Attach 回填
// ——Manager 由 NewServer 创建，晚于 handler 注入，LogicHandler.
// AttachSendError 两阶段先例）。
func NewWSAdapter(svc *Service) *WSAdapter { return &WSAdapter{svc: svc} }

// Attach 回填连接注册表（Server 构造后、开始监听前调用一次；未挂时
// Online 恒 false——安全失败 7101）。
func (a *WSAdapter) Attach(mgr ConnManagerView) { a.mgr = mgr }

// HandleSignal 处理上行 CmdSignal 帧（wire.LogicHandler 分派入口）：
// 解码 → to 解析 → Service 核心。错误返回由 wire 经 sendErr 组 CmdError
// 帧回发送方（uid/seq/traceId 取自帧头，既有错误面复用）。
func (a *WSAdapter) HandleSignal(ctx context.Context, uid uint64, frame *protocol.Frame) *pkgerrors.Error {
	sig := &protocol.Signal{}
	if err := proto.Unmarshal(frame.GetPayload(), sig); err != nil {
		return pkgerrors.Wrap(err, pkgerrors.ErrMessageDecode.Code, pkgerrors.ErrMessageDecode.Message)
	}
	toUID, err := strconv.ParseUint(sig.GetTo(), 10, 64)
	if err != nil {
		return pkgerrors.Wrap(err, pkgerrors.ErrInvalidParam.Code, pkgerrors.ErrInvalidParam.Message)
	}
	return a.svc.HandleSignal(ctx, uid, toUID, sig, frame.GetHeader().GetTraceId())
}

// Online 实现 PeerRouter：委托连接注册表（未挂 false）。
func (a *WSAdapter) Online(uid uint64) bool {
	return a.mgr != nil && a.mgr.Online(uid)
}

// PushSignal 实现 PeerRouter：Signal 编码为 CmdSignal 转发帧（cmd=6、
// traceId 回显、无 seq/ack）→ proto 帧序列化 → ConnManager.Push 非阻塞
// 投递；false 视同离线（D3 → 7101）。
func (a *WSAdapter) PushSignal(uid uint64, sig *protocol.Signal, traceID string) error {
	payload, err := proto.Marshal(sig)
	if err != nil {
		return fmt.Errorf("marshal signal payload: %w", err)
	}
	b, err := proto.Marshal(&protocol.Frame{
		Header:  &protocol.Header{Cmd: protocol.CmdSignal, TraceId: traceID},
		Payload: payload,
	})
	if err != nil {
		return fmt.Errorf("marshal signal frame: %w", err)
	}
	if a.mgr == nil || !a.mgr.Push(uid, b) {
		return fmt.Errorf("signal peer unreachable: uid=%d", uid)
	}
	return nil
}
