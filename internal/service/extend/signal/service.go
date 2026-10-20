package signal

import (
	"context"
	"fmt"
	"strconv"
	"time"

	pkgerrors "chatchat/pkg/errors"
	"chatchat/pkg/protocol"

	"go.uber.org/zap"
)

// PeerRouter 信令对端路由窄接口（D1/D3，CAP-301）：Service 对「对端在哪、
// 何种面」的全部依赖收敛为两方法——WS 数据面由 ConnManagerView 结构化
// 满足，gRPC 契约面由流注册表满足（同一核心两投递面，ADR-0011）。本包
// 不 import internal/server（禁止事项），装配层注入实现。
type PeerRouter interface {
	// Online 判定 uid 当前是否可达（WS：本机连接注册表；gRPC：流注册表）。
	Online(uid uint64) bool
	// PushSignal 向 uid 投递一帧信令（traceID 为回显锚——WS 面写入转发帧
	// Header.TraceId，gRPC 面仅日志用）；非 nil 返回 = 不可达/投递失败
	//（视同离线，D3 → 7101）。实现必须非阻塞或有界（慢消费者断开语义）。
	PushSignal(uid uint64, sig *protocol.Signal, traceID string) error
}

// Service 信令服务核心（CAP-301/302/303）：五型分流 + 会话表校验 +
// PeerRouter 转发。错误面 = *pkgerrors.Error（nil=成功），供适配层组
// CmdError 回帧（WS 面）或日志收敛（gRPC 契约面，ADR-0011 登记）。
//
// 冻结语义（plan D2/D3/D6 + C2 联调修正 + W4-M05 D10 to 收紧）：invite
// 建会话（活跃重复 7102；对端离线 7101 但会话保留——主叫可换 callId 重邀）；
// offer/answer/ICE 须在册、发送者为参与者且 to=对端参与者（7102，D10），
// 对端不可达 7101 且附带拆会话；hangup 在册分支同收紧 to=对端（7102）后
// 拆会话尽力转发——对不存在/已结束会话的 hangup 宽容化 best-effort 转发
// 且不报 7102、to 任意（C2 联调发现 touch 不覆盖纯 P2P 媒体期：>TTL 会话
// 回收后挂断仍须达对端；宁转发勿丢弃，对端按 callId 幂等收尾）。SDP/ICE
// 不读不验；不排序不聚合；isGroup 不校验透传。
type Service struct {
	sessions *sessionStore
	router   PeerRouter
	logger   *zap.Logger
}

// NewService 构造信令核心：ttl 为会话存活上限（config [signal].
// session_ttl_sec）；router 经 AttachRouter 二阶段回填（装配序：
// adapter 需先持 svc 构造，svc 再回填 router 指针——LogicHandler.
// AttachSendError 先例）。
func NewService(ttl time.Duration, logger *zap.Logger) *Service {
	if logger == nil {
		logger = zap.NewNop()
	}
	return &Service{
		sessions: newSessionStore(ttl, nil, logger),
		logger:   logger,
	}
}

// AttachRouter 回填对端路由（构造后、开始服务前调用一次；未回填时
// Online 恒 false——一切帧 7101 安全失败）。
func (s *Service) AttachRouter(r PeerRouter) { s.router = r }

// RunSweep 启动 TTL 清理循环（宿主 goroutine 属主 cmd/gateway，ctx 退出；
// sessionStore.RunSweep 透传——sweep 与本 Service 共享同一会话表）。
func (s *Service) RunSweep(ctx context.Context) { s.sessions.RunSweep(ctx) }

// HandleSignal 处理一帧上行信令（两适配面共用入口）：fromUID 为鉴权 uid
// （连接身份，D4——sig.From 一律覆写为该值，客户端自报仅占位不采信）；
// toUID 为 Signal.to 解析结果（适配层 ParseUint 先例 HandleChat:113）；
// traceID 为上行帧链路锚（转发帧回显）。返回 nil=转发成功（成功路径静默，
// 无 ack 帧——D6；错误路径返回 7101/7102/5001 供上层组 CmdError）。
func (s *Service) HandleSignal(_ context.Context, fromUID, toUID uint64, sig *protocol.Signal, traceID string) *pkgerrors.Error {
	if sig == nil {
		return pkgerrors.ErrMessageDecode // 防御：适配层已保证非 nil
	}
	sig.From = strconv.FormatUint(fromUID, 10) // D4：鉴权 uid 权威覆写

	callID := sig.GetCallId()
	switch sig.GetSignalType() {
	case protocol.SignalTypeInvite:
		if !s.sessions.register(callID, fromUID, toUID) {
			return pkgerrors.ErrSignalSession // 7102：重复/空 callId（原会话不变）
		}
		if e := s.deliver(toUID, sig, traceID); e != nil {
			// D3：invite 阶段对端不可达不拆会话——主叫可换 callId 重拨；
			// 本 callId 会话由 TTL 兜底回收。
			return e // 7101
		}
		s.logger.Info("signal session established",
			zap.String("callId", callID),
			zap.Uint64("caller", fromUID), zap.Uint64("callee", toUID),
			zap.String("traceId", traceID))
		return nil

	case protocol.SignalTypeOffer, protocol.SignalTypeAnswer, protocol.SignalTypeICECandidate:
		peer, ok := s.sessions.peer(callID, fromUID)
		if !ok {
			return pkgerrors.ErrSignalSession // 7102：不在册/已结束/非参与者
		}
		// W4-M05 plan D10 收紧（M04-C2 移交）：参与者帧的 to 必须是对端参与者
		//（7102）——在册会话存在明确参照，不接受向会话外第三方转发。
		if toUID != peer {
			s.logger.Warn("signal target rejected (not session peer)",
				zap.String("callId", callID),
				zap.Uint64("from", fromUID), zap.Uint64("to", toUID),
				zap.Uint64("peer", peer),
				zap.Int32("signalType", sig.GetSignalType()),
				zap.String("traceId", traceID))
			return pkgerrors.ErrSignalSession // 7102
		}
		if e := s.deliver(toUID, sig, traceID); e != nil {
			s.sessions.remove(callID) // D3：转发阶段对端不可达附带拆会话
			return e                  // 7101
		}
		// C2 裁决 D1（P2-1 修复）：转发成功刷新 lastSeen——P2P 媒体期长通话
		//（无服务端帧）不被 TTL sweep 误删（CAP-305；invite 不刷：register
		// 即建；deliver 失败路径已拆会话无需刷）。
		s.sessions.touch(callID)
		s.logger.Debug("signal frame forwarded",
			zap.Int32("signalType", sig.GetSignalType()),
			zap.String("callId", callID),
			zap.Uint64("from", fromUID), zap.Uint64("to", toUID),
			zap.String("traceId", traceID))
		return nil

	case protocol.SignalTypeHangup:
		peer, isParticipant := s.sessions.peer(callID, fromUID)
		if !isParticipant {
			// C2 联调修正（P2-1 媒体期边界）：纯 P2P 媒体期无服务端信令帧，
			// lastSeen 不续期，超 TTL 会话被回收后挂断会丢——挂断是幂等
			// 清理信号，对不存在/已结束会话**best-effort 向对端转发且不报
			// 7102**（宁转发勿丢弃：对端按 callId 匹配收尾，未知会话挂断
			// 由客户端幂等忽略）。迟到 offer/answer/ICE 仍 7102（防会话外
			// 扰），仅 hangup 宽容。宽容路径 to 任意维持（无会话可参照，
			// M04-C2 裁决不变，W4-M05 D10 再确认）。
			if e := s.deliver(toUID, sig, traceID); e != nil {
				return e // 7101：对端离线（发送方收 7101 即终态）
			}
			s.logger.Info("signal hangup forwarded for expired/unknown session",
				zap.String("callId", callID),
				zap.Uint64("by", fromUID), zap.Uint64("peer", toUID),
				zap.String("traceId", traceID))
			return nil
		}
		// 在册分支的 to 收紧（W4-M05 plan D10）：在册会话有明确对端参照，
		// to 非对端 → 7102（与 offer/answer/ICE 同判据）。
		if toUID != peer {
			s.logger.Warn("signal target rejected (not session peer)",
				zap.String("callId", callID),
				zap.Uint64("from", fromUID), zap.Uint64("to", toUID),
				zap.Uint64("peer", peer),
				zap.Int32("signalType", sig.GetSignalType()),
				zap.String("traceId", traceID))
			return pkgerrors.ErrSignalSession // 7102
		}
		s.sessions.remove(callID) // 先拆（推失败也已拆——hangup 语义优先）
		if e := s.deliver(toUID, sig, traceID); e != nil {
			return e // 7101：对端已离线（会话已拆，客户端收 7101 即终态）
		}
		s.logger.Info("signal session closed (hangup)",
			zap.String("callId", callID),
			zap.Uint64("by", fromUID), zap.Uint64("peer", toUID),
			zap.String("traceId", traceID))
		return nil

	default:
		// D6：signalType 须为五型之一，非法值复用 5001 面（解码语义）。
		return pkgerrors.Wrap(
			fmt.Errorf("invalid signalType %d", sig.GetSignalType()),
			pkgerrors.ErrMessageDecode.Code, pkgerrors.ErrMessageDecode.Message)
	}
}

// deliver 对端投递（D3）：Online=false 或 Push 失败均归一 7101（显式回执
// ——补齐 go-chat 静默丢弃缺陷）；PushSignal 实现保证非阻塞/有界。
func (s *Service) deliver(toUID uint64, sig *protocol.Signal, traceID string) *pkgerrors.Error {
	if s.router == nil || !s.router.Online(toUID) {
		s.logger.Warn("signal peer offline",
			zap.Uint64("to", toUID), zap.String("callId", sig.GetCallId()),
			zap.Int32("signalType", sig.GetSignalType()), zap.String("traceId", traceID))
		return pkgerrors.ErrSignalPeerOffline
	}
	if err := s.router.PushSignal(toUID, sig, traceID); err != nil {
		s.logger.Warn("signal push failed (treated as offline)",
			zap.Uint64("to", toUID), zap.String("callId", sig.GetCallId()),
			zap.Int32("signalType", sig.GetSignalType()), zap.String("traceId", traceID),
			zap.Error(err))
		return pkgerrors.Wrap(err, pkgerrors.ErrSignalPeerOffline.Code, pkgerrors.ErrSignalPeerOffline.Message)
	}
	return nil
}
