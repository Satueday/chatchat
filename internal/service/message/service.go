package message

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	stderrors "errors"
	"fmt"
	"strconv"
	"time"

	"chatchat/internal/dao"
	"chatchat/internal/model"
	pkgerrors "chatchat/pkg/errors"
	"chatchat/pkg/global"
	"chatchat/pkg/protocol"

	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
)

// Service 为单聊 Logic 服务（W2-M03）：上行 CmdChat 帧 → 校验 → 服务端
// 权威覆写 → seq → 落库 → Deliverer（noop 占位）。
// W2-M05-C2：seq 供给收窄为 PendingSeqGen（原子分配+在途标记），落库成败
// 均 best-effort 清除标记（裁决 D9，见 clearPendingBestEffort）。
// W3-M02：注入单聊好友门槛 ChatGate（5005 之后、seq 分配之前拒绝非好友，
// plan §4 裁决 D4）。
// W3-M04：增群聊链路 HandleGroupChat（CmdGroupChat → g_{groupId} 会话，
// 校验链 §6.1 冻结顺序）与群域依赖 groups/gmembers（直接依赖 dao 接口，
// service/message 已 import dao 有先例；nil=群链路不可用，单聊不受影响）。
// W4-M01：增计数埋点 counter（落库成功后同步调一次，D2）。
// W4-M02：增热点专属通道 hotDeliver（W4-M02-plan D2/D3）——HandleGroupChat
// 复用群存在校验 GetByID 行的 HotLevel 做行快照路由：HotLevel>0 且
// hotDeliver≠nil → hot 分支（跳过成员快照、信封 Members 空、投递
// hotDeliver 到 chatchat-hot-group）；else → 普通通道路径原样。nil=
// 热点消息降级走普通 deliverer（memory 降级模式装配形态，plan §9）。
//
// 分层约定：*protocol.ChatMessage 只在本入口解码，DAO 一律 *model.Message
// （go-chat SaveMessage(*protocol.Message) proto 穿透反例规避）；本包不
// import gin/gorm/internal/server/hotdetect（server.Handler 适配由 cmd
// 装配层承担，hotdetect.Recorder 经下方窄接口结构化满足——服务保持网关
// 无关与热点实现无关）。依赖全构造注入（repo/users/seq/deliverer/hotDeliver/
// gate/groups/gmembers/counter/logger）。
type Service struct {
	repo       dao.MessageRepo
	users      dao.UserRepo
	seq        dao.PendingSeqGen
	deliverer  Deliverer
	hotDeliver Deliverer
	gate       ChatGate
	groups     dao.GroupRepo
	gmembers   dao.GroupMemberRepo
	counter    GroupMsgCounter
	stats      MsgStats
	logger     *zap.Logger
}

// GroupMsgCounter 群消息计数窄接口（W4-M01 plan D2）：HandleGroupChat 在
// 消息落库成功后、「group message saved」日志后、成员快照前同步调用一次
// （本地 Redis pipeline 亚毫秒级）；hotdetect.Recorder 结构化满足，本包不
// import hotdetect（ChatGate/Deliverer 先例，装配层连接）。nil=不计数
// （单测/骨架装配容忍，groups/gmembers nil 先例同款）——计数失败仅 Warn
// 不影响发送返回（消息已落库即事实，统计非正确性数据，下周期聚合自愈）。
type GroupMsgCounter interface {
	// RecordGroupMsg 对一条已落库成功的群消息计数；错误原样上抛由调用方
	// Warn 消化（基础设施错误如实传播，不打业务码）。
	RecordGroupMsg(ctx context.Context, groupID uint64) error
}

// MsgStats 上行消息接收计数窄接口（W4-M05 plan D5）：HandleChat/
// HandleGroupChat 入口（cmd 类型校验通过后）同步调用一次——gateway expvar
// msg_received_total（单聊/群聊分列）的埋点面。nil=不计数（单测/骨架装配
// 容忍，counter/gate 先例）；实现须并发安全（expvar.Int 满足）。
type MsgStats interface {
	// ChatReceived 单聊上行帧入口计数。
	ChatReceived()
	// GroupChatReceived 群聊上行帧入口计数。
	GroupChatReceived()
}

// ChatGate 单聊发送门槛窄接口（W3-M02，plan D4）：HandleChat 在接收者
// 存在性校验（5005）之后、ConvID 计算/seq 分配之前调用，返回非 nil 即
// 拒绝（避免为将被拒绝的消息分配 seq）；nil 依赖=不设门槛（单测/旧装配
// 兼容）。包内定义+装配注入的结构化类型反转（PendMarker/Pusher 同款
// 先例）——本包不 import friend，实现在 internal/service/friend，
// cmd 装配层连接。
type ChatGate interface {
	// AllowChat 判定 fromUID→toUID 单聊是否放行（nil=放行）；
	// 返回 *pkgerrors.Error 由 HandleChat 透传（CmdError 回帧）。
	AllowChat(ctx context.Context, fromUID, toUID uint64) *pkgerrors.Error
}

// NewService 构造单聊 Logic 服务。seq 为 W2-M05-C2 收窄后的 PendingSeqGen
// （同位数替换 M03 的 SeqGen，装配点全枚举更新）；gate 为 W3-M02 增参
// （第 5 位，nil=不设门槛）；groups/gmembers 为 W3-M04 增参（counter 前；
// 群聊链路依赖，nil=群聊链路拒绝使用——单聊不受影响）；counter 为
// W4-M01 增参（logger 前，nil=不计数）；hotDeliver 为 W4-M02 增参
// （counter 前，nil=热点消息降级走普通 deliverer——memory 降级模式装配
// 形态，W4-M02-plan D3；普通路径零波及）；stats 为 W4-M05 增参
// （logger 前，nil=不计数——上行接收计数埋点，plan D5）。
func NewService(repo dao.MessageRepo, users dao.UserRepo, seq dao.PendingSeqGen, d Deliverer, gate ChatGate, groups dao.GroupRepo, gmembers dao.GroupMemberRepo, hotDeliver Deliverer, counter GroupMsgCounter, stats MsgStats, logger *zap.Logger) *Service {
	if logger == nil {
		logger = zap.NewNop()
	}
	return &Service{repo: repo, users: users, seq: seq, deliverer: d, hotDeliver: hotDeliver, gate: gate, groups: groups, gmembers: gmembers, counter: counter, stats: stats, logger: logger}
}

// HandleChat 处理网关上行帧：仅 CmdChat(3) 走单聊链路，其余 cmd 记日志
// 忽略（心跳已被网关消化；ACK/拉取经装配层 AckHandler/PullHandler 分派，
// 群聊 CmdGroupChat 走 HandleGroupChat 独立入口——W3-M04 起 wire 显式
// 分派，不再落入本方法）。
// 返回 *pkgerrors.Error（nil=成功），由装配层 adapter 转 server.SendError
// 回 CmdError 帧；校验链与覆写顺序见 W2-M03-plan §6.3（冻结）。
func (s *Service) HandleChat(ctx context.Context, uid uint64, frame *protocol.Frame) *pkgerrors.Error {
	if frame.GetHeader().GetCmd() != protocol.CmdChat {
		s.logger.Debug("ignore non-chat frame",
			zap.Int32("cmd", frame.GetHeader().GetCmd()), zap.Uint64("uid", uid))
		return nil
	}
	if s.stats != nil {
		s.stats.ChatReceived() // W4-M05 D5：上行接收计数（入口，nil 容忍）
	}

	cm := &protocol.ChatMessage{}
	if err := proto.Unmarshal(frame.GetPayload(), cm); err != nil {
		return pkgerrors.Wrap(err, pkgerrors.ErrMessageDecode.Code, pkgerrors.ErrMessageDecode.Message)
	}

	toUID, err := strconv.ParseUint(cm.GetTo(), 10, 64)
	if err != nil {
		return pkgerrors.Wrap(err, pkgerrors.ErrInvalidParam.Code, pkgerrors.ErrInvalidParam.Message)
	}
	if toUID == uid {
		return pkgerrors.ErrMessageSelfSend
	}
	if len(cm.GetContent()) > global.MessageContentMaxBytes {
		return pkgerrors.ErrContentTooLarge
	}
	if _, err := s.users.GetByID(ctx, toUID); err != nil {
		if isUserNotFound(err) {
			return pkgerrors.Wrap(err, pkgerrors.ErrReceiverNotFound.Code, pkgerrors.ErrReceiverNotFound.Message)
		}
		s.logger.Error("receiver lookup failed", zap.Uint64("to", toUID), zap.Error(err))
		return pkgerrors.ErrInternal
	}
	if s.gate != nil {
		if e := s.gate.AllowChat(ctx, uid, toUID); e != nil {
			return e // 3005（gate 开+非好友）；顺序在 seq 分配前
		}
	}

	convID := ConvID(uid, toUID)
	msgID := newMsgID()
	// D9 写路径时序：NextPending（原子分配+在途标记）→ Create → 成败均
	// best-effort ClearPending。失败 fail-closed（5002，零 Create）：seq
	// 供给管线故障时不确定是否已登记标记，宁可拒绝写。
	seq, err := s.seq.NextPending(ctx, convID)
	if err != nil {
		s.logger.Error("seq generate failed", zap.String("conversationId", convID), zap.Error(err))
		return pkgerrors.Wrap(err, pkgerrors.ErrSeqGenerate.Code, pkgerrors.ErrSeqGenerate.Message)
	}

	contentType := cm.GetContentType()
	if contentType == 0 {
		contentType = protocol.ContentTypeText
	}
	msg := &model.Message{
		MsgID:          msgID,
		ConversationID: convID,
		FromUID:        uid,
		ToUID:          toUID,
		ContentType:    contentType,
		Content:        cm.GetContent(),
		URL:            cm.GetUrl(),
		MsgSeq:         seq,
		// P2 回补（W2-M03 遗留）：显式赋值 UTC 时间，落库值不再依赖 ORM
		// 自动填充；下行帧 timestamp=CreatedAt.UnixMilli() 由投递层消费。
		CreatedAt: time.Now().UTC(),
	}
	if err := s.repo.Create(ctx, msg); err != nil {
		s.clearPendingBestEffort(ctx, convID, seq) // D9：失败分支顺手清（该 seq 成为永久空洞，标记滞留只会无谓截断）
		s.logger.Error("message save failed",
			zap.String("msgId", msgID), zap.String("conversationId", convID),
			zap.Uint64("msgSeq", seq), zap.Error(err))
		return pkgerrors.Wrap(err, pkgerrors.ErrMessageSave.Code, pkgerrors.ErrMessageSave.Message)
	}
	s.clearPendingBestEffort(ctx, convID, seq) // D9：成功分支必清（在途停滞窗缩至毫秒级）

	s.logger.Info("message saved",
		zap.String("traceId", frame.GetHeader().GetTraceId()),
		zap.String("msgId", msgID),
		zap.String("conversationId", convID),
		zap.Uint64("from", uid), zap.Uint64("to", toUID),
		zap.Uint64("msgSeq", seq))

	// 投递尽力而为：错误不回滚落库、不影响成功返回（Deliverer 契约）。
	// traceId 以包内私有 ctx key 注入（裁决 D9：投递层读取写入下行帧
	// Header.TraceId，实现上行→落库日志→下行帧全链贯穿）。
	if err := s.deliverer.Deliver(withTraceID(ctx, frame.GetHeader().GetTraceId()), msg, []uint64{toUID}); err != nil {
		s.logger.Warn("deliver failed (message already saved)",
			zap.String("msgId", msgID), zap.Error(err))
	}
	return nil
}

// groupConvPrefix 群会话 ID 前缀（spec §5：conv=g_{groupId}）。
// 写路径拼接（groupConvID）、读侧成员判定（PullService）、下行帧 cmd 对称
// （buildChatFrame）共用同一常量——前缀口径单点冻结。
const groupConvPrefix = "g_"

// groupConvID 服务端拼接群会话 ID（W3-M04 裁决 D2：不信任客户端 conv
// 自报，由 groupId 唯一构造）。
func groupConvID(groupID uint64) string {
	return groupConvPrefix + strconv.FormatUint(groupID, 10)
}

// HandleGroupChat 处理群聊上行帧（W3-M04，CmdGroupChat=4）。校验链顺序
// 冻结（plan §6.1）：解码 5001 → to 解析 groupId 1001 → 内容超长 5006 →
// 群存在 4001 → 发送者是成员 4002 → 未禁言 5010（muted_until > now(UTC)，
// W3-M01 口径）→ conv=g_{groupId} → NextPending 5002 → Create 5003（成败
// 均 best-effort 清 pend，D9 时序沿用）→ 发送时成员快照（ListByGroup，
// 裁决 D9）→ Deliver(msg, members)。
// 与单聊的差异：无好友门槛（群聊不适用 gate）、无 5004 自发校验
// （to=groupId 异域）；ToUID 列承载 groupId（uint64 同列，conv 前缀区分
// 语义，裁决 D10——与 DDL message.to_uid 注释一致）。
// members=全成员含发送者自身（写扩散全员，客户端按 conv+from 判定自发
// 消息去重呈现；M05 信箱语义）；快照失败 → Warn+跳过 Deliver（消息已
// 落库，拉取兜底），仍返回 nil（发送成功语义保留）。
func (s *Service) HandleGroupChat(ctx context.Context, uid uint64, frame *protocol.Frame) *pkgerrors.Error {
	if frame.GetHeader().GetCmd() != protocol.CmdGroupChat {
		s.logger.Debug("ignore non-group-chat frame",
			zap.Int32("cmd", frame.GetHeader().GetCmd()), zap.Uint64("uid", uid))
		return nil
	}
	if s.stats != nil {
		s.stats.GroupChatReceived() // W4-M05 D5：上行接收计数（入口，nil 容忍）
	}
	if s.groups == nil || s.gmembers == nil {
		// 装配错误（群域 repo 未注入）：拒绝使用而非 panic——单聊链路不受
		// 影响（cmd/logic 骨架注入 nil 的形态见 ADR-0008）。
		s.logger.Error("group chat handler used without group repos (assembly error)",
			zap.Uint64("uid", uid))
		return pkgerrors.ErrInternal
	}

	cm := &protocol.ChatMessage{}
	if err := proto.Unmarshal(frame.GetPayload(), cm); err != nil {
		return pkgerrors.Wrap(err, pkgerrors.ErrMessageDecode.Code, pkgerrors.ErrMessageDecode.Message)
	}

	groupID, err := strconv.ParseUint(cm.GetTo(), 10, 64)
	if err != nil {
		return pkgerrors.Wrap(err, pkgerrors.ErrInvalidParam.Code, pkgerrors.ErrInvalidParam.Message)
	}
	if len(cm.GetContent()) > global.MessageContentMaxBytes {
		return pkgerrors.ErrContentTooLarge
	}
	// 行快照路由判定源（W4-M02 D2）：群存在校验既有读返回行携带 HotLevel
	// ——判定复用该行快照，零额外查询；判定时点=本时刻（路由快照语义，
	// 切换窗用例按此快照断言预期通道）。GetByID 失败即发送拒绝（4001/
	// ErrInternal 既有语义，正确性保持——无独立降级面）。
	groupRow, err := s.groups.GetByID(ctx, groupID)
	if err != nil {
		if isGroupNotFound(err) {
			return pkgerrors.Wrap(err, pkgerrors.ErrGroupNotFound.Code, pkgerrors.ErrGroupNotFound.Message)
		}
		s.logger.Error("group lookup failed", zap.Uint64("groupId", groupID), zap.Error(err))
		return pkgerrors.ErrInternal
	}
	member, err := s.gmembers.GetMember(ctx, groupID, uid)
	if err != nil {
		if isNotGroupMember(err) {
			return pkgerrors.Wrap(err, pkgerrors.ErrNotGroupMember.Code, pkgerrors.ErrNotGroupMember.Message)
		}
		s.logger.Error("group member lookup failed",
			zap.Uint64("groupId", groupID), zap.Uint64("uid", uid), zap.Error(err))
		return pkgerrors.ErrInternal
	}
	// 禁言判定：muted_until > now(UTC)（W3-M01 冻结口径；nil/过去时刻=未禁言，
	// 等于此刻不算——严格大于）。
	if until := member.MutedUntil; until != nil && until.After(time.Now().UTC()) {
		return pkgerrors.ErrMemberMuted
	}

	convID := groupConvID(groupID)
	msgID := newMsgID()
	// D9 写路径时序沿用（W2-M05-C2）：NextPending → Create → 成败均清标记。
	seq, err := s.seq.NextPending(ctx, convID)
	if err != nil {
		s.logger.Error("seq generate failed", zap.String("conversationId", convID), zap.Error(err))
		return pkgerrors.Wrap(err, pkgerrors.ErrSeqGenerate.Code, pkgerrors.ErrSeqGenerate.Message)
	}

	contentType := cm.GetContentType()
	if contentType == 0 {
		contentType = protocol.ContentTypeText
	}
	msg := &model.Message{
		MsgID:          msgID,
		ConversationID: convID,
		FromUID:        uid,
		ToUID:          groupID, // 裁决 D10：ToUID 列承载 groupId，语义由 conv 前缀区分
		ContentType:    contentType,
		Content:        cm.GetContent(),
		URL:            cm.GetUrl(),
		MsgSeq:         seq,
		CreatedAt:      time.Now().UTC(),
	}
	if err := s.repo.Create(ctx, msg); err != nil {
		s.clearPendingBestEffort(ctx, convID, seq)
		s.logger.Error("message save failed",
			zap.String("msgId", msgID), zap.String("conversationId", convID),
			zap.Uint64("msgSeq", seq), zap.Error(err))
		return pkgerrors.Wrap(err, pkgerrors.ErrMessageSave.Code, pkgerrors.ErrMessageSave.Message)
	}
	s.clearPendingBestEffort(ctx, convID, seq)

	s.logger.Info("group message saved",
		zap.String("traceId", frame.GetHeader().GetTraceId()),
		zap.String("msgId", msgID),
		zap.String("conversationId", convID),
		zap.Uint64("from", uid), zap.Uint64("groupId", groupID),
		zap.Uint64("msgSeq", seq))

	// 热点计数埋点（W4-M01 plan D2：落库成功后、成员快照前，同步一次
	// Redis pipeline）：失败仅 Warn 不影响发送返回（统计非正确性数据，
	// 下周期聚合自愈）；nil=不计数（骨架/单测装配）。
	if s.counter != nil {
		if err := s.counter.RecordGroupMsg(ctx, groupID); err != nil {
			s.logger.Warn("group msg count failed (message already saved)",
				zap.Uint64("groupId", groupID),
				zap.Uint64("msgSeq", seq),
				zap.Error(err))
		}
	}

	// 热点专属通道路由（W4-M02 plan D2）：行快照 HotLevel>0 且 hotDeliver
	// 注入 → hot 分支——跳过成员快照（Timeline 写不需要 members，500 成员
	// 快照查询同步省去）、信封 Members 空、投递 hotDeliver（chatchat-hot-
	// group，分区键 conversationId 同群保序）；消费侧写 group_timeline 一行
	// （写放大 500→1，hot 分支绝不写 inbox_delivery——唯一例外为摘标切换
	// P3 回写）。hotDeliver=nil（memory 降级模式）→ 落入普通通道（写扩散
	// 照常，读扩散不承载）。投递 fire-and-forget（Deliverer 契约）：错误仅
	// Warn——消息已落库，Timeline 缺口由切换 P1 补齐自愈兜底。
	if groupRow.HotLevel > 0 && s.hotDeliver != nil {
		if err := s.hotDeliver.Deliver(withTraceID(ctx, frame.GetHeader().GetTraceId()), msg, nil); err != nil {
			s.logger.Warn("hot deliver failed (message already saved, timeline backfill will heal)",
				zap.String("msgId", msgID),
				zap.String("conversationId", convID),
				zap.Uint64("groupId", groupID),
				zap.Uint64("msgSeq", seq),
				zap.Error(err))
		}
		return nil
	}

	// 发送时成员快照（裁决 D9）：ListByGroup ≤500（JoinUnderLimit 上限保证），
	// 含发送者自身；发送后入群的新成员不在本条消息投递列表（快照语义固化，
	// CASE-I07）。快照失败=Deliver 输入不可得：落库已成事实，Warn+跳过
	// 投递（离线路径由拉取兜底），不向发送方报错。
	rows, err := s.gmembers.ListByGroup(ctx, groupID)
	if err != nil {
		s.logger.Warn("group member snapshot failed (message already saved, skip deliver)",
			zap.String("msgId", msgID),
			zap.String("conversationId", convID),
			zap.Uint64("groupId", groupID),
			zap.Uint64("msgSeq", seq),
			zap.Error(err))
		return nil
	}
	members := make([]uint64, 0, len(rows))
	for _, r := range rows {
		members = append(members, r.UserID)
	}

	// 投递尽力而为（Deliverer 契约）：错误不回滚落库、不影响成功返回。
	if err := s.deliverer.Deliver(withTraceID(ctx, frame.GetHeader().GetTraceId()), msg, members); err != nil {
		s.logger.Warn("deliver failed (message already saved)",
			zap.String("msgId", msgID),
			zap.String("conversationId", convID),
			zap.Uint64("msgSeq", seq),
			zap.Int("memberCount", len(members)),
			zap.Error(err))
	}
	return nil
}

// isGroupNotFound 判定错误是否群未命中哨兵（4001，含 wrap 链穿透）。
func isGroupNotFound(err error) bool {
	var e *pkgerrors.Error
	return stderrors.As(err, &e) && e.Code == pkgerrors.ErrGroupNotFound.Code
}

// isNotGroupMember 判定错误是否群成员未命中哨兵（4002，含 wrap 链穿透）。
func isNotGroupMember(err error) bool {
	var e *pkgerrors.Error
	return stderrors.As(err, &e) && e.Code == pkgerrors.ErrNotGroupMember.Code
}

// clearPendingBestEffort 尽力清除 seq 在途标记（W2-M05-C2 裁决 D9）：
// Create 成败两分支均调用；清除失败仅 Warn——残留键使拉取侧把该 seq 视为
// 在途（安全方向的假阳性：截断重拉而非跳过丢失），≤global.PendingMarkerTTL
// 自动回收，消息不丢。
func (s *Service) clearPendingBestEffort(ctx context.Context, convID string, seq uint64) {
	if err := s.seq.ClearPending(ctx, convID, seq); err != nil {
		s.logger.Warn("pend marker clear failed (best-effort)",
			zap.String("conversationId", convID),
			zap.Uint64("msgSeq", seq),
			zap.Error(err))
	}
}

// ConvID 生成单聊会话 ID：uid 数值升序拼接 "min|max"（W2-M03 裁决 D1：
// 数值序非字典序，避免 "10"<"9" 错序；同 pair 无序入参同结果）。
func ConvID(uidA, uidB uint64) string {
	if uidA > uidB {
		uidA, uidB = uidB, uidA
	}
	return strconv.FormatUint(uidA, 10) + "|" + strconv.FormatUint(uidB, 10)
}

// newMsgID 生成全局唯一消息 ID：crypto/rand 16 字节 hex（32 字符，
// 对齐 W2-M02 newTraceID 先例；uk_msg_id 兜底唯一性）。
func newMsgID() string {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		// crypto/rand 失败极罕见（系统熵池枯竭）：走兜底路径。
		return fallbackMsgID()
	}
	return hex.EncodeToString(b)
}

// fallbackMsgID 为 newMsgID 的熵池枯竭兜底：纳秒时间戳定宽 32 hex 字符
// （P3 回补：与正常路径长度恒一致；纳秒粒度保唯一概率，冲突时 uk_msg_id
// 拦截返回 5003，不静默丢消息）。
func fallbackMsgID() string {
	return fmt.Sprintf("%032x", time.Now().UnixNano())
}

// isUserNotFound 判定错误是否用户未命中哨兵（2002，含 wrap 链穿透）。
func isUserNotFound(err error) bool {
	var e *pkgerrors.Error
	return stderrors.As(err, &e) && e.Code == pkgerrors.ErrUserNotFound.Code
}
