package pushpull

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"chatchat/internal/dao"
	"chatchat/internal/model"
	"chatchat/internal/mq"
	pkgerrors "chatchat/pkg/errors"

	"go.uber.org/zap"
)

// notifyTimeout 轻通知闭包内单次外部 I/O 链（成员查/在线查/发布）的
// 总预算——通知是可丢旁路，慢依赖不得长期占用池 worker（agent.md §13.2
// 外部 I/O 有超时）。
const notifyTimeout = 5 * time.Second

// HotService 为热点群专属通道消费服务（W4-M02 plan D4，mq.Handler 形态；
// 与既有 Service 并列，独立 Consumer/Handler/协程资源——ADR-0010 D1）：
// 消费 chatchat-hot-group 信封（members 空）→ group_timeline 恰一行幂等
// 写入（读扩散写一份：500 人群 1 消息 = 1 行 Timeline + 0 行信箱，
// CAP-103）。
//
// W4-M03 轻通知接线（plan D3，CAP-201）：Timeline Append 成功后经
// NotifyDispatcher.Submit 通知闭包（先存储后信号顺序，gotify 先例）——
// 闭包内 ListByGroup 成员发现 → OnlineMany 批量在线判定 → 在线列表空则
// 零发送 → 否则 push topic 单信封（kind=1、members=在线列表、message 携
// conv/seq 锚、traceId 透传）。通知可丢：闭包内任一步失败 Error 日志
// （6003 语境码值字段）不重试——拉取兜底；Submit 被丢（池满/已关）Warn。
// 聚合窗口显式不做（plan §2 延后项）。
//
// 错误分类（与既有 Service D9 同构）：解码失败=确定性毒信封，Error 日志
// + return nil 跳过（重投无意义，防分区停滞）；Timeline 写失败=return err
// 重投（至少一次；uk_group_seq DoNothing 幂等吸收重复=恰一份写）。Timeline
// 写入同步于 Handle 内、绝不入旁路池（commit 语义绑定，plan D5）。
//
// 轻通知依赖面（gmembers/presence/producer/dispatcher）任一未注入 → 通知
// 链停用（M02 中间态保持：无推送，仅 Timeline 写入——安全失败先例
// PullOptions.GroupMembers）。本包不 import gin/gorm/sarama；消费 goroutine
// 属主为 cmd/pushpull 装配层。
type HotService struct {
	timeline   dao.GroupTimelineRepo
	gmembers   dao.GroupMemberRepo
	presence   dao.PresenceChecker
	producer   mq.Producer
	pushTopic  string
	dispatcher *NotifyDispatcher
	stats      Stats
	logger     *zap.Logger
}

// 编译期接口实现断言（Handle 方法签名即 mq.Handler）。
var _ mq.Handler = (*HotService)(nil).Handle

// NewHotService 构造 hot 消费服务。timeline 为 group_timeline 仓储（必需）；
// gmembers/presence/producer/pushTopic/dispatcher 为轻通知依赖面（W4-M03
// plan D3）——任一零值（nil/空串）即通知链停用，仅 Timeline 写入（M02
// 行为冻结面，测试/旧装配兼容）；stats 为 W4-M05 增参（nil=不观测——消费
// 入口计数埋点，plan D5）；logger 为 nil 时以 zap.NewNop 兜底
// （NewService 先例）。
func NewHotService(timeline dao.GroupTimelineRepo, gmembers dao.GroupMemberRepo,
	presence dao.PresenceChecker, producer mq.Producer, pushTopic string,
	dispatcher *NotifyDispatcher, stats Stats, logger *zap.Logger) *HotService {
	if logger == nil {
		logger = zap.NewNop()
	}
	return &HotService{
		timeline:   timeline,
		gmembers:   gmembers,
		presence:   presence,
		producer:   producer,
		pushTopic:  pushTopic,
		dispatcher: dispatcher,
		stats:      stats,
		logger:     logger,
	}
}

// Handle 实现 mq.Handler：解码信封 → Timeline Append（同步幂等）→ return
// nil commit。信封 Message 全量字段照携（hot 分支路由侧契约），本方法消费
// conv（g_{groupId} 反解 groupID）/msg_seq/msg_id 三字段；TraceID 透传日志
// （C7：gateway→hot 信封→pushpull 日志 traceId 贯穿）。
//
// conv 非法（无 g_ 前缀/非数值群 ID）按毒信封跳过：路由侧仅产生 g_ 前缀
// 信封（groupConvID 单点构造），非 g_ 即确定性不可处理，重投无意义。
func (s *HotService) Handle(ctx context.Context, m *mq.Message) error {
	if m == nil {
		return nil // mq 层保证非 nil；防御性跳过（Service 同款）
	}
	if s.stats != nil {
		s.stats.Consumed() // W4-M05 D5：消费入口计数（nil 容忍）
	}
	d, err := mq.UnmarshalDelivery(m.Value)
	if err != nil {
		s.logger.Error("pushpull hot: 信封解码失败，跳过（确定性毒消息）",
			zap.String("topic", m.Topic),
			zap.Int32("partition", m.Partition),
			zap.Int64("offset", m.Offset),
			zap.Error(err))
		return nil
	}
	msg := d.Message
	if msg == nil {
		s.logger.Error("pushpull hot: 信封缺消息体，跳过（确定性毒消息）",
			zap.String("topic", m.Topic),
			zap.Int32("partition", m.Partition),
			zap.Int64("offset", m.Offset))
		return nil
	}
	groupID, gerr := parseGroupConvID(msg.ConversationID)
	if gerr != nil {
		s.logger.Error("pushpull hot: 会话前缀非法，跳过（确定性毒消息）",
			zap.String("topic", m.Topic),
			zap.Int32("partition", m.Partition),
			zap.Int64("offset", m.Offset),
			zap.String("conversationId", msg.ConversationID),
			zap.Error(gerr))
		return nil
	}

	// Timeline 幂等写入（CAP-103）：同 (group,seq) 重投恰一份（uk DoNothing）。
	// 同步执行——失败 return err 不 commit，mq 重投；Timeline 写入不入池。
	if err := s.timeline.Append(ctx, &model.GroupTimeline{
		GroupID: groupID,
		MsgSeq:  msg.MsgSeq,
		MsgID:   msg.MsgID,
	}); err != nil {
		s.logger.Error("pushpull hot: Timeline 写入失败，重投",
			zap.String("traceId", d.TraceID),
			zap.String("msgId", msg.MsgID),
			zap.String("conversationId", msg.ConversationID),
			zap.Uint64("groupId", groupID),
			zap.Uint64("msgSeq", msg.MsgSeq),
			zap.Error(err))
		return fmt.Errorf("pushpull hot: Timeline 写入失败: %w", err)
	}

	// 写放大可观测（plan §4）：Debug 级避免刷屏（Info 仅错误与切换事件）。
	s.logger.Debug("pushpull hot: Timeline 行写入完成",
		zap.String("traceId", d.TraceID),
		zap.String("msgId", msg.MsgID),
		zap.Uint64("groupId", groupID),
		zap.Uint64("msgSeq", msg.MsgSeq),
		zap.String("topic", m.Topic))

	// 轻通知接线（W4-M03 plan D3）：Timeline 成功后 Submit 通知闭包——
	// 先存储后信号（gotify 先例）；Timeline 写入本身不入池（上方同步完成）。
	s.submitNotify(groupID, msg, d.TraceID)
	return nil
}

// canNotify 轻通知依赖面完整性判定（任一缺失即停用通知链——M02 行为
// 冻结面：仅 Timeline 写入）。
func (s *HotService) canNotify() bool {
	return s.gmembers != nil && s.presence != nil &&
		s.producer != nil && s.pushTopic != "" && s.dispatcher != nil
}

// submitNotify 向旁路池提交轻通知闭包（Timeline 写成功后调用）。闭包仅
// 捕获依赖接口与值（无连接句柄，plan §6）；Submit 被拒/被丢（池满或已关）
// Warn——通知可丢，拉取兜底（CAP-201 不变量）。
func (s *HotService) submitNotify(groupID uint64, msg *model.Message, traceID string) {
	if !s.canNotify() {
		s.logger.Debug("pushpull hot: 轻通知依赖未装配，跳过（M02 行为冻结面）",
			zap.Uint64("groupId", groupID),
			zap.Uint64("msgSeq", msg.MsgSeq))
		return
	}
	// 闭包捕获面：接口（gmembers/presence/producer/logger）与值
	//（groupID/conv/msg/traceID）——不捕获 ctx（消费会话 ctx 随 Handle
	// 返回可能取消，闭包内自建带超时预算的独立 ctx）。
	gmembers, presence, producer := s.gmembers, s.presence, s.producer
	pushTopic, logger := s.pushTopic, s.logger
	conv := msg.ConversationID
	ok := s.dispatcher.Submit(func() {
		notifyOnlineMembers(gmembers, presence, producer, pushTopic, logger,
			groupID, conv, msg, traceID)
	})
	if !ok {
		s.logger.Warn("pushpull hot: 轻通知任务未入队（池满丢弃/池已关闭，拉取兜底）",
			zap.String("traceId", traceID),
			zap.String("msgId", msg.MsgID),
			zap.String("conversationId", conv),
			zap.Uint64("groupId", groupID),
			zap.Uint64("msgSeq", msg.MsgSeq))
	}
}

// notifyOnlineMembers 轻通知闭包体（池 worker 内执行，panic 由 dispatcher
// recover 吸收）：ListByGroup → OnlineMany → 在线列表空则零发送 → 否则
// push topic 单信封（kind=1、members=在线列表、message 携 conv/seq 锚、
// traceId 透传——gateway 侧按 kind 构 CmdNotify 帧，W4-M03 D2/D3）。
// 任一步失败 Error 日志（6003 语境码值字段）不重试——通知可丢，拉取兜底。
func notifyOnlineMembers(gmembers dao.GroupMemberRepo, presence dao.PresenceChecker,
	producer mq.Producer, pushTopic string, logger *zap.Logger,
	groupID uint64, conv string, msg *model.Message, traceID string) {
	ctx, cancel := context.WithTimeout(context.Background(), notifyTimeout)
	defer cancel()

	rows, err := gmembers.ListByGroup(ctx, groupID)
	if err != nil {
		logger.Error("pushpull hot: 轻通知成员查询失败，放弃（拉取兜底）",
			zap.String("traceId", traceID),
			zap.String("msgId", msg.MsgID),
			zap.Uint64("groupId", groupID),
			zap.Int32("code", errCodeOf(err, pkgerrors.ErrHotQueryFailed.Code)),
			zap.Error(err))
		return
	}
	if len(rows) == 0 {
		return // 空群零发送（成员发现面即空——零信封零 Send）
	}
	uids := make([]uint64, 0, len(rows))
	for _, r := range rows {
		uids = append(uids, r.UserID)
	}
	onlineMap, err := presence.OnlineMany(ctx, uids)
	if err != nil {
		logger.Error("pushpull hot: 轻通知在线判定失败，放弃（拉取兜底）",
			zap.String("traceId", traceID),
			zap.String("msgId", msg.MsgID),
			zap.Uint64("groupId", groupID),
			zap.Int("memberCount", len(uids)),
			zap.Int32("code", errCodeOf(err, pkgerrors.ErrHotQueryFailed.Code)),
			zap.Error(err))
		return
	}
	onlineUIDs := make([]uint64, 0, len(uids))
	for _, uid := range uids {
		if onlineMap[uid] {
			onlineUIDs = append(onlineUIDs, uid)
		}
	}
	if len(onlineUIDs) == 0 {
		logger.Debug("pushpull hot: 全员离线，零发送（拉取兜底）",
			zap.String("traceId", traceID),
			zap.Uint64("groupId", groupID),
			zap.Uint64("msgSeq", msg.MsgSeq))
		return
	}
	value, err := mq.MarshalDelivery(&mq.Delivery{
		Message: msg,
		Members: onlineUIDs,
		TraceID: traceID,
		Kind:    mq.DeliveryKindNotify,
	})
	if err != nil {
		// 防御分支（正常链路不可达，MarshalDelivery 前置 nil/seq 守卫同
		// 普通通道）：确定性编码失败放弃。
		logger.Error("pushpull hot: 轻通知信封编码失败，放弃",
			zap.String("traceId", traceID),
			zap.String("msgId", msg.MsgID),
			zap.Int("onlineCount", len(onlineUIDs)),
			zap.Error(err))
		return
	}
	if err := producer.Send(ctx, &mq.Message{
		Topic: pushTopic,
		Key:   []byte(conv), // 分区键 conversationId（与普通 push 同口径）
		Value: value,
	}); err != nil {
		logger.Error("pushpull hot: 轻通知信封发布失败，放弃（拉取兜底）",
			zap.String("traceId", traceID),
			zap.String("msgId", msg.MsgID),
			zap.Uint64("groupId", groupID),
			zap.Int("onlineCount", len(onlineUIDs)),
			zap.String("pushTopic", pushTopic),
			zap.Int32("code", errCodeOf(err, pkgerrors.ErrHotQueryFailed.Code)),
			zap.Error(err))
		return
	}
	// 下发可观测（plan §4 可观测性——M07 串联链末端衔接面）。
	logger.Info("pushpull hot: 轻通知已发布",
		zap.String("traceId", traceID),
		zap.Uint64("groupId", groupID),
		zap.String("conversationId", conv),
		zap.Uint64("maxSeq", msg.MsgSeq),
		zap.Int("onlineCount", len(onlineUIDs)),
		zap.Bool("dropped", false))
}

// errCodeOf 提取错误的业务码数值（日志字段用，6003 语境回退——轻通知
// 链路的热点降级口径）：*pkgerrors.Error 取其 Code，非业务错误回退
// fallback。与 service/message.errCodeOf 同构（本包不 import service）。
func errCodeOf(err error, fallback int32) int32 {
	var e *pkgerrors.Error
	if errors.As(err, &e) {
		return e.Code
	}
	return fallback
}

// groupConvPrefix 群会话 ID 前缀（与 message.groupConvPrefix 同口径；
// 本包私有拷贝——pushpull 不 import service/message，前缀契约由
// ADR-0010 D2 冻结为公开数据契约）。
const groupConvPrefix = "g_"

// parseGroupConvID 反解群会话 ID：g_{groupId} → groupId。非 g_ 前缀或
// 数值解析失败返回错误（毒信封判定源）。
func parseGroupConvID(convID string) (uint64, error) {
	if len(convID) <= len(groupConvPrefix) || convID[:len(groupConvPrefix)] != groupConvPrefix {
		return 0, fmt.Errorf("pushpull hot: 会话 %q 缺 %q 前缀", convID, groupConvPrefix)
	}
	id, err := strconv.ParseUint(convID[len(groupConvPrefix):], 10, 64)
	if err != nil {
		return 0, fmt.Errorf("pushpull hot: 会话 %q 群 ID 解析失败: %w", convID, err)
	}
	return id, nil
}
