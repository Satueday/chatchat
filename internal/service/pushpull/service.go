// Package pushpull 为推拉投递服务核心（W3-M05）：独立进程消费投递信封
// topic（chatchat-message，ADR-0007）→ 写扩散信箱投递记录（全员幂等批量，
// inbox_delivery 为事实源）→ Redis presence 批量在线判定 → 在线成员合并
// 发布单封推送信封（members=在线列表，复用冻结信封零新 proto）到 push
// topic（chatchat-push，ADR-0009）由 gateway 侧推送引擎逐成员本地投递
// 落地 WS 下行。
//
// 错误分类冻结（裁决 D9）：解码失败=确定性毒消息，Error 日志+return nil
// 跳过（重投无意义，防分区停滞——ADR-0007 无 DLQ 取舍下的兜底口径）；
// 信箱/presence/推送发布三类基础设施错误=return err 重投（至少一次；
// 下游经 uk_user_conv_seq 幂等与信封快照成员天然吸收重复）。
//
// 依赖面窄接口注入（mq.Consumer/Producer + dao.InboxDeliveryRepo/
// dao.PresenceChecker）：本包不 import gin/gorm/sarama；消费 goroutine
// 属主为 cmd/pushpull 装配层（ctx + WaitGroup），本包无自有协程。
package pushpull

import (
	"context"
	"fmt"

	"chatchat/internal/dao"
	"chatchat/internal/model"
	"chatchat/internal/mq"

	"go.uber.org/zap"
)

// Stats pushpull 消费/推送观测窄接口（W4-M05 plan D5）：两条消费 Handle
// 入口各计一次 Consumed；Service.Handle 推送段成败计 PushResult——nil=
// 不观测（单测/旧装配容忍，message.MsgStats 先例）；实现须并发安全
// （expvar.Int 满足）。
type Stats interface {
	// Consumed 一条信封消费入口计数（普通/hot 两通道共用）。
	Consumed()
	// PushResult 在线推送发布结果计数（ok=true 成功一次/false 失败一次
	// ——失败计数按处理尝试口径，重投后的再次尝试独立计数）。
	PushResult(ok bool)
}

// Service 为 pushpull 消费服务（mq.Handler 形态）。字段全部只读，并发
// 安全（Handle 可被消费会话多 claim 并发调用——各调用无共享可变状态）。
type Service struct {
	inbox     dao.InboxDeliveryRepo
	presence  dao.PresenceChecker
	producer  mq.Producer
	pushTopic string
	stats     Stats
	logger    *zap.Logger
}

// 编译期接口实现断言（Handle 方法签名即 mq.Handler）。
var _ mq.Handler = (*Service)(nil).Handle

// NewService 构造消费服务。pushTopic 为在线推送落地 topic（[kafka].
// push_topic）；stats 为 W4-M05 增参（nil=不观测——消费/推送计数埋点，
// plan D5）；logger 为 nil 时以 zap.NewNop 兜底（KafkaDeliverer 先例）。
func NewService(inbox dao.InboxDeliveryRepo, presence dao.PresenceChecker,
	producer mq.Producer, pushTopic string, stats Stats, logger *zap.Logger) *Service {
	if logger == nil {
		logger = zap.NewNop()
	}
	return &Service{
		inbox:     inbox,
		presence:  presence,
		producer:  producer,
		pushTopic: pushTopic,
		stats:     stats,
		logger:    logger,
	}
}

// Handle 实现 mq.Handler（D3/D9 冻结流程；W3-M05-C2 推送面合并后形态）：
//
//	解码失败 → Error（topic/partition/offset）+ return nil 跳过
//	→ BatchCreate 全员（在线+离线——信箱是事实源，推送是优化路径）
//	  错误 → return err 重投（幂等保护）
//	→ presence.OnlineMany 一次批量判定全体成员（MGET 单往返）
//	  错误 → return err 重投
//	→ 在线列表非空 → 一次 MarshalDelivery(members=在线列表) +
//	  一次 producer.Send(pushTopic, key=convId)；在线列表为空 → 零 Send
//	  发布/编码错误分类见下
//	→ 消费 Info（traceId/msgId/conv/seq/memberCount/onlineCount）。
//
// 合并推送依据（W3-M05-C2 根因③修复）：gateway PushHandler 本就遍历
// members 本地投递（MemoryDeliverer.Deliver 逐成员 Online 判定+Push），
// 逐成员拆封是多余往返——K 在线成员从 K 次编码+K 次同步 Send（每次
// 2-5ms 串行累加）收敛为 1 次；members 语义不变（快照成员列表），仅
// 从「全体成员」改为「在线成员子集」，gateway 零改动。
//
// 推送信封重编码（MarshalDelivery）失败按毒信封跳过（Error+return nil）：
// 解码端已验证 msgSeq≥0（≤MaxInt64）与字符串 UTF-8，正常链路重编码必成
// ——该分支为防御纵深（如未来信封字段演进引入新的确定性编码失败面），
// 确定性失败重投只会永久停滞分区（信箱已落，成员走拉取兜底）；
// producer.Send 失败为基础设施瞬时错误，return err 重投（信箱已幂等，
// 重投仅重发布推送）。
//
// 日志脱敏：仅记元数据（msgId/conv/seq/成员计数），信封 Value 含用户
// content 不入日志（M03 约束延续）。
func (s *Service) Handle(ctx context.Context, m *mq.Message) error {
	if m == nil {
		return nil // mq 层保证非 nil；防御性跳过（无日志面可定位的信息）
	}
	if s.stats != nil {
		s.stats.Consumed() // W4-M05 D5：消费入口计数（nil 容忍）
	}
	d, err := mq.UnmarshalDelivery(m.Value)
	if err != nil {
		s.logger.Error("pushpull: 信封解码失败，跳过（确定性毒消息）",
			zap.String("topic", m.Topic),
			zap.Int32("partition", m.Partition),
			zap.Int64("offset", m.Offset),
			zap.Error(err))
		return nil
	}
	msg := d.Message

	// 信箱写扩散：全员一行（uk_user_conv_seq 幂等——重投恰好一次写）。
	records := make([]*model.InboxDelivery, len(d.Members))
	for i, uid := range d.Members {
		records[i] = &model.InboxDelivery{
			UserID:         uid,
			ConversationID: msg.ConversationID,
			MsgID:          msg.MsgID,
			MsgSeq:         msg.MsgSeq,
		}
	}
	if err := s.inbox.BatchCreate(ctx, records); err != nil {
		s.logger.Error("pushpull: 信箱批量写入失败，重投",
			zap.String("msgId", msg.MsgID),
			zap.String("conversationId", msg.ConversationID),
			zap.Uint64("msgSeq", msg.MsgSeq),
			zap.Int("memberCount", len(d.Members)),
			zap.Error(err))
		return fmt.Errorf("pushpull: 信箱批量写入失败: %w", err)
	}

	// 批量在线判定（MGET 单往返，根因②修复）→ 构建在线列表。
	onlineUIDs := make([]uint64, 0, len(d.Members))
	if len(d.Members) > 0 {
		onlineMap, err := s.presence.OnlineMany(ctx, d.Members)
		if err != nil {
			s.logger.Error("pushpull: presence 批量判定失败，重投",
				zap.String("msgId", msg.MsgID),
				zap.Int("memberCount", len(d.Members)),
				zap.Error(err))
			return fmt.Errorf("pushpull: presence 批量判定失败: %w", err)
		}
		for _, uid := range d.Members {
			if onlineMap[uid] {
				onlineUIDs = append(onlineUIDs, uid)
			}
		}
	}

	// 在线列表非空：单信封合并一次发布（members=在线列表，根因③修复）；
	// 全离线/空成员：零 Send——离线成员拉取兜底（D11 竞态容忍）。
	if len(onlineUIDs) > 0 {
		value, err := mq.MarshalDelivery(&mq.Delivery{Message: msg, Members: onlineUIDs, TraceID: d.TraceID})
		if err != nil {
			// 防御分支（正常链路不可达，见方法注）：确定性编码失败跳过。
			s.logger.Error("pushpull: 推送信封编码失败，跳过推送",
				zap.String("msgId", msg.MsgID),
				zap.Int("onlineCount", len(onlineUIDs)),
				zap.Error(err))
			return nil
		}
		if err := s.producer.Send(ctx, &mq.Message{
			Topic: s.pushTopic,
			Key:   []byte(msg.ConversationID), // 分区键 conversationId（D10，与投递主题一致）
			Value: value,
		}); err != nil {
			if s.stats != nil {
				s.stats.PushResult(false) // W4-M05 D5：推送失败计数（重投后独立再计）
			}
			s.logger.Error("pushpull: 推送信封发布失败，重投",
				zap.String("msgId", msg.MsgID),
				zap.Int("onlineCount", len(onlineUIDs)),
				zap.String("pushTopic", s.pushTopic),
				zap.Error(err))
			return fmt.Errorf("pushpull: 推送信封发布失败: %w", err)
		}
		if s.stats != nil {
			s.stats.PushResult(true) // W4-M05 D5：推送成功计数
		}
	}

	s.logger.Info("pushpull: 投递信封处理完成",
		zap.String("traceId", d.TraceID),
		zap.String("msgId", msg.MsgID),
		zap.String("conversationId", msg.ConversationID),
		zap.Uint64("msgSeq", msg.MsgSeq),
		zap.Int("memberCount", len(d.Members)),
		zap.Int("onlineCount", len(onlineUIDs)))
	return nil
}
