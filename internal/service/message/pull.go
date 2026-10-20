// PullService 为拉取与历史消息服务（W2-M05，spec §6.1/§6.3 拉取半环）：
// CmdPullRequest{conversationId, afterSeq, limit} → 成员校验(5009) →
// AfterSeq 增量查询（msg_seq ASC、limit+1 探测 hasMore）→ 间隙状态机
// （W2-M05-C2 D12：缺失 seq 探测在途标记，在途截断防迟到提交越过游标）
// → MaxSeq → WS 路径组 CmdPullResponse 回帧（Header.Seq/TraceId 回显，
// 帧预算截断）→ AdvanceSynced 推进 max_synced_seq（末条 seq；空结果不
// 推进；查询路径绝不读 max_synced_seq——重复拉取幂等的充要条件，D5）。
// W3-M04：成员校验扩展 g_ 分支（群会话查群成员表，裁决 D6）。
// W3-M06：增 InboxSummary 会话发现清单（离线消息与漫游，裁决 D1~D5——
// 信箱聚合∩漫游窗口→synced 差集；query/History/HandlePull 语义零改动）。
//
// 分层约定与 Service/MemoryDeliverer 同源：*protocol.ChatMessage 仅在组帧
// 边界出现（chatMessageOf 共享转换），DAO 一律 *model.Message；REST 经
// History 导出 DTO 消费（router 不 import dao）；本包不 import
// gin/gorm/internal/server（Pusher 窄接口由装配层适配，M04 D5 先例）。

package message

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"chatchat/internal/dao"
	"chatchat/internal/model"
	pkgerrors "chatchat/pkg/errors"
	"chatchat/pkg/global"
	"chatchat/pkg/protocol"

	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
)

// PullHandler 为网关 CmdPullRequest 帧入口（acker 同构，W2-M05）：uid 一律
// 取鉴权连接；返回 *pkgerrors.Error 由装配层 sendErr 回 CmdError 帧。
type PullHandler interface {
	HandlePull(ctx context.Context, uid uint64, frame *protocol.Frame) *pkgerrors.Error
}

// HotChecker 热点群判定窄接口（W4-M03 裁决 D4）：hotdetect.HotDetector
// 结构化满足（IsHot 语义——Redis 优先、miss/错误回源 MySQL hot_level、
// 双败 6003），本包不 import hotdetect（GroupMsgCounter 先例：消费方定义
// 窄接口，实现细节不泄漏）。6003/判定失败由本包消费为「降级 message
// 单源」（ADR-0010 D2 消费兑现），不替实现方决定降级语义。
type HotChecker interface {
	// IsHot 判定群是否处于 hot 标记态；错误（双源均败 6003 等）原样上抛。
	IsHot(ctx context.Context, groupID uint64) (bool, error)
}

// PullOptions 为 PullService 装配参数（W2-M05 裁决 D4/D6；W2-M05-C2 增 Pend；
// W3-M04 增 GroupMembers；W3-M06 增 Inbox/RoamDays；W4-M03 增 Hot/Timeline）。
type PullOptions struct {
	// Pusher WS 回帧通道。REST 装配可为 nil（HandlePull 拒用返回 9001），
	// History 纯查询不受影响。
	Pusher Pusher
	// FrameBudget 单帧字节预算（=ws.max_message_bytes；REST 无此约束可为
	// 0）。≤0 视为无限制——测试便利，装配层必须注入正值（见 fitFrame）。
	FrameBudget int
	// Pend 在途标记探针（W2-M05-C2 裁决 D8/D12）：query 间隙判定用。
	// nil = 缺失 seq 一律按永久空洞跳过（修复前行为，D14 前提②；测试
	// 便利）——装配层必须注入，否则 seq 迟到提交防丢机制失效。
	Pend dao.PendMarker
	// GroupMembers 群成员表（W3-M04 裁决 D6）：g_{groupId} 会话读侧成员
	// 判定（GetMember 存在即成员）。nil = g_ 一律 5009 安全失败（防忘
	// 注入时越权放行；装配层必须注入——cmd/api/cmd/gateway 均已注入）。
	// 单聊 "min|max" 路径不经此依赖，nil 不影响。W4-M03 起兼为
	// InboxSummary 热点增补的 ListByUser 数据源（D6）。
	GroupMembers dao.GroupMemberRepo
	// Hot 热点判定面（W4-M03 裁决 D4，CAP-203）：g_ 会话 query 成员校验后
	// IsHot 判定，true 走 Timeline 读路径 + message 权威兜底。nil = 热点
	// 分支不启用（全走 message 单源——安全失败先例 GroupMembers；测试
	// 便利）。装配层应注入（gateway/api）。
	Hot HotChecker
	// Timeline 热点群读扩散行仓储（W4-M03 D4）：ListAfter 为热点分支索引
	// 源、MaxSeq 为 InboxSummary 增补的 lastSeq 提示面。nil 同 Hot（分支
	// 不启用）。
	Timeline dao.GroupTimelineRepo
	// Inbox 信箱聚合读面（W3-M06 裁决 D1）：InboxSummary 清单数据源
	//（dao.Summary——GROUP BY 会话 MAX(msg_seq)∩漫游窗口）。类型为窄接口
	// dao.InboxSummaryReader（按消费面最小注入；dao.NewInboxDeliveryRepo
	// 产物即满足）。nil = 未装配（旧装配/桩）→ InboxSummary 9001 拒用
	//（Pusher nil 拒用先例）；拉取/历史路径不经此依赖。
	Inbox dao.InboxSummaryReader
	// RoamDays 漫游窗口天数（[inbox].roam_days，CAP-002）：>0 时清单仅聚合
	// created_at >= now-N*24h 的信箱行（施加位=清单过滤，D3——发现面受限，
	// 拉取/历史不受窗口限制）；≤0 视为无窗口（全量聚合）。W4-M03 起同样
	// 施加于热点增补行（时间面取 message 行 created_at，D6）。
	RoamDays int
}

// DTO 为历史/拉取消息 DTO（REST {code,message,data} 的 data 元素，
// 字段对齐 chatMessageOf 转换口径：from/to 为 uid 字符串、
// timestamp=CreatedAt.UnixMilli）。W3-M04 由 MessageItem 更名（revive
// 结巴名，group.DTO/friend.DTO 先例；波及仅本文件，字段与 JSON 契约
// 逐字不变）。
type DTO struct {
	MsgID          string `json:"msgId"`
	ConversationID string `json:"conversationId"`
	From           string `json:"from"`
	To             string `json:"to"`
	MsgSeq         int64  `json:"msgSeq"`
	Timestamp      int64  `json:"timestamp"`
	ContentType    int32  `json:"contentType"`
	Content        string `json:"content"`
	URL            string `json:"url"`
}

// HistoryResult 为历史查询结果 DTO（镜像 PullResponse 语义，D6）。
type HistoryResult struct {
	ConversationID string `json:"conversationId"`
	Messages       []DTO  `json:"messages"`
	MaxSeq         uint64 `json:"maxSeq"`
	HasMore        bool   `json:"hasMore"`
}

// InboxItem 为会话发现清单项（W3-M06 裁决 D2 冻结：仅 {conversationId,
// lastSeq}——不含 unreadCount（ADR-0009 §5 归 W4）；lastSeq=漫游窗口内
// 信箱该会话 MAX(msg_seq)。清单不含消息内容（零敏感面）。
type InboxItem struct {
	ConversationID string `json:"conversationId"`
	LastSeq        uint64 `json:"lastSeq"`
}

// PullService 为拉取服务本体：依赖全构造注入，无状态（并发安全）。
// synced 为 cursors 的清单读面投影（W3-M06）：具体仓库（*inboxCursorRepo）
// 实现 dao.InboxSyncedReader，构造期一次断言取得——InboxCursorRepo 写面
// 契约不动（D5 冻结面零波及）；不满足（旧桩）时为 nil，InboxSummary 拒用。
type PullService struct {
	repo        dao.MessageRepo
	cursors     dao.InboxCursorRepo
	synced      dao.InboxSyncedReader
	inbox       dao.InboxSummaryReader
	roamDays    int
	pend        dao.PendMarker
	gmembers    dao.GroupMemberRepo
	hot         HotChecker
	timeline    dao.GroupTimelineRepo
	pusher      Pusher
	frameBudget int
	now         func() time.Time // 漫游窗口计算基准（默认 time.Now；测试覆写，group.M01 先例）
	logger      *zap.Logger
}

// 编译期接口实现断言。
var _ PullHandler = (*PullService)(nil)

// NewPullService 构造拉取服务。logger 为 nil 时以 zap.NewNop 兜底。
func NewPullService(repo dao.MessageRepo, cursors dao.InboxCursorRepo, opts PullOptions, logger *zap.Logger) *PullService {
	if logger == nil {
		logger = zap.NewNop()
	}
	// 清单读面投影（W3-M06）：cursors 动态类型实现 InboxSyncedReader 即
	// 取得（生产装配 dao.NewInboxCursorRepo 产物恒满足）；否则 synced=nil
	// ——仅 InboxSummary 拒用，拉取/历史路径不受影响。
	synced, _ := cursors.(dao.InboxSyncedReader)
	return &PullService{
		repo:        repo,
		cursors:     cursors,
		synced:      synced,
		inbox:       opts.Inbox,
		roamDays:    opts.RoamDays,
		pend:        opts.Pend,
		gmembers:    opts.GroupMembers,
		hot:         opts.Hot,
		timeline:    opts.Timeline,
		pusher:      opts.Pusher,
		frameBudget: opts.FrameBudget,
		now:         time.Now,
		logger:      logger,
	}
}

// pullData 为 History/HandlePull 共享的查询结果内部形态：items 供 REST，
// msgs 保留 *model.Message 供游标推进取末条 seq（DTO 不回转）。
type pullData struct {
	items   []DTO
	msgs    []*model.Message
	maxSeq  uint64
	hasMore bool
}

// History 纯查询+游标推进（REST/WS 共用语义，D3/D5/D6）：成员校验(5009)→
// limit 归一→AfterSeq(limit+1)→截断+hasMore→MaxSeq→AdvanceSynced（返回集
// 末条 seq，与 WS 路径同义——REST 收到即已同步；失败仅 Error 日志不影响
// 结果）。返回错误均为 *pkgerrors.Error 哨兵链（5009/5008），由 router
// serviceFail 映射 403/500。
func (s *PullService) History(ctx context.Context, uid uint64, conversationID string, afterSeq uint64, limit int32) (*HistoryResult, error) {
	data, e := s.query(ctx, uid, conversationID, afterSeq, limit)
	if e != nil {
		return nil, e
	}
	if last := data.lastSeq(); last != nil {
		if err := s.cursors.AdvanceSynced(ctx, uid, conversationID, *last); err != nil {
			s.logger.Error("inbox cursor advanceSynced failed",
				zap.Uint64("uid", uid),
				zap.String("conversationId", conversationID),
				zap.Uint64("lastSeq", *last),
				zap.Int32("code", errCodeOf(err, pkgerrors.ErrCursorUpdate.Code)),
				zap.Error(err))
		}
	}
	messages := data.items
	if messages == nil {
		messages = []DTO{} // REST 序列化为 []，与 proto 空列表语义对齐
	}
	return &HistoryResult{
		ConversationID: conversationID,
		Messages:       messages,
		MaxSeq:         data.maxSeq,
		HasMore:        data.hasMore,
	}, nil
}

// InboxSummary 生成会话发现清单（W3-M06 裁决 D1~D4，CAP-001/002/003；
// 登录/重连补齐发现路径，REST GET /api/message/inbox 消费）：
//
//	漫游窗口 since = now().UTC() - RoamDays*24h（≤0 视为无窗口，since 零值
//	全量聚合）→ dao.Summary（窗口内信箱按会话 MAX(msg_seq) 聚合）→
//	dao.ListSynced 差集（仅 lastSeq > inbox_cursor.max_synced_seq 的会话——
//	已全同步会话不出现：重复登录清单空，门槛「不重复拉取」服务端可证）。
//
// Inbox 未注入（误接/旧装配）→ 9001 拒用（Pusher nil 拒用先例；cursors
// 不满足 InboxSyncedReader 同归）；DB 错误包装 5008（D5 读侧统一口径）。
// 单设备假设：游标不分设备（多端同步延后 plan §2.2）。纯读路径——无游标
// 推进、无副作用；pull.query/History/HandlePull 语义零触碰（D5 冻结面）。
func (s *PullService) InboxSummary(ctx context.Context, uid uint64) ([]InboxItem, error) {
	if s.inbox == nil || s.synced == nil {
		s.logger.Error("inbox summary used without inbox repo (assembly error)",
			zap.Uint64("uid", uid))
		return nil, pkgerrors.ErrInternal
	}

	var since time.Time
	if s.roamDays > 0 {
		since = s.now().UTC().Add(-time.Duration(s.roamDays) * 24 * time.Hour)
	}

	rows, err := s.inbox.Summary(ctx, uid, since)
	if err != nil {
		s.logger.Error("inbox summary query failed",
			zap.Uint64("uid", uid),
			zap.Int("roamDays", s.roamDays),
			zap.Int32("code", errCodeOf(err, pkgerrors.ErrMessageQuery.Code)),
			zap.Error(err))
		return nil, pkgerrors.Wrap(err, pkgerrors.ErrMessageQuery.Code, pkgerrors.ErrMessageQuery.Message)
	}
	synced, err := s.synced.ListSynced(ctx, uid)
	if err != nil {
		s.logger.Error("inbox synced list failed",
			zap.Uint64("uid", uid),
			zap.Int32("code", errCodeOf(err, pkgerrors.ErrMessageQuery.Code)),
			zap.Error(err))
		return nil, pkgerrors.Wrap(err, pkgerrors.ErrMessageQuery.Code, pkgerrors.ErrMessageQuery.Message)
	}

	items := make([]InboxItem, 0, len(rows))
	for _, r := range rows {
		if r.MaxSeq > synced[r.ConversationID] { // D4：仅未齐会话（缺行按 0）
			items = append(items, InboxItem{ConversationID: r.ConversationID, LastSeq: r.MaxSeq})
		}
	}

	// 热点会话增补（W4-M03 裁决 D6，CAP-205）：用户所在群 ∩ 当前 hot →
	// Timeline.MaxSeq > 用户 synced 即有差值，列入清单（lastSeq=Timeline.
	// MaxSeq 提示面；拉取完整性仍由 query 热点分支的 message 权威保证）。
	// 时间面：漫游窗开启时取 message 表该 seq 行 created_at 过滤（无行
	// 残行跳过）；同 conv 与信箱行合并为单行取 max（P3 回写后双源并存，
	// CASE-211）。增补段任一群失败跳过记 Warn——清单是提示面，信箱行为
	// 不受影响（plan §4 错误处理）。
	items = s.appendHotSummaryItems(ctx, uid, since, synced, items)

	s.logger.Info("inbox summary served",
		zap.Uint64("uid", uid),
		zap.Int("conversations", len(items)),
		zap.Int("roamDays", s.roamDays))
	return items, nil
}

// appendHotSummaryItems 就地增补热点群差值行并返回增补后清单（D6 见
// InboxSummary 注）。依赖面（Hot/Timeline/GroupMembers）任一未注入 →
// 增补段整体停用（安全失败先例 GroupMembers——清单退化为 W3-M06 信箱
// 聚合面，零行为回归）。
func (s *PullService) appendHotSummaryItems(ctx context.Context, uid uint64, since time.Time, synced map[string]uint64, items []InboxItem) []InboxItem {
	if s.hot == nil || s.timeline == nil || s.gmembers == nil {
		return items
	}
	groups, err := s.gmembers.ListByUser(ctx, uid)
	if err != nil {
		s.logger.Warn("inbox summary hot supplement skipped: group list failed",
			zap.Uint64("uid", uid),
			zap.Error(err))
		return items
	}
	byConv := make(map[string]int, len(items))
	for i, it := range items {
		byConv[it.ConversationID] = i
	}
	for _, g := range groups {
		hot, herr := s.hot.IsHot(ctx, g.GroupID)
		if herr != nil {
			s.logger.Warn("inbox summary hot supplement: IsHot failed, skip group",
				zap.Uint64("uid", uid),
				zap.Uint64("groupId", g.GroupID),
				zap.Int32("code", errCodeOf(herr, pkgerrors.ErrHotQueryFailed.Code)),
				zap.Error(herr))
			continue
		}
		if !hot {
			continue
		}
		tlMax, terr := s.timeline.MaxSeq(ctx, g.GroupID)
		if terr != nil {
			s.logger.Warn("inbox summary hot supplement: timeline max seq failed, skip group",
				zap.Uint64("uid", uid),
				zap.Uint64("groupId", g.GroupID),
				zap.Error(terr))
			continue
		}
		if tlMax == 0 {
			continue // Timeline 空：无差值语义
		}
		conv := groupConvPrefix + strconv.FormatUint(g.GroupID, 10)
		if tlMax <= synced[conv] {
			continue // 已追平（无差值不列入）
		}
		// 漫游窗过滤（D6）：时间面取 message 表该 seq 行 created_at——
		// Timeline 残行（message 行缺失）跳过（提示面退化，正确性不受影响）。
		if !since.IsZero() {
			rows, rerr := s.repo.AfterSeq(ctx, conv, tlMax-1, 1)
			if rerr != nil {
				s.logger.Warn("inbox summary hot supplement: roam row lookup failed, skip group",
					zap.Uint64("uid", uid),
					zap.Uint64("groupId", g.GroupID),
					zap.Error(rerr))
				continue
			}
			if len(rows) == 0 || rows[0].MsgSeq != tlMax || rows[0].CreatedAt.Before(since) {
				continue
			}
		}
		// 合并（CASE-211）：同 conv 单行取 max（信箱行 P3 回写后与增补行并存）。
		if idx, ok := byConv[conv]; ok {
			if tlMax > items[idx].LastSeq {
				items[idx].LastSeq = tlMax
			}
			continue
		}
		byConv[conv] = len(items)
		items = append(items, InboxItem{ConversationID: conv, LastSeq: tlMax})
	}
	return items
}

// HandlePull 实现 PullHandler（CAP-002）：解码 PullRequest（失败 5001，
// afterSeq<0 视为参数非法 1001）→ query（5009/5008）→ AdvanceSynced
// （末条 seq，失败仅 Error 日志含 code，不影响响应，D5）→ 组
// CmdPullResponse 帧（Header.Seq 回显请求 seq、TraceId 回显/缺失补生成）
// → 帧预算截断（D4）→ Pusher.Push(请求者)。
func (s *PullService) HandlePull(ctx context.Context, uid uint64, frame *protocol.Frame) *pkgerrors.Error {
	if s.pusher == nil {
		// REST 装配（Pusher=nil）误接 WS 入口属装配错误：拒绝使用，
		// 经 logicHandler sendErr 回 CmdError（9001）。
		s.logger.Error("pull handler used without pusher (assembly error)",
			zap.Uint64("uid", uid))
		return pkgerrors.ErrInternal
	}

	pr := &protocol.PullRequest{}
	if err := proto.Unmarshal(frame.GetPayload(), pr); err != nil {
		return pkgerrors.Wrap(err, pkgerrors.ErrMessageDecode.Code, pkgerrors.ErrMessageDecode.Message)
	}
	if pr.GetAfterSeq() < 0 {
		return pkgerrors.ErrInvalidParam
	}

	start := time.Now()
	header := frame.GetHeader()
	data, e := s.query(ctx, uid, pr.GetConversationId(), uint64(pr.GetAfterSeq()), pr.GetLimit())
	if e != nil {
		return e
	}

	// 组 CmdPullResponse：消息体复用 chatMessageOf（与投递下行帧同一口径）。
	msgs := make([]*protocol.ChatMessage, 0, len(data.msgs))
	for _, m := range data.msgs {
		msgs = append(msgs, chatMessageOf(m))
	}
	resp := &protocol.PullResponse{
		ConversationId: pr.GetConversationId(),
		Messages:       msgs,
		MaxSeq:         int64(data.maxSeq),
		HasMore:        data.hasMore,
	}

	traceID := header.GetTraceId()
	if traceID == "" {
		traceID = newTraceID16() // 缺失补生成（与投递侧 traceIDFromContext 同语义）
	}
	respHeader := &protocol.Header{Cmd: protocol.CmdPullResponse, Seq: header.GetSeq(), TraceId: traceID}

	payloadBytes, dropped, err := fitFrame(resp, respHeader, s.frameBudget)
	if err != nil {
		s.logger.Error("marshal pull response failed",
			zap.Uint64("uid", uid),
			zap.String("conversationId", pr.GetConversationId()),
			zap.String("traceId", traceID),
			zap.Error(err))
		return pkgerrors.ErrInternal
	}
	if dropped > 0 {
		// 截断记 Info（D4）：requested/returned/truncated/budget。
		s.logger.Info("pull response truncated to frame budget",
			zap.Int("requested", len(data.msgs)),
			zap.Int("returned", len(resp.GetMessages())),
			zap.Int("truncated", dropped),
			zap.Int("budget", s.frameBudget))
	}

	// 游标推进（D5）：按**返回集**（截断后）末条 seq 推进——客户端实际收到
	// 的范围才是已同步范围；预算截断时按返回集而非查询集，保证 hasMore
	// 翻页衔接。仅非空返回集推进；失败仅 Error 日志（code 数值字段，M04
	// P3 教训），不影响响应。
	if n := len(resp.GetMessages()); n > 0 {
		lastSeq := uint64(resp.GetMessages()[n-1].GetMsgSeq())
		if err := s.cursors.AdvanceSynced(ctx, uid, pr.GetConversationId(), lastSeq); err != nil {
			s.logger.Error("inbox cursor advanceSynced failed",
				zap.Uint64("uid", uid),
				zap.String("conversationId", pr.GetConversationId()),
				zap.Uint64("lastSeq", lastSeq),
				zap.String("traceId", traceID),
				zap.Int32("code", errCodeOf(err, pkgerrors.ErrCursorUpdate.Code)),
				zap.Error(err))
		}
	}

	s.logger.Info("pull served",
		zap.Uint64("uid", uid),
		zap.String("conversationId", pr.GetConversationId()),
		zap.Uint64("afterSeq", uint64(pr.GetAfterSeq())),
		zap.Int32("limit", pr.GetLimit()),
		zap.Int("returned", len(resp.GetMessages())),
		zap.Uint64("maxSeq", data.maxSeq),
		zap.Bool("hasMore", resp.GetHasMore()),
		zap.Int64("costMs", time.Since(start).Milliseconds()),
		zap.String("traceId", traceID))

	if !s.pusher.Push(uid, payloadBytes) {
		// false=离线或 send 缓冲满被踢（Pusher 契约）：拉取幂等，客户端重拉
		// 同批无副作用；仅 Warn（不发 CmdError——客户端连接已不可用）。
		s.logger.Warn("pull response push failed (offline or slow consumer)",
			zap.Uint64("uid", uid),
			zap.String("conversationId", pr.GetConversationId()),
			zap.Int("returned", len(resp.GetMessages())),
			zap.String("traceId", traceID))
	}
	return nil
}

// query 为 History/HandlePull 共享查询链（D3；W2-M05-C2 增 D12 间隙状态机；
// W3-M04 D6 增 g_ 群会话成员分支）：成员校验（单聊 conv "min|max" uid∈conv；
// g_ 前缀解析 groupId 查群成员表——存在即成员，非成员/群不存在/畸形/未注入
// 一律 5009，读取路径按权限而非参数，D1 口径区分）→
// limit∈[1,50] 归一（≤0 或 >50 → PullLimitDefault）→ 单次 AfterSeq(limit+1)
// hasMore 探测（seq 空洞下精确，Stage1 取舍）→ 截断至 limit → 内存间隙扫描
// （applyGapPolicy：在途截断/永久跳过）→ MaxSeq（DB MAX，独立于探测）。
// 最终 hasMore = 探测 hasMore || 间隙截断发生。
func (s *PullService) query(ctx context.Context, uid uint64, conversationID string, afterSeq uint64, limit int32) (*pullData, *pkgerrors.Error) {
	if e := s.checkReadMembership(ctx, uid, conversationID); e != nil {
		return nil, e
	}
	normalized := normalizeLimit(limit)

	// 热点分支（W4-M03 裁决 D4，CAP-203）：IsHot=true 走 Timeline 读路径 +
	// message 表权威；任何失败面（6003/Timeline 查询/内容 miss/覆盖不足）
	// 均降级 message 单源（下方既有路径原样服务——降级正确性永远成立）。
	if msgs, maxSeq, hasMore, handled := s.queryHot(ctx, uid, conversationID, afterSeq, normalized); handled {
		return buildPullData(msgs, maxSeq, hasMore), nil
	}

	msgs, err := s.repo.AfterSeq(ctx, conversationID, afterSeq, normalized+1)
	if err != nil {
		s.logger.Error("message AfterSeq failed",
			zap.Uint64("uid", uid),
			zap.String("conversationId", conversationID),
			zap.Uint64("afterSeq", afterSeq),
			zap.Error(err))
		return nil, pkgerrors.Wrap(err, pkgerrors.ErrMessageQuery.Code, pkgerrors.ErrMessageQuery.Message)
	}
	hasMore := len(msgs) > normalized
	if hasMore {
		msgs = msgs[:normalized] // 第 limit+1 条仅为探测标记，不返回
	}

	msgs, gapTruncated := s.applyGapPolicy(ctx, uid, conversationID, afterSeq, msgs)
	if gapTruncated {
		hasMore = true // D12：截断即 more（客户端以 afterSeq=返回集末条重拉填补）
	}

	maxSeq, err := s.repo.MaxSeq(ctx, conversationID)
	if err != nil {
		s.logger.Error("message MaxSeq failed",
			zap.Uint64("uid", uid),
			zap.String("conversationId", conversationID),
			zap.Error(err))
		return nil, pkgerrors.Wrap(err, pkgerrors.ErrMessageQuery.Code, pkgerrors.ErrMessageQuery.Message)
	}

	items := make([]DTO, 0, len(msgs))
	for _, m := range msgs {
		cm := chatMessageOf(m)
		items = append(items, DTO{
			MsgID:          cm.GetMsgId(),
			ConversationID: cm.GetConversationId(),
			From:           cm.GetFrom(),
			To:             cm.GetTo(),
			MsgSeq:         cm.GetMsgSeq(),
			Timestamp:      cm.GetTimestamp(),
			ContentType:    cm.GetContentType(),
			Content:        cm.GetContent(),
			URL:            cm.GetUrl(),
		})
	}
	return &pullData{items: items, msgs: msgs, maxSeq: maxSeq, hasMore: hasMore}, nil
}

// queryHot 热点拉取分支（W4-M03 裁决 D4，CAP-203；Timeline 读路径 + message
// 表权威双层）：
//
//	成员校验后 IsHot（HotChecker——ADR-0010 D2 消费兑现）→ true：
//	  Timeline.ListAfter(afterSeq, limit+1)（索引源，uk_group_seq 覆盖）
//	  → ListByMsgIDs 批查内容 → 按 seq 组装（服务端单源组装配，结果无重复
//	  seq）→ 前缀缺口（Timeline 起点晚于 afterSeq+1——P3-1 边界：hotDeliver
//	  丢弃前缀的消息仍在 message 表）以 message AfterSeq 补齐该段；
//	  maxSeq/hasMore 唯一权威 = message.MaxSeq（D4 冻结）。
//
// 降级面（返回 handled=false，由既有 message 单源路径原样服务——降级正确
// 性永远成立）：IsHot 失败（6003 语境 Debug 日志）/IsHot=false（摘标后自动
// 切回信箱语义，无缝）/Timeline 或内容查询失败/内容 miss/页内 seq 跳变或
// 尾部覆盖不足（hot 消费 lag/hotDeliver 丢弃——CAP-203「结果 ⊇ message 表
// 该段」不变量的结构保证）。
func (s *PullService) queryHot(ctx context.Context, uid uint64, conv string, afterSeq uint64, normalized int) (msgs []*model.Message, maxSeq uint64, hasMore, handled bool) {
	if s.hot == nil || s.timeline == nil || !strings.HasPrefix(conv, groupConvPrefix) {
		return nil, 0, false, false // 未装配/单聊：不接管
	}
	groupID, err := strconv.ParseUint(strings.TrimPrefix(conv, groupConvPrefix), 10, 64)
	if err != nil {
		return nil, 0, false, false // checkReadMembership 已拦截畸形 g_（不可达防御）
	}
	hot, err := s.hot.IsHot(ctx, groupID)
	if err != nil {
		s.logger.Debug("hot pull degrade: IsHot failed, message source only",
			zap.String("conversationId", conv),
			zap.Uint64("uid", uid),
			zap.Int32("code", errCodeOf(err, pkgerrors.ErrHotQueryFailed.Code)),
			zap.Error(err))
		return nil, 0, false, false
	}
	if !hot {
		return nil, 0, false, false
	}
	// maxSeq/hasMore 唯一权威（D4）：message 表 MaxSeq——热点分支失败时由
	// 普通路径同点查询并包装 5008 上抛，此处不重复包装。
	maxSeq, err = s.repo.MaxSeq(ctx, conv)
	if err != nil {
		return nil, 0, false, false
	}
	if maxSeq <= afterSeq {
		return nil, maxSeq, false, true // 空窗：message 无该段（Timeline 不可能更全）
	}
	rows, err := s.timeline.ListAfter(ctx, groupID, afterSeq, normalized+1)
	if err != nil {
		s.logger.Debug("hot pull degrade: timeline list failed",
			zap.String("conversationId", conv),
			zap.Uint64("uid", uid),
			zap.Uint64("afterSeq", afterSeq),
			zap.Error(err))
		return nil, 0, false, false
	}
	ids := make([]string, 0, len(rows))
	for _, r := range rows {
		ids = append(ids, r.MsgID)
	}
	contents, err := s.repo.ListByMsgIDs(ctx, ids)
	if err != nil {
		s.logger.Debug("hot pull degrade: list by msg ids failed",
			zap.String("conversationId", conv),
			zap.Uint64("uid", uid),
			zap.Int("timelineRows", len(rows)),
			zap.Error(err))
		return nil, 0, false, false
	}
	byID := make(map[string]*model.Message, len(contents))
	for _, m := range contents {
		byID[m.MsgID] = m
	}
	msgs = make([]*model.Message, 0, len(rows))
	for _, r := range rows {
		m, ok := byID[r.MsgID]
		if !ok || m.MsgSeq != r.MsgSeq {
			// 内容 miss（理论上仅 hotDeliver 丢弃面）——整体降级（正确性优先）。
			s.logger.Debug("hot pull degrade: timeline row content miss",
				zap.String("conversationId", conv),
				zap.Uint64("msgSeq", r.MsgSeq),
				zap.String("msgId", r.MsgID))
			return nil, 0, false, false
		}
		msgs = append(msgs, m)
	}
	prefixRows := 0
	if len(rows) > 0 && rows[0].MsgSeq > afterSeq+1 {
		// 前缀缺口兜底（D4/P3-1 关闭面）：以 message AfterSeq 补齐
		// (afterSeq, rows[0].MsgSeq) 段——该段消息仍在 message 表（hotDeliver
		// 丢弃的只是 Timeline 行）。取数上限钳制在一页预算内。
		fetch := rows[0].MsgSeq - afterSeq - 1
		if fetch > uint64(normalized) {
			fetch = uint64(normalized)
		}
		pre, perr := s.repo.AfterSeq(ctx, conv, afterSeq, int(fetch))
		if perr != nil {
			s.logger.Debug("hot pull degrade: prefix fill failed",
				zap.String("conversationId", conv),
				zap.Uint64("uid", uid),
				zap.Uint64("afterSeq", afterSeq),
				zap.Error(perr))
			return nil, 0, false, false
		}
		msgs = mergeBySeq(pre, msgs)
		prefixRows = len(pre)
	}
	// 覆盖校验（CAP-203「结果 ⊇ message 表该段」的结构保证）：
	// 页内 seq 跳变（内部缺口——hot 消费 lag/hotDeliver 丢弃段仍在 message）
	// 或页未满而 message MaxSeq 超出页尾（尾部缺口——lag）→ Timeline 覆盖
	// 不足，整体降级 message 单源（间隙状态机/双游标语义完整保留）。
	if !contiguousFrom(msgs, afterSeq) {
		s.logger.Debug("hot pull degrade: page gap detected",
			zap.String("conversationId", conv),
			zap.Uint64("afterSeq", afterSeq),
			zap.Int("timelineRows", len(rows)))
		return nil, 0, false, false
	}
	if len(msgs) < normalized {
		last := afterSeq
		if n := len(msgs); n > 0 {
			last = msgs[n-1].MsgSeq
		}
		if last < maxSeq {
			s.logger.Debug("hot pull degrade: tail shortfall vs message maxSeq",
				zap.String("conversationId", conv),
				zap.Uint64("lastSeq", last),
				zap.Uint64("maxSeq", maxSeq))
			return nil, 0, false, false
		}
	}
	if n := len(msgs); n > 0 {
		hasMore = msgs[n-1].MsgSeq < maxSeq
	}
	s.logger.Debug("hot pull served from timeline",
		zap.String("conversationId", conv),
		zap.Uint64("uid", uid),
		zap.Uint64("afterSeq", afterSeq),
		zap.Int("timelineRows", len(rows)),
		zap.Int("fallbackRows", prefixRows),
		zap.Uint64("maxSeq", maxSeq),
		zap.Bool("hasMore", hasMore))
	return msgs, maxSeq, hasMore, true
}

// buildPullData 组装查询结果内部形态（W4-M03 自 query 提取：热点分支与
// message 单源路径共享同一 DTO 转换口径）。
func buildPullData(msgs []*model.Message, maxSeq uint64, hasMore bool) *pullData {
	items := make([]DTO, 0, len(msgs))
	for _, m := range msgs {
		cm := chatMessageOf(m)
		items = append(items, DTO{
			MsgID:          cm.GetMsgId(),
			ConversationID: cm.GetConversationId(),
			From:           cm.GetFrom(),
			To:             cm.GetTo(),
			MsgSeq:         cm.GetMsgSeq(),
			Timestamp:      cm.GetTimestamp(),
			ContentType:    cm.GetContentType(),
			Content:        cm.GetContent(),
			URL:            cm.GetUrl(),
		})
	}
	return &pullData{items: items, msgs: msgs, maxSeq: maxSeq, hasMore: hasMore}
}

// mergeBySeq 归并两个按 msg_seq ASC 的切片并按 seq 去重（前缀兜底段与
// Timeline 组装段合并——两源内容均出自 message 表，同 seq 行同值，保留
// 任一等价）。
func mergeBySeq(a, b []*model.Message) []*model.Message {
	out := make([]*model.Message, 0, len(a)+len(b))
	i, j := 0, 0
	for i < len(a) && j < len(b) {
		switch {
		case a[i].MsgSeq < b[j].MsgSeq:
			out = append(out, a[i])
			i++
		case a[i].MsgSeq > b[j].MsgSeq:
			out = append(out, b[j])
			j++
		default:
			out = append(out, a[i]) // 同 seq 去重
			i++
			j++
		}
	}
	out = append(out, a[i:]...)
	out = append(out, b[j:]...)
	return out
}

// contiguousFrom 校验 msgs 自 afterSeq+1 起 seq 严格连续（无缺口无重复）。
func contiguousFrom(msgs []*model.Message, afterSeq uint64) bool {
	expected := afterSeq + 1
	for _, m := range msgs {
		if m.MsgSeq != expected {
			return false
		}
		expected++
	}
	return true
}

// lastSeq 返回返回列表末条 msgSeq（空结果 nil——不推进游标，D5）。
func (d *pullData) lastSeq() *uint64 {
	if len(d.msgs) == 0 {
		return nil
	}
	seq := d.msgs[len(d.msgs)-1].MsgSeq
	return &seq
}

// checkReadMembership 读侧成员校验（W3-M04 裁决 D6）：conv 空 → 5009；
// 单聊 "min|max" → isConvMember（W2 冻结口径不变）；g_ 前缀 → 解析
// groupId 查群成员表（GetMember 存在即成员）。g_ 分支的失败面（非成员/
// 群不存在/畸形后缀/GroupMembers 未注入）统一 5009 安全失败——读取路径
// 按权限而非参数，与 W2 D1 口径同源；GetMember 基础设施错误亦 fail-closed
// 归 5009（Error 日志区分观测面，不向客户端泄漏内部状态）。
// ACK 写路径不经此函数（HandleAck 的 isConvMember 单聊解析保留——群 ACK
// 归 M05，冻结口径不动）。
func (s *PullService) checkReadMembership(ctx context.Context, uid uint64, conv string) *pkgerrors.Error {
	if conv == "" {
		return pkgerrors.ErrNotConvMember
	}
	if !strings.HasPrefix(conv, groupConvPrefix) {
		if !isConvMember(uid, conv) {
			return pkgerrors.ErrNotConvMember
		}
		return nil
	}
	groupID, err := strconv.ParseUint(strings.TrimPrefix(conv, groupConvPrefix), 10, 64)
	if err != nil {
		return pkgerrors.ErrNotConvMember // 畸形 g_ 后缀（如 "g_"、"g_abc"、"g_-1"）
	}
	if s.gmembers == nil {
		s.logger.Warn("group conv read denied: GroupMembers not injected (assembly error)",
			zap.Uint64("uid", uid), zap.String("conversationId", conv))
		return pkgerrors.ErrNotConvMember
	}
	if _, err := s.gmembers.GetMember(ctx, groupID, uid); err != nil {
		if !isNotGroupMember(err) { // 4002 哨兵（含群不存在时成员行未命中）
			s.logger.Error("group member lookup failed (fail-closed)",
				zap.Uint64("uid", uid), zap.Uint64("groupId", groupID), zap.Error(err))
		}
		return pkgerrors.ErrNotConvMember
	}
	return nil
}

// applyGapPolicy 为 D12 间隙判定状态机：对单次 AfterSeq 返回集做内存扫描，
// expected 自 afterSeq+1 逐行推进；遇缺失 seq 逐个 HasPending 探测：
//   - 在途 → 截断至当前安全前缀，返回 truncated=true（hasMore 合成 true；
//     AdvanceSynced 按截断后返回集末条推进——D5 游标安全性由截断自动成立，
//     服务端 synced 游标不再越过间隙）；
//   - 永久（探测 false 或达探测上限）→ 跳过继续扫描（M03 D3 空洞无害，
//     原语义保持）；
//   - HasPending Redis 错误 → fail-closed 视在途截断（可用性退化为稍后
//     重拉，不丢数据）；
//   - 探测上限 = global.PullLimitDefault/请求（防单请求 Redis 放大），达限
//     后剩余缺失按永久跳过。
//
// 不变性（D14）：①单次查询+内存扫描，不迭代查询；②s.pend==nil（未装配）
// 缺失一律按永久——修复前行为，连续/无标记数据输出逐字节不变；③空结果
// 不判间隙（afterSeq≥maxSeq 无缺失可能）。
func (s *PullService) applyGapPolicy(ctx context.Context, uid uint64, conversationID string, afterSeq uint64, msgs []*model.Message) ([]*model.Message, bool) {
	if s.pend == nil || len(msgs) == 0 {
		return msgs, false
	}
	probes := 0
	expected := afterSeq + 1
	safe := make([]*model.Message, 0, len(msgs))
	for _, m := range msgs {
		for expected < m.MsgSeq {
			inFlight := false
			if probes < global.PullLimitDefault {
				probes++
				pending, err := s.pend.HasPending(ctx, conversationID, expected)
				if err != nil {
					// fail-closed（D12）：探测失败按在途处理——截断+重拉
					// 的可用性代价换取不丢。
					inFlight = true
					s.logger.Warn("pend marker probe failed (fail-closed)",
						zap.Uint64("uid", uid),
						zap.String("conversationId", conversationID),
						zap.Uint64("missingSeq", expected),
						zap.Error(err))
				} else {
					inFlight = pending
				}
			}
			if inFlight {
				return safe, true
			}
			expected++ // 永久空洞：跳过
		}
		safe = append(safe, m)
		expected = m.MsgSeq + 1
	}
	return safe, false
}

// normalizeLimit 归一拉取条数（D3）：≤0 或 >PullLimitDefault(50) → 50。
func normalizeLimit(limit int32) int {
	if limit <= 0 || limit > int32(global.PullLimitDefault) {
		return global.PullLimitDefault
	}
	return int(limit)
}

// fitFrame 组帧并按预算截断（D4 冻结）：全量 marshal 超预算 → 从 messages
// 尾部逐条移除重编（≤limit 次迭代，精确 ≤ 预算，不做估算）→ 截断后
// hasMore=true（无论探测结果——客户端 afterSeq 取响应内末条 seq 翻页，
// 不推进则重拉同批幂等无害）。budget ≤0 视为无限制（测试便利；装配层
// 必须注入正值）。返回帧字节、截断条数。
func fitFrame(resp *protocol.PullResponse, header *protocol.Header, budget int) ([]byte, int, error) {
	dropped := 0
	for {
		payload, err := proto.Marshal(resp)
		if err != nil {
			return nil, 0, fmt.Errorf("marshal PullResponse: %w", err)
		}
		frameBytes, err := proto.Marshal(&protocol.Frame{Header: header, Payload: payload})
		if err != nil {
			return nil, 0, fmt.Errorf("marshal Frame: %w", err)
		}
		if budget <= 0 || len(frameBytes) <= budget || len(resp.GetMessages()) == 0 {
			return frameBytes, dropped, nil
		}
		if dropped == 0 {
			resp.HasMore = true // 截断即还有更多（在下次重编前生效）
		}
		resp.Messages = resp.Messages[:len(resp.Messages)-1]
		dropped++
	}
}
