package hotdetect

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"chatchat/internal/dao"
	"chatchat/internal/model"

	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
)

// 摘标切换协议（W4-M02 plan D6，ADR-0010 核心；CAP-104）。
//
// M01「判定即摘标」升级为三阶段编排，状态机由 UnmarkSwitcher 持有、
// Detector 在低周期 streak≥N 分支逐轮驱动（Advance）：
//
//	P1 Timeline 补齐：以 message 表为源（AfterSeq 游标分批），将
//	   [Timeline 现有最小 seq（hot 窗口起点；Timeline 空=no-op）,
//	    会话当前 MaxSeq] 范围 UPSERT 进 Timeline（uk 幂等）——自愈
//	   hot 消费失败缺口，使 Timeline 成为 hot 窗口完整事实源；
//	P2 摘标：调用注入的 unmark 动作（Detector.unmarkGroup——M01 双落
//	   原样：MySQL hot_level=0 先 → Redis DEL；路由即停：行快照路由下
//	   新消息 GetByID 见 0 走普通通道）；
//	P3 回写扩散：Timeline 全量行 × 当前成员 → inbox_delivery
//	   BatchCreate（uk 幂等；hot 分支唯一的信箱写点，plan §6 禁止事项
//	   的唯一例外）；完成判定=连续两轮 progress==Timeline.MaxSeq 稳定
//	   （吸收在途/重打标追赶）；完成 → 清状态键。
//
// 不变量：任一阶段失败保持当前阶段下轮重试（全幂等）；P2 后新消息走
// 普通通道（inbox 全写）；P3 回写使已拉取过 Timeline 消息的成员重复
// 接收——客户端 (conversationId, msgSeq) 去重呈现（冻结契约：至少一次
// + 去重 = 恰好一次）。mark 侧不要求 Timeline 预回填（路由与打标一致性
// 由「行快照路由 + MySQL 先写 + 消息表恒全量」保证，D6）。
//
// 状态存储：Redis Hash group:{id}:hot:backfill（phase/progress/stable，
// TTL 护栏 24h）+ 全局索引 SET group:hot:backfilling（member=groupId）——
// 索引为 P2 后的推进驱动源：摘标后群退出 hot 集与 recent 候选窗，聚合轮
// 经 Pending() 把切换在途群并回候选（plan §6 集成测试「真聚合循环」语义）。
const (
	// backfillTTL 状态键护栏：覆盖 P1+P3 分钟级回写 + 两轮稳定窗（默认
	// 分钟级聚合周期）后仍留量级余量；超时即弃（消息表恒全量，切换可
	// 重触发），不视为数据丢失面。
	backfillTTL = 24 * time.Hour

	phaseP1 = "p1" // Timeline 补齐
	phaseP2 = "p2" // 摘标（双落）
	phaseP3 = "p3" // 回写扩散 + 两轮稳定

	fieldPhase    = "phase"
	fieldProgress = "progress"
	fieldStable   = "stable"

	// backfillIndexKey 切换在途群索引（SET，member=groupId）：聚合轮候选
	// 第三源（recent ZSET ∪ MySQL hot 集 ∪ 本索引）。
	backfillIndexKey = "group:hot:backfilling"
)

// backfillScanSize P1/P3 单批扫描行数（plan §6：200 消息/批）。
const backfillScanSize = 200

// backfillRowChunk P3 单次 BatchCreate 行数上限（plan §6：≤5000 行/批，
// CreateInBatches 内部再按 500 行/语句切分）。
const backfillRowChunk = 5000

// backfillKey 切换状态键 group:{id}:hot:backfill。
func backfillKey(groupID uint64) string {
	return fmt.Sprintf("group:%d:hot:backfill", groupID)
}

// hotConvPrefix 群会话 ID 前缀（g_{groupId}，spec §5；message/pushpull 包
// 同口径私有拷贝——前缀契约经 ADR-0010 D2 冻结为公开数据契约，包间不
// 建依赖）。
const hotConvPrefix = "g_"

// hotConvID 拼接群会话 ID。
func hotConvID(groupID uint64) string {
	return hotConvPrefix + strconv.FormatUint(groupID, 10)
}

// UnmarkAction 摘标动作（P2 执行体）：由 Detector 注入自身 unmarkGroup
// （M01 双落实现复用，plan D6「调用 M01 unmarkGroup」字面；方法值注入
// 避免 switcher→detector 反向依赖与构造环）。
type UnmarkAction func(ctx context.Context, groupID uint64) error

// UnmarkSwitcher 摘标切换编排器（CAP-104）：状态机推进为幂等分批设计——
// 每次 Advance 在 ctx 预算（聚合轮 30s 沿用）内尽力推进，超时=部分进度
// 持久于状态键、下轮续；nil 注入 Detector 时 M01 行为保底（判定即摘标）。
// 依赖 MessageRepo/GroupTimelineRepo/GroupMemberRepo/InboxDeliveryRepo
// 四 repo（hotdetect 已依赖 dao 先例延续）+ Redis 状态键。goroutine 属主
// 为 Detector.Run 聚合轮（无自有协程）。
type UnmarkSwitcher struct {
	rdb      redis.UniversalClient
	msgs     dao.MessageRepo
	timeline dao.GroupTimelineRepo
	gmembers dao.GroupMemberRepo
	inbox    dao.InboxDeliveryRepo
	logger   *zap.Logger
}

// NewUnmarkSwitcher 构造切换编排器；rdb 与四 repo 由装配层创建与关闭；
// logger 为 nil 时 zap.NewNop 兜底。
func NewUnmarkSwitcher(rdb redis.UniversalClient, msgs dao.MessageRepo,
	timeline dao.GroupTimelineRepo, gmembers dao.GroupMemberRepo,
	inbox dao.InboxDeliveryRepo, logger *zap.Logger) *UnmarkSwitcher {
	if logger == nil {
		logger = zap.NewNop()
	}
	return &UnmarkSwitcher{
		rdb:      rdb,
		msgs:     msgs,
		timeline: timeline,
		gmembers: gmembers,
		inbox:    inbox,
		logger:   logger,
	}
}

// backfillState 状态键视图（Hash 三字段）。
type backfillState struct {
	phase    string
	progress uint64
	stable   int
}

// Advance 推进切换状态机一轮（聚合轮低周期驱动；CAP-104）：返回
// (finished, err)——finished=true 表示协议完成（状态已清）；err 非 nil
// 表示本轮失败（保持当前阶段，下轮重试，全幂等）。unmark 为 P2 执行体
// （nil 视为装配错误返回错误，不静默跳过摘标）。
//
// 单轮尽力语义：ctx 预算内连续推进 P1→P2→P3（P1/P3 各自分批），每批
// 落库后即持久 progress——超时返回 (false, nil) 由下轮续。
func (s *UnmarkSwitcher) Advance(ctx context.Context, groupID uint64, unmark UnmarkAction) (bool, error) {
	st, err := s.loadState(ctx, groupID)
	if err != nil {
		return false, err
	}
	if st.phase == "" {
		// 首轮初始化：P1 起步（progress 由 P1 依 Timeline.MinSeq 定位）。
		st.phase = phaseP1
		if err := s.saveState(ctx, groupID, st); err != nil {
			return false, err
		}
	}

	for {
		if err := ctx.Err(); err != nil {
			// 预算尽：当前进度已随各批持久，下轮续（非错误语义）。
			return false, nil
		}
		switch st.phase {
		case phaseP1:
			done, aerr := s.advanceP1(ctx, groupID, st)
			if aerr != nil {
				return false, aerr
			}
			if !done {
				return false, nil // 部分进度已持久，下轮续
			}
			st.phase, st.progress, st.stable = phaseP2, 0, 0
			if serr := s.saveState(ctx, groupID, st); serr != nil {
				return false, serr
			}
		case phaseP2:
			if unmark == nil {
				return false, fmt.Errorf("hotdetect switcher group %d: unmark action not injected (assembly error)", groupID)
			}
			if uerr := unmark(ctx, groupID); uerr != nil {
				return false, fmt.Errorf("hotdetect switcher group %d: unmark (P2): %w", groupID, uerr)
			}
			s.logger.Info("hotdetect switch: group unmarked (P2 done, routing stopped, backfill continues)",
				zap.Uint64("groupId", groupID))
			st.phase, st.progress, st.stable = phaseP3, 0, 0
			if serr := s.saveState(ctx, groupID, st); serr != nil {
				return false, serr
			}
		case phaseP3:
			return s.advanceP3(ctx, groupID, st)
		default:
			// 状态键脏数据（历史/手工写入）：回 P1 重走（全幂等，无破坏面）。
			s.logger.Warn("hotdetect switch: unknown phase, reset to p1",
				zap.Uint64("groupId", groupID), zap.String("phase", st.phase))
			st.phase, st.progress, st.stable = phaseP1, 0, 0
			if serr := s.saveState(ctx, groupID, st); serr != nil {
				return false, serr
			}
		}
	}
}

// advanceP1 Timeline 补齐一批或多批（plan D6 P1）：游标自 st.progress
// （首轮由 Timeline.MinSeq 定位=hot 窗口起点；Timeline 空=no-op 直接完成）
// 以 message 表 AfterSeq 扫描至会话 MaxSeq，逐批 UPSERT 进 Timeline。
// 返回 done=是否完成（部分进度已持久）。
func (s *UnmarkSwitcher) advanceP1(ctx context.Context, groupID uint64, st *backfillState) (bool, error) {
	conv := hotConvID(groupID)
	if st.progress == 0 {
		minSeq, err := s.timeline.MinSeq(ctx, groupID)
		if err != nil {
			return false, fmt.Errorf("hotdetect switcher group %d: timeline min seq (P1): %w", groupID, err)
		}
		if minSeq == 0 {
			// Timeline 空=no-op（plan D6 字面）：hot 窗口无行，补齐面为空。
			return true, nil
		}
		st.progress = minSeq // 游标=窗口起点（AfterSeq 严格大于）
		if err := s.saveState(ctx, groupID, st); err != nil {
			return false, err
		}
	}
	target, err := s.msgs.MaxSeq(ctx, conv)
	if err != nil {
		return false, fmt.Errorf("hotdetect switcher group %d: message max seq (P1): %w", groupID, err)
	}
	if st.progress >= target {
		return true, nil // 已追平（前轮完成重入/零增量）
	}

	advanced := false
	for st.progress < target {
		if err := ctx.Err(); err != nil {
			return false, nil // 预算尽：进度已逐批持久
		}
		batch, aerr := s.msgs.AfterSeq(ctx, conv, st.progress, backfillScanSize)
		if aerr != nil {
			return false, fmt.Errorf("hotdetect switcher group %d: after seq %d (P1): %w", groupID, st.progress, aerr)
		}
		if len(batch) == 0 {
			break // 目标 seq 间的空洞扫描完毕（无更多行）
		}
		rows := make([]*model.GroupTimeline, len(batch))
		for i, m := range batch {
			rows[i] = &model.GroupTimeline{GroupID: groupID, MsgSeq: m.MsgSeq, MsgID: m.MsgID}
		}
		if aerr := s.timeline.AppendBatch(ctx, rows); aerr != nil {
			return false, fmt.Errorf("hotdetect switcher group %d: timeline append batch (P1): %w", groupID, aerr)
		}
		st.progress = batch[len(batch)-1].MsgSeq
		advanced = true
		if serr := s.saveState(ctx, groupID, st); serr != nil {
			return false, serr
		}
	}
	if advanced {
		s.logger.Info("hotdetect switch: timeline backfill progress (P1)",
			zap.Uint64("groupId", groupID),
			zap.Uint64("progress", st.progress),
			zap.Uint64("target", target))
	}
	return st.progress >= target, nil
}

// advanceP3 回写扩散一轮（plan D6 P3）：ListAfter 游标扫描 Timeline 行 ×
// 当前成员展开 → BatchCreate 分块（单次 ≤backfillRowChunk 行）；追平
// Timeline.MaxSeq 时计稳定轮——连续两轮 progress==maxSeq 且本轮无推进
// 才清态（CASE-110 口径：追平轮计 1，再追平轮（有推进）重计 1，无推进
// 稳定轮清态）。
func (s *UnmarkSwitcher) advanceP3(ctx context.Context, groupID uint64, st *backfillState) (bool, error) {
	target, err := s.timeline.MaxSeq(ctx, groupID)
	if err != nil {
		return false, fmt.Errorf("hotdetect switcher group %d: timeline max seq (P3): %w", groupID, err)
	}

	// 成员快照一轮一取（P3 期间成员可变——按当前成员回写；≤500）。
	members := make([]uint64, 0, 16)
	if st.progress < target {
		rows, merr := s.gmembers.ListByGroup(ctx, groupID)
		if merr != nil {
			return false, fmt.Errorf("hotdetect switcher group %d: member list (P3): %w", groupID, merr)
		}
		for _, r := range rows {
			members = append(members, r.UserID)
		}
	}

	// 单次 BatchCreate 行上限换算的消息子批量（≤5000 行护栏；成员为 0 时
	// 仍逐行扫描推进游标——空群回写为纯游标推进）。
	subSize := backfillScanSize
	if n := len(members); n > 0 && backfillRowChunk/n < subSize {
		if backfillRowChunk/n < 1 {
			subSize = 1 // 成员数 >5000 的防御钳制（群上限 500，不可达）
		} else {
			subSize = backfillRowChunk / n
		}
	}

	advanced := false
	for st.progress < target {
		if err := ctx.Err(); err != nil {
			return false, nil // 预算尽：进度已逐批持久
		}
		rows, lerr := s.timeline.ListAfter(ctx, groupID, st.progress, subSize)
		if lerr != nil {
			return false, fmt.Errorf("hotdetect switcher group %d: timeline list after %d (P3): %w", groupID, st.progress, lerr)
		}
		if len(rows) == 0 {
			break // 空洞扫描完毕（progress 与 target 间无行）
		}
		if len(members) > 0 {
			records := make([]*model.InboxDelivery, 0, len(rows)*len(members))
			for _, row := range rows {
				for _, uid := range members {
					records = append(records, &model.InboxDelivery{
						UserID:         uid,
						ConversationID: hotConvID(groupID),
						MsgID:          row.MsgID,
						MsgSeq:         row.MsgSeq,
					})
				}
			}
			if berr := s.inbox.BatchCreate(ctx, records); berr != nil {
				return false, fmt.Errorf("hotdetect switcher group %d: inbox batch create (P3): %w", groupID, berr)
			}
		}
		st.progress = rows[len(rows)-1].MsgSeq
		advanced = true
		if serr := s.saveState(ctx, groupID, st); serr != nil {
			return false, serr
		}
	}

	if st.progress >= target {
		if advanced {
			st.stable = 1 // 追平轮计 1（新水平第一轮）
		} else {
			st.stable++ // 无推进稳定轮累计
		}
		if st.stable >= 2 {
			if cerr := s.clearState(ctx, groupID); cerr != nil {
				return false, cerr
			}
			s.logger.Info("hotdetect switch: backfill complete (protocol done, state cleared)",
				zap.Uint64("groupId", groupID),
				zap.Uint64("progress", st.progress),
				zap.Uint64("target", target))
			return true, nil
		}
		if serr := s.saveState(ctx, groupID, st); serr != nil {
			return false, serr
		}
		s.logger.Info("hotdetect switch: backfill caught up, awaiting stable round (P3)",
			zap.Uint64("groupId", groupID),
			zap.Uint64("progress", st.progress),
			zap.Uint64("target", target),
			zap.Int("stable", st.stable))
	} else if advanced {
		s.logger.Info("hotdetect switch: backfill progress (P3)",
			zap.Uint64("groupId", groupID),
			zap.Uint64("progress", st.progress),
			zap.Uint64("target", target))
	}
	return false, nil
}

// Pending 返回切换在途群 ID 集（索引 SET SMEMBERS）：聚合轮候选第三源
// （P2 摘标后群退出 hot 集/recent 窗，仍需驱动 P3 至完成）。
func (s *UnmarkSwitcher) Pending(ctx context.Context) ([]uint64, error) {
	members, err := s.rdb.SMembers(ctx, backfillIndexKey).Result()
	if err != nil {
		return nil, fmt.Errorf("hotdetect switcher smembers %s: %w", backfillIndexKey, err)
	}
	ids := make([]uint64, 0, len(members))
	for _, m := range members {
		id, cerr := strconv.ParseUint(m, 10, 64)
		if cerr != nil {
			continue // 非法 member（历史脏数据）跳过，不阻断整轮
		}
		ids = append(ids, id)
	}
	return ids, nil
}

// loadState 读状态键（键不存在返回零值 state——phase=="" 由 Advance
// 判定首轮初始化）。
func (s *UnmarkSwitcher) loadState(ctx context.Context, groupID uint64) (*backfillState, error) {
	m, err := s.rdb.HGetAll(ctx, backfillKey(groupID)).Result()
	if err != nil {
		return nil, fmt.Errorf("hotdetect switcher hgetall %s: %w", backfillKey(groupID), err)
	}
	st := &backfillState{phase: m[fieldPhase]}
	if v, ok := m[fieldProgress]; ok {
		st.progress, _ = strconv.ParseUint(v, 10, 64) // 解析失败按 0（幂等重扫）
	}
	if v, ok := m[fieldStable]; ok {
		st.stable, _ = strconv.Atoi(v) // 同上
	}
	return st, nil
}

// saveState 持久状态（Hash 三字段 + TTL 刷新 + 索引 SADD 同步）。
func (s *UnmarkSwitcher) saveState(ctx context.Context, groupID uint64, st *backfillState) error {
	key := backfillKey(groupID)
	pipe := s.rdb.Pipeline()
	pipe.HSet(ctx, key, map[string]string{
		fieldPhase:    st.phase,
		fieldProgress: strconv.FormatUint(st.progress, 10),
		fieldStable:   strconv.Itoa(st.stable),
	})
	pipe.Expire(ctx, key, backfillTTL)
	pipe.SAdd(ctx, backfillIndexKey, strconv.FormatUint(groupID, 10))
	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("hotdetect switcher save state %s: %w", key, err)
	}
	return nil
}

// clearState 清状态（协议完成）：DEL 状态键 + SREM 索引（同一轮次幂等——
// DEL/SREM 对不存在键均为 no-op）。
func (s *UnmarkSwitcher) clearState(ctx context.Context, groupID uint64) error {
	pipe := s.rdb.Pipeline()
	pipe.Del(ctx, backfillKey(groupID))
	pipe.SRem(ctx, backfillIndexKey, strconv.FormatUint(groupID, 10))
	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("hotdetect switcher clear state group %d: %w", groupID, err)
	}
	return nil
}
