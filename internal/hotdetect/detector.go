package hotdetect

import (
	"context"
	"errors"
	"fmt"
	"runtime/debug"
	"sort"
	"time"

	"chatchat/config"
	"chatchat/internal/dao"
	"chatchat/internal/leader"
	pkgerrors "chatchat/pkg/errors"

	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
)

// aggregateTimeout 单轮聚合的查询/写入超时上限（reconcileQueryTimeout 先例；
// 一轮内候选发现 + 逐群评估共享该预算）。
const aggregateTimeout = 30 * time.Second

// hotdetectLeaderKey 聚合轮门控 Redis 锁键（W4-M05 plan D7：
// chatchat:leader:hotdetect——多 api 实例同轮至多一执行者）。
const hotdetectLeaderKey = "chatchat:leader:hotdetect"

// RoundGate 聚合每轮咨询锁（W4-M05 plan D7）：leader.Gate 别名；nil=不门控
// （make run 单实例与测试零影响）。TTL 由 Run 按 interval×2 注入。
type RoundGate = leader.Gate

// Detector 聚合宿主（api 进程，plan D3）：周期阈值判定 + 双落打标/摘标 +
// IsHot 查询。桶键构造、窗口取数、streak 状态机全私有；now 未导出注入
// （fake 时钟，server-master 三层范式）。W4-M02：增 switcher（UnmarkSwitcher，
// plan D6）——低周期 streak≥N 分支由「判定即摘标」升级为三阶段切换协议
// （P1 Timeline 补齐 → P2 摘标 → P3 回写扩散）；nil 注入=M01 行为保底。
type Detector struct {
	store    *bucketStore
	stats    dao.HotGroupStatRepo
	groups   dao.GroupRepo
	switcher *UnmarkSwitcher
	opts     options
	logger   *zap.Logger
	now      func() time.Time // 默认 time.Now，单测注入 fake 时钟
}

// 编译期接口实现断言：*Detector 满足对外 HotDetector 契约（Run 为宿主
// 装配面附加方法，不进接口——temp-w4 §5 C2）。
var _ HotDetector = (*Detector)(nil)

// NewDetector 构造聚合器（cmd/api 装配用）：rdb/stats/groups/switcher 由
// 装配层创建与关闭；cfg 的范围与对齐约束已由 config.validate fail-fast
// 前置把关（校验前置，D10）。switcher 为 W4-M02 增参（nil=M01 行为：
// streak≥N 即摘标，切换协议不启用）。
func NewDetector(rdb redis.UniversalClient, stats dao.HotGroupStatRepo, groups dao.GroupRepo,
	switcher *UnmarkSwitcher, cfg config.HotDetect, logger *zap.Logger) *Detector {
	if logger == nil {
		logger = zap.NewNop()
	}
	opts := newOptions(cfg)
	return &Detector{
		store:    newBucketStore(rdb, opts, time.Now),
		stats:    stats,
		groups:   groups,
		switcher: switcher,
		opts:     opts,
		logger:   logger,
		now:      time.Now,
	}
}

// Run 阻塞聚合循环（宿主 goroutine 持有，RunReconcile 先例）：启动即跑
// 一轮 → interval 周期 ticker → ctx.Done 返回（goroutine 由调用方
// WaitGroup 回收）。gate 为 W4-M05 增参（plan D7：Redis 每轮咨询锁——多
// api 实例同轮至多一执行者；nil=不门控；TTL=interval×2，进程崩溃锁自然
// 过期；正确性兜底=聚合/切换全幂等）。单轮 30s 超时；读/写失败仅 Error
// 日志不中断循环（下轮幂等自愈）；单群失败不中断其他群（per-group
// recover，plan §6）。UnmarkSwitcher 驱动随本循环同门控（宿主同循环）。
func (d *Detector) Run(ctx context.Context, gate RoundGate) {
	if d.leaderThisRound(ctx, gate) {
		d.runOnce(ctx)
	}
	ticker := time.NewTicker(d.opts.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if d.leaderThisRound(ctx, gate) {
				d.runOnce(ctx)
			}
		}
	}
}

// leaderThisRound 竞取本轮执行权：nil gate 恒 true（不门控）；Acquire
// false（他实例持锁或 Redis 不可达）跳轮——宁缺勿重，正确性由幂等兜底。
func (d *Detector) leaderThisRound(ctx context.Context, gate RoundGate) bool {
	if gate == nil {
		return true
	}
	return gate.Acquire(ctx, hotdetectLeaderKey, 2*d.opts.interval)
}

// runOnce 执行单轮聚合：候选发现（ZSET recent ∪ MySQL hot 集——静默 hot
// 群也要参与低周期计数，否则永不被评估摘标，plan D1；W4-M02 增第三源
// ∪ 切换在途索引——P2 摘标后群退出前两源，P3 回写仍需逐轮驱动至完成，
// plan D6）→ 升序逐群评估 → Debug 摘要（M05「热点群数量」指标预留可
// 聚合字段）。
func (d *Detector) runOnce(ctx context.Context) {
	qctx, cancel := context.WithTimeout(ctx, aggregateTimeout)
	defer cancel()
	now := d.now()

	zsetIDs, err := d.store.candidates(qctx, now)
	if err != nil {
		d.logger.Error("hotdetect candidate query failed",
			zap.Int32("code", pkgerrors.ErrHotStatQuery.Code), zap.Error(err))
		return
	}
	if err := d.store.pruneRecent(qctx, now); err != nil {
		// 裁剪失败不影响本轮候选（仅注册表多留旧成员，下轮重试自愈）。
		d.logger.Error("hotdetect recent prune failed",
			zap.Int32("code", pkgerrors.ErrHotStatQuery.Code), zap.Error(err))
	}
	hotIDs, err := d.groups.ListHotGroupIDs(qctx)
	if err != nil {
		d.logger.Error("hotdetect hot set query failed",
			zap.Int32("code", pkgerrors.ErrHotStatQuery.Code), zap.Error(err))
		return
	}
	// 切换在途群（索引 SET）：读取失败不阻断本轮（前两源照常，下轮重试
	// 自愈——P3 全幂等，晚一轮推进无正确性影响）。
	var pendingIDs []uint64
	backfilling := make(map[uint64]bool)
	if d.switcher != nil {
		pendingIDs, err = d.switcher.Pending(qctx)
		if err != nil {
			d.logger.Error("hotdetect backfill pending query failed",
				zap.Int32("code", pkgerrors.ErrHotStatQuery.Code), zap.Error(err))
			pendingIDs = nil
		}
		for _, id := range pendingIDs {
			backfilling[id] = true
		}
	}

	// 候选 ∪ hot 集 ∪ 切换在途 去重后按群 ID 升序（plan §4 调用流程）。
	hotSet := make(map[uint64]struct{}, len(hotIDs))
	for _, id := range hotIDs {
		hotSet[id] = struct{}{}
	}
	seen := make(map[uint64]struct{}, len(zsetIDs)+len(hotIDs)+len(pendingIDs))
	cand := make([]uint64, 0, len(zsetIDs)+len(hotIDs)+len(pendingIDs))
	for _, id := range append(append(zsetIDs, hotIDs...), pendingIDs...) {
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		cand = append(cand, id)
	}
	sort.Slice(cand, func(i, j int) bool { return cand[i] < cand[j] })

	marked, unmarked := 0, 0
	for _, id := range cand {
		_, isHot := hotSet[id]
		m, u := d.evaluateGroup(qctx, id, isHot, backfilling[id], now)
		if m {
			marked++
		}
		if u {
			unmarked++
		}
	}
	d.logger.Debug("hotdetect scan complete",
		zap.Int("candidates", len(cand)),
		zap.Int("evaluated", len(cand)),
		zap.Int("marked", marked),
		zap.Int("unmarked", unmarked))
}

// evaluateGroup 评估单群（CAP-002/003/004 核心）：窗口 MGET → 阈值判定
// （含等号，整数算术 windowSum*60 ≥ threshold*windowSec）→ 打标/摘标
// 状态机 → 周期增量落 stat。返回 (marked, unmarked) 供轮摘要计数。
// 单群失败（含 panic）不中断其他群：错误 Error 日志（6001 读面/6002 写面）
// 后返回，下轮重评幂等自愈。
// W4-M02：backfilling=切换协议在途（switcher 状态存在）——低周期时优先
// 推进 Advance（P3 驱动；期间重新打标由 hotNow 分支处置，P3 续行）。
func (d *Detector) evaluateGroup(ctx context.Context, groupID uint64, isHot, backfilling bool, now time.Time) (marked, unmarked bool) {
	defer func() {
		if p := recover(); p != nil {
			// 防单群坏数据/实现缺陷卡死整轮聚合（plan §6 关键实现细节）。
			// 堆栈 debug.Stack：展平栈仍含 evaluateGroup 帧，供定位案发现场。
			d.logger.Error("hotdetect group evaluation panicked",
				zap.Uint64("groupId", groupID),
				zap.Int32("code", pkgerrors.ErrHotStatQuery.Code),
				zap.Any("panic", p),
				zap.String("stack", string(debug.Stack())))
		}
	}()

	windowSum, err := d.store.windowSum(ctx, groupID, now)
	if err != nil {
		d.logger.Error("hotdetect window read failed",
			zap.Uint64("groupId", groupID),
			zap.Int32("code", pkgerrors.ErrHotStatQuery.Code), zap.Error(err))
		return false, false
	}
	periodSum, err := d.store.periodSum(ctx, groupID, now)
	if err != nil {
		d.logger.Error("hotdetect period read failed",
			zap.Uint64("groupId", groupID),
			zap.Int32("code", pkgerrors.ErrHotStatQuery.Code), zap.Error(err))
		return false, false
	}

	windowSec := int64(d.opts.window / time.Second)
	hotNow := windowSum*60 >= d.opts.thresholdMsgPerMin*windowSec // ≥ 含等号（D10/CASE-012）
	rate := float64(windowSum) * 60 / float64(windowSec)          // msg/min，日志面

	switch {
	case hotNow:
		// 高周期：streak 清零重计（D5）；未 hot → 双落打标；已 hot → SET 刷
		// TTL（D4：每周期重评仍 hot 时刷新，TTL 丢失由 IsHot 回源自愈）。
		// 切换在途群重新打标：P3 暂行一轮（状态保留，低周期续推进——
		// plan D6「期间重新打标：P3 继续，不误清态」）。
		if err := d.store.delStreak(ctx, groupID); err != nil {
			d.logger.Error("hotdetect streak reset failed",
				zap.Uint64("groupId", groupID),
				zap.Int32("code", pkgerrors.ErrHotStatQuery.Code), zap.Error(err))
			return false, false
		}
		if !isHot {
			if err := d.markGroup(ctx, groupID); err != nil {
				d.logger.Error("hotdetect mark failed",
					zap.Uint64("groupId", groupID),
					zap.Int32("code", pkgerrors.ErrHotMarkWrite.Code), zap.Error(err))
				return false, false
			}
			d.logger.Info("group marked hot",
				zap.Uint64("groupId", groupID),
				zap.Float64("rate", rate),
				zap.Int64("thresholdMsgPerMin", d.opts.thresholdMsgPerMin),
				zap.Int64("windowSum", windowSum))
			marked = true
		} else if err := d.store.setHot(ctx, groupID); err != nil {
			d.logger.Error("hotdetect hot key refresh failed",
				zap.Uint64("groupId", groupID),
				zap.Int32("code", pkgerrors.ErrHotMarkWrite.Code), zap.Error(err))
		}
	case backfilling:
		// 切换协议在途（P1/P2/P3 任一阶段或两轮稳定窗）且低周期：逐轮推进
		//（P2 摘标后群已非 hot——本分支是 P3 的唯一驱动源；全幂等，失败
		// 保持阶段下轮重试，plan D6）。
		finished, aerr := d.switcher.Advance(ctx, groupID, d.unmarkGroup)
		if aerr != nil {
			d.logger.Error("hotdetect switch advance failed",
				zap.Uint64("groupId", groupID),
				zap.Int32("code", pkgerrors.ErrHotMarkWrite.Code), zap.Error(aerr))
			return false, false
		}
		if finished {
			d.logger.Info("group unmarked hot (switch protocol complete)",
				zap.Uint64("groupId", groupID),
				zap.Float64("rate", rate),
				zap.Int64("thresholdMsgPerMin", d.opts.thresholdMsgPerMin),
				zap.Int64("windowSum", windowSum))
			unmarked = true
		}
	case isHot:
		// 低周期且 hot：streak 跨周期累计；连续 N 周期低于阈值 → 摘标。
		streak, err := d.store.incrStreak(ctx, groupID)
		if err != nil {
			d.logger.Error("hotdetect streak incr failed",
				zap.Uint64("groupId", groupID),
				zap.Int32("code", pkgerrors.ErrHotStatQuery.Code), zap.Error(err))
			return false, false
		}
		if streak >= d.opts.lowPeriods {
			if d.switcher == nil {
				// M01 语义保底：判定即摘标（无切换协议）。
				if err := d.unmarkGroup(ctx, groupID); err != nil {
					d.logger.Error("hotdetect unmark failed",
						zap.Uint64("groupId", groupID),
						zap.Int32("code", pkgerrors.ErrHotMarkWrite.Code), zap.Error(err))
					return false, false
				}
			} else {
				// W4-M02 D6：三阶段切换协议首轮进入（P1 补齐 → P2 摘标 →
				// P3 回写；本调用尽力推进，完成与否由返回值区分）。
				finished, aerr := d.switcher.Advance(ctx, groupID, d.unmarkGroup)
				if aerr != nil {
					d.logger.Error("hotdetect switch advance failed",
						zap.Uint64("groupId", groupID),
						zap.Int32("code", pkgerrors.ErrHotMarkWrite.Code), zap.Error(aerr))
					return false, false
				}
				if !finished {
					break // 协议在途：本轮不计数（完成轮计入 unmarked）
				}
			}
			d.logger.Info("group unmarked hot",
				zap.Uint64("groupId", groupID),
				zap.Float64("rate", rate),
				zap.Int64("thresholdMsgPerMin", d.opts.thresholdMsgPerMin),
				zap.Int64("windowSum", windowSum),
				zap.Int64("lowPeriods", streak))
			unmarked = true
		}
	}

	// 周期增量 > 0 → hot_group_stat UPSERT（CAP-006；零活动群不产行，D9）。
	if periodSum > 0 {
		if err := d.stats.UpsertWindow(ctx, groupID, d.store.periodWindowTS(now), uint64(periodSum)); err != nil {
			d.logger.Error("hotdetect stat upsert failed",
				zap.Uint64("groupId", groupID),
				zap.Int32("code", pkgerrors.ErrHotStatQuery.Code), zap.Error(err))
		}
	}
	return marked, unmarked
}

// markGroup 打标双落（CAP-003）：MySQL hot_level=1 先（持久事实源）→ Redis
// hot 键后（查询热路径缓存）。任一步失败返回 6002 包装错误（调用方 Error
// 日志，下一周期重评幂等自愈——MySQL 已写而 Redis 未写的中间态由回源收敛）。
func (d *Detector) markGroup(ctx context.Context, groupID uint64) error {
	if err := d.groups.UpdateHotLevel(ctx, groupID, 1); err != nil {
		return pkgerrors.Wrap(fmt.Errorf("mark group %d: update hot_level=1: %w", groupID, err),
			pkgerrors.ErrHotMarkWrite.Code, pkgerrors.ErrHotMarkWrite.Message)
	}
	if err := d.store.setHot(ctx, groupID); err != nil {
		return pkgerrors.Wrap(fmt.Errorf("mark group %d: set hot key: %w", groupID, err),
			pkgerrors.ErrHotMarkWrite.Code, pkgerrors.ErrHotMarkWrite.Message)
	}
	return nil
}

// unmarkGroup 摘标双落（CAP-004；M02「Timeline 补齐后摘标」编排缝——本
// 方法即摘标动作的收敛点，D5）：MySQL hot_level=0 先 → DEL hot 键 +
// DEL streak 键。DEL 失败仅 Error 日志不返回错误（键有 TTL 兜底 + IsHot
// 以 MySQL 为回源事实，D4）；MySQL 失败返回 6002 包装错误。
func (d *Detector) unmarkGroup(ctx context.Context, groupID uint64) error {
	if err := d.groups.UpdateHotLevel(ctx, groupID, 0); err != nil {
		return pkgerrors.Wrap(fmt.Errorf("unmark group %d: update hot_level=0: %w", groupID, err),
			pkgerrors.ErrHotMarkWrite.Code, pkgerrors.ErrHotMarkWrite.Message)
	}
	if err := d.store.delHot(ctx, groupID); err != nil {
		d.logger.Error("hotdetect hot key del failed (ttl will reclaim)",
			zap.Uint64("groupId", groupID),
			zap.Int32("code", pkgerrors.ErrHotMarkWrite.Code), zap.Error(err))
	}
	if err := d.store.delStreak(ctx, groupID); err != nil {
		d.logger.Error("hotdetect streak del failed (ttl will reclaim)",
			zap.Uint64("groupId", groupID),
			zap.Int32("code", pkgerrors.ErrHotMarkWrite.Code), zap.Error(err))
	}
	return nil
}

// IsHot 双源查询（CAP-005）：Redis 命中→返回；miss/错误→MySQL hot_level
// （>0 即 hot）；双源均败→6003 上抛（M02 降级判定输入）。群不存在（4001）
// 返回 (false, nil)——不存在的群无热点语义，非查询失败。
func (d *Detector) IsHot(ctx context.Context, groupID uint64) (bool, error) {
	hot, err := d.store.hotExists(ctx, groupID)
	if err == nil {
		return hot, nil
	}
	if !errors.Is(err, redis.Nil) {
		// Redis 基础设施错误（非 miss）：降级回源 MySQL，不在此返回。
		d.logger.Warn("hotdetect hot key read failed, fall back to mysql",
			zap.Uint64("groupId", groupID), zap.Error(err))
	}
	g, gerr := d.groups.GetByID(ctx, groupID)
	if gerr != nil {
		if isGroupNotFound(gerr) {
			return false, nil
		}
		return false, pkgerrors.Wrap(fmt.Errorf("ishot group %d: mysql fallback: %w", groupID, gerr),
			pkgerrors.ErrHotQueryFailed.Code, pkgerrors.ErrHotQueryFailed.Message)
	}
	return g.HotLevel > 0, nil
}

// isGroupNotFound 判定错误是否群未命中哨兵（4001，含 wrap 链穿透——
// message 包同款先例）。
func isGroupNotFound(err error) bool {
	var e *pkgerrors.Error
	return errors.As(err, &e) && e.Code == pkgerrors.ErrGroupNotFound.Code
}
