package dao

import (
	"context"

	"chatchat/internal/model"
	pkgerrors "chatchat/pkg/errors"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// groupTimelineBatchSize 单语句批量上限（plan §6 关键实现细节：switcher
// P1 补齐批处理分块 200/批；消费路径单行 Append 不经批）。超出部分由
// CreateInBatches 自动切分多语句，单批行数恒 ≤ 该值（≤5000 行/批护栏远低）。
const groupTimelineBatchSize = 200

// GroupTimelineRepo 热点群 Timeline 数据访问契约（W4-M02 冻结，CAP-103/104
// 原语）：写面 Append 幂等（uk_group_seq 冲突静默）；读面 MaxSeq/MinSeq/
// ListAfter 为 P1 补齐、P3 回写与 M03 拉取侧共用的游标原语（AfterSeq 同构）。
type GroupTimelineRepo interface {
	// Append 幂等写入一行：INSERT ... ON CONFLICT (group_id, msg_seq)
	// DO NOTHING——同 (group,seq) 重投/重跑恰一份（消费至少一次 + uk 幂等
	// = 恰好一次写）。CreatedAt 由 GORM 自动填充，DDL DEFAULT 兜底。
	Append(ctx context.Context, row *model.GroupTimeline) error
	// AppendBatch 幂等批量写入（P1 补齐用）：语义同 Append，按
	// groupTimelineBatchSize 分块；空入参直接返回 nil（不触库）。
	AppendBatch(ctx context.Context, rows []*model.GroupTimeline) error
	// MaxSeq 返回该群 Timeline 已有最大 msg_seq（无行返回 0）。失败包装
	// ErrMessageQuery(5008)。
	MaxSeq(ctx context.Context, groupID uint64) (uint64, error)
	// MinSeq 返回该群 Timeline 已有最小 msg_seq（无行返回 0）。失败包装
	// 5008。P1 以其为 hot 窗口起点（Timeline 空=no-op 的判定源）。
	MinSeq(ctx context.Context, groupID uint64) (uint64, error)
	// ListAfter 返回该群 Timeline 内 msg_seq 严格大于 afterSeq 的行，按
	// msg_seq ASC，至多 limit 条（P3 回写扩散的游标扫描原语）。无命中行
	// 返回空切片（非 error）。失败包装 5008。
	ListAfter(ctx context.Context, groupID uint64, afterSeq uint64, limit int) ([]*model.GroupTimeline, error)
}

// groupTimelineRepo 为 GroupTimelineRepo 的 GORM 实现：持有连接句柄，无状态。
type groupTimelineRepo struct {
	db *gorm.DB
}

// 编译期接口实现断言。
var _ GroupTimelineRepo = (*groupTimelineRepo)(nil)

// NewGroupTimelineRepo 构造 Timeline 仓储；db 由装配层创建与关闭。
func NewGroupTimelineRepo(db *gorm.DB) GroupTimelineRepo {
	return &groupTimelineRepo{db: db}
}

// Append 幂等落一行（MySQL 方言下 OnConflict DoNothing 由驱动改写为
// INSERT IGNORE / ON DUPLICATE KEY 无操作——与 InboxDeliveryRepo.
// BatchCreate 同构先例）。冲突行静默跳过（重投吸收），不视为错误。
func (r *groupTimelineRepo) Append(ctx context.Context, row *model.GroupTimeline) error {
	return r.db.WithContext(ctx).
		Clauses(clause.OnConflict{DoNothing: true}).
		Create(row).Error
}

// AppendBatch 幂等批量落行：每批独立语句，批间无事务包裹（失败整批
// 重试，已入库批次经 uk 幂等吸收——至少一次 + DoNothing = 恰一份）。
func (r *groupTimelineRepo) AppendBatch(ctx context.Context, rows []*model.GroupTimeline) error {
	if len(rows) == 0 {
		return nil
	}
	return r.db.WithContext(ctx).
		Clauses(clause.OnConflict{DoNothing: true}).
		CreateInBatches(rows, groupTimelineBatchSize).Error
}

// MaxSeq 该群 Timeline 最大 msg_seq；无行返回 0（COALESCE 空 NULL 归一，
// MessageRepo.MaxSeq 同构）。
func (r *groupTimelineRepo) MaxSeq(ctx context.Context, groupID uint64) (uint64, error) {
	var maxSeq uint64 // revive：局部名避开内建 max（messageRepo 先例）
	err := r.db.WithContext(ctx).
		Model(&model.GroupTimeline{}).
		Where("group_id = ?", groupID).
		Select("COALESCE(MAX(msg_seq), 0)").
		Scan(&maxSeq).Error
	if err != nil {
		return 0, pkgerrors.Wrap(err, pkgerrors.ErrMessageQuery.Code, pkgerrors.ErrMessageQuery.Message)
	}
	return maxSeq, nil
}

// MinSeq 该群 Timeline 最小 msg_seq；无行返回 0。
func (r *groupTimelineRepo) MinSeq(ctx context.Context, groupID uint64) (uint64, error) {
	var minSeq uint64 // revive：局部名避开内建 min
	err := r.db.WithContext(ctx).
		Model(&model.GroupTimeline{}).
		Where("group_id = ?", groupID).
		Select("COALESCE(MIN(msg_seq), 0)").
		Scan(&minSeq).Error
	if err != nil {
		return 0, pkgerrors.Wrap(err, pkgerrors.ErrMessageQuery.Code, pkgerrors.ErrMessageQuery.Message)
	}
	return minSeq, nil
}

// ListAfter 游标扫描（msg_seq 严格大于 afterSeq、ASC、Limit）；uk_group_seq
// 左前缀 (group_id, msg_seq) 覆盖过滤与排序。
func (r *groupTimelineRepo) ListAfter(ctx context.Context, groupID uint64, afterSeq uint64, limit int) ([]*model.GroupTimeline, error) {
	var rows []*model.GroupTimeline
	err := r.db.WithContext(ctx).
		Where("group_id = ? AND msg_seq > ?", groupID, afterSeq).
		Order("msg_seq ASC").
		Limit(limit).
		Find(&rows).Error
	if err != nil {
		return nil, pkgerrors.Wrap(err, pkgerrors.ErrMessageQuery.Code, pkgerrors.ErrMessageQuery.Message)
	}
	return rows, nil
}
