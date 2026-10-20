package dao

import (
	"context"
	"time"

	"chatchat/internal/model"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// HotGroupStatRepo 热点群统计数据访问契约（W4-M01 冻结，CAP-006）：写面
// 仅 UPSERT——聚合任务每周期对周期增量 >0 的群落一行；读面（归档/观测）
// 在 W4 范围外（裁剪任务移交登记见 plan §10 行 4）。
type HotGroupStatRepo interface {
	// UpsertWindow 幂等写入一行统计：INSERT ... ON DUPLICATE KEY UPDATE
	// msg_count（uk_group_window 冲突时后写覆盖）——聚合重跑/多实例重复
	// 聚合（M05 前）经此幂等自愈；仅 msg_count>0 的调用有意义（零活动群
	// 不产行由调用方保证，plan §4 D9）。
	UpsertWindow(ctx context.Context, groupID uint64, windowTS time.Time, msgCount uint64) error
}

// hotGroupStatRepo 为 HotGroupStatRepo 的 GORM 实现：持有连接句柄，无状态。
type hotGroupStatRepo struct {
	db *gorm.DB
}

// 编译期接口实现断言。
var _ HotGroupStatRepo = (*hotGroupStatRepo)(nil)

// NewHotGroupStatRepo 构造热点群统计仓储；db 由装配层创建与关闭。
func NewHotGroupStatRepo(db *gorm.DB) HotGroupStatRepo {
	return &hotGroupStatRepo{db: db}
}

// UpsertWindow 幂等落行：MySQL 方言下 OnConflict DoUpdates 渲染为
// INSERT ... ON DUPLICATE KEY UPDATE `msg_count`=VALUES(`msg_count`)。
// windowTS 由调用方（hotdetect 聚合）对齐桶边界，本层不做时间取整
// （对齐口径单点归 Detector，D9）。
func (r *hotGroupStatRepo) UpsertWindow(ctx context.Context, groupID uint64, windowTS time.Time, msgCount uint64) error {
	return r.db.WithContext(ctx).
		Clauses(clause.OnConflict{
			Columns:   []clause.Column{{Name: "group_id"}, {Name: "window_ts"}},
			DoUpdates: clause.AssignmentColumns([]string{"msg_count"}),
		}).
		Create(&model.HotGroupStat{GroupID: groupID, WindowTS: windowTS, MsgCount: msgCount}).Error
}
