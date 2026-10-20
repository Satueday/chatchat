package model

import "time"

// HotGroupStat 对应 hot_group_stat 表（W4-M01，spec §10）：热点群打标
// 统计——每群每聚合周期恰一行（window_ts=周期起点 UTC、对齐桶边界，
// plan §4 D9；非滑动窗口快照）。(GroupID, WindowTS) 唯一（uk_group_window）：
// 聚合重跑经 UpsertWindow 的 ON DUPLICATE KEY UPDATE 幂等自愈（M05 多实例
// 重复聚合的收口前提）。归档/裁剪任务在 W4 范围外（量级评估见 plan §10）。
type HotGroupStat struct {
	ID        uint64    `gorm:"column:id"`
	GroupID   uint64    `gorm:"column:group_id"`   // 群ID
	WindowTS  time.Time `gorm:"column:window_ts"`  // 统计窗口起点（UTC，对齐聚合周期与桶边界）
	MsgCount  uint64    `gorm:"column:msg_count"`  // 窗口内消息数（时间桶聚合）
	CreatedAt time.Time `gorm:"column:created_at"` // GORM 自动填充（NowFunc=UTC），DDL DEFAULT 兜底
}

// TableName 显式表名（GORM 默认复数化为 hot_group_stats，必须覆盖）。
func (HotGroupStat) TableName() string { return "hot_group_stat" }
