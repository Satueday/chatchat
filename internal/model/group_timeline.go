package model

import "time"

// GroupTimeline 对应 group_timeline 表（W4-M02，spec §7.2/§10；plan D7）：
// 热点群读扩散 Timeline——每群每消息恰一行（uk_group_seq 幂等主键）。
// msg_id 引用 message 表、不冗余内容（消息表恒全量，读侧 M03 按 msg_id
// 批量取内容或 join）；写路径两源：hot 消费者 HotService（每消息 UPSERT
// 一行）与摘标切换 UnmarkSwitcher P1 补齐（AfterSeq 游标自 message 表
// 批量 UPSERT，自愈消费失败缺口）。分表分桶下月（plan §2 Out of Scope）。
type GroupTimeline struct {
	ID        uint64    `gorm:"column:id"`
	GroupID   uint64    `gorm:"column:group_id"`   // 群ID（g_{groupId} 会话）
	MsgSeq    uint64    `gorm:"column:msg_seq"`    // 会话内序号（与 message.msg_seq 同源，排序/去重核心）
	MsgID     string    `gorm:"column:msg_id"`     // 消息ID（引用 message 表；幂等主键为 uk_group_seq）
	CreatedAt time.Time `gorm:"column:created_at"` // GORM 自动填充（NowFunc=UTC），DDL DEFAULT 兜底
}

// TableName 显式表名（GORM 默认复数化为 group_timelines，必须覆盖）。
func (GroupTimeline) TableName() string { return "group_timeline" }
