package model

import "time"

// InboxCursor 对应 inbox_cursor 表（W2-M04）：用户×会话粒度信箱游标。
// 双游标分工（W2-M04-plan 裁决 D2）：MaxRecvSeq 推送侧（ACK 推进，本模块）；
// MaxSyncedSeq 拉取侧（W2-M05 建 AdvanceSynced 推进，W3 信箱复用）。
// (UserID, ConversationID) 唯一（uk_user_conv）；游标单调不回退（GREATEST 语义）。
type InboxCursor struct {
	ID             uint64    `gorm:"column:id"`
	UserID         uint64    `gorm:"column:user_id"`
	ConversationID string    `gorm:"column:conversation_id"`
	MaxSyncedSeq   uint64    `gorm:"column:max_synced_seq"`
	MaxRecvSeq     uint64    `gorm:"column:max_recv_seq"`
	CreatedAt      time.Time `gorm:"column:created_at"` // GORM 自动填充（NowFunc=UTC），DDL DEFAULT 兜底
	UpdatedAt      time.Time `gorm:"column:updated_at"` // GORM 自动填充，DDL ON UPDATE 兜底
}

// TableName 显式表名（GORM 默认复数化为 inbox_cursors，必须覆盖）。
func (InboxCursor) TableName() string { return "inbox_cursor" }
