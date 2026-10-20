package model

import "time"

// InboxDelivery 对应 inbox_delivery 表（W3-M05）：写扩散信箱投递记录，
// 每条消息 × 每个成员一行（「10 人群发 1 条 = 10 行」门槛的直接断言面）。
// (UserID, ConversationID, MsgSeq) 唯一（uk_user_conv_seq，ADR-0007 强键
// 口径——去重锚点 (conv,seq) 优先于 msgId）：消费重投经 BatchCreate 的
// OnConflict DoNothing 达成「重投恰好一次写」；MsgID 仅关联 trace/排障。
// 记录删除/TTL（漫游窗口）归 W3-M06 读侧；本表只写不读（M05 阶段）。
type InboxDelivery struct {
	ID             uint64    `gorm:"column:id"`
	UserID         uint64    `gorm:"column:user_id"`
	ConversationID string    `gorm:"column:conversation_id"`
	MsgID          string    `gorm:"column:msg_id"`
	MsgSeq         uint64    `gorm:"column:msg_seq"`
	CreatedAt      time.Time `gorm:"column:created_at"` // GORM 自动填充（NowFunc=UTC），DDL DEFAULT 兜底
}

// TableName 显式表名（GORM 默认复数化为 inbox_deliveries，必须覆盖）。
func (InboxDelivery) TableName() string { return "inbox_delivery" }
