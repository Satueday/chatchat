package model

import "time"

// Message 对应 message 表（W2-M03）：单聊消息落库行。
// MsgID 服务端生成全局唯一（uk_msg_id 兜底）；(ConversationID, MsgSeq)
// 联合唯一（uk_conv_seq）是排序/去重/补齐的核心锚点（spec §1.1/§10）。
// FromUID/ToUID 列名规避 MySQL 保留字 from，语义对齐 ChatMessage.from/to。
type Message struct {
	ID             uint64    `gorm:"column:id"`
	MsgID          string    `gorm:"column:msg_id"`
	ConversationID string    `gorm:"column:conversation_id"`
	FromUID        uint64    `gorm:"column:from_uid"`
	ToUID          uint64    `gorm:"column:to_uid"`
	ContentType    int32     `gorm:"column:content_type"`
	Content        string    `gorm:"column:content"`
	URL            string    `gorm:"column:url"`
	MsgSeq         uint64    `gorm:"column:msg_seq"`
	CreatedAt      time.Time `gorm:"column:created_at"` // GORM 自动填充（NowFunc=UTC），DDL DEFAULT 兜底
}

// TableName 显式表名（GORM 默认复数化为 messages，必须覆盖）。
func (Message) TableName() string { return "message" }
