package model

import (
	"encoding/json"
	"time"
)

// Session 对应 session 表：会话（W2 使用；conversation_id 唯一）。
type Session struct {
	ID             uint64          `gorm:"column:id"`
	ConversationID string          `gorm:"column:conversation_id"` // 单聊 u1|u2 排序拼接；群聊 g_{groupId}
	Type           int8            `gorm:"column:type"`            // 0=单聊 1=群聊
	Members        json.RawMessage `gorm:"column:members"`         // JSON 列：成员 uid 数组（格式 W2 冻结）
	CreatedAt      time.Time       `gorm:"column:created_at"`
	UpdatedAt      time.Time       `gorm:"column:updated_at"`
}

// TableName 显式表名（默认复数化 sessions，须覆盖）。
func (Session) TableName() string { return "session" }
