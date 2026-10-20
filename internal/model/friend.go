package model

import "time"

// Friend 对应 friend 表：好友关系（W3 使用；无外键，联合唯一 uk(user_id, friend_id)）。
type Friend struct {
	ID        uint64    `gorm:"column:id"`
	UserID    uint64    `gorm:"column:user_id"`
	FriendID  uint64    `gorm:"column:friend_id"`
	Status    int8      `gorm:"column:status"` // 0=申请中 1=已好友 2=已拉黑
	Remark    string    `gorm:"column:remark"`
	CreatedAt time.Time `gorm:"column:created_at"`
	UpdatedAt time.Time `gorm:"column:updated_at"`
}

// TableName 显式表名（默认复数化 friends，须覆盖）。
func (Friend) TableName() string { return "friend" }
