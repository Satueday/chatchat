package model

import "time"

// GroupMember 对应 group_member 表：群成员（W3 使用；联合唯一 uk(group_id, user_id)）。
// JoinedAt 带 autoCreateTime：GORM 对非 CreatedAt/UpdatedAt 命名的时间列不会
// 自动填充，零值 time.Time 以 '0000-00-00' 落库会触发 MySQL 严格模式 1292——
// 由 tag 显式接管（与 DDL DEFAULT CURRENT_TIMESTAMP(3) 同语义，值取
// GORM NowFunc=UTC）。
type GroupMember struct {
	ID         uint64     `gorm:"column:id"`
	GroupID    uint64     `gorm:"column:group_id"`
	UserID     uint64     `gorm:"column:user_id"`
	Role       int8       `gorm:"column:role"`        // 0=成员 1=管理员 2=群主
	MutedUntil *time.Time `gorm:"column:muted_until"` // 禁言到期（W3-M01 增列；nil=未禁言）
	JoinedAt   time.Time  `gorm:"column:joined_at;autoCreateTime"`
}

// TableName 显式表名（默认复数化 group_members，须覆盖）。
func (GroupMember) TableName() string { return "group_member" }
