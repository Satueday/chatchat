package model

import "time"

// GroupInfo 对应 group_info 表：群组元信息（W3 使用）。
type GroupInfo struct {
	ID           uint64    `gorm:"column:id"`
	Name         string    `gorm:"column:name"`
	OwnerID      uint64    `gorm:"column:owner_id"`
	MaxMember    int32     `gorm:"column:max_member"`   // 成员上限（spec §8.2 硬性指标）
	HotLevel     int8      `gorm:"column:hot_level"`    // 热点等级（0=普通，1=热点；>1 预留；W4-M01 D6）
	Status       int8      `gorm:"column:status"`       // 0=正常 1=解散
	Announcement string    `gorm:"column:announcement"` // 群公告（W3-M01 增列；≤512 字符）
	CreatedAt    time.Time `gorm:"column:created_at"`
	UpdatedAt    time.Time `gorm:"column:updated_at"`
}

// TableName 显式表名（默认复数化 group_infos，须覆盖）。
func (GroupInfo) TableName() string { return "group_info" }
