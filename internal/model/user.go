// Package model GORM 数据模型，与根 chatchat.sql 逐列对齐（SQL 为唯一事实源，
// 全程禁用 AutoMigrate，W1-M00 裁决）。所有模型显式 TableName 与逐列 column tag；
// 时间统一 time.Time（DATETIME(3)、UTC 存储），可空时间列用指针；
// session.Members 用标准库 json.RawMessage 映射 JSON 列（不引 gorm.io/datatypes）。
package model

import "time"

// User 对应 user 表：注册/登录主表（phone 唯一，username W1 可空）。
type User struct {
	ID        uint64    `gorm:"column:id"`
	Username  string    `gorm:"column:username"`
	Password  string    `gorm:"column:password"` // bcrypt 哈希，禁止明文
	Phone     string    `gorm:"column:phone"`
	Avatar    string    `gorm:"column:avatar"`
	CreatedAt time.Time `gorm:"column:created_at"` // GORM 自动填充（NowFunc=UTC），DDL DEFAULT 兜底
	UpdatedAt time.Time `gorm:"column:updated_at"` // GORM 自动填充，DDL ON UPDATE 兜底
}

// TableName 显式表名（GORM 默认复数化为 users，必须覆盖）。
func (User) TableName() string { return "user" }
