package model

import "time"

// UserDevice 对应 user_device 表：ADR-0003 仅建表，W1 无写入逻辑。
type UserDevice struct {
	ID          uint64     `gorm:"column:id"`
	UserID      uint64     `gorm:"column:user_id"`
	DeviceID    string     `gorm:"column:device_id"`
	Platform    string     `gorm:"column:platform"`      // ios/android/web
	LastLoginAt *time.Time `gorm:"column:last_login_at"` // DATETIME(3) NULL → 指针零值 nil
	CreatedAt   time.Time  `gorm:"column:created_at"`
}

// TableName 显式表名（默认复数化 user_devices，须覆盖）。
func (UserDevice) TableName() string { return "user_device" }
