package dao

import (
	"fmt"
	"net/url"
	"time"

	"chatchat/config"

	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

// mysqlDSN 构造 go-sql-driver DSN（私有纯函数，离线可单测，D15）。
// 密码与 loc 经 url.QueryEscape：防密码含 ':'/'@'/'/' 等结构性字符破坏 DSN 解析
// （loc 属 query 值，驱动会 QueryUnescape，UTC 无特殊字符原样输出）。
// 超时固定 10s（连接建立超时，防启动挂起）。DSN 不入日志。
func mysqlDSN(cfg config.MySQL) string {
	return fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?charset=%s&parseTime=%t&loc=%s&timeout=10s",
		cfg.User,
		url.QueryEscape(cfg.Password.Value()),
		cfg.Host, cfg.Port, cfg.Database,
		cfg.Charset, cfg.ParseTime,
		url.QueryEscape(cfg.Loc),
	)
}

// NewMySQLDB 构造 GORM MySQL 连接（Config 输入、池参数可配、句柄由调用方 Close）。
// - TranslateError:true——1062/1451 等驱动错误译为 gorm.ErrDuplicatedKey 等，供 mapDBError 收敛；
// - NowFunc:UTC——GORM 自动填充时间统一 UTC（DDL DATETIME(3) UTC 存储，D10 双轨）；
// - Logger:logger.Discard——dao 层不打印 SQL（DSN/参数不落日志）。
// 构造失败上抛（含驱动 Initialize 失败）；错误不包含 DSN/密码。
func NewMySQLDB(cfg config.MySQL) (*gorm.DB, error) {
	db, err := gorm.Open(mysql.Open(mysqlDSN(cfg)), &gorm.Config{
		TranslateError: true,
		NowFunc:        func() time.Time { return time.Now().UTC() },
		Logger:         logger.Discard,
	})
	if err != nil {
		return nil, fmt.Errorf("初始化 MySQL 连接失败: %w", err)
	}
	sqlDB, err := db.DB()
	if err != nil {
		return nil, fmt.Errorf("获取底层 SQL 句柄失败: %w", err)
	}
	sqlDB.SetMaxOpenConns(cfg.MaxOpenConns)
	sqlDB.SetMaxIdleConns(cfg.MaxIdleConns)
	sqlDB.SetConnMaxLifetime(time.Duration(cfg.ConnMaxLifetimeSec) * time.Second)
	return db, nil
}
