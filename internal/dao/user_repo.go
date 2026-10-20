// Package dao 数据访问层（W1-M05）：Repository 窄接口 + GORM 实现、
// MySQL/Redis 连接构造。接口签名零 GORM 类型（service 从第一天面向接口、可 mock）；
// DB/Redis 句柄由装配层（cmd，W1-M06）持有并 defer Close，本包不设包级全局句柄、
// 不打印 SQL（logger.Discard）、DSN/密码不入日志与错误信息。
// 驱动错误经 mapDBError 收敛为 pkg/errors 哨兵，service 层不接触 GORM 错误。
package dao

import (
	"context"
	"errors"
	"fmt"

	"chatchat/internal/model"
	pkgerrors "chatchat/pkg/errors"

	"gorm.io/gorm"
)

// UserRepo 用户表数据访问契约（W1-M05 冻结；mock 于 W1-M06）。
type UserRepo interface {
	// Create 插入用户并回填 ID；phone 唯一冲突返回 ErrPhoneExists。
	Create(ctx context.Context, u *model.User) error
	// GetByPhone 按手机号查询；未命中返回 ErrUserNotFound。
	GetByPhone(ctx context.Context, phone string) (*model.User, error)
	// GetByID 按主键查询；未命中返回 ErrUserNotFound。
	GetByID(ctx context.Context, id uint64) (*model.User, error)
	// UpdateProfile 仅更新 username/avatar（列白名单）；目标不存在返回 ErrUserNotFound。
	UpdateProfile(ctx context.Context, id uint64, u *model.User) error
}

// userRepo 为 UserRepo 的 GORM 实现：持有连接句柄，无状态。
type userRepo struct {
	db *gorm.DB
}

// NewUserRepo 构造 UserRepo；db 由装配层创建与关闭。
func NewUserRepo(db *gorm.DB) UserRepo {
	return &userRepo{db: db}
}

// Create 插入用户（GORM 自动回填 ID 与 created_at/updated_at）。
// 不做 Count 预查（go-chat TOCTOU 反面教材）：唯一冲突依赖 uk_phone 即时暴露。
func (r *userRepo) Create(ctx context.Context, u *model.User) error {
	if err := r.db.WithContext(ctx).Create(u).Error; err != nil {
		return mapDBError(err)
	}
	return nil
}

// GetByPhone 按手机号取单条（phone 唯一，Take 不加排序）。
func (r *userRepo) GetByPhone(ctx context.Context, phone string) (*model.User, error) {
	var u model.User
	if err := r.db.WithContext(ctx).Where("phone = ?", phone).Take(&u).Error; err != nil {
		return nil, mapDBError(err)
	}
	return &u, nil
}

// GetByID 按主键取单条。
func (r *userRepo) GetByID(ctx context.Context, id uint64) (*model.User, error) {
	var u model.User
	if err := r.db.WithContext(ctx).Take(&u, id).Error; err != nil {
		return nil, mapDBError(err)
	}
	return &u, nil
}

// UpdateProfile 列白名单更新：Select 限定 username/avatar，
// phone/password 永不可经此路径修改（D9）；结构体更新配合 Select 可写入零值。
// RowsAffected==0 视为用户不存在（GORM Updates 对未命中主键静默 0 行）。
func (r *userRepo) UpdateProfile(ctx context.Context, id uint64, u *model.User) error {
	res := r.db.WithContext(ctx).
		Model(&model.User{}).
		Where("id = ?", id).
		Select("username", "avatar").
		Updates(u)
	if res.Error != nil {
		return mapDBError(res.Error)
	}
	if res.RowsAffected == 0 {
		return pkgerrors.Wrap(fmt.Errorf("update profile affected no rows, id=%d", id),
			pkgerrors.ErrUserNotFound.Code, pkgerrors.ErrUserNotFound.Message)
	}
	return nil
}

// mapDBError 将 GORM（TranslateError:true 已开启）错误收敛为 pkg/errors 哨兵：
// 唯一键冲突→ErrPhoneExists(2001)、记录未命中→ErrUserNotFound(2002)、其余原样上抛。
// 约定使用哨兵的 .Code/.Message 字段（文案随常量定义，调用点不散落拼串）；
// cause 保留驱动链（仅日志面经 Error() 可见；gorm 翻译后的错误不含驱动细节，
// 对外响应另经 pkg/common.Fail 消毒归一）。
func mapDBError(err error) error {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, gorm.ErrDuplicatedKey):
		return pkgerrors.Wrap(err, pkgerrors.ErrPhoneExists.Code, pkgerrors.ErrPhoneExists.Message)
	case errors.Is(err, gorm.ErrRecordNotFound):
		return pkgerrors.Wrap(err, pkgerrors.ErrUserNotFound.Code, pkgerrors.ErrUserNotFound.Message)
	default:
		return err
	}
}
