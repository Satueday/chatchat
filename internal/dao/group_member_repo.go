package dao

import (
	"context"
	"errors"
	"fmt"
	"time"

	"chatchat/internal/model"
	pkgerrors "chatchat/pkg/errors"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// GroupMemberRepo 群成员表数据访问契约（W3-M01 冻结）。入群走
// JoinUnderLimit 事务密闭（plan D4：FOR UPDATE 串行化 + COUNT 上限 + uk
// 唯一三重防线，并发下成员数不越 max_member）；错误按 group_member 表
// 口径收敛为 4xxx 哨兵。
type GroupMemberRepo interface {
	// JoinUnderLimit 事务入群：FOR UPDATE 锁定群行（同群入群串行化）→
	// COUNT 成员数（≥ maxMember 返回 ErrGroupFull(4004)）→ 插入成员行
	// （role=0；uk 冲突返回 ErrAlreadyInGroup(4005)；群不存在 4001）。
	// 不做成员预查（plan D3：uk 冲突即 4005，免 TOCTOU 双查）。
	JoinUnderLimit(ctx context.Context, groupID, userID uint64, maxMember int32) error
	// GetMember 查询单成员行；未命中返回 ErrNotGroupMember(4002)。
	GetMember(ctx context.Context, groupID, userID uint64) (*model.GroupMember, error)
	// Remove 删除成员行（退群/踢人共用）；行不存在返回 ErrNotGroupMember(4002)。
	Remove(ctx context.Context, groupID, userID uint64) error
	// ListByGroup 群成员列表（id ASC，即入群顺序）。
	ListByGroup(ctx context.Context, groupID uint64) ([]*model.GroupMember, error)
	// ListByUser 用户的群列表（id ASC）。
	ListByUser(ctx context.Context, userID uint64) ([]*model.GroupMember, error)
	// UpdateRole 更新成员角色（设/撤管理员、转让后角色修正）；
	// 行不存在返回 ErrTargetNotMember(4006)。同值设置幂等成功。
	UpdateRole(ctx context.Context, groupID, userID uint64, role int8) error
	// UpdateMutedUntil 更新禁言到期（nil=解禁置 NULL）；行不存在返回
	// ErrTargetNotMember(4006)。重复解禁幂等成功。
	UpdateMutedUntil(ctx context.Context, groupID, userID uint64, mutedUntil *time.Time) error
	// CountByGroup 群成员数（群信息 DTO 拼装）。
	CountByGroup(ctx context.Context, groupID uint64) (int64, error)
	// CountByGroups 批量群成员数（我的群列表单查，GROUP BY 聚合）；
	// 空入参返回空 map 不触库。
	CountByGroups(ctx context.Context, groupIDs []uint64) (map[uint64]int64, error)
}

// groupMemberRepo 为 GroupMemberRepo 的 GORM 实现：持有连接句柄，无状态。
type groupMemberRepo struct {
	db *gorm.DB
}

// NewGroupMemberRepo 构造 GroupMemberRepo；db 由装配层创建与关闭。
func NewGroupMemberRepo(db *gorm.DB) GroupMemberRepo {
	return &groupMemberRepo{db: db}
}

// JoinUnderLimit 入群密闭事务（plan D4）：锁定读为当前读语义（不受
// REPEATABLE READ 快照影响），同群入群在群行锁上串行化；COUNT 与 INSERT
// 间无并发窗口，并发越限不可达。上限按调用方传入的 maxMember（群行
// max_member 不可变，服务层取自 GetByID 后透传）。
func (r *groupMemberRepo) JoinUnderLimit(ctx context.Context, groupID, userID uint64, maxMember int32) error {
	return r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var g model.GroupInfo
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
			Where("id = ?", groupID).Take(&g).Error; err != nil {
			return mapGroupDBError(err) // RecordNotFound→4001
		}
		var count int64
		if err := tx.Model(&model.GroupMember{}).
			Where("group_id = ?", groupID).Count(&count).Error; err != nil {
			return err
		}
		if count >= int64(maxMember) {
			return pkgerrors.Wrap(fmt.Errorf("group %d members %d >= max %d", groupID, count, maxMember),
				pkgerrors.ErrGroupFull.Code, pkgerrors.ErrGroupFull.Message)
		}
		if err := tx.Create(&model.GroupMember{GroupID: groupID, UserID: userID}).Error; err != nil {
			return mapGroupMemberDBError(err) // DuplicatedKey→4005
		}
		return nil
	})
}

// GetMember 查询 (group_id, user_id) 成员行；未命中 4002。
func (r *groupMemberRepo) GetMember(ctx context.Context, groupID, userID uint64) (*model.GroupMember, error) {
	var m model.GroupMember
	if err := r.db.WithContext(ctx).
		Where("group_id = ? AND user_id = ?", groupID, userID).Take(&m).Error; err != nil {
		return nil, mapGroupMemberDBError(err)
	}
	return &m, nil
}

// Remove 删除成员行；DELETE 无同值歧义，RowsAffected==0 即行不存在→4002。
func (r *groupMemberRepo) Remove(ctx context.Context, groupID, userID uint64) error {
	res := r.db.WithContext(ctx).
		Where("group_id = ? AND user_id = ?", groupID, userID).
		Delete(&model.GroupMember{})
	if res.Error != nil {
		return mapGroupMemberDBError(res.Error)
	}
	if res.RowsAffected == 0 {
		return pkgerrors.Wrap(fmt.Errorf("remove affected no rows, member (%d,%d) missing", groupID, userID),
			pkgerrors.ErrNotGroupMember.Code, pkgerrors.ErrNotGroupMember.Message)
	}
	return nil
}

// ListByGroup 群成员列表（id ASC）。
func (r *groupMemberRepo) ListByGroup(ctx context.Context, groupID uint64) ([]*model.GroupMember, error) {
	var ms []*model.GroupMember
	if err := r.db.WithContext(ctx).
		Where("group_id = ?", groupID).Order("id ASC").Find(&ms).Error; err != nil {
		return nil, mapGroupMemberDBError(err)
	}
	return ms, nil
}

// ListByUser 用户的群列表（id ASC）。
func (r *groupMemberRepo) ListByUser(ctx context.Context, userID uint64) ([]*model.GroupMember, error) {
	var ms []*model.GroupMember
	if err := r.db.WithContext(ctx).
		Where("user_id = ?", userID).Order("id ASC").Find(&ms).Error; err != nil {
		return nil, mapGroupMemberDBError(err)
	}
	return ms, nil
}

// UpdateRole 更新角色。RowsAffected==0 两义（行不存在 / 同值 UPDATE 的
// changed-rows 口径），存在性复查消歧：行存在幂等成功，不存在 4006
// （额外 SELECT 仅发生在 0 行路径；group_member 无 updated_at 列，
// 同值重复设管理员必然报 0，消歧为必经路径）。
func (r *groupMemberRepo) UpdateRole(ctx context.Context, groupID, userID uint64, role int8) error {
	return r.updateColumn(ctx, groupID, userID, "role", role,
		pkgerrors.ErrTargetNotMember.Code, pkgerrors.ErrTargetNotMember.Message)
}

// UpdateMutedUntil 更新禁言到期（nil 置 NULL；重复解禁幂等）。消歧语义
// 同 UpdateRole，行不存在返回 4006。指针先解包：接口内装 typed-nil
// *time.Time 时 GORM 参数绑定不生成 NULL 赋值，须传显式 nil interface。
func (r *groupMemberRepo) UpdateMutedUntil(ctx context.Context, groupID, userID uint64, mutedUntil *time.Time) error {
	var value any
	if mutedUntil != nil {
		value = *mutedUntil
	}
	return r.updateColumn(ctx, groupID, userID, "muted_until", value,
		pkgerrors.ErrTargetNotMember.Code, pkgerrors.ErrTargetNotMember.Message)
}

// updateColumn 单列更新公共实现：map 形式 Updates 显式写列（nil 值落 NULL
// ——Update(column, value) 对 typed-nil *time.Time 不会生成 NULL 赋值，
// 解禁路径依赖 map 语义）。0 行受影响时复查存在性（group_member 无
// updated_at，同值 UPDATE 在 changed-rows 口径下必然报 0，不能直接判缺失）。
func (r *groupMemberRepo) updateColumn(ctx context.Context, groupID, userID uint64, column string, value any, code int32, message string) error {
	res := r.db.WithContext(ctx).
		Model(&model.GroupMember{}).
		Where("group_id = ? AND user_id = ?", groupID, userID).
		Updates(map[string]any{column: value})
	if res.Error != nil {
		return mapGroupMemberDBError(res.Error)
	}
	if res.RowsAffected > 0 {
		return nil
	}
	var n int64
	if err := r.db.WithContext(ctx).Model(&model.GroupMember{}).
		Where("group_id = ? AND user_id = ?", groupID, userID).Count(&n).Error; err != nil {
		return err
	}
	if n == 0 {
		return pkgerrors.Wrap(fmt.Errorf("update %s affected no rows, member (%d,%d) missing", column, groupID, userID),
			code, message)
	}
	return nil
}

// CountByGroup 群成员数。
func (r *groupMemberRepo) CountByGroup(ctx context.Context, groupID uint64) (int64, error) {
	var n int64
	if err := r.db.WithContext(ctx).
		Model(&model.GroupMember{}).Where("group_id = ?", groupID).Count(&n).Error; err != nil {
		return 0, err
	}
	return n, nil
}

// CountByGroups 批量群成员数（GROUP BY 聚合单查）；空入参短路返回空 map。
// 无成员的群不出现在结果，调用方按缺省 0 取值。
func (r *groupMemberRepo) CountByGroups(ctx context.Context, groupIDs []uint64) (map[uint64]int64, error) {
	res := make(map[uint64]int64, len(groupIDs))
	if len(groupIDs) == 0 {
		return res, nil
	}
	var rows []struct {
		GroupID uint64 `gorm:"column:group_id"`
		N       int64  `gorm:"column:n"`
	}
	err := r.db.WithContext(ctx).
		Model(&model.GroupMember{}).
		Select("group_id, COUNT(*) AS n").
		Where("group_id IN ?", groupIDs).
		Group("group_id").Scan(&rows).Error
	if err != nil {
		return nil, err
	}
	for _, row := range rows {
		res[row.GroupID] = row.N
	}
	return res, nil
}

// mapGroupMemberDBError 将 GORM 错误按 group_member 表口径收敛（W3-M01）：
// 唯一键冲突（uk_group_user）→ErrAlreadyInGroup(4005)、记录未命中→
// ErrNotGroupMember(4002)、其余原样上抛。与 mapGroupDBError 的 4001
// 语义按表分化（群不存在 vs 非成员）。
func mapGroupMemberDBError(err error) error {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, gorm.ErrDuplicatedKey):
		return pkgerrors.Wrap(err, pkgerrors.ErrAlreadyInGroup.Code, pkgerrors.ErrAlreadyInGroup.Message)
	case errors.Is(err, gorm.ErrRecordNotFound):
		return pkgerrors.Wrap(err, pkgerrors.ErrNotGroupMember.Code, pkgerrors.ErrNotGroupMember.Message)
	default:
		return err
	}
}
