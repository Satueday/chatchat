package dao

import (
	"context"
	"errors"
	"fmt"

	"chatchat/internal/model"
	pkgerrors "chatchat/pkg/errors"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// GroupRepo 群组表数据访问契约（W3-M01 冻结）。跨表复合操作（建群带群主
// 成员行、转让群主）沉事务方法内（plan D5，server-master 先例）；单表操作
// 不包事务。错误经 mapGroupDBError 按表收敛为 4xxx 哨兵。
type GroupRepo interface {
	// CreateWithOwner 事务：group_info 一行 + 群主 group_member 行（role=2，
	// 由本方法保证，不信任调用方）原子落库；g.ID 回填供调用方使用。
	// 中途失败整体回滚（go-chat 无主群反面教材）。
	CreateWithOwner(ctx context.Context, g *model.GroupInfo, owner *model.GroupMember) error
	// GetByID 按主键查询；未命中返回 ErrGroupNotFound(4001)。
	GetByID(ctx context.Context, id uint64) (*model.GroupInfo, error)
	// ListByIDs 批量查询（id ASC）；空入参直接返回空切片不触库。
	ListByIDs(ctx context.Context, ids []uint64) ([]*model.GroupInfo, error)
	// UpdateAnnouncement 单列白名单更新（空串=清空）；目标不存在返回
	// ErrGroupNotFound(4001)。同值重复设置幂等成功（changed-rows 口径消歧）。
	UpdateAnnouncement(ctx context.Context, id uint64, content string) error
	// TransferOwner 事务：FOR UPDATE 锁定群行校验 owner（不匹配 4003）→
	// 校验目标为成员（未命中 4006）→ owner_id 变更 + 新群主 role=2 + 旧群主
	// role=1 原子提交；群不存在 4001。
	TransferOwner(ctx context.Context, groupID, fromUID, toUID uint64) error
	// FindOverLimit 对账查询：成员数超过 max_member 的群（JOIN + GROUP BY +
	// HAVING，plan D9）；只查不修（告警归 Reconciler）。
	FindOverLimit(ctx context.Context) ([]OverLimitGroup, error)
	// UpdateHotLevel 单列白名单更新 hot_level（W4-M01，D6 语义 0=普通 1=热点
	// >1 预留——本方法不校验值域，分级扩展下月）。同值重复更新幂等成功
	//（changed-rows 口径消歧同 UpdateAnnouncement）；目标不存在 4001。
	UpdateHotLevel(ctx context.Context, id uint64, level int8) error
	// ListHotGroupIDs 查询 hot_level>0 的群 ID 列表（id ASC）。空结果返回
	// 空切片（语义为查询，必须触库——W4-M01 plan §6：静默 hot 群也要参与
	// 聚合评估，否则永不被摘标）。
	ListHotGroupIDs(ctx context.Context) ([]uint64, error)
	// CountAll 全表群计数（SELECT COUNT(*)，W4-M05 plan §6：对账每轮摘要
	// 的 groups_scanned 来源——每 300s 一次可受，非热路径）。
	CountAll(ctx context.Context) (int64, error)
}

// OverLimitGroup 对账超限群：群元信息 + 实际成员数。
type OverLimitGroup struct {
	Group       model.GroupInfo
	MemberCount int64
}

// groupRepo 为 GroupRepo 的 GORM 实现：持有连接句柄，无状态。
type groupRepo struct {
	db *gorm.DB
}

// NewGroupRepo 构造 GroupRepo；db 由装配层创建与关闭。
func NewGroupRepo(db *gorm.DB) GroupRepo {
	return &groupRepo{db: db}
}

// CreateWithOwner 建群事务：插入 group_info（回填自增 ID）→ 插入群主成员行
// （GroupID/Role 由本方法覆写，保证一次业务动作两表联动的原子性与不变量）。
// 任一步失败整体回滚。
func (r *groupRepo) CreateWithOwner(ctx context.Context, g *model.GroupInfo, owner *model.GroupMember) error {
	return r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := tx.Create(g).Error; err != nil {
			return mapGroupDBError(err)
		}
		owner.GroupID = g.ID
		owner.Role = 2
		if err := tx.Create(owner).Error; err != nil {
			return mapGroupMemberDBError(err)
		}
		return nil
	})
}

// GetByID 按主键取单条；未命中 4001。
func (r *groupRepo) GetByID(ctx context.Context, id uint64) (*model.GroupInfo, error) {
	var g model.GroupInfo
	if err := r.db.WithContext(ctx).Take(&g, id).Error; err != nil {
		return nil, mapGroupDBError(err)
	}
	return &g, nil
}

// ListByIDs 批量查询；空入参短路返回（空切片、零查询）。
func (r *groupRepo) ListByIDs(ctx context.Context, ids []uint64) ([]*model.GroupInfo, error) {
	if len(ids) == 0 {
		return []*model.GroupInfo{}, nil
	}
	var gs []*model.GroupInfo
	if err := r.db.WithContext(ctx).Where("id IN ?", ids).Order("id ASC").Find(&gs).Error; err != nil {
		return nil, err
	}
	return gs, nil
}

// UpdateAnnouncement 单列更新。RowsAffected==0 有两义（行不存在 / 值未变化
// ——MySQL 默认 changed-rows 口径下同值 UPDATE 报 0），以存在性复查消歧：
// 行存在视为幂等成功，不存在返回 4001（额外 SELECT 仅发生在 0 行路径）。
func (r *groupRepo) UpdateAnnouncement(ctx context.Context, id uint64, content string) error {
	res := r.db.WithContext(ctx).
		Model(&model.GroupInfo{}).
		Where("id = ?", id).
		Update("announcement", content)
	if res.Error != nil {
		return mapGroupDBError(res.Error)
	}
	if res.RowsAffected > 0 {
		return nil
	}
	var n int64
	if err := r.db.WithContext(ctx).Model(&model.GroupInfo{}).
		Where("id = ?", id).Count(&n).Error; err != nil {
		return err
	}
	if n == 0 {
		return pkgerrors.Wrap(fmt.Errorf("update announcement affected no rows, group %d missing", id),
			pkgerrors.ErrGroupNotFound.Code, pkgerrors.ErrGroupNotFound.Message)
	}
	return nil
}

// TransferOwner 转让群主事务（plan D5/D6）：锁定读群行（当前读，串行化并发
// 转让）→ 校验 fromUID 持有群（4003）→ 校验目标为成员（4006）→
// owner_id + 双向角色一事务提交，无中间态可观测。
func (r *groupRepo) TransferOwner(ctx context.Context, groupID, fromUID, toUID uint64) error {
	return r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var g model.GroupInfo
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
			Where("id = ?", groupID).Take(&g).Error; err != nil {
			return mapGroupDBError(err) // RecordNotFound→4001
		}
		if g.OwnerID != fromUID {
			return pkgerrors.Wrap(fmt.Errorf("group %d owner %d != operator %d", groupID, g.OwnerID, fromUID),
				pkgerrors.ErrGroupPermDenied.Code, pkgerrors.ErrGroupPermDenied.Message)
		}
		var target model.GroupMember
		if err := tx.Where("group_id = ? AND user_id = ?", groupID, toUID).Take(&target).Error; err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return pkgerrors.Wrap(err, pkgerrors.ErrTargetNotMember.Code, pkgerrors.ErrTargetNotMember.Message)
			}
			return err
		}
		if err := tx.Model(&model.GroupInfo{}).Where("id = ?", groupID).
			Update("owner_id", toUID).Error; err != nil {
			return err
		}
		if err := tx.Model(&model.GroupMember{}).
			Where("group_id = ? AND user_id = ?", groupID, toUID).
			Update("role", 2).Error; err != nil {
			return err
		}
		if err := tx.Model(&model.GroupMember{}).
			Where("group_id = ? AND user_id = ?", groupID, fromUID).
			Update("role", 1).Error; err != nil {
			return err
		}
		return nil
	})
}

// FindOverLimit 对账查询（plan D9）：按 group_info.id 分组（主键函数依赖，
// ONLY_FULL_GROUP_BY 合规），HAVING 过滤实际成员数 > max_member 的群。
// 固定语句无用户输入。返回空切片表示无超限（非错误）。
func (r *groupRepo) FindOverLimit(ctx context.Context) ([]OverLimitGroup, error) {
	var rows []struct {
		model.GroupInfo
		MemberCount int64 `gorm:"column:member_count"`
	}
	err := r.db.WithContext(ctx).Raw(
		"SELECT `group_info`.*, COUNT(`group_member`.`id`) AS member_count " +
			"FROM `group_info` JOIN `group_member` ON `group_member`.`group_id` = `group_info`.`id` " +
			"GROUP BY `group_info`.`id` HAVING COUNT(`group_member`.`id`) > `group_info`.`max_member`",
	).Scan(&rows).Error
	if err != nil {
		return nil, err
	}
	overs := make([]OverLimitGroup, 0, len(rows))
	for _, row := range rows {
		overs = append(overs, OverLimitGroup{Group: row.GroupInfo, MemberCount: row.MemberCount})
	}
	return overs, nil
}

// UpdateHotLevel 单列白名单更新（W4-M01）：map 形式 Updates 圈定仅 hot_level
// 一列可变（防全字段覆写）。RowsAffected==0 两义（行不存在/值未变化）以
// 存在性复查消歧（UpdateAnnouncement 先例）：行存在视为幂等成功，不存在
// 返回 4001。热点聚合评估的群来自 ZSET ∪ hot 集（均有建群行先置），缺行
// 属并发删群等异常路径，如实上抛由聚合循环记录。
func (r *groupRepo) UpdateHotLevel(ctx context.Context, id uint64, level int8) error {
	res := r.db.WithContext(ctx).
		Model(&model.GroupInfo{}).
		Where("id = ?", id).
		Updates(map[string]any{"hot_level": level})
	if res.Error != nil {
		return mapGroupDBError(res.Error)
	}
	if res.RowsAffected > 0 {
		return nil
	}
	var n int64
	if err := r.db.WithContext(ctx).Model(&model.GroupInfo{}).
		Where("id = ?", id).Count(&n).Error; err != nil {
		return err
	}
	if n == 0 {
		return pkgerrors.Wrap(fmt.Errorf("update hot_level=%d affected no rows, group %d missing", level, id),
			pkgerrors.ErrGroupNotFound.Code, pkgerrors.ErrGroupNotFound.Message)
	}
	return nil
}

// ListHotGroupIDs 查询 hot_level>0 的群 ID（id ASC，W4-M01）。空结果返回
// 空切片（查询语义恒触库）；索引面：hot_level 无索引——全表扫描量级=群表
// 行数（W4 千级），聚合周期一次可接受（量级护栏归 plan §10 评估）。
func (r *groupRepo) ListHotGroupIDs(ctx context.Context) ([]uint64, error) {
	var ids []uint64
	if err := r.db.WithContext(ctx).Model(&model.GroupInfo{}).
		Where("hot_level > 0").Order("id ASC").Pluck("id", &ids).Error; err != nil {
		return nil, err
	}
	if ids == nil {
		ids = []uint64{}
	}
	return ids, nil
}

// CountAll 全表群计数（W4-M05 plan §6）：SELECT COUNT(*)——对账每轮摘要
// 的 groups_scanned 来源；每 300s 一次，非热路径。
func (r *groupRepo) CountAll(ctx context.Context) (int64, error) {
	var n int64
	if err := r.db.WithContext(ctx).Model(&model.GroupInfo{}).Count(&n).Error; err != nil {
		return 0, err
	}
	return n, nil
}

// mapGroupDBError 将 GORM 错误按 group_info 表口径收敛（W3-M01）：
// 唯一键冲突→ErrAlreadyInGroup(4005)（本域唯一键仅 uk_group_user，群表
// 无业务唯一键，冲突即成员行重插）、记录未命中→ErrGroupNotFound(4001)、
// 其余原样上抛。与 mapDBError(2001)/mapMessageDBError(5003) 按表分化。
func mapGroupDBError(err error) error {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, gorm.ErrDuplicatedKey):
		return pkgerrors.Wrap(err, pkgerrors.ErrAlreadyInGroup.Code, pkgerrors.ErrAlreadyInGroup.Message)
	case errors.Is(err, gorm.ErrRecordNotFound):
		return pkgerrors.Wrap(err, pkgerrors.ErrGroupNotFound.Code, pkgerrors.ErrGroupNotFound.Message)
	default:
		return err
	}
}
