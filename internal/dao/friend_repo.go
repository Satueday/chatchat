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

// FriendRepo 好友关系表数据访问契约（W3-M02 冻结）。关系为双行存储
// （(A,B) 与 (B,A) 各一行，plan D1：双向列表查询与 per-owner remark 语义，
// go-chat 单行存储的不对称缺陷规避）；同意（双行写）/删除（双行删）为
// 跨行复合操作，沉事务方法内原子提交（server-master 先例）。错误经
// mapFriendDBError 收敛为 3xxx 哨兵。
type FriendRepo interface {
	// CreateRequest 插入申请行（status 由调用方置 0，UserID/FriendID 归
	// 调用方）；uk_user_friend 冲突返回 ErrFriendApplyExists(3002)
	//（并发申请唯一性兜底，免 TOCTOU 预查，plan §2）。
	CreateRequest(ctx context.Context, f *model.Friend) error
	// GetRelation 查询 (userID, friendID) 关系行；未命中返回 (nil, nil)——
	// 好友域 miss 是 Apply 需区分「无关系/申请中/已好友」三态的正常分支
	// 而非错误，仅 DB 错误为 error（plan D7 偏离既有 Repo 未命中即哨兵
	// 先例的声明，理由固化于此）。
	GetRelation(ctx context.Context, userID, friendID uint64) (*model.Friend, error)
	// AcceptRequest 同意事务：正向行 (applicant,target) 0→1（条件含
	// status=0；0 行→ErrFriendRequestNotFound(3003)。status 0→1 是真变更，
	// 无 changed-rows 口径歧义，直接判定免存在性复查——与 W3-M01 单列
	// 更新消歧场景不同的依据）+ 反向行 (target,applicant) OnConflict
	// upsert→1（覆盖互逆 pending 与异常残留，MySQL 生成 ON DUPLICATE KEY
	// UPDATE status）。
	AcceptRequest(ctx context.Context, applicant, target uint64) error
	// RejectRequest 删除 (applicant,target) status=0 申请行；0 行→3003。
	// 拒绝即删行（plan D2：DDL 无拒绝态，uk 下保留行会阻塞再申请），
	// 删除后可重新申请。
	RejectRequest(ctx context.Context, applicant, target uint64) error
	// RemoveFriend 删除好友事务：正向行 (userID,friendUID) status=1 删除
	//（0 行→ErrNotFriend(3005)，DELETE 无同值歧义）+ 反向行删除（容忍
	// 缺失，异常残留行不阻塞本操作）。
	RemoveFriend(ctx context.Context, userID, friendUID uint64) error
	// ListFriends 好友列表（(userID,*,status=1)，id ASC 即成为好友顺序）。
	ListFriends(ctx context.Context, userID uint64) ([]*model.Friend, error)
	// ListPendingRequests 收到的待处理申请（(*,userID,status=0)，id ASC）。
	ListPendingRequests(ctx context.Context, userID uint64) ([]*model.Friend, error)
	// IsFriend (userID,friendID,status=1) 存在性窄查询（单聊发送门槛专用，
	// 不取行内容）。
	IsFriend(ctx context.Context, userID, friendID uint64) (bool, error)
}

// friendRepo 为 FriendRepo 的 GORM 实现：持有连接句柄，无状态。
type friendRepo struct {
	db *gorm.DB
}

// NewFriendRepo 构造 FriendRepo；db 由装配层创建与关闭。
func NewFriendRepo(db *gorm.DB) FriendRepo {
	return &friendRepo{db: db}
}

// CreateRequest 插入申请行；uk_user_friend 冲突→3002（并发兜底）。
func (r *friendRepo) CreateRequest(ctx context.Context, f *model.Friend) error {
	if err := r.db.WithContext(ctx).Create(f).Error; err != nil {
		return mapFriendDBError(err)
	}
	return nil
}

// GetRelation 查询 (userID, friendID) 行；未命中 (nil, nil)（D7 契约，
// RecordNotFound 在此吞咽不映射哨兵），其余错误原样上抛。
func (r *friendRepo) GetRelation(ctx context.Context, userID, friendID uint64) (*model.Friend, error) {
	var f model.Friend
	err := r.db.WithContext(ctx).
		Where("user_id = ? AND friend_id = ?", userID, friendID).Take(&f).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, nil // miss 是状态分支而非错误（D7）
		}
		return nil, err
	}
	return &f, nil
}

// AcceptRequest 同意事务（plan D1）：正向 0→1 + 反向 upsert→1 同事务提交，
// 无中间态可观测；反向行已存在（互逆 pending/异常残留）时仅覆写 status=1，
// 不存在时插入新行（status=1，成为好友时刻=行 updated_at/插入时刻）。
func (r *friendRepo) AcceptRequest(ctx context.Context, applicant, target uint64) error {
	return r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		// 条件含 status=0：行不存在或已处理（已同意/异常态）均 0 行→3003。
		res := tx.Model(&model.Friend{}).
			Where("user_id = ? AND friend_id = ? AND status = ?", applicant, target, 0).
			Update("status", 1)
		if res.Error != nil {
			return mapFriendDBError(res.Error)
		}
		if res.RowsAffected == 0 {
			return pkgerrors.Wrap(
				fmt.Errorf("accept affected no rows, pending request (%d,%d) missing", applicant, target),
				pkgerrors.ErrFriendRequestNotFound.Code, pkgerrors.ErrFriendRequestNotFound.Message)
		}
		// 反向 upsert：MySQL 生成 INSERT ... ON DUPLICATE KEY UPDATE `status`=1
		//（uk_user_friend 命中冲突列，DoUpdates 仅覆写 status；updated_at 由
		// DDL ON UPDATE CURRENT_TIMESTAMP 自动推进）。
		if err := tx.Clauses(clause.OnConflict{
			Columns:   []clause.Column{{Name: "user_id"}, {Name: "friend_id"}},
			DoUpdates: clause.AssignmentColumns([]string{"status"}),
		}).Create(&model.Friend{UserID: target, FriendID: applicant, Status: 1}).Error; err != nil {
			return mapFriendDBError(err)
		}
		return nil
	})
}

// RejectRequest 删除申请行（DELETE WHERE status=0）；0 行→3003（行不存在
// 或已被处理）。DELETE 无同值歧义，无需存在性复查。
func (r *friendRepo) RejectRequest(ctx context.Context, applicant, target uint64) error {
	res := r.db.WithContext(ctx).
		Where("user_id = ? AND friend_id = ? AND status = ?", applicant, target, 0).
		Delete(&model.Friend{})
	if res.Error != nil {
		return mapFriendDBError(res.Error)
	}
	if res.RowsAffected == 0 {
		return pkgerrors.Wrap(
			fmt.Errorf("reject affected no rows, pending request (%d,%d) missing", applicant, target),
			pkgerrors.ErrFriendRequestNotFound.Code, pkgerrors.ErrFriendRequestNotFound.Message)
	}
	return nil
}

// RemoveFriend 删除好友事务（plan D3）：正向行 (userID,friendUID) status=1
// 删除（0 行→3005）；反向行无条件删除且容忍 0 行（事务不变量下正反向
// 同生同灭，反向缺失仅可能来自人工干预/异常）。
func (r *friendRepo) RemoveFriend(ctx context.Context, userID, friendUID uint64) error {
	return r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		res := tx.Where("user_id = ? AND friend_id = ? AND status = ?", userID, friendUID, 1).
			Delete(&model.Friend{})
		if res.Error != nil {
			return mapFriendDBError(res.Error)
		}
		if res.RowsAffected == 0 {
			return pkgerrors.Wrap(
				fmt.Errorf("remove affected no rows, friend (%d,%d) missing", userID, friendUID),
				pkgerrors.ErrNotFriend.Code, pkgerrors.ErrNotFriend.Message)
		}
		if err := tx.Where("user_id = ? AND friend_id = ?", friendUID, userID).
			Delete(&model.Friend{}).Error; err != nil {
			return mapFriendDBError(err)
		}
		return nil
	})
}

// ListFriends 好友列表（(userID,*,status=1)，id ASC）。
func (r *friendRepo) ListFriends(ctx context.Context, userID uint64) ([]*model.Friend, error) {
	var fs []*model.Friend
	if err := r.db.WithContext(ctx).
		Where("user_id = ? AND status = ?", userID, 1).
		Order("id ASC").Find(&fs).Error; err != nil {
		return nil, mapFriendDBError(err)
	}
	return fs, nil
}

// ListPendingRequests 收到的待处理申请（(*,userID,status=0)，id ASC）。
func (r *friendRepo) ListPendingRequests(ctx context.Context, userID uint64) ([]*model.Friend, error) {
	var fs []*model.Friend
	if err := r.db.WithContext(ctx).
		Where("friend_id = ? AND status = ?", userID, 0).
		Order("id ASC").Find(&fs).Error; err != nil {
		return nil, mapFriendDBError(err)
	}
	return fs, nil
}

// IsFriend (userID,friendID,status=1) 存在性窄查询（uk 保证 ≤1 行，
// Count 即存在性；门槛高频调用不取整行）。
func (r *friendRepo) IsFriend(ctx context.Context, userID, friendID uint64) (bool, error) {
	var n int64
	if err := r.db.WithContext(ctx).Model(&model.Friend{}).
		Where("user_id = ? AND friend_id = ? AND status = ?", userID, friendID, 1).
		Count(&n).Error; err != nil {
		return false, mapFriendDBError(err)
	}
	return n > 0, nil
}

// mapFriendDBError 将 GORM 错误按 friend 表口径收敛（W3-M02）：
// 唯一键冲突（uk_user_friend）→ErrFriendApplyExists(3002)、其余原样上抛。
// RecordNotFound 不在此映射——GetRelation 的 miss 走 (nil,nil) 契约（D7），
// 与 mapDBError(2001)/mapGroupDBError(4001) 的未命中哨兵语义按域分化。
func mapFriendDBError(err error) error {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, gorm.ErrDuplicatedKey):
		return pkgerrors.Wrap(err, pkgerrors.ErrFriendApplyExists.Code, pkgerrors.ErrFriendApplyExists.Message)
	default:
		return err
	}
}
