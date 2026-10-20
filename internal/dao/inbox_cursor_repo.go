package dao

import (
	"context"

	"chatchat/internal/model"
	pkgerrors "chatchat/pkg/errors"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// InboxCursorRepo 信箱游标数据访问契约（W2-M04 冻结；W2-M05 扩拉取侧推进）。
// 按用户批量读 synced（清单差集过滤）归 W3-M06 的窄读接口
// InboxSyncedReader——本写面契约不动（gateway/装配与既有替身零波及）。
type InboxCursorRepo interface {
	// AdvanceRecv 单调推进 max_recv_seq = GREATEST(旧值, seq)，行不存在则
	// 插入（upsert，uk_user_conv）。重复/乱序 ACK 天然幂等（不回退）。
	// 失败包装 ErrCursorUpdate(5007)，cause 保留驱动错误仅供日志面。
	AdvanceRecv(ctx context.Context, userID uint64, conversationID string, seq uint64) error
	// AdvanceSynced 单调推进 max_synced_seq = GREATEST(旧值, seq)（拉取侧，
	// W2-M05；与 AdvanceRecv 同构 upsert）。空结果不调用由服务层保证；
	// 失败包装 ErrCursorUpdate(5007)（哨兵文案不区分 recv/synced，D1）。
	AdvanceSynced(ctx context.Context, userID uint64, conversationID string, seq uint64) error
}

// InboxSyncedReader 游标 synced 批量读面（W3-M06 裁决 D4，CAP-001/003）：
// 读用户全部会话的 max_synced_seq——清单差集过滤专用（W2-M05 D5 冻结面
// 「query 绝不读 synced」之外的唯一授权读点，清单端点专属）；窄接口按
// 消费面最小注入（写面 InboxCursorRepo 契约不动）。实现：*inboxCursorRepo。
type InboxSyncedReader interface {
	// ListSynced 返回 userID 全部会话的 max_synced_seq（无行会话不在 map
	// 中，调用侧按零值参与差集）；失败包装 ErrMessageQuery(5008)
	//（D5：读侧查询失败统一口径，与清单聚合读同码）。
	ListSynced(ctx context.Context, userID uint64) (map[string]uint64, error)
}

// inboxCursorRepo 为 InboxCursorRepo 的 GORM 实现：持有连接句柄，无状态。
type inboxCursorRepo struct {
	db *gorm.DB
}

// 编译期接口实现断言：具体仓库同时满足写面与 synced 批量读面。
var (
	_ InboxCursorRepo   = (*inboxCursorRepo)(nil)
	_ InboxSyncedReader = (*inboxCursorRepo)(nil)
)

// NewInboxCursorRepo 构造 InboxCursorRepo；db 由装配层创建与关闭。
func NewInboxCursorRepo(db *gorm.DB) InboxCursorRepo {
	return &inboxCursorRepo{db: db}
}

// AdvanceRecv 以 clause.OnConflict 参数化 upsert 实现 GREATEST 单调推进：
// INSERT ... ON DUPLICATE KEY UPDATE
//
//	max_recv_seq = GREATEST(inbox_cursor.max_recv_seq, VALUES(max_recv_seq))
//
// 插入值经 Create 占位符参数化（非 Raw 拼接）；冲突判定列即 uk_user_conv。
// updated_at 由 DDL ON UPDATE CURRENT_TIMESTAMP(3) 随行实际变更自动维护
// （重复 ACK 值不变时行不变，updated_at 不动，符合幂等语义）。
func (r *inboxCursorRepo) AdvanceRecv(ctx context.Context, userID uint64, conversationID string, seq uint64) error {
	row := &model.InboxCursor{
		UserID:         userID,
		ConversationID: conversationID,
		MaxSyncedSeq:   0, // 拉取侧游标不因 ACK 推进（D2）
		MaxRecvSeq:     seq,
	}
	err := r.db.WithContext(ctx).Clauses(clause.OnConflict{
		Columns: []clause.Column{{Name: "user_id"}, {Name: "conversation_id"}},
		DoUpdates: clause.Assignments(map[string]any{
			"max_recv_seq": gorm.Expr("GREATEST(`max_recv_seq`, VALUES(`max_recv_seq`))"),
		}),
	}).Create(row).Error
	if err != nil {
		return pkgerrors.Wrap(err, pkgerrors.ErrCursorUpdate.Code, pkgerrors.ErrCursorUpdate.Message)
	}
	return nil
}

// AdvanceSynced 与 AdvanceRecv 同构（W2-M05），仅推进列不同：
//
//	max_synced_seq = GREATEST(inbox_cursor.max_synced_seq, VALUES(max_synced_seq))
//
// 插入行 max_recv_seq=0（推送侧游标不因拉取推进，与 AdvanceRecv 的
// MaxSyncedSeq=0 对称）；DoUpdates 仅覆盖 max_synced_seq，已存在的
// max_recv_seq 不受影响。
func (r *inboxCursorRepo) AdvanceSynced(ctx context.Context, userID uint64, conversationID string, seq uint64) error {
	row := &model.InboxCursor{
		UserID:         userID,
		ConversationID: conversationID,
		MaxSyncedSeq:   seq,
		MaxRecvSeq:     0, // 推送侧游标不因拉取推进（D2 对称）
	}
	err := r.db.WithContext(ctx).Clauses(clause.OnConflict{
		Columns: []clause.Column{{Name: "user_id"}, {Name: "conversation_id"}},
		DoUpdates: clause.Assignments(map[string]any{
			"max_synced_seq": gorm.Expr("GREATEST(`max_synced_seq`, VALUES(`max_synced_seq`))"),
		}),
	}).Create(row).Error
	if err != nil {
		return pkgerrors.Wrap(err, pkgerrors.ErrCursorUpdate.Code, pkgerrors.ErrCursorUpdate.Message)
	}
	return nil
}

// ListSynced（W3-M06-plan §6.1 SQL 冻结）：
//
//	SELECT conversation_id, max_synced_seq FROM inbox_cursor WHERE user_id = ?
//
// uk_user_conv 左前缀 (user_id) 覆盖；占位参数化（非 Raw 拼接）；无行用户
// 返回空 map（非 error）——清单差集把缺行会话按 synced=0 处理。
func (r *inboxCursorRepo) ListSynced(ctx context.Context, userID uint64) (map[string]uint64, error) {
	var rows []struct {
		ConversationID string `gorm:"column:conversation_id"`
		MaxSyncedSeq   uint64 `gorm:"column:max_synced_seq"`
	}
	err := r.db.WithContext(ctx).
		Model(&model.InboxCursor{}).
		Select("conversation_id, max_synced_seq").
		Where("user_id = ?", userID).
		Scan(&rows).Error
	if err != nil {
		return nil, pkgerrors.Wrap(err, pkgerrors.ErrMessageQuery.Code, pkgerrors.ErrMessageQuery.Message)
	}
	synced := make(map[string]uint64, len(rows))
	for _, row := range rows {
		synced[row.ConversationID] = row.MaxSyncedSeq
	}
	return synced, nil
}
