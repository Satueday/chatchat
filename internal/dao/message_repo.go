package dao

import (
	"context"
	"errors"

	"chatchat/internal/model"
	pkgerrors "chatchat/pkg/errors"

	"gorm.io/gorm"
)

// MessageRepo 消息表数据访问契约（W2-M03 冻结；W2-M05 扩拉取查询）。
type MessageRepo interface {
	// Create 插入消息并回填自增 ID；msg_id 或 (conversation_id, msg_seq)
	// 唯一冲突返回 ErrMessageSave(5003)（按表分化映射，不复用 user 表的
	// 2001 语义），cause 保留驱动错误仅供日志面。
	Create(ctx context.Context, m *model.Message) error
	// AfterSeq 返回会话内 msg_seq 严格大于 afterSeq 的消息，按 msg_seq
	// ASC，至多 limit 条（调用方传 limit+1 做 hasMore 探测，W2-M05）。
	// 占位符参数化（非 Raw 拼接），依赖 uk_conv_seq 索引左前缀。
	// 失败包装 ErrMessageQuery(5008)。
	AfterSeq(ctx context.Context, conversationID string, afterSeq uint64, limit int) ([]*model.Message, error)
	// ListByMsgIDs 批量按 msg_id 查消息行（W4-M03 热点拉取内容组装，
	// CAP-203 原语）：入参去重、空集直返空切片不触库；命中行按
	// (conversation_id, msg_seq) ASC 排序返回（调用方按 seq 对齐 Timeline
	// 行组装）；未命中 id 静默缺席（miss 段由调用方 AfterSeq 兜底——
	// plan D4 缺口兜底语义）。失败包装 ErrMessageQuery(5008)。
	ListByMsgIDs(ctx context.Context, msgIDs []string) ([]*model.Message, error)
	// MaxSeq 返回会话已落库最大 msg_seq（无消息返回 0）。失败包装 5008。
	MaxSeq(ctx context.Context, conversationID string) (uint64, error)
}

// messageRepo 为 MessageRepo 的 GORM 实现：持有连接句柄，无状态。
type messageRepo struct {
	db *gorm.DB
}

// NewMessageRepo 构造 MessageRepo；db 由装配层创建与关闭。
func NewMessageRepo(db *gorm.DB) MessageRepo {
	return &messageRepo{db: db}
}

// Create 插入消息（GORM 自动回填 ID 与 created_at）。
func (r *messageRepo) Create(ctx context.Context, m *model.Message) error {
	if err := r.db.WithContext(ctx).Create(m).Error; err != nil {
		return mapMessageDBError(err)
	}
	return nil
}

// AfterSeq 增量查询（W2-M05 冻结 §6.1）：msg_seq 严格大于 afterSeq、ASC、
// Limit——全量语义由调用方传 afterSeq=0 达成（seq 从 1 起，>0 即全量）。
// 无命中行返回空切片（非 error）。
func (r *messageRepo) AfterSeq(ctx context.Context, conversationID string, afterSeq uint64, limit int) ([]*model.Message, error) {
	var msgs []*model.Message
	err := r.db.WithContext(ctx).
		Where("conversation_id = ? AND msg_seq > ?", conversationID, afterSeq).
		Order("msg_seq ASC").
		Limit(limit).
		Find(&msgs).Error
	if err != nil {
		return nil, pkgerrors.Wrap(err, pkgerrors.ErrMessageQuery.Code, pkgerrors.ErrMessageQuery.Message)
	}
	return msgs, nil
}

// MaxSeq 返回会话已落库最大 msg_seq；无消息返回 0。COALESCE 把空聚合的
// NULL 归一为 0（database/sql 不支持将 NULL Scan 进 uint64，且契约要求
// 「无行→0」）；MAX 无 GROUP BY 恒返回一行。
func (r *messageRepo) MaxSeq(ctx context.Context, conversationID string) (uint64, error) {
	var maxSeq uint64 // revive：局部名避开内建 max（W3-M04 移交修复）
	err := r.db.WithContext(ctx).
		Model(&model.Message{}).
		Where("conversation_id = ?", conversationID).
		Select("COALESCE(MAX(msg_seq), 0)").
		Scan(&maxSeq).Error
	if err != nil {
		return 0, pkgerrors.Wrap(err, pkgerrors.ErrMessageQuery.Code, pkgerrors.ErrMessageQuery.Message)
	}
	return maxSeq, nil
}

// ListByMsgIDs 批量按 msg_id 查消息行（W4-M03，CAP-203 原语）：入参去重
// （uk_msg_id 点查语义下重复入参无额外信息）、空集直返空切片不触库；
// WHERE msg_id IN 占位符参数化，命中行按 (conversation_id, msg_seq) ASC
// 排序（热点拉取组装面：Timeline 行序与内容行序同构对齐）；未命中 id
// 静默缺席——miss 判定归调用方（AfterSeq 兜底，plan D4）。
func (r *messageRepo) ListByMsgIDs(ctx context.Context, msgIDs []string) ([]*model.Message, error) {
	if len(msgIDs) == 0 {
		return []*model.Message{}, nil
	}
	seen := make(map[string]struct{}, len(msgIDs))
	uniq := make([]string, 0, len(msgIDs))
	for _, id := range msgIDs {
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		uniq = append(uniq, id)
	}
	var msgs []*model.Message
	err := r.db.WithContext(ctx).
		Where("msg_id IN ?", uniq).
		Order("conversation_id ASC, msg_seq ASC").
		Find(&msgs).Error
	if err != nil {
		return nil, pkgerrors.Wrap(err, pkgerrors.ErrMessageQuery.Code, pkgerrors.ErrMessageQuery.Message)
	}
	return msgs, nil
}

// mapMessageDBError 将 GORM 错误收敛为 pkg/errors 哨兵（message 表口径）：
// Create 分支——唯一键冲突（uk_msg_id / uk_conv_seq）→ErrMessageSave(5003)，
// 其余原样上抛由调用方按写路径口径包装；
// 查询分支（AfterSeq/MaxSeq）不经此函数，直接包装 ErrMessageQuery(5008)
// （W2-M05 D1：查询/写路径错误码分化，Create 的 5003 语义不变）。
// 记录未命中→ErrUserNotFound 不适用（保留 default）。
// 与 userRepo.mapDBError 的 2001 绑定按表分化（W1-M05 P3-2 预告）。
func mapMessageDBError(err error) error {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, gorm.ErrDuplicatedKey):
		return pkgerrors.Wrap(err, pkgerrors.ErrMessageSave.Code, pkgerrors.ErrMessageSave.Message)
	default:
		return err
	}
}
