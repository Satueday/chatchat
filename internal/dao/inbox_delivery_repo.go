package dao

import (
	"context"
	"time"

	"chatchat/internal/model"
	pkgerrors "chatchat/pkg/errors"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// inboxDeliveryBatchSize 单语句批量上限（temp-w3 §10 冻结：快照上限即批量
// 边界——群成员快照 ≤500，单批 INSERT 参数量有界）；超出部分由
// CreateInBatches 自动切分多语句。
const inboxDeliveryBatchSize = 500

// InboxDeliveryRepo 信箱投递记录数据访问契约（W3-M05 冻结，CAP-001）：
// 写面（读侧漫游/清单归 W3-M06 的窄读接口 InboxSummaryReader，本契约
// 不动——pushpull 写路径装配与既有替身零波及）。
type InboxDeliveryRepo interface {
	// BatchCreate 幂等批量写入：INSERT ... ON DUPLICATE KEY（MySQL 方言下
	// OnConflict DoNothing 由驱动改写为主键自赋值无操作）——uk_user_conv_seq
	// 冲突行静默跳过，消费重投达成「恰好一次写」。单语句批量 ≤500；
	// 空入参直接返回 nil（不触库）。
	BatchCreate(ctx context.Context, records []*model.InboxDelivery) error
}

// ConversationSummary 为信箱会话聚合行（W3-M06 清单端点 SQL 投影 DTO：
// {conversation_id, MAX(msg_seq)}——uk 聚合面，非表行）。
type ConversationSummary struct {
	ConversationID string `gorm:"column:conversation_id"`
	MaxSeq         uint64 `gorm:"column:max_seq"`
}

// InboxSummaryReader 信箱会话聚合读面（W3-M06 裁决 D1/D3，CAP-001/002）：
// 窄接口按消费面最小注入（清单服务仅消费聚合读，写面 InboxDeliveryRepo
// 契约不动）。实现：*inboxDeliveryRepo（NewInboxDeliveryRepo 产物即满足）。
type InboxSummaryReader interface {
	// Summary 按用户聚合信箱会话（GROUP BY conversation_id，MAX(msg_seq)）；
	// since 非零时仅聚合 created_at>=since 的行（漫游窗口施加位=清单过滤，
	// D3——发现即受限，拉取/历史不受窗口限制）；失败包装
	// ErrMessageQuery(5008)（D5：读侧查询失败统一口径）。
	Summary(ctx context.Context, userID uint64, since time.Time) ([]ConversationSummary, error)
}

// InboxDeliveryStore 信箱仓储读写全集（W3-M06）：具体仓库双面实现，装配层
// 单句柄注入（NewInboxDeliveryRepo 返回本类型——写路径按 InboxDeliveryRepo
// 消费、清单路径按 InboxSummaryReader 消费，均静态满足）。
type InboxDeliveryStore interface {
	InboxDeliveryRepo
	InboxSummaryReader
}

// inboxDeliveryRepo 为 InboxDeliveryRepo 的 GORM 实现：持有连接句柄，无状态。
type inboxDeliveryRepo struct {
	db *gorm.DB
}

// 编译期接口实现断言：具体仓库满足写面、读面与读写全集。
var (
	_ InboxDeliveryRepo  = (*inboxDeliveryRepo)(nil)
	_ InboxSummaryReader = (*inboxDeliveryRepo)(nil)
	_ InboxDeliveryStore = (*inboxDeliveryRepo)(nil)
)

// NewInboxDeliveryRepo 构造信箱仓储（W3-M06 起返回读写全集
// InboxDeliveryStore——既有写路径调用点按 InboxDeliveryRepo 消费不受
// 波及）；db 由装配层创建与关闭。
func NewInboxDeliveryRepo(db *gorm.DB) InboxDeliveryStore {
	return &inboxDeliveryRepo{db: db}
}

// BatchCreate 幂等批量写入（CAP-001）。无事务包裹：每批独立语句，批间无
// 原子性诉求（调用方为至少一次消费——失败整批重投，已入库批次经 uk 幂等
// 吸收）。CreatedAt 由 GORM 自动填充（NowFunc=UTC），DDL DEFAULT 兜底。
func (r *inboxDeliveryRepo) BatchCreate(ctx context.Context, records []*model.InboxDelivery) error {
	if len(records) == 0 {
		return nil
	}
	return r.db.WithContext(ctx).
		Clauses(clause.OnConflict{DoNothing: true}).
		CreateInBatches(records, inboxDeliveryBatchSize).Error
}

// Summary（W3-M06-plan §6.1 SQL 冻结）：
//
//	SELECT conversation_id, MAX(msg_seq) AS max_seq FROM inbox_delivery
//	WHERE user_id = ? [AND created_at >= ?] GROUP BY conversation_id
//
// （AS max_seq 为结果列命名——MAX 无别名时驱动列名为表达式原文，Scan
// 列映射落空；聚合语义与冻结 SQL 逐字一致。）
// uk_user_conv_seq 最左前缀 (user_id) 覆盖过滤与分组；created_at 无索引——
// 窗口过滤在该用户行集上执行（量级=用户信箱行数，W3 可接受，plan §10）。
// 占位参数化（非 Raw 拼接）；since 零值=不过滤（全量聚合，D3）；无命中行
// 返回空切片（非 error）。
func (r *inboxDeliveryRepo) Summary(ctx context.Context, userID uint64, since time.Time) ([]ConversationSummary, error) {
	q := r.db.WithContext(ctx).
		Model(&model.InboxDelivery{}).
		Select("conversation_id, MAX(msg_seq) AS max_seq").
		Where("user_id = ?", userID)
	if !since.IsZero() {
		q = q.Where("created_at >= ?", since)
	}
	var rows []ConversationSummary
	if err := q.Group("conversation_id").Scan(&rows).Error; err != nil {
		return nil, pkgerrors.Wrap(err, pkgerrors.ErrMessageQuery.Code, pkgerrors.ErrMessageQuery.Message)
	}
	return rows, nil
}
