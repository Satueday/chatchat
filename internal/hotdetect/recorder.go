package hotdetect

import (
	"context"

	"go.uber.org/zap"
)

// recorder 为 Recorder 的实现（gateway/logic 埋点用）：仅 Redis 桶存取
// 依赖（轻量，D7），无 MySQL 往返（计数热路径禁 DB——plan §6 禁止事项）。
type recorder struct {
	store  *bucketStore
	logger *zap.Logger
}

// 编译期接口实现断言。
var _ Recorder = (*recorder)(nil)

// newRecorder 内部构造（同包单测注入 fake 时钟的 bucketStore 用；
// 装配层走 NewRecorder）。
func newRecorder(store *bucketStore, logger *zap.Logger) *recorder {
	if logger == nil {
		logger = zap.NewNop()
	}
	return &recorder{store: store, logger: logger}
}

// RecordGroupMsg 实现计数（CAP-001）：单 pipeline INCRBY+EXPIRE+ZADD 单
// 往返（本地 Redis 亚毫秒级，D2 同步埋点裁决）。错误原样上抛（%w 保留
// 链路、不打业务码），调用方（message.HandleGroupChat）Warn 不阻断发送。
func (r *recorder) RecordGroupMsg(ctx context.Context, groupID uint64) error {
	if err := r.store.incr(ctx, groupID, r.store.now()); err != nil {
		r.logger.Warn("group msg count failed (non-blocking)",
			zap.Uint64("groupId", groupID), zap.Error(err))
		return err
	}
	return nil
}
