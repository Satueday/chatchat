package dao

import (
	"context"
	"fmt"

	"github.com/redis/go-redis/v9"
)

// PresenceChecker 为 pushpull 消费方的在线判定窄接口（W3-M05 裁决 D12）：
// server.PresenceStore 仅写无读（W2 域零触达），读实现挂 dao——消费方定义
// 接口的依赖倒置（gotify 先例）。键契约与写入方互引注释维持一致性：
// server/presence_redis.go 的 presenceKey（"online:{uid}"，值为网关节点
// 标识、TTL>0 由心跳续期）——两侧键字面独立定义，漂移由集成测试经真实
// gateway 连接跨包验证兜底（pushpull CASE-I02）。
type PresenceChecker interface {
	// Online 判定 uid 是否在线：Redis EXISTS online:{uid}。
	// 错误（Redis 不可达等）原样上抛，由调用方按基础设施错误分类处理
	//（pushpull D9：return err 重投）。
	Online(ctx context.Context, uid uint64) (bool, error)

	// OnlineMany 批量在线判定（MGET 单往返；结果与入参顺序对齐，键缺失=离线）。
	// 空入参返回空 map 不触库。单节点约束：MGET 要求键同节点——W3 单节点 Redis
	// 满足；若 W4+ 引入 Cluster 须改 pipeline 形态或 hash-tag（本注释即约束登记）。
	OnlineMany(ctx context.Context, uids []uint64) (map[uint64]bool, error)
}

// presenceChecker 为 PresenceChecker 的 go-redis 实现：持有客户端句柄，无状态。
type presenceChecker struct {
	rdb redis.UniversalClient
}

// NewPresenceChecker 构造在线判定器。rdb 与 gateway 共用同一 Redis 拓扑
// （装配层各自 NewRedisClient 或复用句柄均可）。
func NewPresenceChecker(rdb redis.UniversalClient) PresenceChecker {
	return &presenceChecker{rdb: rdb}
}

// Online 实现 PresenceChecker：EXISTS online:{uid}（键 TTL 由 gateway 心跳
// 维护，本实现只读；键不存在=离线，含「判在线后瞬间过期」的竞态口径——
// 由推送侧 ConnManager.Online 二次判定与拉取兜底吸收，ADR-0009 D11）。
//
// W3-M05-C2 起消费路径走 OnlineMany（MGET 单往返批量判定），本方法保留
// 给单点判定的兼容调用方。
func (p *presenceChecker) Online(ctx context.Context, uid uint64) (bool, error) {
	n, err := p.rdb.Exists(ctx, fmt.Sprintf("online:%d", uid)).Result()
	if err != nil {
		return false, err
	}
	return n > 0, nil
}

// OnlineMany 实现 PresenceChecker：MGET online:{uid}... 单往返批量判定
// （W3-M05-C2 根因②修复——Handle 逐成员串行 EXISTS 的 N 次往返收敛为 1 次）。
// go-redis MGet 结果切片与入参键序对齐，nil ⇔ 键缺失=离线；错误原样上抛
// （D9 基础设施错误分类）。空入参短路返回空 map 不触库。竞态口径与 Online
// 一致（判在线后瞬间过期由推送侧二次判定与拉取兜底吸收）。
func (p *presenceChecker) OnlineMany(ctx context.Context, uids []uint64) (map[uint64]bool, error) {
	res := make(map[uint64]bool, len(uids))
	if len(uids) == 0 {
		return res, nil
	}
	keys := make([]string, len(uids))
	for i, uid := range uids {
		keys[i] = fmt.Sprintf("online:%d", uid)
	}
	vals, err := p.rdb.MGet(ctx, keys...).Result()
	if err != nil {
		return nil, err
	}
	for i, uid := range uids {
		res[uid] = i < len(vals) && vals[i] != nil // nil=键缺失=离线
	}
	return res, nil
}
