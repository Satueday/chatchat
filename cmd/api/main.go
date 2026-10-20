// Package main 是 ChatChat api 服务入口（W1-M06：完整装配 + 监听 + 优雅退出）。
//
// 装配顺序（W1-M00-plan.md §4 生命周期基线；gotify 样板）：
//  1. config.Load("config.toml")：默认值 → toml → CHATCHAT_ env 覆盖 → 字段级校验；
//     校验失败 → stderr 逐行输出字段错误 + return 1；
//  2. log.New(cfg.Log)：结构化日志，defer Close（逆序回收第一环）；
//  3. dao.NewMySQLDB：GORM 连接（Ping 隐含在 Open），defer sqlDB.Close；
//  4. dao.NewRedisClient：Ping 验证连通（W1 仅资源就绪证明，无业务键），defer Close；
//  5. jwt.NewManager：HS256 签发器（secret 来自 env 注入，TTL 取 config）；
//  6. user/message/group.NewService + router.New：业务与路由装配（依赖全部注入）；
//  7. http.Server 监听（Read/WriteTimeout 10s），goroutine ListenAndServe；
//     群对账 Reconciler 同期起 goroutine（W3-M01 D2：api 进程宿主，goroutine
//     属主本函数，退出路径 cancel + WaitGroup 回收）；
//     热点打标聚合 Detector.Run 同范式 goroutine（W4-M01 D3：api 进程宿主，
//     启动即一轮 → ticker → cancel + Wait 回收）；
//  8. signal(SIGINT/SIGTERM) → 10s 超时 Shutdown；ErrServerClosed 忽略；
//  9. 任一资源失败 → 结构化日志 + return 1，已初始化资源经 defer 逆序回收。
package main

import (
	"context"
	"errors"
	"expvar"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"runtime"
	"sync"
	"syscall"
	"time"

	"chatchat/config"
	"chatchat/internal/dao"
	"chatchat/internal/hotdetect"
	"chatchat/internal/leader"
	"chatchat/internal/readyz"
	"chatchat/internal/router"
	"chatchat/internal/service/friend"
	"chatchat/internal/service/group"
	"chatchat/internal/service/message"
	"chatchat/internal/service/user"
	"chatchat/pkg/common/jwt"
	"chatchat/pkg/global"
	"chatchat/pkg/log"

	"github.com/gin-gonic/gin"
	"go.uber.org/zap"
)

// buildVersion 版本注入点（W4-M05 plan §6：-ldflags "-X main.buildVersion=..."
// 覆写；缺省回落 global.Version 源码常量——四入口统一形态）。
var buildVersion = global.Version

// main 仅做退出码透传；全部逻辑在 run 中以便表驱动测试（gotify 样板）。
func main() {
	os.Exit(run())
}

// run 执行完整装配并阻塞至信号或监听错误，返回进程退出码。
func run() int {
	// 步骤 1：加载配置（缺文件→默认值继续；校验失败→字段级错误非零退出）。
	cfg, err := config.Load("config.toml")
	if err != nil {
		// errors.Join 聚合的字段级错误逐行输出（每行含 <段>.<字段>: 原因）。
		var joined interface{ Unwrap() []error }
		if errors.As(err, &joined) {
			for _, e := range joined.Unwrap() {
				fmt.Fprintln(os.Stderr, e)
			}
		} else {
			fmt.Fprintln(os.Stderr, err)
		}
		return 1
	}

	// 步骤 2：结构化日志（level/encoder/output 来自 cfg.Log；file 模式句柄由 Close 释放）。
	lg, err := log.New(cfg.Log, log.WithService("chatchat-api"))
	if err != nil {
		fmt.Fprintln(os.Stderr, "init logger failed:", err)
		return 1
	}
	defer func() { _ = lg.Close() }()

	// 缺 config.toml 属预期：默认值继续，但按承诺输出 warn（README/plan §2 行为契约）。
	if _, statErr := os.Stat("config.toml"); os.IsNotExist(statErr) {
		lg.Warn("config.toml not found, using built-in defaults")
	}

	lg.Info("chatchat version " + buildVersion)

	// 步骤 3：MySQL（GORM；句柄由装配层持有并 Close）。
	gdb, err := dao.NewMySQLDB(cfg.MySQL)
	if err != nil {
		lg.Error("init mysql failed", zap.Error(err))
		return 1
	}
	sqlDB, err := gdb.DB()
	if err != nil {
		lg.Error("get sql db handle failed", zap.Error(err))
		return 1
	}
	defer func() { _ = sqlDB.Close() }()

	// 步骤 4：Redis（Ping 验证连通即资源就绪，W1 无业务键）。
	ctx := context.Background()
	rdb, err := dao.NewRedisClient(ctx, cfg.Redis)
	if err != nil {
		lg.Error("init redis failed", zap.Error(err))
		return 1
	}
	defer func() { _ = rdb.Close() }()

	// 步骤 5：JWT manager（secret 仅 env 注入，已过 ≥32 字节校验）。
	jm := jwt.NewManager(cfg.JWT.Secret.Value(),
		time.Duration(cfg.JWT.AccessTTLSec)*time.Second,
		time.Duration(cfg.JWT.RefreshTTLSec)*time.Second)

	// 步骤 6/7：业务 service + 路由装配。
	userSvc := user.NewService(dao.NewUserRepo(gdb), jm, cfg.User, lg.Logger)
	// 热点群打标聚合 Detector 构造（W4-M01 plan D3：api 进程宿主——与对账
	// 同进程，后台任务宿主统一，M05 leader 选举一处治理；gateway 无状态
	// 水平扩展不承载）。W4-M02：注入 UnmarkSwitcher（plan D6）——摘标由
	// 「判定即摘标」升级为 P1 Timeline 补齐 → P2 摘标 → P3 回写扩散三阶段
	// 协议；四 repo 依赖（message/group_timeline/group_member/inbox_delivery）
	// 装配层注入。构造前置到 pullSvc 之前（W4-M03）：同一实例兼为拉取侧
	// IsHot 消费面（Run 循环见下方，IsHot 无状态读零共享态冲突）。
	// groupRepo 提升（W4-M05）：Detector 聚合候选源与群业务/热点计数共用。
	groupRepo := dao.NewGroupRepo(gdb)
	switcher := hotdetect.NewUnmarkSwitcher(rdb,
		dao.NewMessageRepo(gdb),
		dao.NewGroupTimelineRepo(gdb),
		dao.NewGroupMemberRepo(gdb),
		dao.NewInboxDeliveryRepo(gdb),
		lg.Logger)
	detector := hotdetect.NewDetector(rdb, dao.NewHotGroupStatRepo(gdb),
		groupRepo, switcher, cfg.HotDetect, lg.Logger)

	// 历史消息拉取（W2-M05）+ 会话发现清单（W3-M06）：REST 与 WS 同义；
	// Pusher 留 nil（HTTP 出口无下行帧通道，HandlePull 误接会被拒用）；
	// Pend 注入在途标记探针（W2-M05-C2 D12 间隙判定，装配层必须注入——
	// api 已持有 rdb）；GroupMembers 注入群会话读侧成员判定（W3-M04 D6：
	// g_ conv 查群表，漏注入则 g_ 一律 5009 安全失败）；Inbox+RoamDays
	// 注入清单读面与漫游窗口（W3-M06 D1/D3：GET /api/message/inbox 消费，
	// 漏注入 Inbox 则该端点 9001 拒用——拉取/历史路径不受影响）；
	// Hot/Timeline 注入热点拉取分支与清单热点增补（W4-M03 D4/D6——Hot
	// 复用上方 detector 的 IsHot 消费面）。
	pullSvc := message.NewPullService(dao.NewMessageRepo(gdb), dao.NewInboxCursorRepo(gdb),
		message.PullOptions{
			Pend:         dao.NewRedisPendMarker(rdb),
			GroupMembers: dao.NewGroupMemberRepo(gdb),
			Hot:          detector,
			Timeline:     dao.NewGroupTimelineRepo(gdb),
			Inbox:        dao.NewInboxDeliveryRepo(gdb),
			RoamDays:     cfg.Inbox.RoamDays,
		}, lg.Logger)
	// 群组业务（W3-M01）：repos + service 注入路由；Reconciler 以 api 进程
	// 为宿主（plan D2），下方 goroutine 持有、shutdown 时 cancel + Wait 回收。
	// W4-M05：对账轮观测经 RoundStats 窄接口注入（reconcile_rounds/
	// overlimit_groups expvar 落点）；groupRepo 复用上方 Detector 装配变量
	//（hot_group_count Func 共用）。
	roundsVar := expvar.NewInt("chatchat_reconcile_rounds")
	overlimitVar := expvar.NewInt("chatchat_overlimit_groups")
	groupSvc := group.NewService(groupRepo, dao.NewGroupMemberRepo(gdb),
		cfg.Group, apiRoundStats{rounds: roundsVar, overlimit: overlimitVar}, lg.Logger)
	// 好友业务（W3-M02）：REST 六端点注入路由（api 为 REST 宿主；发送门槛
	// gate 由 gateway/logic 消费，本进程不装 message 写链路）。
	friendSvc := friend.NewService(dao.NewFriendRepo(gdb), dao.NewUserRepo(gdb),
		cfg.Friend, lg.Logger)
	engine := router.New(router.Deps{
		User:    userSvc,
		Message: pullSvc,
		Group:   groupSvc,
		Friend:  friendSvc,
		JWT:     jm,
		Logger:  lg.Logger,
		Mode:    cfg.Server.Mode,
	})

	// readyz（W4-M05 plan D8，CAP-505）：MySQL+Redis Ping（2s 探针超时，
	// handler 内控）——依赖就绪分级探针，静态 healthz 语义维持不变。探测
	// 路径无访问日志面可豁免（engine 仅 recovery 中间件，gin 无访问日志）。
	engine.GET("/readyz", gin.WrapH(readyz.New(
		readyz.Checker{Name: "mysql", Check: sqlDB.PingContext},
		readyz.Checker{Name: "redis", Check: func(ctx context.Context) error { return rdb.Ping(ctx).Err() }},
	)))
	// 指标端点（W4-M05 plan D5，CAP-504）：/debug/vars 挂 expvar（gin 侧
	// WrapH）；hot_group_count 为 Func——查询时取 ListHotGroupIDs len
	//（scrape 低频假设，plan §10；查询失败 -1 哨兵）。无鉴权为 0.1.0 已知
	// 边界（plan §8 登记下月 token 化）。
	engine.GET("/debug/vars", gin.WrapH(expvar.Handler()))
	// goroutine 运行时指标（W4-M07 plan D3，CAP-702 前置）：查询时取
	// runtime.NumGoroutine——压测前后采样对比判泄漏（宽限量级，pprof 不挂
	// 为门槛允许的登记口径）。
	expvar.Publish("chatchat_goroutines", expvar.Func(func() any {
		return runtime.NumGoroutine()
	}))
	expvar.Publish("chatchat_hot_group_count", expvar.Func(func() any {
		fctx, fcancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer fcancel()
		ids, err := groupRepo.ListHotGroupIDs(fctx)
		if err != nil {
			return -1
		}
		return len(ids)
	}))

	// 群成员数对账（W3-M01 CAP-010）：goroutine 阻塞跑 RunReconcile
	//（启动即一轮 → ticker 周期），属主本函数；两退出路径均先 rcancel
	// 再 Wait（ctx.Done 即返 + WaitGroup 回收，无泄漏）。W4-M05 plan D7：
	// 注入 Redis 每轮咨询锁——多 api 实例同轮至多一执行者（nil 容忍接口，
	// Redis 不可达=跳轮不 panic，正确性由对账幂等兜底）。
	gate := leader.NewRedisGate(rdb)
	rctx, rcancel := context.WithCancel(context.Background())
	var reconcileWG sync.WaitGroup
	reconcileWG.Add(1)
	go func() {
		defer reconcileWG.Done()
		groupSvc.RunReconcile(rctx, gate)
	}()

	// 热点聚合循环启动（Detector 构造已前置至 pullSvc 之前，W4-M03）：
	// Run 同 RunReconcile 范式：启动即一轮 → ticker → ctx.Done 返回，
	// goroutine 属主本函数，两退出路径均先 hcancel 再 Wait 回收。
	// W4-M05 plan D7：同 gate 门控（UnmarkSwitcher 驱动同循环随门控）。
	hctx, hcancel := context.WithCancel(context.Background())
	var hotWG sync.WaitGroup
	hotWG.Add(1)
	go func() {
		defer hotWG.Done()
		detector.Run(hctx, gate)
	}()

	// 步骤 8：HTTP 监听（goroutine；ErrServerClosed 为 Shutdown 后正常返回）。
	srv := &http.Server{
		Addr:         cfg.Server.Addr,
		Handler:      engine,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
	}
	errCh := make(chan error, 1)
	go func() {
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
		}
	}()

	// 步骤 9：信号驱动优雅退出（10s 超时 Shutdown；资源经上方 defer 逆序回收）。
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	select {
	case err := <-errCh:
		lg.Error("http server error", zap.Error(err))
		rcancel()
		reconcileWG.Wait()
		hcancel()
		hotWG.Wait()
		return 1
	case sig := <-sigCh:
		lg.Info("shutting down", zap.String("signal", sig.String()))
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := srv.Shutdown(shutdownCtx); err != nil {
			lg.Error("graceful shutdown failed", zap.Error(err))
			rcancel()
			reconcileWG.Wait()
			hcancel()
			hotWG.Wait()
			return 1
		}
		rcancel()
		reconcileWG.Wait()
		hcancel()
		hotWG.Wait()
		lg.Info("shutdown complete")
	}

	return 0
}

// apiRoundStats 对账轮观测适配器（W4-M05 plan D5/移交 2）：group.RoundStats
// 窄接口 → expvar 两计数——rounds 单调累加（每轮 +1），overlimit 为当前
// 超限群数 gauge（Set 语义）。
type apiRoundStats struct {
	rounds    *expvar.Int
	overlimit *expvar.Int
}

// ReconcileRound 实现 group.RoundStats（scanned 仅入日志摘要，expvar 面不落）。
func (s apiRoundStats) ReconcileRound(_, overlimit int64) {
	s.rounds.Add(1)
	s.overlimit.Set(overlimit)
}
