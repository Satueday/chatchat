// Package main 是 ChatChat logic 服务入口（W2-M03：四服务骨架第三入口；
// W2-M06 补 [logic].addr 同端口 mux /healthz——兑现 M03 偏差 a）。
//
// 形态裁决（W2-M03-plan §4 D5 → W3-M04 用户授权通路形态 B，ADR-0008）：
// 实际数据通路维持 gateway 进程内闭环（校验→seq→落库→CmdError 同步回帧
// 在 gateway 进程完成，Deliverer 切 Kafka 生产——「先落库后写 MQ」不变式
// 由 HandleChat/HandleGroupChat 内在保证）；本入口保持独立进程骨架——
// 完成全部依赖装配（MySQL/Redis/repos/Service）后阻塞等信号优雅退出，
// 证明四服务形态可独立启动。W3-M04 原「cmd/logic 接入真实帧消费」预期
// 不满足（多实例演进时的帧 topic/gRPC 迁移面见 ADR-0008 偏差登记）。
//
// 装配顺序（对齐 cmd/api 范式）：config → log → MySQL → Redis →
// repos/user+message + RedisPendingSeqGen（W2-M05-C2 原子分配+在途标记）→
// NoopDeliverer → message.Service（群域 repo 注入 nil：骨架无群聊帧来源，
// 单聊装配结构与 cmd/gateway 同构）→
// mux{GET /healthz} 监听 [logic].addr → signal 优雅退出 → defer 逆序回收。
package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"chatchat/config"
	"chatchat/internal/dao"
	"chatchat/internal/service/friend"
	"chatchat/internal/service/message"
	"chatchat/pkg/global"
	"chatchat/pkg/log"

	"go.uber.org/zap"
)

// buildVersion 版本注入点（W4-M05 plan §6：-ldflags "-X main.buildVersion=..."
// 覆写；缺省回落 global.Version 源码常量——四入口统一形态）。
var buildVersion = global.Version

// main 仅做退出码透传；全部逻辑在 run 中以便表驱动测试。
func main() {
	os.Exit(run())
}

// run 执行装配并阻塞至信号，返回进程退出码。
func run() int {
	cfg, err := config.Load("config.toml")
	if err != nil {
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

	lg, err := log.New(cfg.Log, log.WithService("chatchat-logic"))
	if err != nil {
		fmt.Fprintln(os.Stderr, "init logger failed:", err)
		return 1
	}
	defer func() { _ = lg.Close() }()

	if _, statErr := os.Stat("config.toml"); os.IsNotExist(statErr) {
		lg.Warn("config.toml not found, using built-in defaults")
	}

	lg.Info("chatchat version " + buildVersion)

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

	rdb, err := dao.NewRedisClient(context.Background(), cfg.Redis)
	if err != nil {
		lg.Error("init redis failed", zap.Error(err))
		return 1
	}
	defer func() { _ = rdb.Close() }()

	// 好友门槛（W3-M02 plan D4）：与 cmd/gateway 同构装配 friend gate
	//（[friend].require_friend_for_chat 默认开，配置可放开）。
	friendSvc := friend.NewService(dao.NewFriendRepo(gdb), dao.NewUserRepo(gdb),
		cfg.Friend, lg.Logger)

	svc := message.NewService(
		dao.NewMessageRepo(gdb),
		dao.NewUserRepo(gdb),
		dao.NewRedisPendingSeqGen(rdb), // W2-M05-C2：原子分配+在途标记（D8/D9）
		message.NoopDeliverer{Logger: lg.Logger},
		friendSvc, // W3-M02：单聊好友门槛 gate 注入
		nil,       // W3-M04：群域 repo（骨架无群聊帧来源，nil=群链路拒用，ADR-0008）
		nil,
		nil, // W4-M02：热点专属通道（骨架无群聊帧来源，nil=降级普通通道）
		nil, // W4-M01：计数埋点（骨架无群聊帧来源，nil=不计数）
		nil, // W4-M05：接收计数埋点（骨架无帧来源，nil=不计数）
		lg.Logger,
	)
	_ = svc // 骨架无帧来源（通路形态 B：真实链路在 cmd/gateway 进程内，ADR-0008）；装配完成即骨架目标

	// 健康端点（W2-M06）：[logic].addr 同端口 mux，探针统一 GET /healthz
	// → 200 "ok"（CAP-005；ReadHeaderTimeout 防慢速攻击）。
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	httpSrv := &http.Server{
		Addr:              cfg.Logic.Addr,
		Handler:           mux,
		ReadHeaderTimeout: 10 * time.Second,
	}
	errCh := make(chan error, 1)
	go func() {
		if err := httpSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
		}
	}()

	lg.Info("logic skeleton ready (pathway form B: live chain in gateway process, see ADR-0008)")

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	select {
	case err := <-errCh:
		lg.Error("http server error", zap.Error(err))
		return 1
	case sig := <-sigCh:
		lg.Info("shutting down", zap.String("signal", sig.String()))
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := httpSrv.Shutdown(shutdownCtx); err != nil {
			lg.Error("graceful shutdown failed", zap.Error(err))
			return 1
		}
		lg.Info("shutdown complete")
	}

	return 0
}
