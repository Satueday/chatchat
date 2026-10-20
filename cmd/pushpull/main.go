// Package main 是 ChatChat pushpull（推拉投递）服务入口（W2-M06 空闲骨架，
// W3-M05 接入真实装配——骨架结构不变承诺兑现）。
//
// 职责（ADR-0009）：消费投递信封 topic（[kafka].topic，组 [kafka].
// consumer_group）→ 写扩散信箱（inbox_delivery 全员幂等批量）→ Redis
// presence 在线判定 → 在线成员发布单成员推送信封到 push topic（[kafka].
// push_topic）→ gateway 侧推送引擎落地 WS 下行。
// W4-M02 增热点专属通道（ADR-0010）：第二 Consumer（[kafka].hot_topic +
// [kafka].hot_consumer_group，独立 rebalance/lag 域）→ HotService 写
// group_timeline 读扩散一行/消息 + NotifyDispatcher 旁路可丢弃协程池；
// W4-M03 轻通知接线（plan D3）：Timeline 写成功后 Submit 通知闭包（成员
// 发现 → 在线判定 → push topic kind=1 信封）→ gateway 侧 CmdNotify 直推。
//
// 装配顺序（W3-M05-plan §6.2 冻结；W4-M02 平行扩展第二 Consumer）：
// config → log → MySQL → Redis → mq.Consumer（chatchat-message/
// chatchat-pushpull）→ mq.Consumer#2（chatchat-hot-group/
// chatchat-pushpull-hot）→ mq.Producer（push topic）→ NotifyDispatcher →
// pushpull.NewService + NewHotService → goroutine×2 Consume（WaitGroup
// 属主）→ mux{GET /healthz} → signal → cancel → Wait → dispatcher.Close →
// consumer.Close ×2 → producer.Close → defer 逆序（redis/mysql/log）。
// 初始化失败一律上抛终止（ADR-0001）；Close 全部入退出链。
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
	"chatchat/internal/mq"
	"chatchat/internal/readyz"
	"chatchat/internal/service/pushpull"
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

	lg, err := log.New(cfg.Log, log.WithService("chatchat-pushpull"))
	if err != nil {
		fmt.Fprintln(os.Stderr, "init logger failed:", err)
		return 1
	}
	defer func() { _ = lg.Close() }()

	if _, statErr := os.Stat("config.toml"); os.IsNotExist(statErr) {
		lg.Warn("config.toml not found, using built-in defaults")
	}

	lg.Info("chatchat version " + buildVersion)

	// MySQL：信箱写扩散（InboxDeliveryRepo）。
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

	// Redis：presence 在线判定（online:{uid}，与 gateway 共用拓扑）。
	rdb, err := dao.NewRedisClient(context.Background(), cfg.Redis)
	if err != nil {
		lg.Error("init redis failed", zap.Error(err))
		return 1
	}
	defer func() { _ = rdb.Close() }()

	// 消费端：投递信封 topic（组内水平扩展，rebalance 透明）。
	consumer, err := mq.NewConsumer(mq.Config{
		Brokers: cfg.Kafka.Brokers,
		Topic:   cfg.Kafka.Topic,
		Group:   cfg.Kafka.ConsumerGroup,
	}, lg.Logger)
	if err != nil {
		lg.Error("init kafka consumer failed", zap.Error(err))
		return 1
	}

	// 热点专属通道消费端（W4-M02 plan D4，ADR-0010 D1）：独立 topic
	//（[kafka].hot_topic）+ 独立消费组（hot_consumer_group）——与普通
	// 消费组 rebalance/lag 域互不耦合；HotService 写 group_timeline。
	hotConsumer, err := mq.NewConsumer(mq.Config{
		Brokers: cfg.Kafka.Brokers,
		Topic:   cfg.Kafka.HotTopic,
		Group:   cfg.Kafka.HotConsumerGroup,
	}, lg.Logger)
	if err != nil {
		lg.Error("init kafka hot consumer failed", zap.Error(err))
		return 1
	}

	// hot 旁路可丢弃协程池（W4-M02 plan D5）：M02 阶段池内无真实业务任务
	//（CmdNotify 归 M03），机制就绪；Timeline 写入永不入池（HotService
	// 同步执行）。Close 入退出链（drain 语义）。
	dispatcher := pushpull.NewNotifyDispatcher(cfg.PushPull.HotNotifyWorkers,
		cfg.PushPull.HotNotifyQueue, lg.Logger)

	// 生产端：在线推送落地 topic（topic 逐消息指定，此处只带 brokers）。
	producer, err := mq.NewProducer(mq.Config{Brokers: cfg.Kafka.Brokers}, lg.Logger)
	if err != nil {
		lg.Error("init kafka producer failed", zap.Error(err))
		return 1
	}

	// 消费/推送观测埋点（W4-M05 plan D5，CAP-504）：expvar 四计数——
	// msg_consumed_total（两通道消费入口）、push_total/push_fail_total
	//（推送段成败）、notify_dropped_total（旁路池丢弃，Func 查询时取）。
	consumedVar := expvar.NewInt("chatchat_msg_consumed_total")
	pushVar := expvar.NewInt("chatchat_push_total")
	pushFailVar := expvar.NewInt("chatchat_push_fail_total")
	stats := ppStats{consumed: consumedVar, push: pushVar, pushFail: pushFailVar}
	expvar.Publish("chatchat_notify_dropped_total", expvar.Func(func() any {
		return dispatcher.Dropped()
	}))

	svc := pushpull.NewService(
		dao.NewInboxDeliveryRepo(gdb),
		dao.NewPresenceChecker(rdb),
		producer,
		cfg.Kafka.PushTopic,
		stats,
		lg.Logger,
	)

	// HotService（W4-M02 D4 + W4-M03 D3）：解码 → group_timeline 幂等写 →
	// 轻通知闭包 Submit（Timeline 成功后；成员发现/在线判定/推送发布）→
	// commit。轻通知依赖面全装配（gmembers/presence/producer/pushTopic/
	// dispatcher）——任一缺失即通知链停用（M02 行为冻结面兜底）。
	hotSvc := pushpull.NewHotService(
		dao.NewGroupTimelineRepo(gdb),
		dao.NewGroupMemberRepo(gdb),
		dao.NewPresenceChecker(rdb),
		producer,
		cfg.Kafka.PushTopic,
		dispatcher,
		stats,
		lg.Logger)

	// 消费 goroutine：ctx 取消 → Consume 优雅退出（已处理位移保留）。
	// 第二 Consumer（hot）独立 goroutine + 同一 WaitGroup（plan D4：既有
	// 装配范式平行扩展，双退出路径统一 cancel → Wait 回收）。
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := consumer.Consume(ctx, svc.Handle); err != nil {
			lg.Error("pushpull consume exited with error", zap.Error(err))
		}
	}()
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := hotConsumer.Consume(ctx, hotSvc.Handle); err != nil {
			lg.Error("pushpull hot consume exited with error", zap.Error(err))
		}
	}()

	// lag 轮询器（W4-M05 plan D5/TGT-506，CAP-504）：ClusterAdmin 每 30s
	// 轮询三组消费 lag——message/hot_group 两 pushpull 组 + push 组
	//（组名=chatchat-gw-{gatewayID}，gatewayID 归一与 cmd/gateway 同构；
	// compose 经 CHATCHAT_WS_GATEWAY_ID 注入稳定值）。初始化失败上抛终止
	//（fail-fast 与消费端同口径，ADR-0001）。goroutine 属主本函数：ctx
	// cancel → Run 返回 → 退出链 Close sarama 句柄。
	gatewayID := cfg.WS.GatewayID
	if gatewayID == "" {
		if host, herr := os.Hostname(); herr == nil && host != "" {
			gatewayID = host
		} else {
			gatewayID = "gateway-unknown"
		}
	}
	lagVars := map[string]*expvar.Int{
		"message":   expvar.NewInt("chatchat_consumer_lag_message"),
		"hot_group": expvar.NewInt("chatchat_consumer_lag_hot_group"),
		"push":      expvar.NewInt("chatchat_consumer_lag_push"),
	}
	lagPoller, err := mq.NewLagPoller(cfg.Kafka.Brokers, []mq.LagWatch{
		{Topic: cfg.Kafka.Topic, Group: cfg.Kafka.ConsumerGroup, Label: "message"},
		{Topic: cfg.Kafka.HotTopic, Group: cfg.Kafka.HotConsumerGroup, Label: "hot_group"},
		{Topic: cfg.Kafka.PushTopic, Group: "chatchat-gw-" + gatewayID, Label: "push"},
	}, 30*time.Second, func(label string, lag int64) {
		if v, ok := lagVars[label]; ok {
			v.Set(lag)
		}
	}, lg.Logger)
	if err != nil {
		lg.Error("init lag poller failed", zap.Error(err))
		return 1
	}
	lagDone := make(chan struct{})
	go func() {
		defer close(lagDone)
		lagPoller.Run(ctx)
	}()

	// 健康端点：[pushpull].addr 同端口 mux，探针统一 GET /healthz →
	// 200 "ok"（CAP-005；ReadHeaderTimeout 防慢速攻击）。
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	// readyz（W4-M05 plan D8，CAP-505）：MySQL+Redis Ping；mux 无访问日志
	// 面，探针路径天然豁免。Kafka 消费端构造期 fail-fast、运行期故障由
	// 消费重试日志面暴露——不进 readyz（plan D8 登记口径）。
	mux.Handle("GET /readyz", readyz.New(
		readyz.Checker{Name: "mysql", Check: sqlDB.PingContext},
		readyz.Checker{Name: "redis", Check: func(ctx context.Context) error { return rdb.Ping(ctx).Err() }},
	))
	// 指标端点（W4-M05 plan D5，CAP-504）：/debug/vars 挂 expvar（无鉴权
	// 为 0.1.0 已知边界，plan §8 登记下月 token 化）。
	// goroutine 运行时指标（W4-M07 plan D3）：查询时取 runtime.NumGoroutine。
	mux.Handle("/debug/vars", expvar.Handler())
	expvar.Publish("chatchat_goroutines", expvar.Func(func() any {
		return runtime.NumGoroutine()
	}))
	httpSrv := &http.Server{
		Addr:              cfg.PushPull.Addr,
		Handler:           mux,
		ReadHeaderTimeout: 10 * time.Second,
	}
	errCh := make(chan error, 1)
	go func() {
		if err := httpSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
		}
	}()

	lg.Info("pushpull ready",
		zap.String("topic", cfg.Kafka.Topic),
		zap.String("group", cfg.Kafka.ConsumerGroup),
		zap.String("pushTopic", cfg.Kafka.PushTopic),
		zap.String("hotTopic", cfg.Kafka.HotTopic),
		zap.String("hotGroup", cfg.Kafka.HotConsumerGroup))

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	select {
	case err := <-errCh:
		lg.Error("http server error", zap.Error(err))
		// 退出链与信号路径同构（W4-M03 TGT-209 吸收 M02 移交 P3-④：早退
		// 路径补 Close——停消费 → drain 旁路池 → 逆序关闭句柄）。
		cancel()
		wg.Wait()
		dispatcher.Close()
		if err := consumer.Close(); err != nil {
			lg.Error("kafka consumer close failed", zap.Error(err))
		}
		if err := hotConsumer.Close(); err != nil {
			lg.Error("kafka hot consumer close failed", zap.Error(err))
		}
		if err := producer.Close(); err != nil {
			lg.Error("kafka producer close failed", zap.Error(err))
		}
		return 1
	case sig := <-sigCh:
		lg.Info("shutting down", zap.String("signal", sig.String()))
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer shutdownCancel()
		if err := httpSrv.Shutdown(shutdownCtx); err != nil {
			lg.Error("graceful shutdown failed", zap.Error(err))
		}
	}

	// 退出链：停 HTTP → 停消费（cancel → Wait 双 Consumer，位移保留）→
	// 停 lag 轮询（Run 返回后 Close sarama 句柄）→ 关旁路池（有界 drain）
	// → 逆序关闭 consumer/producer → defer 逆序回收 Redis/MySQL/日志。
	cancel()
	wg.Wait()
	waitLag(lagDone)
	if lerr := lagPoller.Close(); lerr != nil {
		lg.Error("lag poller close failed", zap.Error(lerr))
	}
	dispatcher.Close()
	if err := consumer.Close(); err != nil {
		lg.Error("kafka consumer close failed", zap.Error(err))
	}
	if err := hotConsumer.Close(); err != nil {
		lg.Error("kafka hot consumer close failed", zap.Error(err))
	}
	if err := producer.Close(); err != nil {
		lg.Error("kafka producer close failed", zap.Error(err))
	}
	lg.Info("shutdown complete")
	return 0
}

// ppStats pushpull 消费/推送观测适配器（W4-M05 plan D5）：pushpull.Stats
// 窄接口 → expvar 三计数（expvar.Int 并发安全）。
type ppStats struct {
	consumed *expvar.Int
	push     *expvar.Int
	pushFail *expvar.Int
}

// Consumed 实现 pushpull.Stats（两通道消费入口各 +1）。
func (s ppStats) Consumed() { s.consumed.Add(1) }

// PushResult 实现 pushpull.Stats（推送段成败计数）。
func (s ppStats) PushResult(ok bool) {
	if ok {
		s.push.Add(1)
		return
	}
	s.pushFail.Add(1)
}

// waitLag 等 lag 轮询 goroutine 退出（上界 15s——sarama Net 超时 10s 级
// 的收口兜底；超时直接返回，句柄随后 Close 强收）。
func waitLag(done <-chan struct{}) {
	select {
	case <-done:
	case <-time.After(15 * time.Second):
	}
}
