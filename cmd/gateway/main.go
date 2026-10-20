// Package main 是 ChatChat gateway（WS 网关）服务入口（W2-M02 建立，
// W2-M03 挂接单聊 Logic，W2-M04 挂接内存投递与 ACK，W2-M05 挂接拉取与
// 历史补齐，W2-M06 分派/推送适配上移 internal/wire 共用装配层，
// W3-M04 增 CmdGroupChat 群聊链路与 [deliver].driver 投递切换——kafka
// 为默认生产形态、memory 为降级开关，ADR-0008；W4-M01 增 hotdetect
// Recorder 群消息计数埋点注入 message.Logic；W4-M04 增信令面——signal.
// Service/WSAdapter 六路分派挂接（CmdSignal）、gRPC Exchange 契约面
// loopback 监听与 sweep TTL 清理入装配/退出链（ADR-0011）。
//
// 装配顺序（对齐 cmd/api 范式；gotify 样板）：
//  1. config.Load("config.toml")：默认值 → toml → CHATCHAT_ env → 字段级校验
//     （失败 → stderr 字段错误 + return 1）；
//  2. log.New(cfg.Log, WithService("chatchat-gateway"))，defer Close；
//  3. dao.NewMySQLDB + dao.NewRedisClient（gateway 承载 Logic 需要 MySQL：
//     MessageRepo/UserRepo/GroupRepo/GroupMemberRepo/InboxCursorRepo），
//     defer 逆序 Close；
//  4. 投递装配按 [deliver].driver 分支（kafka→mq.NewProducer+KafkaDeliverer，
//     初始化失败上抛；memory→MemoryDeliverer）+ wire.NewConnPusher +
//     message.NewService（单聊+群聊 Logic）+ message.NewPullService（拉取/
//     历史，M05）+ wire.NewLogicHandler（CmdChat/CmdGroupChat/CmdAck/
//     CmdPullRequest/default 五路分派，错误经 SendError 回 CmdError 帧——
//     分派实现归 internal/wire，本入口仅装配）——W2 数据通路为进程内挂接
//     （W2-M03-plan 裁决 D5；W3-M04 裁决通路形态 B 维持进程内前段+MQ 后段，
//     演进路径见 ADR-0008）；
//  5. mux{GET /ws → ServeWS, GET /healthz, / → web/ 静态托管（C2 D3 同源
//     联调页）} + http.Server(cfg.WS.Addr)；
//  6. signal → 10s Shutdown → KafkaDeliverer/producer Close（kafka driver）
//     → push consumer Close（kafka driver，W3-M05：cancel→Wait→Close）
//     → MemoryDeliverer.Close()（停 timer 清 pending；kafka driver 下兼为
//     推送引擎，消费先停再关引擎）→ Manager.CloseAll()（排空全部连接、
//     删在线键）→ defer 逆序回收 MySQL/Redis/日志。
package main

import (
	"context"
	"errors"
	"expvar"
	"fmt"
	"net"
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
	"chatchat/internal/mq"
	"chatchat/internal/readyz"
	"chatchat/internal/server"
	wsignal "chatchat/internal/service/extend/signal" // 别名避让 os/signal（下方 signal.Notify）
	"chatchat/internal/service/friend"
	"chatchat/internal/service/message"
	"chatchat/internal/wire"
	"chatchat/pkg/common/jwt"
	"chatchat/pkg/global"
	"chatchat/pkg/log"
	"chatchat/pkg/protocol"

	"go.uber.org/zap"
	"google.golang.org/grpc"
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

	lg, err := log.New(cfg.Log, log.WithService("chatchat-gateway"))
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

	jm := jwt.NewManager(cfg.JWT.Secret.Value(),
		time.Duration(cfg.JWT.AccessTTLSec)*time.Second,
		time.Duration(cfg.JWT.RefreshTTLSec)*time.Second)

	// 投递装配（W2-M04 内存实现 → W3-M04 [deliver].driver 开关，ADR-0008
	// 通路形态 B）：kafka（默认）→ mq.NewProducer + KafkaDeliverer（生产真实
	// 形态——先落库后写 MQ，初始化失败上抛终止，producer 句柄 Close 入退出
	// 链，ADR-0001）；memory（降级开关）→ MemoryDeliverer 原路径。两形态共用
	// ConnPusher/游标 repo（拉取链路不变）。MemoryDeliverer 在 kafka driver 下
	// 升格为「推送引擎 + AckHandler」（W3-M05 裁决 D5）：经 push topic 消费
	// 链获得真实调用方（NewPushHandler → Deliver），复用其全部 pending/Push/
	// 重推/ACK 机制与 ADR-0005 参数；GroupMembers 注入启用群聊 ACK（g_ 成员
	// 判定查群表，D5/CAP-004）。
	pusher := wire.NewConnPusher()
	cursors := dao.NewInboxCursorRepo(gdb)
	memDeliverer := message.NewMemoryDeliverer(
		pusher,
		cursors,
		message.DeliveryOptions{
			RetryBase:    time.Duration(cfg.Deliver.RetryBaseMS) * time.Millisecond,
			RetryMax:     cfg.Deliver.RetryMax,
			GroupMembers: dao.NewGroupMemberRepo(gdb), // W3-M05：群 ACK g_ 成员判定
		},
		lg.Logger,
	)
	var deliverer message.Deliverer = memDeliverer
	var closeKafka func() error
	// hotDeliver 为热点专属通道投递器（W4-M02 plan D1/D3）：Kafka 形态下
	// 复用同一 mq.Producer（逐消息 topic 指定，mq 接口零改动）构造第二个
	// KafkaDeliverer 实例（topic=[kafka].hot_topic）；memory 降级形态保持
	// nil——内存模式不承载读写扩散，热点消息降级走普通 deliverer。
	// producer 句柄由 closeKafka 统一关闭，第二实例不重复入退出链。
	var hotDeliver message.Deliverer
	// pushConsumerClose 停推送消费链（kafka driver 下非 nil）：cancel 触发
	// Consume 优雅退出（位移保留）→ Close 消费组。次序入退出链 D6：
	// Shutdown → producer Close → 本函数 → memDeliverer.Close → CloseAll
	//（消费先停再关引擎）。
	var pushConsumerClose func()
	if cfg.Deliver.Driver == "kafka" {
		producer, err := mq.NewProducer(mq.Config{Brokers: cfg.Kafka.Brokers, Topic: cfg.Kafka.Topic}, lg.Logger)
		if err != nil {
			lg.Error("init kafka producer failed", zap.Error(err))
			return 1
		}
		kd := message.NewKafkaDeliverer(producer, cfg.Kafka.Topic, lg.Logger)
		deliverer = kd
		closeKafka = kd.Close // producer.Close 透传（KafkaDeliverer.Close 具体类型方法）
		hotDeliver = message.NewKafkaDeliverer(producer, cfg.Kafka.HotTopic, lg.Logger)

		// 推送消费者（W3-M05 裁决 D4，ADR-0009）：per-gateway 广播组
		// chatchat-gw-{gatewayID}——每网关独立组收全量 push 项，本地
		// ConnManager 过滤（多网关演进就绪）；组名派生真实 gatewayID
		//（Options.normalize 的 os.Hostname 兜底同构）。
		gatewayID := cfg.WS.GatewayID
		if gatewayID == "" {
			if host, herr := os.Hostname(); herr == nil && host != "" {
				gatewayID = host
			} else {
				gatewayID = "gateway-unknown"
			}
		}
		pushConsumer, err := mq.NewConsumer(mq.Config{
			Brokers: cfg.Kafka.Brokers,
			Topic:   cfg.Kafka.PushTopic,
			Group:   "chatchat-gw-" + gatewayID,
		}, lg.Logger)
		if err != nil {
			lg.Error("init push consumer failed", zap.Error(err))
			return 1
		}
		pushCtx, pushCancel := context.WithCancel(context.Background())
		var pushWG sync.WaitGroup
		pushWG.Add(1)
		go func() {
			defer pushWG.Done()
			if err := pushConsumer.Consume(pushCtx, message.NewPushHandler(memDeliverer, pusher, lg.Logger)); err != nil {
				lg.Error("push consumer exited with error", zap.Error(err))
			}
		}()
		pushConsumerClose = func() {
			pushCancel()
			pushWG.Wait()
			if err := pushConsumer.Close(); err != nil {
				lg.Error("push consumer close failed", zap.Error(err))
			}
		}
	}

	// 好友门槛（W3-M02 plan D4）：friend.Service 结构化实现 message.ChatGate，
	// 开关随 [friend].require_friend_for_chat（默认开）；非好友发送在 5005
	// 校验后、seq 分配前被拒（3005 CmdError 回帧）。
	friendSvc := friend.NewService(dao.NewFriendRepo(gdb), dao.NewUserRepo(gdb),
		cfg.Friend, lg.Logger)

	// 热点计数埋点（W4-M01 plan D2/D7）：hotdetect.Recorder 结构化满足
	// message.GroupMsgCounter（本入口不 import hotdetect 接口之外的内部件）；
	// HandleGroupChat 落库成功后同步单 pipeline 计数，失败仅 Warn 不阻断发送。
	recorder := hotdetect.NewRecorder(rdb, cfg.HotDetect, lg.Logger)

	// 上行接收计数埋点（W4-M05 plan D5）：message.MsgStats 窄接口 → expvar
	// 单聊/群聊分列计数（chatchat_msg_received_{chat,group}_total）。
	chatRecvVar := expvar.NewInt("chatchat_msg_received_chat_total")
	groupRecvVar := expvar.NewInt("chatchat_msg_received_group_total")

	logicSvc := message.NewService(
		dao.NewMessageRepo(gdb),
		dao.NewUserRepo(gdb),
		dao.NewRedisPendingSeqGen(rdb), // W2-M05-C2：原子分配+在途标记（D8/D9）
		deliverer,
		friendSvc,                   // W3-M02：单聊好友门槛 gate 注入
		dao.NewGroupRepo(gdb),       // W3-M04：群聊链路（HandleGroupChat）
		dao.NewGroupMemberRepo(gdb), // W3-M04：群成员/禁言校验+读侧成员判定
		hotDeliver,                  // W4-M02：热点专属通道（kafka 第二实例/memory nil 降级）
		recorder,                    // W4-M01：群消息计数埋点（Redis 时间桶）
		gwMsgStats{chat: chatRecvVar, group: groupRecvVar}, // W4-M05：接收计数埋点
		lg.Logger,
	)

	// 拉取装配（W2-M05）：与投递共用 ConnPusher/游标 repo；帧预算取
	// ws.max_message_bytes（WS 单帧上限即响应帧上限，plan §4 D4）；
	// Pend 注入在途标记探针（W2-M05-C2 D12 间隙判定，装配层必须注入）；
	// GroupMembers 注入群会话读侧成员判定（W3-M04 D6——漏注入则 g_ 一律
	// 5009 安全失败，装配层必须注入）；Hot/Timeline 注入热点拉取分支与
	// 清单热点增补（W4-M03 D4/D6——查询侧 Detector 仅消费 IsHot，Run 不
	// 在本进程启动：聚合/切换宿主归 cmd/api；switcher nil 为安全缺省）。
	pullSvc := message.NewPullService(
		dao.NewMessageRepo(gdb),
		cursors,
		message.PullOptions{
			Pusher:       pusher,
			FrameBudget:  int(cfg.WS.MaxMessageBytes),
			Pend:         dao.NewRedisPendMarker(rdb),
			GroupMembers: dao.NewGroupMemberRepo(gdb),
			Hot: hotdetect.NewDetector(rdb,
				dao.NewHotGroupStatRepo(gdb), dao.NewGroupRepo(gdb),
				nil, cfg.HotDetect, lg.Logger),
			Timeline: dao.NewGroupTimelineRepo(gdb),
		},
		lg.Logger,
	)

	wsOpts := server.Options{
		HeartbeatTimeout: time.Duration(cfg.WS.HeartbeatTimeoutSec) * time.Second,
		WriteTimeout:     time.Duration(cfg.WS.WriteTimeoutSec) * time.Second,
		SendBufferSize:   cfg.WS.SendChannelSize,
		MaxMessageBytes:  cfg.WS.MaxMessageBytes,
		OnlineTTL:        time.Duration(cfg.WS.OnlineTTLSec) * time.Second,
		GatewayID:        cfg.WS.GatewayID,
	}

	// 信令装配（W4-M04，ADR-0011 D1）：Service 核心（会话表+TTL）+ WSAdapter
	//（数据面 PeerRouter，Manager 两阶段回填）+ gRPC Exchange 契约面
	//（loopback :cfg.Signal.Addr——config 校验已强制 loopback）+ sweep
	// goroutine（ctx 退出，宿主本进程）。router=WSAdapter：gRPC 上行帧与
	// WS 上行帧同走 Service 核心、路由到 WS 在线对端（生产拓扑——gRPC 面
	// 为契约/测试面；bufconn 闭环用例侧装配 registry 为 router，见
	// test/signal_integration_test.go）。
	signalSvc := wsignal.NewService(
		time.Duration(cfg.Signal.SessionTTLSec)*time.Second, lg.Logger)
	signalAd := wsignal.NewWSAdapter(signalSvc)
	signalSvc.AttachRouter(signalAd)

	// Logic 挂接（W2-M03 建立、W2-M06 上移 wire）：六路分派 handler 指针
	// 注入（CmdChat/CmdGroupChat/CmdAck/CmdPullRequest/CmdSignal/default）+
	// AttachSendError 回填 SendError；pusher 回填 Manager。acker=memDeliverer
	//（kafka driver 下仅承担 ACK，见上方装配注）。
	handler := wire.NewLogicHandler(logicSvc, memDeliverer, pullSvc, signalAd)
	srv := server.NewServer(jm, server.NewRedisPresence(rdb), handler, wsOpts, lg.Logger)
	handler.AttachSendError(srv.SendError)
	pusher.Attach(srv.Manager())
	signalAd.Attach(srv.Manager()) // 数据面 router 回填（Manager 随 Server 构造）

	// gRPC Exchange 契约面：loopback 监听（[signal].addr 校验强制非对外）；
	// GracefulStop（5s 兜底 Stop）入退出链。
	grpcSrv := grpc.NewServer()
	protocol.RegisterSignalServer(grpcSrv, wsignal.NewGRPCServer(signalSvc, lg.Logger))
	grpcLis, err := net.Listen("tcp", cfg.Signal.Addr)
	if err != nil {
		lg.Error("signal grpc listen failed", zap.String("addr", cfg.Signal.Addr), zap.Error(err))
		return 1
	}
	grpcErrCh := make(chan error, 1)
	go func() {
		if err := grpcSrv.Serve(grpcLis); err != nil {
			grpcErrCh <- err
		}
	}()

	// sweep：会话 TTL 清理循环（RunReconcile 范式；退出链内 cancel+等待）。
	// defer 兜底错误返回路径（正常退出链下方显式调用先发生；cancel 幂等）。
	sweepCtx, sweepStop := context.WithCancel(context.Background())
	defer sweepStop()
	sweepDone := make(chan struct{})
	go func() {
		defer close(sweepDone)
		signalSvc.RunSweep(sweepCtx)
	}()

	mux := http.NewServeMux()
	mux.HandleFunc("GET /ws", srv.ServeWS)
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	// readyz（W4-M05 plan D8，CAP-505）：MySQL+Redis Ping（Kafka producer
	// 构造期已 fail-fast，运行期故障由推送路径日志面暴露——不进 readyz，
	// plan D8 登记）；mux 无访问日志面，探针路径天然豁免。
	mux.Handle("GET /readyz", readyz.New(
		readyz.Checker{Name: "mysql", Check: sqlDB.PingContext},
		readyz.Checker{Name: "redis", Check: func(ctx context.Context) error { return rdb.Ping(ctx).Err() }},
	))
	// 指标端点（W4-M05 plan D5，CAP-504）：/debug/vars 挂 expvar；
	// online_connections 为 Func——ConnManager.Count() 查询时取（经装配层
	// 引用，server 零改动，plan §6）。无鉴权为 0.1.0 已知边界（§8 登记）。
	mux.Handle("/debug/vars", expvar.Handler())
	// goroutine 运行时指标（W4-M07 plan D3）：查询时取 runtime.NumGoroutine。
	expvar.Publish("chatchat_goroutines", expvar.Func(func() any {
		return runtime.NumGoroutine()
	}))
	expvar.Publish("chatchat_online_connections", expvar.Func(func() any {
		return srv.Manager().Count()
	}))
	// 静态托管 web/（W4-M04-C2 裁决 D3）：浏览器真实联调页同源供给——WS
	// checkOrigin 要求 Origin 与 Host 同 host:port，页面必须由本网关自身
	// 提供；浏览器 WebSocket 无法自定义 Authorization 头，鉴权经 ?token=
	// query 回退（server.bearerToken，安全登记 ADR-0011 §6）。/ws 与
	// /healthz 已先注册、优先匹配，不被覆盖；web/ 缺失时该前缀 404 不影响
	// 服务面。运行目录口径=仓库根（make run 先例；web/ 相对路径由此解析）。
	mux.Handle("/", http.FileServer(http.Dir("web")))

	httpSrv := &http.Server{
		Addr:    cfg.WS.Addr,
		Handler: mux,
		// ReadHeaderTimeout 防慢速攻击；WS 连接升级后的读写超时由
		// Client 的 deadline 管理（hijack 后 http.Server 超时不再生效）。
		ReadHeaderTimeout: 10 * time.Second,
	}
	errCh := make(chan error, 1)
	go func() {
		if err := httpSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
		}
	}()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	select {
	case err := <-errCh:
		lg.Error("http server error", zap.Error(err))
		return 1
	case gerr := <-grpcErrCh:
		lg.Error("signal grpc server error", zap.Error(gerr))
		return 1
	case sig := <-sigCh:
		lg.Info("shutting down", zap.String("signal", sig.String()))
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := httpSrv.Shutdown(shutdownCtx); err != nil {
			lg.Error("graceful shutdown failed", zap.Error(err))
			return 1
		}
		// 退出链（M04 裁决 D7 + W3-M04 kafka driver + W3-M05 push 链 +
		// W4-M04 信令面）：停 HTTP → 停 gRPC 契约面（GracefulStop 5s 兜底
		// Stop——排空在途 Exchange 流）→ 停 sweep（cancel → 等待 goroutine
		// 回收）→ 停投递（kafka：KafkaDeliverer/producer Close，错误仅记
		// 日志；memory：停 timer 清 pending，幂等）→ 停推送消费链（cancel →
		// Wait → Close，D6：消费先停再关引擎）→ ACK 处理器/推送引擎收尾
		//（memDeliverer.Close，两 driver 形态均安全）→ 排空全部 WS 连接
		//（写协程退出、在线键删除）→ defer 逆序回收 MySQL/Redis。
		grpcStopped := make(chan struct{})
		go func() {
			grpcSrv.GracefulStop()
			close(grpcStopped)
		}()
		select {
		case <-grpcStopped:
		case <-time.After(5 * time.Second):
			lg.Warn("signal grpc GracefulStop timeout, forcing Stop")
			grpcSrv.Stop()
			<-grpcStopped
		}
		sweepStop()
		select {
		case <-sweepDone:
		case <-time.After(2 * time.Second):
			lg.Warn("signal sweep teardown timeout")
		}
		if closeKafka != nil {
			if err := closeKafka(); err != nil {
				lg.Error("kafka deliverer close failed", zap.Error(err))
			}
		}
		if pushConsumerClose != nil {
			pushConsumerClose()
		}
		memDeliverer.Close()
		srv.Manager().CloseAll()
		lg.Info("shutdown complete")
	}

	return 0
}

// gwMsgStats 上行消息接收计数适配器（W4-M05 plan D5）：message.MsgStats
// 窄接口 → expvar 单聊/群聊分列计数（expvar.Int 并发安全）。
type gwMsgStats struct {
	chat  *expvar.Int
	group *expvar.Int
}

// ChatReceived 实现 message.MsgStats（单聊入口 +1）。
func (m gwMsgStats) ChatReceived() { m.chat.Add(1) }

// GroupChatReceived 实现 message.MsgStats（群聊入口 +1）。
func (m gwMsgStats) GroupChatReceived() { m.group.Add(1) }
