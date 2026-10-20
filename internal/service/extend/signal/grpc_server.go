package signal

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"time"

	"chatchat/pkg/protocol"

	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// sendChanSize 每流下行缓冲（有界，D8/关键约束）：满即断流（慢消费者
// 断开重连——gotify 骨架口径；16 足够信令每通话 5~10 帧的突发）。
const sendChanSize = 16

// GRPCServer Exchange 双向流服务（CAP-304，TGT-305；spec §8.4 字面契约面）：
// 每流按 metadata "uid" 注册到流路由表，上行帧委托同一 Service 核心，
// 下行帧经流回投。流生命周期：每流两泵（Recv 循环/Send chan 写侧），
// 三路退出（ctx/EOF/错误）全回收；Send chan 有界满即断流。
//
// 错误回执形态（ADR-0011 登记）：gRPC 契约面无 CmdError 帧——Service
// 错误在 Recv 泵内 Warn 收敛、流不中断（用户面 7101/7102 显式回执归
// WS 数据面）；测试以 Service 级错误返回断言离线语义（CASE-310）。
type GRPCServer struct {
	protocol.UnimplementedSignalServer
	svc    *Service
	sinks  *streamRegistry
	logger *zap.Logger
}

// NewGRPCServer 构造 Exchange 服务。svc 需已 AttachRouter（装配面经
// streamRegistry 亦可为 router——bufconn 闭环用例形态：双端皆 gRPC 流；
// 生产 gateway 装配 router=WSAdapter，gRPC 上行路由到 WS 在线对端）。
func NewGRPCServer(svc *Service, logger *zap.Logger) *GRPCServer {
	if logger == nil {
		logger = zap.NewNop()
	}
	return &GRPCServer{svc: svc, sinks: newStreamRegistry(), logger: logger}
}

// uidFromMetadata 从流 ctx 提取 metadata "uid"（十进制串，D4：loopback
// 信任边界内由适配层/测试注入；非法/缺失为协议用法错误，断流）。
func uidFromMetadata(ctx context.Context) (uint64, error) {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return 0, fmt.Errorf("missing incoming metadata")
	}
	vals := md.Get("uid")
	if len(vals) == 0 {
		return 0, fmt.Errorf("missing uid metadata")
	}
	uid, err := strconv.ParseUint(vals[0], 10, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid uid metadata: %w", err)
	}
	return uid, nil
}

// Exchange 实现 SignalServer：双泵 + 属主 watcher。
//
//	Recv 泵（goroutine A）：循环 Recv → to 解析 → svc.HandleSignal；
//	Send 泵（goroutine B）：drain 有界 chan → stream.Send；
//	本方法（watcher）：select 任一退出信号 → kill sink → 带防御上限等
//	两泵回收 → return（handler 返回即 gRPC 关流，反向解除 A 的 Recv
//	阻塞——满即断流路径依赖此闭环）。
func (g *GRPCServer) Exchange(stream protocol.Signal_ExchangeServer) error {
	uid, err := uidFromMetadata(stream.Context())
	if err != nil {
		return status.Error(codes.InvalidArgument, err.Error())
	}
	sink := newStreamSink(uid)
	if kicked := g.sinks.add(sink); kicked != nil {
		kicked.kill() // 同 uid 顶替踢旧（ConnManager.Register 先例）
	}
	defer g.sinks.remove(sink)
	defer sink.kill()

	ctx := stream.Context()
	var wg sync.WaitGroup
	recvDone := make(chan struct{})

	// 泵 A：Recv 循环（上行处理串行于本流——Service 核心自带并发安全）。
	wg.Add(1)
	go func() {
		defer wg.Done()
		defer close(recvDone)
		for {
			in, rerr := stream.Recv()
			if rerr != nil {
				return // io.EOF（客户端关发送向）/传输错误/ctx 取消
			}
			g.handleFrame(ctx, uid, in.GetSignal())
		}
	}()

	// 泵 B：Send 写侧（下行有界 chan → stream.Send；dead/ctx 退出）。
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case out := <-sink.ch:
				if serr := stream.Send(out); serr != nil {
					sink.kill() // 写失败=流已坏，联动断流
					return
				}
			case <-sink.dead:
				return
			case <-ctx.Done():
				return
			}
		}
	}()

	// 属主 watcher：任一泵退出或 sink 被外部 kill（慢消费者断流/顶替）
	// 即收尾。ctx.Done 分支冗余覆盖（A/B 均会随之退出）。
	select {
	case <-recvDone:
	case <-sink.dead:
	case <-ctx.Done():
	}
	sink.kill()

	// 泵回收（防御上限 2s，Client.waitFinished 先例）：满即断流路径下 A
	// 阻塞于 Recv，须等本方法 return 关流后解除——上限内未归仅 Warn，
	// 不阻塞关流。
	waitPumps := make(chan struct{})
	go func() { wg.Wait(); close(waitPumps) }()
	select {
	case <-waitPumps:
	case <-time.After(2 * time.Second):
		g.logger.Warn("signal stream pumps teardown timeout", zap.Uint64("uid", uid))
	}
	return nil
}

// handleFrame 单上行帧处理（Recv 泵内）：to 解析（1001 面）→ Service
// 核心；错误 Warn 收敛（契约面无 CmdError 帧，流不断）。
func (g *GRPCServer) handleFrame(ctx context.Context, uid uint64, sig *protocol.Signal) {
	if sig == nil {
		return
	}
	toUID, err := strconv.ParseUint(sig.GetTo(), 10, 64)
	if err != nil {
		g.logger.Warn("signal grpc frame invalid to",
			zap.Uint64("from", uid), zap.String("to", sig.GetTo()),
			zap.String("callId", sig.GetCallId()), zap.Error(err))
		return
	}
	if e := g.svc.HandleSignal(ctx, uid, toUID, sig, ""); e != nil {
		g.logger.Warn("signal grpc frame rejected",
			zap.Uint64("from", uid), zap.Uint64("to", toUID),
			zap.String("callId", sig.GetCallId()), zap.Int32("code", e.Code),
			zap.String("message", e.Message))
	}
}

// ---- 流注册表（gRPC 面的 PeerRouter 实现）----

// streamSink 单条 Exchange 流的下行汇：有界 chan + dead 断流信号。
type streamSink struct {
	uid  uint64
	ch   chan *protocol.SignalFrame
	dead chan struct{}
	once sync.Once
}

// newStreamSink 构造流汇。
func newStreamSink(uid uint64) *streamSink {
	return &streamSink{
		uid:  uid,
		ch:   make(chan *protocol.SignalFrame, sendChanSize),
		dead: make(chan struct{}),
	}
}

// kill 断流（幂等）：关闭 dead——Send 泵退出、注册表置废、watcher 收尾。
func (s *streamSink) kill() { s.once.Do(func() { close(s.dead) }) }

// streamRegistry uid → 流汇注册表（ConnManager 同构语义：顶替踢旧、
// 迟到清理不误删新流；实现 PeerRouter——bufconn 闭环/未来形态 C 装配面）。
type streamRegistry struct {
	mu    sync.RWMutex
	sinks map[uint64]*streamSink
}

// newStreamRegistry 构造空注册表。
func newStreamRegistry() *streamRegistry {
	return &streamRegistry{sinks: make(map[uint64]*streamSink)}
}

// add 注册流汇，同 uid 已有流时返回被顶替者（调用方 kick）。
func (r *streamRegistry) add(s *streamSink) (kicked *streamSink) {
	r.mu.Lock()
	defer r.mu.Unlock()
	old, exists := r.sinks[s.uid]
	r.sinks[s.uid] = s
	if exists && old != s {
		return old
	}
	return nil
}

// remove 注销流汇：仅当 s 仍是该 uid 当前注册者时生效（顶替后旧流的
// 迟到清理不误删新流）。
func (r *streamRegistry) remove(s *streamSink) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if cur, ok := r.sinks[s.uid]; ok && cur == s {
		delete(r.sinks, s.uid)
		return true
	}
	return false
}

// Online 实现 PeerRouter：uid 是否有活跃流。
func (r *streamRegistry) Online(uid uint64) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	_, ok := r.sinks[uid]
	return ok
}

// PushSignal 实现 PeerRouter：非阻塞投递到对端流 chan；缓冲满即断流
// （kill 对端流，D8）并返回错误（视同离线 → 7101）。
func (r *streamRegistry) PushSignal(uid uint64, sig *protocol.Signal, _ string) error {
	r.mu.RLock()
	s, ok := r.sinks[uid]
	r.mu.RUnlock()
	if !ok {
		return fmt.Errorf("no signal stream for uid=%d", uid)
	}
	select {
	case s.ch <- &protocol.SignalFrame{Signal: sig}:
		return nil
	case <-s.dead:
		return fmt.Errorf("signal stream dead for uid=%d", uid)
	default:
		s.kill() // 满即断流：慢消费者断开重连（gotify 骨架）
		return fmt.Errorf("signal stream send buffer full for uid=%d", uid)
	}
}

// Registry 暴露流注册表（装配面：bufconn 闭环用例将其注入 Service 作为
// PeerRouter；生产装配不使用——router=WSAdapter）。
func (g *GRPCServer) Registry() PeerRouter { return g.sinks }

// 编译期断言：GRPCServer 即 SignalServer；streamRegistry 即 PeerRouter。
var (
	_ protocol.SignalServer = (*GRPCServer)(nil)
	_ PeerRouter            = (*streamRegistry)(nil)
)
