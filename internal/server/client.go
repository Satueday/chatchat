package server

import (
	"context"
	"sync"
	"time"

	"chatchat/pkg/protocol"

	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	"github.com/gorilla/websocket"
)

// Client 为单个 WS 连接的载体（W2-M02 CAP-002/003）：一个读协程（readPump，
// 由 ServeWS 所在 handler 协程担任）+ 一个写协程（writePump）。
//
// 写路径唯一不变量：一切下行（心跳应答、Push 投递、错误帧）一律经 send
// channel 由写协程写出——gorilla Conn 非并发写安全（go-chat 读协程直接
// WriteMessage 回 pong 的竞态反例规避）。send 永不被 close（以 done channel
// 作退出信号，消除「向已关闭 channel 发送」panic 面）；shutdown/close 路径
// 经 sync.Once 恰好执行一次。
type Client struct {
	uid      uint64
	conn     *websocket.Conn
	send     chan []byte
	opts     *Options
	logger   *zap.Logger
	presence PresenceStore
	handler  Handler

	// ctx 生命周期 = 连接生命周期：连接关闭时取消，Handler 回调携带。
	ctx    context.Context
	cancel context.CancelFunc

	closeOnce  sync.Once
	finishOnce sync.Once
	done       chan struct{} // shutdown 信号：关闭后读写泵退出
	finished   chan struct{} // finalize 完成信号：CloseAll 等待用
	writeWg    sync.WaitGroup
	malformedN int // 连续帧解码失败计数（仅读协程访问）
}

// newClient 构造连接（不启动泵；由 Server.serve 装配后启动）。
func newClient(ctx context.Context, uid uint64, conn *websocket.Conn, opts *Options,
	presence PresenceStore, handler Handler, logger *zap.Logger) *Client {
	cctx, cancel := context.WithCancel(ctx)
	return &Client{
		uid:      uid,
		conn:     conn,
		send:     make(chan []byte, opts.SendBufferSize),
		opts:     opts,
		logger:   logger,
		presence: presence,
		handler:  handler,
		ctx:      cctx,
		cancel:   cancel,
		done:     make(chan struct{}),
		finished: make(chan struct{}),
	}
}

// UID 返回连接绑定的用户 ID。
func (c *Client) UID() uint64 { return c.uid }

// trySend 非阻塞入队：缓冲满视为慢消费者，踢连接并返回 false（W2-M02
// 冻结策略：不阻塞投递方、不静默丢弃，由断连让客户端重连补齐）。
func (c *Client) trySend(data []byte) bool {
	select {
	case c.send <- data:
		return true
	case <-c.done:
		return false
	default:
		c.logger.Warn("send buffer full, kicking slow consumer",
			zap.Uint64("uid", c.uid))
		c.shutdown()
		return false
	}
}

// shutdown 关闭连接（幂等）：关 done（写泵退出）+ 关 conn（读泵以读错误退出）。
func (c *Client) shutdown() {
	c.closeOnce.Do(func() {
		close(c.done)
		_ = c.conn.Close()
	})
}

// readPump 读协程主体：deadline 驱动心跳超时，帧解码与 cmd 分派。
// 返回即连接终结，由 defer 执行 finalize（单点清理）。
func (c *Client) readPump(onClose func(*Client) bool) {
	defer c.finalize(onClose)

	c.conn.SetReadLimit(c.opts.MaxMessageBytes)
	for {
		// 任何帧（含心跳）都刷新读 deadline：停止发帧超过 HeartbeatTimeout
		// 即读超时退出（spec §8.5「服务端超时踢出」）。
		_ = c.conn.SetReadDeadline(time.Now().Add(c.opts.HeartbeatTimeout))
		_, data, err := c.conn.ReadMessage()
		if err != nil {
			return
		}
		c.handleData(data)
	}
}

// handleData 解码并分派单帧：心跳在网关内应答；其余 cmd 交 Handler；
// 连续 MalformedKickN 次解码失败断连（畸形输入 error 非 panic，W2-M01 先例）。
func (c *Client) handleData(data []byte) {
	frame := &protocol.Frame{}
	if err := proto.Unmarshal(data, frame); err != nil || frame.GetHeader() == nil {
		c.malformedN++
		c.logger.Warn("frame decode failed",
			zap.Uint64("uid", c.uid),
			zap.Int("bytes", len(data)),
			zap.Int("consecutive", c.malformedN))
		if c.malformedN >= c.opts.MalformedKickN {
			c.shutdown()
		}
		return
	}
	c.malformedN = 0

	if frame.GetHeader().GetCmd() == protocol.CmdHeartbeat {
		traceID := frame.GetHeader().GetTraceId()
		if traceID == "" {
			traceID = newTraceID()
		}
		ack := &protocol.Frame{Header: &protocol.Header{
			Cmd:     protocol.CmdHeartbeatAck,
			Seq:     frame.GetHeader().GetSeq(),
			TraceId: traceID,
		}}
		if b, err := proto.Marshal(ack); err == nil {
			c.trySend(b)
		}
		c.refreshPresence()
		return
	}

	// 非心跳帧：traceId 为空则服务端补生成（spec §11 贯穿语义）。
	if frame.GetHeader().GetTraceId() == "" {
		frame.Header.TraceId = newTraceID()
	}
	c.handler.HandleFrame(c.ctx, c.uid, frame)
}

// writePump 写协程主体：唯一写路径；写超时断连（spec §8.5）。
func (c *Client) writePump() {
	defer c.writeWg.Done()
	for {
		select {
		case data := <-c.send:
			_ = c.conn.SetWriteDeadline(time.Now().Add(c.opts.WriteTimeout))
			if err := c.conn.WriteMessage(websocket.BinaryMessage, data); err != nil {
				c.shutdown()
				return
			}
		case <-c.done:
			_ = c.conn.SetWriteDeadline(time.Now().Add(time.Second))
			_ = c.conn.WriteMessage(websocket.CloseMessage,
				websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""))
			return
		}
	}
}

// finalize 单点清理（读协程 defer 调用）：保证恰好一次——
// 关闭 conn/ctx → 注销（仅当前注册者生效；顶替踢旧的迟到清理为 no-op，
// 且不误删在线键）→ 在线键删除（best-effort 2s 超时）→ 通知完成。
func (c *Client) finalize(onClose func(*Client) bool) {
	c.shutdown()
	c.cancel()
	if wasCurrent := onClose(c); wasCurrent {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		if err := c.presence.SetOffline(ctx, c.uid); err != nil {
			c.logger.Warn("presence SetOffline failed", zap.Uint64("uid", c.uid), zap.Error(err))
		}
		cancel()
	}
	c.finishOnce.Do(func() { close(c.finished) })
}

// refreshPresence 心跳续期在线键（best-effort，失败不阻断连接）。
func (c *Client) refreshPresence() {
	ctx, cancel := context.WithTimeout(c.ctx, 2*time.Second)
	defer cancel()
	if err := c.presence.Refresh(ctx, c.uid, c.opts.OnlineTTL); err != nil {
		c.logger.Warn("presence Refresh failed", zap.Uint64("uid", c.uid), zap.Error(err))
	}
}

// waitFinished 阻塞至 finalize 完成（CloseAll 用；上限 5s 防御）。
func (c *Client) waitFinished() {
	select {
	case <-c.finished:
	case <-time.After(5 * time.Second):
		c.logger.Warn("client finalize timeout", zap.Uint64("uid", c.uid))
	}
}

// startPumps 启动写协程并标记 WaitGroup（读泵由调用方协程运行）。
func (c *Client) startPumps() {
	c.writeWg.Add(1)
	go c.writePump()
}

// waitWritePump 等待写协程退出（测试用）。
func (c *Client) waitWritePump() { c.writeWg.Wait() }
