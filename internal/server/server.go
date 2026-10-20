package server

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	stderrors "errors"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"chatchat/pkg/common"
	"chatchat/pkg/common/jwt"
	pkgerrors "chatchat/pkg/errors"
	"chatchat/pkg/protocol"

	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	"github.com/gorilla/websocket"
)

// Handler 为上行帧回调接口：网关完成解码与心跳等生命周期语义后回调，
// 业务逻辑（W2-M03 起 Logic）在装配侧实现并注入。ctx 在连接关闭时取消。
type Handler interface {
	HandleFrame(ctx context.Context, uid uint64, frame *protocol.Frame)
}

// NoopHandler 空实现（装配缺省/测试用）。
type NoopHandler struct{}

// HandleFrame 实现 Handler：无操作。
func (NoopHandler) HandleFrame(context.Context, uint64, *protocol.Frame) {}

// Options 为网关运行参数（默认值与 config [ws] 段一一对应；零值经
// normalize 补默认，供测试直接构造）。
type Options struct {
	HeartbeatTimeout time.Duration // 读空闲超时=心跳超时
	WriteTimeout     time.Duration // 单次写超时
	SendBufferSize   int           // send 缓冲
	MaxMessageBytes  int64         // 单帧上限
	OnlineTTL        time.Duration // 在线键 TTL（须 > HeartbeatTimeout）
	GatewayID        string        // 节点标识（空则 os.Hostname）
	MalformedKickN   int           // 连续解码失败断连阈值
}

// normalize 补零值默认（60s/10s/256/64KB/2×心跳/hostname/3）。
func (o *Options) normalize() {
	if o.HeartbeatTimeout <= 0 {
		o.HeartbeatTimeout = 60 * time.Second
	}
	if o.WriteTimeout <= 0 {
		o.WriteTimeout = 10 * time.Second
	}
	if o.SendBufferSize <= 0 {
		o.SendBufferSize = 256
	}
	if o.MaxMessageBytes <= 0 {
		o.MaxMessageBytes = 65536
	}
	if o.OnlineTTL <= 0 {
		o.OnlineTTL = 2 * o.HeartbeatTimeout
	}
	if o.GatewayID == "" {
		if host, err := os.Hostname(); err == nil && host != "" {
			o.GatewayID = host
		} else {
			o.GatewayID = "gateway-unknown"
		}
	}
	if o.MalformedKickN <= 0 {
		o.MalformedKickN = 3
	}
}

// Server 为 WS 网关握手与升级入口（W2-M02 CAP-001/006）。
type Server struct {
	jwt      *jwt.Manager
	presence PresenceStore
	handler  Handler
	opts     Options
	logger   *zap.Logger
	manager  *ConnManager
	upgrader websocket.Upgrader
}

// NewServer 构造网关（opts 零值字段自动补默认）。
func NewServer(jwtMgr *jwt.Manager, presence PresenceStore, handler Handler, opts Options, logger *zap.Logger) *Server {
	if handler == nil {
		handler = NoopHandler{}
	}
	if logger == nil {
		logger = zap.NewNop()
	}
	opts.normalize()
	return &Server{
		jwt:      jwtMgr,
		presence: presence,
		handler:  handler,
		opts:     opts,
		logger:   logger,
		manager:  NewConnManager(),
		upgrader: websocket.Upgrader{
			ReadBufferSize:  1024,
			WriteBufferSize: 1024,
			CheckOrigin:     checkOrigin,
		},
	}
}

// Manager 返回本机连接注册表（投递入口，M04/W3 消费）。
func (s *Server) Manager() *ConnManager { return s.manager }

// ServeWS 处理 GET /ws：Authorization Bearer 鉴权 → 升级 → 注册（顶替踢旧）
// → 写在线键 → 起读写泵（读泵阻塞本协程直至连接终结）。
// 鉴权失败：HTTP 401 + common.Fail JSON（2004/2005），不升级（plan §7.4）。
func (s *Server) ServeWS(w http.ResponseWriter, r *http.Request) {
	token := bearerToken(r)
	if token == "" {
		s.rejectHandshake(w, pkgerrors.ErrTokenInvalid)
		return
	}
	claims, err := s.jwt.Parse(token, jwt.TypeAccess)
	if err != nil {
		s.rejectHandshake(w, err)
		return
	}
	if claims.UID < 0 {
		s.rejectHandshake(w, pkgerrors.ErrTokenInvalid)
		return
	}
	uid := uint64(claims.UID)

	conn, err := s.upgrader.Upgrade(w, r, nil)
	if err != nil {
		// Upgrade 已写错误响应；仅记录（不含 token）。
		s.logger.Warn("websocket upgrade failed", zap.Uint64("uid", uid), zap.Error(err))
		return
	}

	// 连接级 ctx 以 Background 为父：WS 升级（hijack）后 r.Context() 的
	// 取消时机不再可靠，连接生命周期由读写泵/finalize 自治。
	client := newClient(context.Background(), uid, conn, &s.opts, s.presence, s.handler, s.logger)
	if kicked := s.manager.Register(client); kicked != nil {
		kicked.shutdown()
		s.logger.Info("kicked previous connection", zap.Uint64("uid", uid))
	}

	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	if err := s.presence.SetOnline(ctx, uid, s.opts.GatewayID, s.opts.OnlineTTL); err != nil {
		s.logger.Warn("presence SetOnline failed", zap.Uint64("uid", uid), zap.Error(err))
	}
	cancel()

	s.logger.Info("connection established",
		zap.Uint64("uid", uid),
		zap.String("remote", conn.RemoteAddr().String()),
		zap.String("gateway", s.opts.GatewayID))

	client.startPumps()
	// 读泵占据本 handler 协程直到连接关闭；finalize 在其内部完成注销与在线键删除。
	client.readPump(s.manager.Unregister)

	s.logger.Info("connection closed", zap.Uint64("uid", uid))
}

// SendError 组装并发送 CmdError 帧（W2-M01 cmd.go 注释点名 M02 消费）：
// payload 为统一响应 JSON（common.Fail 消毒后的 {code,message,data}），
// 经 Manager.Push 投递（离线/慢消费者语义同 Push）。
func (s *Server) SendError(uid uint64, seq int64, traceID string, e *pkgerrors.Error) {
	if e == nil {
		e = pkgerrors.ErrInternal
	}
	payload, err := json.Marshal(common.Fail(e))
	if err != nil {
		s.logger.Error("SendError marshal failed", zap.Error(err))
		return
	}
	frame := &protocol.Frame{
		Header:  &protocol.Header{Cmd: protocol.CmdError, Seq: seq, TraceId: traceID},
		Payload: payload,
	}
	b, err := proto.Marshal(frame)
	if err != nil {
		s.logger.Error("SendError frame marshal failed", zap.Error(err))
		return
	}
	s.manager.Push(uid, b)
}

// rejectHandshake 输出 401 JSON（统一响应结构），不升级 WS。
func (s *Server) rejectHandshake(w http.ResponseWriter, err error) {
	code := pkgerrors.ErrInternal.Code
	var e *pkgerrors.Error
	if stderrors.As(err, &e) {
		code = e.Code
	}
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(pkgerrors.HTTPStatus(code))
	if encErr := json.NewEncoder(w).Encode(common.Fail(err)); encErr != nil {
		s.logger.Warn("handshake reject write failed", zap.Error(encErr))
	}
}

// bearerToken 从 Authorization 头提取 Bearer token（大小写容忍；无则空串）。
// C2 定向解冻增补（W4-M04-C2 裁决 D2）：头缺失或非 Bearer 形态时回退
// ?token= query 参数——浏览器 WebSocket API 无法自定义 Authorization 头
// （标准行为，无等价绕行），web 联调页经 query 供给 token。header 优先，
// 两者皆无 → 空串（401 语义不变）。
// 安全登记（ADR-0011 §6）：query token 可能进入访问日志/代理日志——
// 0.1.0 开发态接受，生产硬化项（日志脱敏或一次性票据）已登记。
func bearerToken(r *http.Request) string {
	h := strings.TrimSpace(r.Header.Get("Authorization"))
	if h != "" {
		parts := strings.SplitN(h, " ", 2)
		if len(parts) == 2 && strings.EqualFold(parts[0], "Bearer") {
			if tok := strings.TrimSpace(parts[1]); tok != "" {
				return tok
			}
		}
	}
	return strings.TrimSpace(r.URL.Query().Get("token"))
}

// checkOrigin 升级来源校验：空 Origin（非浏览器客户端）放行；Origin 的
// host:port 与请求 Host 一致放行；其余拒绝。
func checkOrigin(r *http.Request) bool {
	origin := r.Header.Get("Origin")
	if origin == "" {
		return true
	}
	u, err := url.Parse(origin)
	if err != nil || u.Host == "" {
		return false
	}
	return u.Host == r.Host
}

// newTraceID 生成 16 hex 字符链路追踪 ID（上行帧缺 traceId 时服务端补生成）。
func newTraceID() string {
	b := make([]byte, 8)
	if _, err := rand.Read(b); err != nil {
		return "tr-fallback"
	}
	return hex.EncodeToString(b)
}
