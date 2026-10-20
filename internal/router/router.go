// Package router HTTP 路由注册与中间件挂载（W1-M06；W2-M06 增免鉴权
// /healthz 探针；W3-M01 增 /api/group 路由组 11 端点；W3-M02 增 /api/friend
// 路由组 6 端点）。
//
// 定位：handler 薄封装层——bind（DTO 由 service 导出）→service→
// c.JSON(HTTPStatus(err), OK/Fail)，零错误拼串、零业务逻辑；
// JWT 中间件 JWTAuth 供 /me 及后续受保护路由复用（Week 2 WS 鉴权
// 复用的是 pkg/common/jwt.Manager 本体，不经过 gin 中间件）。
package router

import (
	stderrors "errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"

	"chatchat/internal/service/friend"
	"chatchat/internal/service/group"
	"chatchat/internal/service/message"
	"chatchat/internal/service/user"
	"chatchat/pkg/common"
	"chatchat/pkg/common/jwt"
	pkgerrors "chatchat/pkg/errors"

	"github.com/gin-gonic/gin"
	"go.uber.org/zap"
)

// Deps 路由层依赖集合（全部由装配层 main.go 注入）。
type Deps struct {
	User    *user.Service
	Message *message.PullService // 历史消息拉取（W2-M05；nil 时路由不注册）
	Group   *group.Service       // 群组（W3-M01；nil 时路由不注册）
	Friend  *friend.Service      // 好友（W3-M02；nil 时路由不注册）
	JWT     *jwt.Manager
	Logger  *zap.Logger
	Mode    string // debug|release|test（config 校验已保证，default 兜底 release）
}

// New 构造 gin engine：SetMode（Mode 直接映射，非 debug/test 一律 release）→
// gin.New（零内置中间件）→自定义 recovery →免鉴权 /healthz →注册四路由
// （/api/user 组）+ 历史消息路由（Message 注入时）+ 群组路由（Group 注入时）。
func New(d Deps) *gin.Engine {
	switch d.Mode {
	case gin.DebugMode:
		gin.SetMode(gin.DebugMode)
	case gin.TestMode:
		gin.SetMode(gin.TestMode)
	default:
		gin.SetMode(gin.ReleaseMode)
	}

	engine := gin.New()
	engine.Use(recovery(d.Logger))

	// 健康探针（W2-M06 CAP-005）：免鉴权，200 "ok"，不泄漏内部信息
	// （readiness/liveness 分级归 W4 可观测性）。
	engine.GET("/healthz", func(c *gin.Context) {
		c.String(http.StatusOK, "ok")
	})

	g := engine.Group("/api/user")
	g.POST("/register", handleRegister(d))
	g.POST("/login", handleLogin(d))
	g.POST("/refresh", handleRefresh(d))
	g.GET("/me", JWTAuth(d.JWT), handleMe(d))

	// 历史消息分页（W2-M05）：与 WS PullRequest 同义（afterSeq 增量、ASC、
	// limit 归一、maxSeq/hasMore），JWT 保护；Message 未注入（旧装配/桩）不注册。
	// W3-M06 增会话发现清单 GET /inbox（离线消息与漫游，同组挂载）。
	if d.Message != nil {
		m := engine.Group("/api/message")
		m.GET("/history", JWTAuth(d.JWT), handleHistory(d))
		m.GET("/inbox", JWTAuth(d.JWT), handleInbox(d))
	}

	// 群组路由组（W3-M01 plan §6.3 冻结，11 端点全部 JWTAuth 保护；
	// Group 未注入（旧装配/桩）不注册——Message 先例）。
	if d.Group != nil {
		grp := engine.Group("/api/group")
		grp.Use(JWTAuth(d.JWT))
		grp.POST("", handleCreateGroup(d))
		grp.GET("/my", handleMyGroups(d))
		grp.GET("/:id", handleGetGroup(d))
		grp.GET("/:id/members", handleListGroupMembers(d))
		grp.POST("/:id/join", handleJoinGroup(d))
		grp.POST("/:id/quit", handleQuitGroup(d))
		grp.DELETE("/:id/member/:uid", handleKickGroupMember(d))
		grp.POST("/:id/mute", handleMuteGroupMember(d))
		grp.POST("/:id/admin", handleSetGroupAdmin(d))
		grp.POST("/:id/transfer", handleTransferGroupOwner(d))
		grp.PUT("/:id/announcement", handleSetGroupAnnouncement(d))
	}

	// 好友路由组（W3-M02 plan §6.2 冻结，6 端点全部 JWTAuth 保护；
	// Friend 未注入（旧装配/桩）不注册——Group 先例）。
	if d.Friend != nil {
		fr := engine.Group("/api/friend")
		fr.Use(JWTAuth(d.JWT))
		fr.POST("/apply", handleFriendApply(d))
		fr.POST("/approve", handleFriendApprove(d))
		fr.POST("/reject", handleFriendReject(d))
		fr.DELETE("/:uid", handleFriendRemove(d))
		fr.GET("/list", handleFriendList(d))
		fr.GET("/requests", handleFriendRequests(d))
	}
	return engine
}

// recovery 自定义恢复中间件：panic→结构化日志（含 path 与 panic 值）+
// 500 M03 包装（Fail 消毒，不透出堆栈）；AbortWithStatusJSON 防后续 handler 继续。
func recovery(logger *zap.Logger) gin.HandlerFunc {
	return func(c *gin.Context) {
		defer func() {
			if r := recover(); r != nil {
				logger.Error("panic recovered",
					zap.Any("panic", r),
					zap.String("path", c.Request.URL.Path))
				c.AbortWithStatusJSON(http.StatusInternalServerError, common.Fail(pkgerrors.ErrInternal))
			}
		}()
		c.Next()
	}
}

// bearerPrefix Authorization 前缀（含尾空格，大小写不敏感比对）。
const bearerPrefix = "Bearer "

// JWTAuth Bearer 提取→Parse(access)→c.Set("uid")：缺失/非 Bearer/解析失败
// 一律 Abort 401（2004/2005 经 common.Fail 包装，与 M03 对齐）。
// 提取规则与 gotify 一致：前缀 EqualFold 大小写不敏感，取第 8 字符起为 token。
func JWTAuth(jm *jwt.Manager) gin.HandlerFunc {
	return func(c *gin.Context) {
		h := c.GetHeader("Authorization")
		if len(h) <= len(bearerPrefix) || !strings.EqualFold(h[:len(bearerPrefix)], bearerPrefix) {
			c.AbortWithStatusJSON(http.StatusUnauthorized,
				common.Fail(pkgerrors.Wrap(stderrors.New("missing or malformed Authorization header"),
					pkgerrors.ErrTokenInvalid.Code, pkgerrors.ErrTokenInvalid.Message)))
			return
		}
		token := strings.TrimSpace(h[len(bearerPrefix):])
		claims, err := jm.Parse(token, jwt.TypeAccess)
		if err != nil {
			c.AbortWithStatusJSON(http.StatusUnauthorized, common.Fail(err))
			return
		}
		c.Set("uid", claims.UID)
		c.Next()
	}
}

// handleRegister POST /api/user/register。
func handleRegister(d Deps) gin.HandlerFunc {
	return func(c *gin.Context) {
		var req user.RegisterRequest
		if err := c.ShouldBindJSON(&req); err != nil {
			bindFail(c, d.Logger, "register", err)
			return
		}
		res, err := d.User.Register(c.Request.Context(), &req)
		if err != nil {
			serviceFail(c, err)
			return
		}
		c.JSON(http.StatusOK, common.OK(res))
	}
}

// handleLogin POST /api/user/login。
func handleLogin(d Deps) gin.HandlerFunc {
	return func(c *gin.Context) {
		var req user.LoginRequest
		if err := c.ShouldBindJSON(&req); err != nil {
			bindFail(c, d.Logger, "login", err)
			return
		}
		pair, err := d.User.Login(c.Request.Context(), &req)
		if err != nil {
			serviceFail(c, err)
			return
		}
		c.JSON(http.StatusOK, common.OK(pair))
	}
}

// handleRefresh POST /api/user/refresh。
func handleRefresh(d Deps) gin.HandlerFunc {
	return func(c *gin.Context) {
		var req user.RefreshRequest
		if err := c.ShouldBindJSON(&req); err != nil {
			bindFail(c, d.Logger, "refresh", err)
			return
		}
		res, err := d.User.Refresh(c.Request.Context(), req.RefreshToken)
		if err != nil {
			serviceFail(c, err)
			return
		}
		c.JSON(http.StatusOK, common.OK(res))
	}
}

// handleMe GET /api/user/me（JWTAuth 已注入 uid）。
func handleMe(d Deps) gin.HandlerFunc {
	return func(c *gin.Context) {
		v, exists := c.Get("uid")
		uid, ok := v.(int64)
		if !exists || !ok {
			c.AbortWithStatusJSON(http.StatusUnauthorized,
				common.Fail(pkgerrors.Wrap(stderrors.New("uid missing in context"),
					pkgerrors.ErrTokenInvalid.Code, pkgerrors.ErrTokenInvalid.Message)))
			return
		}
		p, err := d.User.GetProfile(c.Request.Context(), uint64(uid))
		if err != nil {
			serviceFail(c, err)
			return
		}
		c.JSON(http.StatusOK, common.OK(p))
	}
}

// handleHistory GET /api/message/history（W2-M05，JWTAuth 已注入 uid）。
// query：conversationId 必填（1001）、afterSeq 默认 0（负值 1001——语义
// 不同不归一）、limit 默认 50（负值放行，由 service.query normalizeLimit
// 归一——与 WS 拉取统一，M05 P3-1/M06 裁决 D4）。响应 data 镜像
// PullResponse 语义。
func handleHistory(d Deps) gin.HandlerFunc {
	return func(c *gin.Context) {
		v, exists := c.Get("uid")
		uid, ok := v.(int64)
		if !exists || !ok {
			c.AbortWithStatusJSON(http.StatusUnauthorized,
				common.Fail(pkgerrors.Wrap(stderrors.New("uid missing in context"),
					pkgerrors.ErrTokenInvalid.Code, pkgerrors.ErrTokenInvalid.Message)))
			return
		}
		conversationID := c.Query("conversationId")
		if conversationID == "" {
			bindFail(c, d.Logger, "history",
				stderrors.New("conversationId required"))
			return
		}
		afterSeq := int64(0)
		if raw := c.Query("afterSeq"); raw != "" {
			n, err := strconv.ParseInt(raw, 10, 64)
			if err != nil || n < 0 {
				bindFail(c, d.Logger, "history",
					stderrors.New("afterSeq must be a non-negative integer"))
				return
			}
			afterSeq = n
		}
		// limit 负值不在此拒绝：负值/超上限统一由 service.query 归一 50
		// （normalizeLimit，与 WS PullRequest 同义）；仅拒绝非整数。
		limit := int64(0)
		if raw := c.Query("limit"); raw != "" {
			n, err := strconv.ParseInt(raw, 10, 32)
			if err != nil {
				bindFail(c, d.Logger, "history",
					stderrors.New("limit must be an integer"))
				return
			}
			limit = n
		}
		res, err := d.Message.History(c.Request.Context(), uint64(uid),
			conversationID, uint64(afterSeq), int32(limit))
		if err != nil {
			serviceFail(c, err)
			return
		}
		c.JSON(http.StatusOK, common.OK(res))
	}
}

// handleInbox GET /api/message/inbox（W3-M06，JWTAuth 已注入 uid）：无参数；
// 返回未同步会话发现清单 [{conversationId,lastSeq}]（漫游窗口内信箱聚合 ∩
// synced 差集，plan D2/D3/D4）。uid 取鉴权——清单不含消息内容（零敏感面）；
// Inbox 未注入（误接）由 service 9001 拒用，serviceFail 映射 500。
func handleInbox(d Deps) gin.HandlerFunc {
	return func(c *gin.Context) {
		uid, ok := requireUID(c)
		if !ok {
			return
		}
		items, err := d.Message.InboxSummary(c.Request.Context(), uid)
		if err != nil {
			serviceFail(c, err)
			return
		}
		c.JSON(http.StatusOK, common.OK(items))
	}
}

// bindFail 绑定失败：400 + 1001（冻结文案），绑定错误细节仅入日志（裁决 9）。
func bindFail(c *gin.Context, logger *zap.Logger, route string, err error) {
	logger.Warn("bind request failed", zap.String("route", route), zap.Error(err))
	c.JSON(http.StatusBadRequest,
		common.Fail(pkgerrors.Wrap(err, pkgerrors.ErrInvalidParam.Code, pkgerrors.ErrInvalidParam.Message)))
}

// ---- W3-M01：/api/group 路由组 11 端点（plan §6.3 冻结，全 JWTAuth）----

// requireUID 提取 JWTAuth 注入的 uid（int64 → uint64）；缺失/类型不符
// 401 应答并返回 false（与 handleMe 同语义，11 端点共用收口）。
func requireUID(c *gin.Context) (uint64, bool) {
	v, exists := c.Get("uid")
	uid, ok := v.(int64)
	if !exists || !ok {
		c.AbortWithStatusJSON(http.StatusUnauthorized,
			common.Fail(pkgerrors.Wrap(stderrors.New("uid missing in context"),
				pkgerrors.ErrTokenInvalid.Code, pkgerrors.ErrTokenInvalid.Message)))
		return 0, false
	}
	return uint64(uid), true
}

// parseUintParam 解析 :id/:uid 路由参数（plan §6.3：ParseUint 失败→1001）。
// 仅拒绝解析失败（负号/非数字/溢出），"0" 语法合法放行至 service 按未命中
// 哨兵处理（4001/4006）。原始值仅入日志（bindFail 既有路径）。
func parseUintParam(c *gin.Context, logger *zap.Logger, route, name string) (uint64, bool) {
	n, err := strconv.ParseUint(c.Param(name), 10, 64)
	if err != nil {
		bindFail(c, logger, route, fmt.Errorf("path param %s = %q is not a valid unsigned integer", name, c.Param(name)))
		return 0, false
	}
	return n, true
}

// handleCreateGroup POST /api/group（建群 {name}）。
func handleCreateGroup(d Deps) gin.HandlerFunc {
	return func(c *gin.Context) {
		uid, ok := requireUID(c)
		if !ok {
			return
		}
		var req group.CreateGroupRequest
		if err := c.ShouldBindJSON(&req); err != nil {
			bindFail(c, d.Logger, "group_create", err)
			return
		}
		dto, err := d.Group.Create(c.Request.Context(), uid, &req)
		if err != nil {
			serviceFail(c, err)
			return
		}
		c.JSON(http.StatusOK, common.OK(dto))
	}
}

// handleMyGroups GET /api/group/my（我的群列表）。
func handleMyGroups(d Deps) gin.HandlerFunc {
	return func(c *gin.Context) {
		uid, ok := requireUID(c)
		if !ok {
			return
		}
		dtos, err := d.Group.ListMyGroups(c.Request.Context(), uid)
		if err != nil {
			serviceFail(c, err)
			return
		}
		c.JSON(http.StatusOK, common.OK(dtos))
	}
}

// handleGetGroup GET /api/group/:id（群信息，任何认证用户，含成员数）。
func handleGetGroup(d Deps) gin.HandlerFunc {
	return func(c *gin.Context) {
		uid, ok := requireUID(c)
		if !ok {
			return
		}
		id, ok := parseUintParam(c, d.Logger, "group_get", "id")
		if !ok {
			return
		}
		dto, err := d.Group.GetGroup(c.Request.Context(), uid, id)
		if err != nil {
			serviceFail(c, err)
			return
		}
		c.JSON(http.StatusOK, common.OK(dto))
	}
}

// handleListGroupMembers GET /api/group/:id/members（成员列表，仅群成员）。
func handleListGroupMembers(d Deps) gin.HandlerFunc {
	return func(c *gin.Context) {
		uid, ok := requireUID(c)
		if !ok {
			return
		}
		id, ok := parseUintParam(c, d.Logger, "group_members", "id")
		if !ok {
			return
		}
		dtos, err := d.Group.ListMembers(c.Request.Context(), uid, id)
		if err != nil {
			serviceFail(c, err)
			return
		}
		c.JSON(http.StatusOK, common.OK(dtos))
	}
}

// handleJoinGroup POST /api/group/:id/join（入群，500 上限事务密闭）。
func handleJoinGroup(d Deps) gin.HandlerFunc {
	return func(c *gin.Context) {
		uid, ok := requireUID(c)
		if !ok {
			return
		}
		id, ok := parseUintParam(c, d.Logger, "group_join", "id")
		if !ok {
			return
		}
		if err := d.Group.Join(c.Request.Context(), uid, id); err != nil {
			serviceFail(c, err)
			return
		}
		c.JSON(http.StatusOK, common.OK(nil))
	}
}

// handleQuitGroup POST /api/group/:id/quit（退群，群主不可退）。
func handleQuitGroup(d Deps) gin.HandlerFunc {
	return func(c *gin.Context) {
		uid, ok := requireUID(c)
		if !ok {
			return
		}
		id, ok := parseUintParam(c, d.Logger, "group_quit", "id")
		if !ok {
			return
		}
		if err := d.Group.Quit(c.Request.Context(), uid, id); err != nil {
			serviceFail(c, err)
			return
		}
		c.JSON(http.StatusOK, common.OK(nil))
	}
}

// handleKickGroupMember DELETE /api/group/:id/member/:uid（踢人）。
func handleKickGroupMember(d Deps) gin.HandlerFunc {
	return func(c *gin.Context) {
		uid, ok := requireUID(c)
		if !ok {
			return
		}
		id, ok := parseUintParam(c, d.Logger, "group_kick", "id")
		if !ok {
			return
		}
		target, ok := parseUintParam(c, d.Logger, "group_kick", "uid")
		if !ok {
			return
		}
		if err := d.Group.Kick(c.Request.Context(), uid, id, target); err != nil {
			serviceFail(c, err)
			return
		}
		c.JSON(http.StatusOK, common.OK(nil))
	}
}

// handleMuteGroupMember POST /api/group/:id/mute（禁言/解禁 {uid, durationSec}）。
func handleMuteGroupMember(d Deps) gin.HandlerFunc {
	return func(c *gin.Context) {
		uid, ok := requireUID(c)
		if !ok {
			return
		}
		id, ok := parseUintParam(c, d.Logger, "group_mute", "id")
		if !ok {
			return
		}
		var req group.MuteRequest
		if err := c.ShouldBindJSON(&req); err != nil {
			bindFail(c, d.Logger, "group_mute", err)
			return
		}
		if err := d.Group.Mute(c.Request.Context(), uid, id, &req); err != nil {
			serviceFail(c, err)
			return
		}
		c.JSON(http.StatusOK, common.OK(nil))
	}
}

// handleSetGroupAdmin POST /api/group/:id/admin（设/撤管理员 {uid, isAdmin}）。
func handleSetGroupAdmin(d Deps) gin.HandlerFunc {
	return func(c *gin.Context) {
		uid, ok := requireUID(c)
		if !ok {
			return
		}
		id, ok := parseUintParam(c, d.Logger, "group_admin", "id")
		if !ok {
			return
		}
		var req group.SetAdminRequest
		if err := c.ShouldBindJSON(&req); err != nil {
			bindFail(c, d.Logger, "group_admin", err)
			return
		}
		if err := d.Group.SetAdmin(c.Request.Context(), uid, id, req.UID, req.IsAdmin); err != nil {
			serviceFail(c, err)
			return
		}
		c.JSON(http.StatusOK, common.OK(nil))
	}
}

// handleTransferGroupOwner POST /api/group/:id/transfer（转让群主 {uid}）。
func handleTransferGroupOwner(d Deps) gin.HandlerFunc {
	return func(c *gin.Context) {
		uid, ok := requireUID(c)
		if !ok {
			return
		}
		id, ok := parseUintParam(c, d.Logger, "group_transfer", "id")
		if !ok {
			return
		}
		var req group.TransferRequest
		if err := c.ShouldBindJSON(&req); err != nil {
			bindFail(c, d.Logger, "group_transfer", err)
			return
		}
		if err := d.Group.Transfer(c.Request.Context(), uid, id, req.UID); err != nil {
			serviceFail(c, err)
			return
		}
		c.JSON(http.StatusOK, common.OK(nil))
	}
}

// handleSetGroupAnnouncement PUT /api/group/:id/announcement（群公告
// {content}，空串=清空）。
func handleSetGroupAnnouncement(d Deps) gin.HandlerFunc {
	return func(c *gin.Context) {
		uid, ok := requireUID(c)
		if !ok {
			return
		}
		id, ok := parseUintParam(c, d.Logger, "group_announcement", "id")
		if !ok {
			return
		}
		var req group.AnnouncementRequest
		if err := c.ShouldBindJSON(&req); err != nil {
			bindFail(c, d.Logger, "group_announcement", err)
			return
		}
		if err := d.Group.SetAnnouncement(c.Request.Context(), uid, id, req.Content); err != nil {
			serviceFail(c, err)
			return
		}
		c.JSON(http.StatusOK, common.OK(nil))
	}
}

// serviceFail service 错误→HTTP 状态码映射 + M03 包装；非 *pkgerrors.Error
// 兜底 500（Fail 归一 9001）。
func serviceFail(c *gin.Context, err error) {
	var e *pkgerrors.Error
	if stderrors.As(err, &e) {
		c.JSON(pkgerrors.HTTPStatus(e.Code), common.Fail(err))
		return
	}
	c.JSON(http.StatusInternalServerError, common.Fail(err))
}

// ---- W3-M02：/api/friend 路由组 6 端点（plan §6.2 冻结，全 JWTAuth）----

// handleFriendApply POST /api/friend/apply（申请 {toUid}）。
func handleFriendApply(d Deps) gin.HandlerFunc {
	return func(c *gin.Context) {
		uid, ok := requireUID(c)
		if !ok {
			return
		}
		var req friend.ApplyRequest
		if err := c.ShouldBindJSON(&req); err != nil {
			bindFail(c, d.Logger, "friend_apply", err)
			return
		}
		if err := d.Friend.Apply(c.Request.Context(), uid, &req); err != nil {
			serviceFail(c, err)
			return
		}
		c.JSON(http.StatusOK, common.OK(nil))
	}
}

// handleFriendApprove POST /api/friend/approve（同意 {uid=申请人}）。
func handleFriendApprove(d Deps) gin.HandlerFunc {
	return func(c *gin.Context) {
		uid, ok := requireUID(c)
		if !ok {
			return
		}
		var req friend.ApproveRequest
		if err := c.ShouldBindJSON(&req); err != nil {
			bindFail(c, d.Logger, "friend_approve", err)
			return
		}
		if err := d.Friend.Approve(c.Request.Context(), uid, req.UID); err != nil {
			serviceFail(c, err)
			return
		}
		c.JSON(http.StatusOK, common.OK(nil))
	}
}

// handleFriendReject POST /api/friend/reject（拒绝 {uid=申请人}）。
func handleFriendReject(d Deps) gin.HandlerFunc {
	return func(c *gin.Context) {
		uid, ok := requireUID(c)
		if !ok {
			return
		}
		var req friend.RejectRequest
		if err := c.ShouldBindJSON(&req); err != nil {
			bindFail(c, d.Logger, "friend_reject", err)
			return
		}
		if err := d.Friend.Reject(c.Request.Context(), uid, req.UID); err != nil {
			serviceFail(c, err)
			return
		}
		c.JSON(http.StatusOK, common.OK(nil))
	}
}

// handleFriendRemove DELETE /api/friend/:uid（删除好友）。
func handleFriendRemove(d Deps) gin.HandlerFunc {
	return func(c *gin.Context) {
		uid, ok := requireUID(c)
		if !ok {
			return
		}
		friendUID, ok := parseUintParam(c, d.Logger, "friend_remove", "uid")
		if !ok {
			return
		}
		if err := d.Friend.Remove(c.Request.Context(), uid, friendUID); err != nil {
			serviceFail(c, err)
			return
		}
		c.JSON(http.StatusOK, common.OK(nil))
	}
}

// handleFriendList GET /api/friend/list（好友列表，仅本人视角）。
func handleFriendList(d Deps) gin.HandlerFunc {
	return func(c *gin.Context) {
		uid, ok := requireUID(c)
		if !ok {
			return
		}
		dtos, err := d.Friend.ListFriends(c.Request.Context(), uid)
		if err != nil {
			serviceFail(c, err)
			return
		}
		c.JSON(http.StatusOK, common.OK(dtos))
	}
}

// handleFriendRequests GET /api/friend/requests（收到的待处理申请）。
func handleFriendRequests(d Deps) gin.HandlerFunc {
	return func(c *gin.Context) {
		uid, ok := requireUID(c)
		if !ok {
			return
		}
		dtos, err := d.Friend.ListRequests(c.Request.Context(), uid)
		if err != nil {
			serviceFail(c, err)
			return
		}
		c.JSON(http.StatusOK, common.OK(dtos))
	}
}
