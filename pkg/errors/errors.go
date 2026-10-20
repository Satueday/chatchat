// Package errors 提供统一业务错误类型、冻结码值哨兵与业务码→HTTP 状态映射。
//
// 定位：零业务依赖纯函数包——仅依赖标准库（fmt 与 net/http 状态常量），
// 不 import gin/gorm/viper/zap/validator，可被 DAO/service/router/WS 各层复用。
//
// 响应面与日志面分离：Error() 字符串含 cause 细节，仅供日志侧使用；
// 对外响应一律经 pkg/common.Fail 消毒归一，cause 内容（SQL/文件路径/堆栈）不透出。
//
// 业务码→HTTP 映射为全局唯一一处（W1-M00 §6.3）：0→200、1001→400、
// 2001→409、2002→404、2003/2004/2005→401、3001→400、3002/3004→409、
// 3003→404、3005→403、4001/4006→404、4002/4003/4007/4008→403、
// 4004/4005→409、5001→400、5002/5003→500、5004→400、5005→404、
// 5006→400、5007/5008→500、5009/5010→403、6001/6002/6003→500（防御性注册，
// W4-M01 热点段无 REST 消费面）、7101/7102→500（防御性注册，W4-M04 信令段
// 仅 WS CmdError 消费面）、9001→500；未知码兜底 500。
//
// 哨兵为包初始化期构造的 *Error 指针，初始化后不可变（调用方不得改写其
// 指向或字段）；码值一经冻结不得挪用（3xxx 好友段自 W3-M02 起启用，
// 6xxx 热点段自 W4-M01 起启用 6001~6003，7xxx 信令段自 W4-M04 起启用
// 7101~7102（段位 71xx 预留）、5xxx 消息段自 W2-M03 起启用、4xxx 群组段
// 自 W3-M01 起启用）。
package errors

import (
	"fmt"
	"net/http"
)

// Error 为统一业务错误类型：Code 为冻结业务码，Message 为对外冻结文案，
// cause 为被包装的底层错误（未导出，经 Unwrap 暴露，仅日志面使用）。
type Error struct {
	Code    int32
	Message string
	cause   error
}

// New 创建无 cause 的业务错误。
func New(code int32, message string) *Error {
	return &Error{Code: code, Message: message}
}

// Wrap 包装底层错误；err 为 nil 时 cause 置 nil（等价 New）。
func Wrap(err error, code int32, message string) *Error {
	return &Error{Code: code, Message: message, cause: err}
}

// Error 实现 error 接口：有 cause 时为 "message: cause"，否则为 message。
// 结果可能含 cause 敏感细节，仅用于日志，不得直接写入响应体。
func (e *Error) Error() string {
	if e.cause != nil {
		return fmt.Sprintf("%s: %v", e.Message, e.cause)
	}
	return e.Message
}

// Unwrap 返回底层 cause（可为 nil），标准库 errors.Is/As 沿链穿透。
func (e *Error) Unwrap() error { return e.cause }

// Is 判定 target：target 为 *Error 时按 Code 相等判定，否则 false。
// （本包不 import 标准库 errors，避免与包名遮蔽纠缠。）
func (e *Error) Is(target error) bool {
	t, ok := target.(*Error)
	return ok && t.Code == e.Code
}

// 31 个冻结码值哨兵：命名与文案逐字对齐 W1-M00 §6.3（8 码）、W2-M03/M04/M05
// 增补（5xxx 消息段）、W3-M01 增补（4xxx 群组段 8 码）、W3-M02 增补
// （3xxx 好友段 5 码）与 W3-M04 增补（5xxx 顺延 5010），
// message 随常量定义，调用点不得散落拼串。
var (
	// OK 成功（code 0）。
	OK = New(0, "ok")
	// ErrInvalidParam 参数错误（绑定/校验失败含手机号格式非法、请求体解析失败）。
	ErrInvalidParam = New(1001, "参数错误")
	// ErrPhoneExists 手机号已存在（注册唯一冲突，含 DB 唯一约束映射）。
	ErrPhoneExists = New(2001, "手机号已存在")
	// ErrUserNotFound 用户不存在（按 ID 查询不存在；登录路径禁用，防枚举）。
	ErrUserNotFound = New(2002, "用户不存在")
	// ErrPasswordMismatch 账号或密码错误（登录失败统一文案，不区分账号不存在/密码错误）。
	ErrPasswordMismatch = New(2003, "账号或密码错误")
	// ErrTokenInvalid token 无效（签名错误、typ 混用、格式非法、未携带 token）。
	ErrTokenInvalid = New(2004, "token 无效")
	// ErrTokenExpired token 过期（exp 已过，与 2004 可区分）。
	ErrTokenExpired = New(2005, "token 过期")
	// ErrMessageDecode 消息解码失败（WS 帧 payload 二次 Unmarshal 失败，W2-M03）。
	ErrMessageDecode = New(5001, "消息解码失败")
	// ErrSeqGenerate 会话序号生成失败（Redis INCR 错误，W2-M03）。
	ErrSeqGenerate = New(5002, "序号生成失败")
	// ErrMessageSave 消息保存失败（落库失败或 msg_id/(conversation_id,msg_seq) 唯一冲突，W2-M03）。
	ErrMessageSave = New(5003, "消息保存失败")
	// ErrMessageSelfSend 不能给自己发消息（to == from，W2-M03）。
	ErrMessageSelfSend = New(5004, "不能给自己发消息")
	// ErrReceiverNotFound 接收者不存在（单聊目标用户缺失，W2-M03）。
	ErrReceiverNotFound = New(5005, "接收者不存在")
	// ErrContentTooLarge 消息内容超长（超过 pkg/global.MessageContentMaxBytes，W2-M03）。
	ErrContentTooLarge = New(5006, "消息内容过长")
	// ErrCursorUpdate 信箱游标更新失败（inbox_cursor AdvanceRecv/AdvanceSynced DB 错误，W2-M04）。
	ErrCursorUpdate = New(5007, "信箱游标更新失败")
	// ErrMessageQuery 消息查询失败（message 表 AfterSeq/MaxSeq DB 错误，W2-M05）。
	ErrMessageQuery = New(5008, "消息查询失败")
	// ErrNotConvMember 非会话成员（拉取/历史读取路径 uid 不属于会话，W2-M05；
	// 写路径（ACK）按参数校验 1001 不动——M04 冻结口径区分）。
	ErrNotConvMember = New(5009, "非会话成员")
	// ErrMemberMuted 已被禁言，无法发言（群聊发送链路 muted_until > now(UTC)，
	// W3-M04；HTTP 403——操作被权限拒绝而非参数错误）。
	ErrMemberMuted = New(5010, "已被禁言，无法发言")
	// ErrGroupNotFound 群不存在（group_info 未命中，W3-M01）。
	ErrGroupNotFound = New(4001, "群不存在")
	// ErrNotGroupMember 非群成员（group_member 未命中，操作者视角，W3-M01）。
	ErrNotGroupMember = New(4002, "非群成员")
	// ErrGroupPermDenied 群组权限不足（操作者角色不满足 D6 矩阵要求，W3-M01）。
	ErrGroupPermDenied = New(4003, "群组权限不足")
	// ErrGroupFull 群成员已满（成员数 ≥ max_member，W3-M01）。
	ErrGroupFull = New(4004, "群成员已满")
	// ErrAlreadyInGroup 已在群内（uk_group_user 冲突，W3-M01）。
	ErrAlreadyInGroup = New(4005, "已在群内")
	// ErrTargetNotMember 目标用户不是群成员（目标视角未命中，W3-M01）。
	ErrTargetNotMember = New(4006, "目标用户不是群成员")
	// ErrMemberOpDenied 不能对该成员执行此操作（踢/禁/转让目标为群主、
	// 自己或越级管理员，W3-M01）。
	ErrMemberOpDenied = New(4007, "不能对该成员执行此操作")
	// ErrOwnerQuitDenied 群主不能退群（须先显式转让群主，W3-M01）。
	ErrOwnerQuitDenied = New(4008, "群主不能退群")
	// ErrFriendApplySelf 不能添加自己为好友（apply 目标=本人，W3-M02）。
	ErrFriendApplySelf = New(3001, "不能添加自己为好友")
	// ErrFriendApplyExists 好友申请已存在（同方向重复申请、互逆 pending 或 uk 冲突，W3-M02）。
	ErrFriendApplyExists = New(3002, "好友申请已存在")
	// ErrFriendRequestNotFound 好友申请不存在或已处理（同意/拒绝定位 (申请人,本人) 行未命中，W3-M02）。
	ErrFriendRequestNotFound = New(3003, "好友申请不存在或已处理")
	// ErrAlreadyFriends 已经是好友（向好友再次发申请，W3-M02）。
	ErrAlreadyFriends = New(3004, "已经是好友")
	// ErrNotFriend 非好友关系（单聊发送门槛拒绝、删除好友目标非好友，W3-M02）。
	ErrNotFriend = New(3005, "非好友关系")
	// ErrHotStatQuery 热点统计查询失败（聚合读窗口/候选失败，日志面，W4-M01）。
	ErrHotStatQuery = New(6001, "热点统计查询失败")
	// ErrHotMarkWrite 热点标记写入失败（打标/摘标双落任一步失败，日志面，W4-M01）。
	ErrHotMarkWrite = New(6002, "热点标记写入失败")
	// ErrHotQueryFailed 热点状态查询失败（IsHot Redis/MySQL 双源均败；上抛供
	// M02 路由降级判定，M01 不替 M02 决定降级行为，W4-M01）。
	ErrHotQueryFailed = New(6003, "热点状态查询失败")
	// ErrSignalPeerOffline 对端不在线（信令不可达：邀请/转发时对端离线或
	// 投递失败，W4-M04，ADR-0011 D5——补齐 go-chat 静默丢弃缺陷的显式回执）。
	ErrSignalPeerOffline = New(7101, "对端不在线")
	// ErrSignalSession 信令会话无效（callId 不存在/已结束/重复邀请/非参与
	// 者——含挂断后迟到帧，W4-M04，ADR-0011 D2/D5）。
	ErrSignalSession = New(7102, "信令会话无效")
	// ErrInternal 内部错误（未分类错误归一，不泄漏堆栈/SQL/文件路径）。
	ErrInternal = New(9001, "内部错误")
)

// httpStatus 为业务码→HTTP 状态映射（全局唯一一处，W1-M00 §6.3；
// map 字面量语法天然保证键不重复）。
var httpStatus = map[int32]int{
	0:    http.StatusOK,
	1001: http.StatusBadRequest,
	2001: http.StatusConflict,
	2002: http.StatusNotFound,
	2003: http.StatusUnauthorized,
	2004: http.StatusUnauthorized,
	2005: http.StatusUnauthorized,
	5001: http.StatusBadRequest,
	5002: http.StatusInternalServerError,
	5003: http.StatusInternalServerError,
	5004: http.StatusBadRequest,
	5005: http.StatusNotFound,
	5006: http.StatusBadRequest,
	5007: http.StatusInternalServerError,
	5008: http.StatusInternalServerError,
	5009: http.StatusForbidden,
	5010: http.StatusForbidden,
	4001: http.StatusNotFound,
	4002: http.StatusForbidden,
	4003: http.StatusForbidden,
	4004: http.StatusConflict,
	4005: http.StatusConflict,
	4006: http.StatusNotFound,
	4007: http.StatusForbidden,
	4008: http.StatusForbidden,
	3001: http.StatusBadRequest,
	3002: http.StatusConflict,
	3003: http.StatusNotFound,
	3004: http.StatusConflict,
	3005: http.StatusForbidden,
	// 6xxx 热点段（W4-M01）：防御性注册 500——M01 无 REST 消费面，哨兵仅
	// 日志面与 M02 降级判定消费；注册保证未来暴露 HTTP 时映射显式可控。
	6001: http.StatusInternalServerError,
	6002: http.StatusInternalServerError,
	6003: http.StatusInternalServerError,
	// 7xxx 信令段（W4-M04，ADR-0011 D5）：防御性注册 500——信令码仅 WS
	// CmdError 帧消费面（6xxx 先例），不经 HTTP；注册保证未来暴露 REST 时
	// 映射显式可控。
	7101: http.StatusInternalServerError,
	7102: http.StatusInternalServerError,
	9001: http.StatusInternalServerError,
}

// HTTPStatus 返回业务码对应的 HTTP 状态码；未知/未注册码兜底 500。
func HTTPStatus(code int32) int {
	if s, ok := httpStatus[code]; ok {
		return s
	}
	return http.StatusInternalServerError
}
