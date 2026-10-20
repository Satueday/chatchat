// Package common 提供统一响应结构与 OK/Fail 构造 helper（W1-M00 §6.5）。
//
// 定位：纯函数包、零 gin 依赖——只构造 Response 结构体，不写 HTTP 响应；
// router 层（W1-M06）以一行组合写出：
//
//	c.JSON(pkgerrors.HTTPStatus(err), common.Fail(err))
//
// Fail 为唯一消毒出口：仅当 err 可提取 *chatchat/pkg/errors.Error 且其 Code
// 属于 8 个冻结码白名单时透传 {code, message, nil}；否则（err 为 nil、非
// *Error、白名单外自造码）一律归一 {9001, "内部错误", nil}——cause 内容
// （SQL/文件路径/堆栈）不透出，响应面与日志面分离。
package common

import (
	stderrors "errors"

	pkgerrors "chatchat/pkg/errors"
)

// Response 为统一响应结构；data 无 omitempty，无内容序列化为 "data":null 不省略。
type Response struct {
	Code    int32  `json:"code"`
	Message string `json:"message"`
	Data    any    `json:"data"`
}

// knownCodes 为冻结码白名单（由哨兵派生，防调用方自造未知码绕过冻结表；
// W1-M03 建 8 码，W2-M03 扩入 5xxx 消息段 6 码，W2-M04 扩入 5007，
// W2-M05 扩入 5008/5009，W3-M01 扩入 4xxx 群组段 8 码，W3-M02 扩入
// 3xxx 好友段 5 码，W3-M04 扩入 5010，W4-M01 扩入 6xxx 热点段 3 码，
// W4-M04 扩入 7xxx 信令段 2 码）。
var knownCodes = map[int32]struct{}{
	pkgerrors.OK.Code:                       {},
	pkgerrors.ErrInvalidParam.Code:          {},
	pkgerrors.ErrPhoneExists.Code:           {},
	pkgerrors.ErrUserNotFound.Code:          {},
	pkgerrors.ErrPasswordMismatch.Code:      {},
	pkgerrors.ErrTokenInvalid.Code:          {},
	pkgerrors.ErrTokenExpired.Code:          {},
	pkgerrors.ErrMessageDecode.Code:         {},
	pkgerrors.ErrSeqGenerate.Code:           {},
	pkgerrors.ErrMessageSave.Code:           {},
	pkgerrors.ErrMessageSelfSend.Code:       {},
	pkgerrors.ErrReceiverNotFound.Code:      {},
	pkgerrors.ErrContentTooLarge.Code:       {},
	pkgerrors.ErrCursorUpdate.Code:          {},
	pkgerrors.ErrMessageQuery.Code:          {},
	pkgerrors.ErrNotConvMember.Code:         {},
	pkgerrors.ErrMemberMuted.Code:           {},
	pkgerrors.ErrGroupNotFound.Code:         {},
	pkgerrors.ErrNotGroupMember.Code:        {},
	pkgerrors.ErrGroupPermDenied.Code:       {},
	pkgerrors.ErrGroupFull.Code:             {},
	pkgerrors.ErrAlreadyInGroup.Code:        {},
	pkgerrors.ErrTargetNotMember.Code:       {},
	pkgerrors.ErrMemberOpDenied.Code:        {},
	pkgerrors.ErrOwnerQuitDenied.Code:       {},
	pkgerrors.ErrFriendApplySelf.Code:       {},
	pkgerrors.ErrFriendApplyExists.Code:     {},
	pkgerrors.ErrFriendRequestNotFound.Code: {},
	pkgerrors.ErrAlreadyFriends.Code:        {},
	pkgerrors.ErrNotFriend.Code:             {},
	pkgerrors.ErrHotStatQuery.Code:          {},
	pkgerrors.ErrHotMarkWrite.Code:          {},
	pkgerrors.ErrHotQueryFailed.Code:        {},
	pkgerrors.ErrSignalPeerOffline.Code:     {},
	pkgerrors.ErrSignalSession.Code:         {},
	pkgerrors.ErrInternal.Code:              {},
}

// OK 构造成功响应 {0, "ok", data}；data 为 nil 时序列化保留 "data":null。
func OK(data any) Response {
	return Response{Code: 0, Message: "ok", Data: data}
}

// Fail 构造失败响应：白名单内 *Error 透传 {Code, Message, nil}，其余归一
// {9001, "内部错误", nil}（固定文案，不含任何 cause 内容）。
func Fail(err error) Response {
	var e *pkgerrors.Error
	if !stderrors.As(err, &e) {
		return failInternal()
	}
	if _, ok := knownCodes[e.Code]; !ok {
		return failInternal()
	}
	return Response{Code: e.Code, Message: e.Message, Data: nil}
}

// failInternal 为归一出口，文案取自 ErrInternal 哨兵（"内部错误"）。
func failInternal() Response {
	return Response{Code: pkgerrors.ErrInternal.Code, Message: pkgerrors.ErrInternal.Message, Data: nil}
}
