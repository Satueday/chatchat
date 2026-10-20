// Package friend 好友业务服务（W3-M02）：申请/同意/拒绝/删除/列表（含收到
// 的待处理申请）全套业务与好友关系双行状态机（0 申请中 → 1 已好友，plan
// D1）；AllowChat 为 message.ChatGate 的结构化实现——单聊好友关系门槛
// （[friend].require_friend_for_chat 默认开，plan §8.4 默认策略，配置可放开）。
//
// 分层约定：只依赖 dao.FriendRepo / dao.UserRepo 接口（构造注入，不 import
// gin/gorm，也不 import message——ChatGate 结构化类型反转依赖，装配层连接）；
// 错误全部 pkg/errors 3xxx 哨兵零拼串（目标用户不存在复用 2002 user 域哨兵
// 透传，friend 域不自造）；操作 Info 日志仅记 op/uid/target_uid 字段（群组
// opLog 同款），门槛拒绝记 Warn（uid/toUid，不含内容）。
//
// 状态机：申请插单行 (applicant,target,0)；同意事务正向 0→1 + 反向
// upsert→1（D1）；拒绝=删申请行（D2，拒绝后可重新申请）；删除好友事务
// 双删（D3，删除后写侧被门槛拒绝、读侧拉取/历史不受影响）。审批操作恒查
// (申请人, 本人) 行（D5：uid=申请人，uk(applicant,me) 无歧义，操作对象
// 天然限定「发给本人的申请」，无越权面）。
package friend

import (
	"context"
	"fmt"
	"time"

	"chatchat/config"
	"chatchat/internal/dao"
	"chatchat/internal/model"
	pkgerrors "chatchat/pkg/errors"

	"go.uber.org/zap"
)

// 好友关系状态常量（对齐 DDL friend.status 注释：0=申请中 1=已好友 2=已
// 拉黑；2 为 W4+ 预留，本期无 API 设置路径，口径归本包，不导出）。
const (
	statusPending int8 = 0
	statusFriend  int8 = 1
)

// ApplyRequest 好友申请请求 {toUid}（目标须存在且非自己）。
type ApplyRequest struct {
	ToUID uint64 `json:"toUid" binding:"required"`
}

// ApproveRequest 同意申请请求（uid=申请人，即发给本人的申请）。
type ApproveRequest struct {
	UID uint64 `json:"uid" binding:"required"`
}

// RejectRequest 拒绝申请请求（uid=申请人，即发给本人的申请）。
type RejectRequest struct {
	UID uint64 `json:"uid" binding:"required"`
}

// DTO 好友视图（plan D9 冻结字段：UID=对方、FriendsSince=好友行
// updated_at 即成为好友时刻；命名对齐 group.DTO 先例——包名 friend 前缀
// 触发 revive stutter 规则，.golangci.yml 零变更约束下不叫 FriendDTO）。
type DTO struct {
	UID          uint64    `json:"uid"`
	FriendsSince time.Time `json:"friendsSince"`
}

// RequestDTO 待处理申请视图（FromUID=申请人、CreatedAt=申请行 created_at，
// plan D9 冻结）。
type RequestDTO struct {
	FromUID   uint64    `json:"fromUid"`
	CreatedAt time.Time `json:"createdAt"`
}

// Service 好友业务服务：依赖全部构造注入；字段初始化后不可变。
type Service struct {
	friends dao.FriendRepo
	users   dao.UserRepo
	gateOn  bool // 单聊好友门槛开关（cfg.Friend.RequireFriendForChat）
	logger  *zap.Logger
}

// NewService 构造 Service（cfg.Friend 提供门槛开关）。
func NewService(friends dao.FriendRepo, users dao.UserRepo, cfg config.Friend, logger *zap.Logger) *Service {
	return &Service{
		friends: friends,
		users:   users,
		gateOn:  cfg.RequireFriendForChat,
		logger:  logger,
	}
}

// opLog 好友操作 Info 日志（字段化，不含内容体）。
func (s *Service) opLog(op string, uid, targetUID uint64) {
	s.logger.Info("friend op",
		zap.String("op", op),
		zap.Uint64("uid", uid),
		zap.Uint64("target_uid", targetUID))
}

// Apply 好友申请：目标非自己（3001）→ 目标存在（2002 user 域哨兵透传）→
// 正向关系三态（已好友 3004 / 同方向申请中 3002）→ 互逆 pending 3002
// （不做自动合并，plan §2 状态机确定性优先，UX 优化延后）→ 插申请行
// (uid,toUid,0)（uk 冲突兜底 3002，并发安全）。
func (s *Service) Apply(ctx context.Context, uid uint64, req *ApplyRequest) error {
	if req.ToUID == uid {
		return pkgerrors.Wrap(fmt.Errorf("apply target is self, uid %d", uid),
			pkgerrors.ErrFriendApplySelf.Code, pkgerrors.ErrFriendApplySelf.Message)
	}
	if _, err := s.users.GetByID(ctx, req.ToUID); err != nil {
		return err // 2002 透传
	}
	rel, err := s.friends.GetRelation(ctx, uid, req.ToUID)
	if err != nil {
		return err
	}
	if rel != nil {
		if rel.Status == statusFriend {
			return pkgerrors.Wrap(fmt.Errorf("relation (%d,%d) already friends", uid, req.ToUID),
				pkgerrors.ErrAlreadyFriends.Code, pkgerrors.ErrAlreadyFriends.Message)
		}
		return pkgerrors.Wrap(fmt.Errorf("pending request (%d,%d) exists", uid, req.ToUID),
			pkgerrors.ErrFriendApplyExists.Code, pkgerrors.ErrFriendApplyExists.Message)
	}
	// 互逆行检查：事务不变量下（同意双行写/删除双行删）正向 miss 时互逆行
	// 存在即 pending（已好友必有正向行，走不到本分支）——恒 3002 fail-closed。
	if rev, err := s.friends.GetRelation(ctx, req.ToUID, uid); err != nil {
		return err
	} else if rev != nil {
		return pkgerrors.Wrap(fmt.Errorf("reverse pending request (%d,%d) exists", req.ToUID, uid),
			pkgerrors.ErrFriendApplyExists.Code, pkgerrors.ErrFriendApplyExists.Message)
	}
	if err := s.friends.CreateRequest(ctx, &model.Friend{
		UserID:   uid,
		FriendID: req.ToUID,
		Status:   statusPending,
	}); err != nil {
		return err // uk 冲突→3002（并发兜底）
	}
	s.opLog("apply", uid, req.ToUID)
	return nil
}

// Approve 同意申请（uid=审批人=本人，applicantUID=申请人）：操作对象恒为
// (申请人, 本人) 行，不存在/已处理 3003；DAO 事务双行 status=1 原子提交
// （同意后会话即建立——ConvID 推导无需落库，单聊链路立即可用，plan D8）。
func (s *Service) Approve(ctx context.Context, uid, applicantUID uint64) error {
	if err := s.friends.AcceptRequest(ctx, applicantUID, uid); err != nil {
		return err // 3003 透传
	}
	s.opLog("approve", uid, applicantUID)
	return nil
}

// Reject 拒绝申请（uid=本人，applicantUID=申请人）：删 (申请人, 本人)
// status=0 行，不存在/已处理 3003；拒绝后行已不存在，申请人可重新申请
// （plan D2）。
func (s *Service) Reject(ctx context.Context, uid, applicantUID uint64) error {
	if err := s.friends.RejectRequest(ctx, applicantUID, uid); err != nil {
		return err // 3003 透传
	}
	s.opLog("reject", uid, applicantUID)
	return nil
}

// Remove 删除好友：DAO 事务双删（正向非好友 3005）；删除后写侧（发送）被
// 门槛拒绝、读侧（拉取/历史）不受影响（读判定纯 ConvID 解析，plan D3）。
func (s *Service) Remove(ctx context.Context, uid, friendUID uint64) error {
	if err := s.friends.RemoveFriend(ctx, uid, friendUID); err != nil {
		return err // 3005 透传
	}
	s.opLog("remove", uid, friendUID)
	return nil
}

// ListFriends 好友列表（仅本人视角，uid 取鉴权 claims；W3 全量返回，分页
// 归 W4+）：(uid,*,status=1) 行映射 DTO（UID=对方、FriendsSince=行
// updated_at）。
func (s *Service) ListFriends(ctx context.Context, uid uint64) ([]DTO, error) {
	rows, err := s.friends.ListFriends(ctx, uid)
	if err != nil {
		return nil, err
	}
	dtos := make([]DTO, 0, len(rows))
	for _, f := range rows {
		dtos = append(dtos, DTO{UID: f.FriendID, FriendsSince: f.UpdatedAt})
	}
	return dtos, nil
}

// ListRequests 收到的待处理申请（(*,uid,status=0) 行映射 RequestDTO：
// FromUID=申请人、CreatedAt=申请行 created_at）。
func (s *Service) ListRequests(ctx context.Context, uid uint64) ([]RequestDTO, error) {
	rows, err := s.friends.ListPendingRequests(ctx, uid)
	if err != nil {
		return nil, err
	}
	dtos := make([]RequestDTO, 0, len(rows))
	for _, f := range rows {
		dtos = append(dtos, RequestDTO{FromUID: f.UserID, CreatedAt: f.CreatedAt})
	}
	return dtos, nil
}

// AllowChat 单聊发送门槛（message.ChatGate 结构化实现，W3-M02 plan D4）：
// 开关关→nil（放行）；开→IsFriend 窄查询，非好友 3005，DB 错误记 Error
// 日志（uid/toUid，不含内容）归一 9001 fail-closed。仅约束写侧（发送），
// 读侧（拉取/历史）不受限。
func (s *Service) AllowChat(ctx context.Context, fromUID, toUID uint64) *pkgerrors.Error {
	if !s.gateOn {
		return nil
	}
	ok, err := s.friends.IsFriend(ctx, fromUID, toUID)
	if err != nil {
		s.logger.Error("friend gate lookup failed",
			zap.Uint64("uid", fromUID), zap.Uint64("toUid", toUID), zap.Error(err))
		return pkgerrors.ErrInternal
	}
	if !ok {
		s.logger.Warn("chat denied: not friends",
			zap.Uint64("uid", fromUID), zap.Uint64("toUid", toUID))
		return pkgerrors.ErrNotFriend
	}
	return nil
}
