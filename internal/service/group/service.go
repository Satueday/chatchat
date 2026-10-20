// Package group 群组业务服务（W3-M01）：建群/入群/退群/踢人/禁言/转让群主/
// 群公告/设管理员全套业务与 D6 角色权限矩阵、500 成员上限强校验（入群
// 事务密闭）与 api 进程内定时对账（RunReconcile，宿主 goroutine 属主
// cmd/api、ctx cancel + WaitGroup 回收）。
//
// 分层约定：只依赖 dao.GroupRepo / dao.GroupMemberRepo 接口（构造注入，
// 不 import gin/gorm）；错误全部 pkg/errors 4xxx 哨兵零拼串（码值/文案随
// 常量定义）；管理操作 Info 日志仅记 op/group_id/uid/target_uid 字段，
// 不含内容体（群名/公告/参数值禁入日志）。
//
// 时间口径：禁言到期一律以 now().UTC() 计算（DSN loc=UTC，防本地时区
// 漂移，plan D8）；now/interval 为未导出可注入字段（同包测试直接覆写，
// 公共构造签名保持最小，plan D11）。
package group

import (
	"context"
	stderrors "errors"
	"fmt"
	"time"
	"unicode/utf8"

	"chatchat/config"
	"chatchat/internal/dao"
	"chatchat/internal/leader"
	"chatchat/internal/model"
	pkgerrors "chatchat/pkg/errors"
	"chatchat/pkg/global"

	"go.uber.org/zap"
)

// 成员角色常量（对齐 DDL group_member.role 注释：0=成员 1=管理员 2=群主；
// 口径归本包，不导出）。
const (
	roleMember int8 = 0
	roleAdmin  int8 = 1
	roleOwner  int8 = 2
)

// defaultMaxMember 建群成员上限服务端定值（spec §8.2 硬性指标；客户端
// 不可设置，plan D8）。与 DDL max_member 默认 500 同值，对账按列值比较
// 而非本常量（FindOverLimit 不引用）。
const defaultMaxMember int32 = 500

// reconcileQueryTimeout 单轮对账查询超时上限（plan D2）。
const reconcileQueryTimeout = 30 * time.Second

// reconcileLeaderKey 对账轮门控 Redis 锁键（W4-M05 plan D7：
// chatchat:leader:reconcile——多 api 实例同轮至多一执行者）。
const reconcileLeaderKey = "chatchat:leader:reconcile"

// RoundGate 对账每轮咨询锁（W4-M05 plan D7）：leader.Gate 别名；nil=不门控
// （make run 单实例与测试零影响）。TTL 由 RunReconcile 按 interval×2 注入。
type RoundGate = leader.Gate

// RoundStats 对账轮观测窄接口（W4-M05 plan D5/§6，移交 2）：每轮执行完成
// 后同步调用一次（api expvar reconcile_rounds/overlimit_groups 落点）；
// nil=不观测（测试/旧装配容忍，counter/gate 先例）。
type RoundStats interface {
	// ReconcileRound 上报一轮对账结果：scanned=全表群数（CountAll，
	// -1=计数查询失败的哨兵值），overlimit=超限群数（FindOverLimit 行数）。
	ReconcileRound(scanned, overlimit int64)
}

// CreateGroupRequest 建群请求（Name 1~64 字符，服务端校验）。
type CreateGroupRequest struct {
	Name string `json:"name" binding:"required"`
}

// MuteRequest 禁言/解禁请求：DurationSec=0 解禁、(0,30d] 禁言时长，
// 负值或超上限 1001（plan D8）。
type MuteRequest struct {
	UID         uint64 `json:"uid" binding:"required"`
	DurationSec int64  `json:"durationSec"`
}

// SetAdminRequest 设/撤管理员请求（仅群主，目标须成员且非群主）。
type SetAdminRequest struct {
	UID     uint64 `json:"uid" binding:"required"`
	IsAdmin bool   `json:"isAdmin"`
}

// TransferRequest 转让群主请求（仅群主，目标须成员且非自己）。
type TransferRequest struct {
	UID uint64 `json:"uid" binding:"required"`
}

// AnnouncementRequest 群公告请求（≤512 字符；空串=清空，合法输入）。
type AnnouncementRequest struct {
	Content string `json:"content"`
}

// DTO 群组视图（建群/群信息/我的群列表共用；MemberCount 由 service
// 拼装——群信息任何认证用户可见，成员列表另受成员门槛限制）。
type DTO struct {
	ID           uint64 `json:"id"`
	Name         string `json:"name"`
	OwnerID      uint64 `json:"ownerId"`
	MaxMember    int32  `json:"maxMember"`
	MemberCount  int64  `json:"memberCount"`
	Announcement string `json:"announcement"`
}

// MemberDTO 成员视图（不 join user 表取用户名——客户端按 userId 缓存
// 用户信息，spec §5；MutedUntil nil=未禁言，发言检查点归 W3-M04）。
type MemberDTO struct {
	UserID     uint64     `json:"userId"`
	Role       int8       `json:"role"`
	MutedUntil *time.Time `json:"mutedUntil"`
	JoinedAt   time.Time  `json:"joinedAt"`
}

// Service 群组业务服务：依赖全部构造注入；字段初始化后不可变。
type Service struct {
	groups   dao.GroupRepo
	members  dao.GroupMemberRepo
	stats    RoundStats // 对账轮观测（W4-M05，nil=不观测）
	logger   *zap.Logger
	now      func() time.Time // 禁言到期计算基准（默认 time.Now；测试覆写）
	interval time.Duration    // 对账周期（cfg.ReconcileIntervalSec 秒换算；测试覆写）
}

// NewService 构造 Service（cfg.Group 提供对账周期）。stats 为 W4-M05 增参
// （nil=不观测——对账每轮摘要/expvar 上报面，窄接口注入先例）。
func NewService(groups dao.GroupRepo, members dao.GroupMemberRepo, cfg config.Group, stats RoundStats, logger *zap.Logger) *Service {
	return &Service{
		groups:   groups,
		members:  members,
		stats:    stats,
		logger:   logger,
		now:      time.Now,
		interval: time.Duration(cfg.ReconcileIntervalSec) * time.Second,
	}
}

// invalidParam 参数类错误统一包装（1001）。
func invalidParam(format string, args ...any) error {
	return pkgerrors.Wrap(fmt.Errorf(format, args...),
		pkgerrors.ErrInvalidParam.Code, pkgerrors.ErrInvalidParam.Message)
}

// permDenied 群组权限不足（4003，操作者角色不满足矩阵行）。
func permDenied(format string, args ...any) error {
	return pkgerrors.Wrap(fmt.Errorf(format, args...),
		pkgerrors.ErrGroupPermDenied.Code, pkgerrors.ErrGroupPermDenied.Message)
}

// opDenied 不能对该成员执行此操作（4007，目标为群主/自己/越级）。
func opDenied(format string, args ...any) error {
	return pkgerrors.Wrap(fmt.Errorf(format, args...),
		pkgerrors.ErrMemberOpDenied.Code, pkgerrors.ErrMemberOpDenied.Message)
}

// targetMiss 目标视角转译（D6）：GetMember 未命中的 4002（操作者口径哨兵）
// 在目标查询点转译为 4006（目标用户不是群成员），cause 链保留；
// 其余错误原样透传。
func targetMiss(err error) error {
	var e *pkgerrors.Error
	if stderrors.As(err, &e) && e.Code == pkgerrors.ErrNotGroupMember.Code {
		return pkgerrors.Wrap(err, pkgerrors.ErrTargetNotMember.Code, pkgerrors.ErrTargetNotMember.Message)
	}
	return err
}

// checkKickMuteHierarchy 踢人/禁言共用的 D6 层级校验：操作者须 ≥ 管理员
// （4003）；目标角色不得 ≥ 操作者角色（同角色或更高一律 4007——群主可
// 操作管理员与成员、管理员仅可操作成员、任何人不可操作群主；对自身操作
// 因同角色自然落入 4007）。
func checkKickMuteHierarchy(opRole, targetRole int8) error {
	if opRole < roleAdmin {
		return permDenied("operator role %d below admin", opRole)
	}
	if targetRole >= opRole {
		return opDenied("target role %d not below operator role %d", targetRole, opRole)
	}
	return nil
}

// opLog 管理操作 Info 日志（字段化，不含内容体）。
func (s *Service) opLog(op string, groupID, uid, targetUID uint64) {
	s.logger.Info("group op",
		zap.String("op", op),
		zap.Uint64("group_id", groupID),
		zap.Uint64("uid", uid),
		zap.Uint64("target_uid", targetUID))
}

// Create 建群：群名长度校验（1~64 字符，rune 口径防 DB 截断）→ 事务落
// group_info + 群主成员行（role=2 由 DAO 保证）→ 返回 DTO。
// 任何认证用户可建群并成为群主（D6）；max_member 服务端定值 500。
func (s *Service) Create(ctx context.Context, uid uint64, req *CreateGroupRequest) (*DTO, error) {
	if n := utf8.RuneCountInString(req.Name); n == 0 || n > global.GroupNameMaxRunes {
		return nil, invalidParam("群名长度 %d 非法，须 1~%d 字符", n, global.GroupNameMaxRunes)
	}
	g := &model.GroupInfo{Name: req.Name, OwnerID: uid, MaxMember: defaultMaxMember}
	owner := &model.GroupMember{UserID: uid, Role: roleOwner}
	if err := s.groups.CreateWithOwner(ctx, g, owner); err != nil {
		return nil, err
	}
	s.opLog("create", g.ID, uid, 0)
	return &DTO{
		ID:           g.ID,
		Name:         g.Name,
		OwnerID:      g.OwnerID,
		MaxMember:    g.MaxMember,
		MemberCount:  1,
		Announcement: g.Announcement,
	}, nil
}

// Join 入群：GetByID 取群（4001）→ JoinUnderLimit 事务密闭（4004 满 /
// 4005 已在群内，uk 冲突即时暴露免 TOCTOU 双查，plan D3）。直接加入式，
// 无申请审批流；退群后可再次入群（uk 行已删）。
func (s *Service) Join(ctx context.Context, uid, groupID uint64) error {
	g, err := s.groups.GetByID(ctx, groupID)
	if err != nil {
		return err
	}
	if err := s.members.JoinUnderLimit(ctx, groupID, uid, g.MaxMember); err != nil {
		return err
	}
	s.opLog("join", groupID, uid, 0)
	return nil
}

// Quit 退群：GetMember（4002）→ 群主不可退（4008，须先显式转让，D6）→
// Remove 删成员行。历史消息与 inbox_cursor 保留（plan D10，M04+ 消费）。
func (s *Service) Quit(ctx context.Context, uid, groupID uint64) error {
	m, err := s.members.GetMember(ctx, groupID, uid)
	if err != nil {
		return err // 4002
	}
	if m.Role == roleOwner {
		return pkgerrors.Wrap(fmt.Errorf("owner %d cannot quit group %d", uid, groupID),
			pkgerrors.ErrOwnerQuitDenied.Code, pkgerrors.ErrOwnerQuitDenied.Message)
	}
	if err := s.members.Remove(ctx, groupID, uid); err != nil {
		return err // 4002
	}
	s.opLog("quit", groupID, uid, 0)
	return nil
}

// Kick 踢人（D6 矩阵）：群存在（4001）→ 操作者是成员（4002）→ 目标是
// 成员（4006）→ 层级校验（4003/4007）→ Remove。
func (s *Service) Kick(ctx context.Context, uid, groupID, targetUID uint64) error {
	if _, err := s.groups.GetByID(ctx, groupID); err != nil {
		return err // 4001
	}
	op, err := s.members.GetMember(ctx, groupID, uid)
	if err != nil {
		return err // 4002
	}
	target, err := s.members.GetMember(ctx, groupID, targetUID)
	if err != nil {
		return targetMiss(err) // 4006（目标视角转译）
	}
	if err := checkKickMuteHierarchy(op.Role, target.Role); err != nil {
		return err // 4003/4007（含踢自己：同角色落入 4007）
	}
	if err := s.members.Remove(ctx, groupID, targetUID); err != nil {
		return err
	}
	s.opLog("kick", groupID, uid, targetUID)
	return nil
}

// Mute 禁言/解禁：时长校验（0=解禁、(0,30d] 合法，负值/超限 1001）→
// 操作者成员（4002）→ 目标成员（4006）→ 层级校验（4003/4007）→
// UpdateMutedUntil（nil 解禁 / now+duration UTC 时间戳）。禁言判定
// （muted_until > now）的发言检查点归 W3-M04。
func (s *Service) Mute(ctx context.Context, uid, groupID uint64, req *MuteRequest) error {
	if req.DurationSec < 0 || req.DurationSec > global.GroupMuteMaxDurationSec {
		return invalidParam("禁言时长 %d 秒非法，须 0（解禁）或 (0,%d]",
			req.DurationSec, global.GroupMuteMaxDurationSec)
	}
	op, err := s.members.GetMember(ctx, groupID, uid)
	if err != nil {
		return err // 4002
	}
	target, err := s.members.GetMember(ctx, groupID, req.UID)
	if err != nil {
		return targetMiss(err) // 4006（目标视角转译）
	}
	if err := checkKickMuteHierarchy(op.Role, target.Role); err != nil {
		return err // 4003/4007（含禁自己/禁群主/管理员越级）
	}
	var until *time.Time
	if req.DurationSec > 0 {
		t := s.now().UTC().Add(time.Duration(req.DurationSec) * time.Second)
		until = &t
	}
	if err := s.members.UpdateMutedUntil(ctx, groupID, req.UID, until); err != nil {
		return err // 4006
	}
	s.opLog("mute", groupID, uid, req.UID)
	return nil
}

// Transfer 转让群主：目标为自己（4007）→ DAO 事务原子完成（锁定行校验
// owner 4003 / 目标成员 4006 / owner_id + 双向角色同事务），旧群主降为
// 管理员（role=1）。
func (s *Service) Transfer(ctx context.Context, uid, groupID, targetUID uint64) error {
	if targetUID == uid {
		return opDenied("transfer target is operator self (%d)", uid)
	}
	if err := s.groups.TransferOwner(ctx, groupID, uid, targetUID); err != nil {
		return err // 4001/4003/4006
	}
	s.opLog("transfer", groupID, uid, targetUID)
	return nil
}

// SetAdmin 设/撤管理员（仅群主）：操作者成员（4002）→ 仅群主（4003）→
// 目标为群主（含自己，4007）→ 目标成员（4006）→ UpdateRole（isAdmin
// →1 / 撤 →0）。
func (s *Service) SetAdmin(ctx context.Context, uid, groupID, targetUID uint64, isAdmin bool) error {
	op, err := s.members.GetMember(ctx, groupID, uid)
	if err != nil {
		return err // 4002
	}
	if op.Role != roleOwner {
		return permDenied("set admin requires owner, operator role %d", op.Role)
	}
	if targetUID == uid {
		return opDenied("set admin target is operator self (%d)", uid)
	}
	target, err := s.members.GetMember(ctx, groupID, targetUID)
	if err != nil {
		return targetMiss(err) // 4006（目标视角转译）
	}
	if target.Role == roleOwner {
		return opDenied("set admin target is owner (%d)", targetUID)
	}
	role := roleMember
	if isAdmin {
		role = roleAdmin
	}
	if err := s.members.UpdateRole(ctx, groupID, targetUID, role); err != nil {
		return err // 4006
	}
	s.opLog("set_admin", groupID, uid, targetUID)
	return nil
}

// SetAnnouncement 群公告（群主或管理员）：长度校验（≤512 字符，空串=清空
// 合法，1001）→ 操作者成员（4002）→ 角色 ≥ 管理员（4003）→ 单列更新
// （群不存在 4001）。内容体不入日志。
func (s *Service) SetAnnouncement(ctx context.Context, uid, groupID uint64, content string) error {
	if n := utf8.RuneCountInString(content); n > global.GroupAnnouncementMaxRunes {
		return invalidParam("公告长度 %d 字符超上限 %d", n, global.GroupAnnouncementMaxRunes)
	}
	op, err := s.members.GetMember(ctx, groupID, uid)
	if err != nil {
		return err // 4002
	}
	if op.Role < roleAdmin {
		return permDenied("announcement requires admin, operator role %d", op.Role)
	}
	if err := s.groups.UpdateAnnouncement(ctx, groupID, content); err != nil {
		return err // 4001（0 行且行不存在）
	}
	s.opLog("announcement", groupID, uid, 0)
	return nil
}

// GetGroup 群信息（任何认证用户，含成员数；D6——uid 仅契约保留不参与
// 鉴权）：GetByID（4001）→ CountByGroup 拼装 DTO。
func (s *Service) GetGroup(ctx context.Context, _ uint64, groupID uint64) (*DTO, error) {
	g, err := s.groups.GetByID(ctx, groupID)
	if err != nil {
		return nil, err // 4001
	}
	n, err := s.members.CountByGroup(ctx, groupID)
	if err != nil {
		return nil, err
	}
	return &DTO{
		ID:           g.ID,
		Name:         g.Name,
		OwnerID:      g.OwnerID,
		MaxMember:    g.MaxMember,
		MemberCount:  n,
		Announcement: g.Announcement,
	}, nil
}

// ListMembers 成员列表（仅群成员，防 userId 枚举，D6）：GetMember（4002）→
// ListByGroup 映射 MemberDTO（MutedUntil nil 透传）。
func (s *Service) ListMembers(ctx context.Context, uid, groupID uint64) ([]MemberDTO, error) {
	if _, err := s.members.GetMember(ctx, groupID, uid); err != nil {
		return nil, err // 4002
	}
	ms, err := s.members.ListByGroup(ctx, groupID)
	if err != nil {
		return nil, err
	}
	dtos := make([]MemberDTO, 0, len(ms))
	for _, m := range ms {
		dtos = append(dtos, MemberDTO{
			UserID:     m.UserID,
			Role:       m.Role,
			MutedUntil: m.MutedUntil,
			JoinedAt:   m.JoinedAt,
		})
	}
	return dtos, nil
}

// ListMyGroups 我的群列表：ListByUser → ListByIDs + CountByGroups 拼装
// （无群返回空切片）。
func (s *Service) ListMyGroups(ctx context.Context, uid uint64) ([]DTO, error) {
	ms, err := s.members.ListByUser(ctx, uid)
	if err != nil {
		return nil, err
	}
	if len(ms) == 0 {
		return []DTO{}, nil
	}
	ids := make([]uint64, 0, len(ms))
	for _, m := range ms {
		ids = append(ids, m.GroupID)
	}
	gs, err := s.groups.ListByIDs(ctx, ids)
	if err != nil {
		return nil, err
	}
	counts, err := s.members.CountByGroups(ctx, ids)
	if err != nil {
		return nil, err
	}
	dtos := make([]DTO, 0, len(gs))
	for _, g := range gs {
		dtos = append(dtos, DTO{
			ID:           g.ID,
			Name:         g.Name,
			OwnerID:      g.OwnerID,
			MaxMember:    g.MaxMember,
			MemberCount:  counts[g.ID], // 无成员群缺省 0
			Announcement: g.Announcement,
		})
	}
	return dtos, nil
}

// RunReconcile 对账阻塞循环（plan D2：宿主 api 进程）：启动即跑一轮 →
// interval 周期 ticker → ctx.Done 返回（goroutine 由调用方 WaitGroup 回收）。
// gate 为 W4-M05 增参（plan D7：Redis 每轮咨询锁——多 api 实例同轮至多一
// 执行者；nil=不门控，make run 单实例与测试零影响；TTL=interval×2 吸收
// 单轮超时，进程崩溃锁自然过期；正确性兜底=对账幂等）。单轮查询 30s 超时；
// 查询失败仅 Error 日志不中断循环；超限群逐群 Error 告警（字段化，只告警
// 不自动踢人）。
func (s *Service) RunReconcile(ctx context.Context, gate RoundGate) {
	s.reconcileOnce(ctx, gate)
	ticker := time.NewTicker(s.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.reconcileOnce(ctx, gate)
		}
	}
}

// reconcileOnce 执行单轮对账：gate 竞取（false=本轮跳过——他实例持锁或
// Redis 不可达，宁缺勿重）→ FindOverLimit（30s 超时上限）→ 逐群 Error
// 告警（group_id/member_count/max_member 字段化，plan D9）→ 每轮 Info
// 摘要（groups_scanned/overlimit，W4-M05 移交 2：memberCount 观测）→
// stats 上报（nil=不观测）。
func (s *Service) reconcileOnce(ctx context.Context, gate RoundGate) {
	if gate != nil && !gate.Acquire(ctx, reconcileLeaderKey, 2*s.interval) {
		s.logger.Debug("group reconcile round skipped (not leader this round)")
		return
	}
	qctx, cancel := context.WithTimeout(ctx, reconcileQueryTimeout)
	defer cancel()
	overs, err := s.groups.FindOverLimit(qctx)
	if err != nil {
		s.logger.Error("group reconcile query failed", zap.Error(err))
		return
	}
	// 全表群计数（plan §6 定：另查 SELECT COUNT(*)，每 300s 一次可受）；
	// 失败仅 Warn 并以 -1 哨兵入摘要（摘要本身仍发——超限数已可得）。
	scanned := int64(-1)
	if n, cerr := s.groups.CountAll(qctx); cerr == nil {
		scanned = n
	} else {
		s.logger.Warn("group count query failed", zap.Error(cerr))
	}
	for _, og := range overs {
		s.logger.Error("group members exceed max_member",
			zap.Uint64("group_id", og.Group.ID),
			zap.Int64("member_count", og.MemberCount),
			zap.Int32("max_member", og.Group.MaxMember))
	}
	s.logger.Info("reconcile round complete",
		zap.Int64("groups_scanned", scanned),
		zap.Int("overlimit", len(overs)))
	if s.stats != nil {
		s.stats.ReconcileRound(scanned, int64(len(overs)))
	}
}
