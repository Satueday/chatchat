// Package user 用户业务服务（W1-M06）：注册（手机号唯一 + bcrypt + 验证码
// mock 钩子）、登录（防枚举统一 2003 + 哑比对时序均衡）、refresh 换发
// （仅产新 access）、个人信息查询。
//
// 分层约定：只依赖 dao.UserRepo 接口与 pkg/common/jwt Manager（均构造注入），
// 不 import gin（handler 薄封装在 internal/router）；错误全部 pkg/errors 哨兵。
// 安全约定：密码与 token 全值禁入日志（日志仅记 reason 枚举 + phone/uid）。
package user

import (
	"context"
	"fmt"
	"regexp"
	"time"

	"chatchat/config"
	"chatchat/internal/dao"
	"chatchat/internal/model"
	"chatchat/pkg/common/jwt"
	pkgerrors "chatchat/pkg/errors"
	"chatchat/pkg/global"

	"go.uber.org/zap"
	"golang.org/x/crypto/bcrypt"
)

// phoneRe 大陆手机号正则（DDL 注释「大陆 11 位」的具体化，service 层校验；
// 包级 MustCompile 只读，W1-M06-plan §3 裁决 6 冻结）。
var phoneRe = regexp.MustCompile(`^1[3-9]\d{9}$`)

// dummyHash 登录防枚举哑比对用的固定 bcrypt 哈希（cost 4 预计算常量，对
// 明文 "chatchat-dummy-password-for-timing" 有效；生成时已自验证明文可
// 匹配、任意其他密码失配）。仅用于 CompareHashAndPassword 拉平「用户不
// 存在」与「密码错误」两路径时序。预计算字面量避免包初始化依赖 bcrypt
// 成功（W1-M06 Stage 4 P3 修复）。
const dummyHash = "$2a$04$OBUJ.K6CkCHHZP5HTOwmVOpTvJELhGWqrzGh.y5.lJttTfsrqyGQe"

// RegisterRequest 注册请求：Phone/Password 必填（binding 由 router 层消费）；
// Code 恒留（ADR-0002：验证码字段不得因校验关闭而移除，无 required）。
type RegisterRequest struct {
	Phone    string `json:"phone" binding:"required"`
	Password string `json:"password" binding:"required"`
	Code     string `json:"code"`
}

// RegisterResult 注册成功响应：仅返回 uid。
type RegisterResult struct {
	UID uint64 `json:"uid"`
}

// LoginRequest 登录请求。
type LoginRequest struct {
	Phone    string `json:"phone" binding:"required"`
	Password string `json:"password" binding:"required"`
}

// TokenPair 登录成功响应：双 token + access 有效期秒数（字段名冻结）。
type TokenPair struct {
	AccessToken     string `json:"access_token"`
	RefreshToken    string `json:"refresh_token"`
	AccessExpiresIn int64  `json:"access_expires_in"`
}

// RefreshRequest refresh 请求体。
type RefreshRequest struct {
	RefreshToken string `json:"refresh_token" binding:"required"`
}

// AccessOnly refresh 成功响应：仅换发 access，不产生新 refresh（CAP-004）。
type AccessOnly struct {
	AccessToken     string `json:"access_token"`
	AccessExpiresIn int64  `json:"access_expires_in"`
}

// Profile 个人信息（/me 响应；username W1 落空串，未来 UpdateProfile 设置）。
type Profile struct {
	UID       uint64    `json:"uid"`
	Username  string    `json:"username"`
	Phone     string    `json:"phone"`
	Avatar    string    `json:"avatar"`
	CreatedAt time.Time `json:"created_at"`
}

// Service 用户业务服务：依赖全部构造注入；字段初始化后不可变。
type Service struct {
	repo       dao.UserRepo
	jm         *jwt.Manager
	cost       int
	verifyEn   bool
	verifyMock string
	logger     *zap.Logger
}

// NewService 构造 Service（cfg.User 提供 bcrypt cost 与验证码开关/mock 值）。
func NewService(repo dao.UserRepo, jm *jwt.Manager, cfg config.User, logger *zap.Logger) *Service {
	return &Service{
		repo:       repo,
		jm:         jm,
		cost:       cfg.BcryptCost,
		verifyEn:   cfg.VerifyCodeEnabled,
		verifyMock: cfg.VerifyCodeMock,
		logger:     logger,
	}
}

// verifyCode 验证码校验钩子（ADR-0002：可替换调用点，不定义接口）：
// 开关关闭直接放行；开启时 code 与 mock 值不等→1001。
func (s *Service) verifyCode(code string) error {
	if !s.verifyEn {
		return nil
	}
	if code != s.verifyMock {
		return pkgerrors.Wrap(fmt.Errorf("验证码不匹配"), pkgerrors.ErrInvalidParam.Code, pkgerrors.ErrInvalidParam.Message)
	}
	return nil
}

// Register 注册：手机号正则→密码长度（bcrypt 72 字节截断防护）→验证码钩子
// →bcrypt 哈希（s.cost）→repo.Create（2001 唯一冲突透传）→返回 uid。
// username 不收（spec §8.1 注册流程=手机号+验证码+密码；未来 UpdateProfile 设置）。
func (s *Service) Register(ctx context.Context, req *RegisterRequest) (*RegisterResult, error) {
	if !phoneRe.MatchString(req.Phone) {
		return nil, pkgerrors.Wrap(fmt.Errorf("手机号格式非法: %s", req.Phone),
			pkgerrors.ErrInvalidParam.Code, pkgerrors.ErrInvalidParam.Message)
	}
	if len(req.Password) > global.PasswordMaxBytes {
		return nil, pkgerrors.Wrap(fmt.Errorf("密码长度超上限 %d 字节", global.PasswordMaxBytes),
			pkgerrors.ErrInvalidParam.Code, pkgerrors.ErrInvalidParam.Message)
	}
	if err := s.verifyCode(req.Code); err != nil {
		return nil, err
	}
	hash, err := bcrypt.GenerateFromPassword([]byte(req.Password), s.cost)
	if err != nil {
		return nil, pkgerrors.Wrap(err, pkgerrors.ErrInternal.Code, pkgerrors.ErrInternal.Message)
	}
	u := &model.User{Phone: req.Phone, Password: string(hash)}
	if err := s.repo.Create(ctx, u); err != nil {
		return nil, err // 2001 唯一冲突透传（含其余 DB 错误原样上抛）
	}
	return &RegisterResult{UID: u.ID}, nil
}

// Login 登录：GetByPhone 失败（含 2002）→哑比对+统一 2003（日志 reason=
// user_not_found）；密码比对失败→2003（reason=password_mismatch）；
// 成功→签发 access/refresh 双 token。两失败路径对外零差异（防枚举）。
func (s *Service) Login(ctx context.Context, req *LoginRequest) (*TokenPair, error) {
	u, err := s.repo.GetByPhone(ctx, req.Phone)
	if err != nil {
		dummyCompare(req.Password) // 拉平时序，结果恒为失配，忽略返回
		// err 的 cause 保留在返回错误链中（router 侧 Fail 消毒不外泄）；
		// 此处补记底层错误，消除 DB 故障在登录路径的日志盲区（Stage 4 P3-2）。
		s.logger.Warn("login failed",
			zap.String("reason", "user_not_found"),
			zap.String("phone", req.Phone),
			zap.Error(err))
		return nil, pkgerrors.Wrap(err, pkgerrors.ErrPasswordMismatch.Code, pkgerrors.ErrPasswordMismatch.Message)
	}
	if err := bcrypt.CompareHashAndPassword([]byte(u.Password), []byte(req.Password)); err != nil {
		s.logger.Warn("login failed",
			zap.String("reason", "password_mismatch"),
			zap.Uint64("uid", u.ID))
		return nil, pkgerrors.Wrap(err, pkgerrors.ErrPasswordMismatch.Code, pkgerrors.ErrPasswordMismatch.Message)
	}
	access, err := s.jm.Generate(u.ID, jwt.TypeAccess)
	if err != nil {
		return nil, pkgerrors.Wrap(err, pkgerrors.ErrInternal.Code, pkgerrors.ErrInternal.Message)
	}
	refresh, err := s.jm.Generate(u.ID, jwt.TypeRefresh)
	if err != nil {
		return nil, pkgerrors.Wrap(err, pkgerrors.ErrInternal.Code, pkgerrors.ErrInternal.Message)
	}
	return &TokenPair{
		AccessToken:     access,
		RefreshToken:    refresh,
		AccessExpiresIn: int64(s.jm.AccessTTL().Seconds()),
	}, nil
}

// Refresh 以 refresh token 换发新 access（typ 严格；不产生新 refresh、
// 不作废旧 access）。jwt.Parse 的 2004/2005 语义错误直接上抛。
// ctx 仅占位（当前实现无 I/O 传播点，router 契约保留参数位）。
func (s *Service) Refresh(_ context.Context, refreshToken string) (*AccessOnly, error) {
	claims, err := s.jm.Parse(refreshToken, jwt.TypeRefresh)
	if err != nil {
		return nil, err
	}
	access, err := s.jm.Generate(uint64(claims.UID), jwt.TypeAccess)
	if err != nil {
		return nil, pkgerrors.Wrap(err, pkgerrors.ErrInternal.Code, pkgerrors.ErrInternal.Message)
	}
	return &AccessOnly{
		AccessToken:     access,
		AccessExpiresIn: int64(s.jm.AccessTTL().Seconds()),
	}, nil
}

// GetProfile 查询个人信息：GetByID 未命中 2002 透传；命中映射 Profile DTO。
func (s *Service) GetProfile(ctx context.Context, uid uint64) (*Profile, error) {
	u, err := s.repo.GetByID(ctx, uid)
	if err != nil {
		return nil, err // 2002 透传
	}
	return &Profile{
		UID:       u.ID,
		Username:  u.Username,
		Phone:     u.Phone,
		Avatar:    u.Avatar,
		CreatedAt: u.CreatedAt,
	}, nil
}

// dummyCompare 用固定 dummyHash 执行一次 bcrypt 比对：结果恒为失配（哑哈希
// 不对应任何真实密码），仅用于消耗与真实比对等量时间（防时序侧信道枚举）。
func dummyCompare(password string) {
	_ = bcrypt.CompareHashAndPassword([]byte(dummyHash), []byte(password))
}
