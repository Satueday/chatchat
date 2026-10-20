// Package jwt 提供 HS256 JWT 签发与校验（W1-M06）。
//
// 定位：pkg/common 下零业务依赖的纯工具组件——不 import config/gin/gorm，
// 密钥与 TTL 经 NewManager 构造注入；Week 2 WS 握手鉴权（cmd/gateway）
// 直接复用本包 Manager 与 Claims（模块定位见 W1-M06-plan §3 裁决 1）。
//
// 错误语义：签名/格式/typ 混用→pkg/errors.ErrTokenInvalid(2004)、
// 过期→ErrTokenExpired(2005)，两者可区分；全部经 pkgerrors.Wrap 返回，
// cause 保留原始错误仅供日志面，对外响应由 pkg/common.Fail 消毒。
//
// claims 自管五字段（uid/typ/iat/exp/jti），不嵌 jwt.RegisteredClaims；
// 为满足 golang-jwt v5 ParseWithClaims 的 Claims 接口约束，按需实现
// GetExpirationTime/GetIssuedAt（零值返回 nil 以跳过对应校验）。
package jwt

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"time"

	pkgerrors "chatchat/pkg/errors"

	"github.com/golang-jwt/jwt/v5"
)

// token 类型常量（Generate/Parse 的 typ 取值域，严格二值）。
const (
	// TypeAccess access token 类型。
	TypeAccess = "access"
	// TypeRefresh refresh token 类型。
	TypeRefresh = "refresh"
)

// Claims 为统一 claims 结构：五字段全自管（json tag snake_case）。
// UID 以 int64 承载（模型 uint64 的安全投影）；Jti 为 crypto/rand 16 字节 hex。
type Claims struct {
	UID int64  `json:"uid"`
	Typ string `json:"typ"`
	Iat int64  `json:"iat"`
	Exp int64  `json:"exp"`
	Jti string `json:"jti"`
}

// GetExpirationTime 实现 golang-jwt v5 Claims 接口：exp>0 时供校验器判过期。
func (c *Claims) GetExpirationTime() (*jwt.NumericDate, error) {
	if c.Exp == 0 {
		return nil, nil
	}
	return jwt.NewNumericDate(time.Unix(c.Exp, 0)), nil
}

// GetIssuedAt 实现 golang-jwt v5 Claims 接口：iat>0 时供校验器判未来时间。
func (c *Claims) GetIssuedAt() (*jwt.NumericDate, error) {
	if c.Iat == 0 {
		return nil, nil
	}
	return jwt.NewNumericDate(time.Unix(c.Iat, 0)), nil
}

// GetNotBefore 实现 Claims 接口：本项目不使用 nbf 声明，恒返回零值
// （校验器跳过对应检查）。
func (c *Claims) GetNotBefore() (*jwt.NumericDate, error) { return nil, nil }

// GetIssuer 实现 Claims 接口：本项目不使用 iss 声明，恒返回零值。
func (c *Claims) GetIssuer() (string, error) { return "", nil }

// GetSubject 实现 Claims 接口：本项目不使用 sub 声明，恒返回零值。
func (c *Claims) GetSubject() (string, error) { return "", nil }

// GetAudience 实现 Claims 接口：本项目不使用 aud 声明，恒返回零值。
func (c *Claims) GetAudience() (jwt.ClaimStrings, error) { return nil, nil }

// Manager 持有签名密钥与两类 token 的 TTL；无锁（字段初始化后不可变）。
type Manager struct {
	secret     []byte
	accessTTL  time.Duration
	refreshTTL time.Duration
}

// NewManager 构造 Manager；secret 长度≥32 字节由 config 校验层保证（本包不重复校验）。
func NewManager(secret string, accessTTL, refreshTTL time.Duration) *Manager {
	return &Manager{
		secret:     []byte(secret),
		accessTTL:  accessTTL,
		refreshTTL: refreshTTL,
	}
}

// AccessTTL 返回 access token 有效期（供调用方计算 access_expires_in）。
func (m *Manager) AccessTTL() time.Duration { return m.accessTTL }

// Generate 签发 HS256 token：typ 严格限定 access/refresh（其余报错）；
// jti 取 crypto/rand 16 字节 hex（32 字符）；iat/exp 为当前 Unix 秒 + 对应 TTL。
func (m *Manager) Generate(uid uint64, typ string) (string, error) {
	var ttl time.Duration
	switch typ {
	case TypeAccess:
		ttl = m.accessTTL
	case TypeRefresh:
		ttl = m.refreshTTL
	default:
		return "", fmt.Errorf("jwt: 非法 token 类型 %q（仅 %s/%s）", typ, TypeAccess, TypeRefresh)
	}

	raw := make([]byte, 16)
	if _, err := rand.Read(raw); err != nil {
		return "", fmt.Errorf("jwt: 生成 jti 失败: %w", err)
	}
	now := time.Now().Unix()
	claims := &Claims{
		UID: int64(uid),
		Typ: typ,
		Iat: now,
		Exp: now + int64(ttl.Seconds()),
		Jti: hex.EncodeToString(raw),
	}
	return jwt.NewWithClaims(jwt.SigningMethodHS256, claims).SignedString(m.secret)
}

// Parse 校验并解析 token：WithValidMethods 锁定 HS256（防 alg 混淆）。
// 错误映射：jwt.ErrTokenExpired→ErrTokenExpired(2005)；签名/格式错→
// ErrTokenInvalid(2004)；claims.Typ≠wantTyp→ErrTokenInvalid；
// 解析成功后再防御性复核 Exp（伪造 exp 缺失/校验器旁路兜底）→ErrTokenExpired。
func (m *Manager) Parse(token, wantTyp string) (*Claims, error) {
	parsed, err := jwt.ParseWithClaims(token, &Claims{}, func(*jwt.Token) (any, error) {
		return m.secret, nil
	}, jwt.WithValidMethods([]string{jwt.SigningMethodHS256.Alg()}))
	if err != nil {
		if errors.Is(err, jwt.ErrTokenExpired) {
			return nil, pkgerrors.Wrap(err, pkgerrors.ErrTokenExpired.Code, pkgerrors.ErrTokenExpired.Message)
		}
		return nil, pkgerrors.Wrap(err, pkgerrors.ErrTokenInvalid.Code, pkgerrors.ErrTokenInvalid.Message)
	}
	if parsed == nil || !parsed.Valid {
		return nil, pkgerrors.Wrap(errors.New("token 无效"), pkgerrors.ErrTokenInvalid.Code, pkgerrors.ErrTokenInvalid.Message)
	}
	claims, ok := parsed.Claims.(*Claims)
	if !ok {
		return nil, pkgerrors.Wrap(errors.New("claims 类型异常"), pkgerrors.ErrTokenInvalid.Code, pkgerrors.ErrTokenInvalid.Message)
	}
	if claims.Typ != wantTyp {
		return nil, pkgerrors.Wrap(fmt.Errorf("token 类型 %q 与期望 %q 不符", claims.Typ, wantTyp),
			pkgerrors.ErrTokenInvalid.Code, pkgerrors.ErrTokenInvalid.Message)
	}
	// 防御性二次校验：exp 已过或缺失（缺失视为立即过期，不给旁路窗口）。
	if claims.Exp <= time.Now().Unix() {
		return nil, pkgerrors.Wrap(errors.New("token 已过期"), pkgerrors.ErrTokenExpired.Code, pkgerrors.ErrTokenExpired.Message)
	}
	return claims, nil
}
