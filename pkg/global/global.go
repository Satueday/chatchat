// Package global 存放全局常量与版本信息。
// 依据 W1-M00-plan.md §3.2：全局状态仅限常量，禁止包级可变变量。
package global

import "time"

// Version 为当前目标发布版本（plan.md 头部目标版本）。
const Version = "0.1.0"

// JWT 与用户默认值常量（W1-M00 §6.4/§6.5 冻结值；时间格式常量待 W1-M05 定值后新增）。
const (
	// AccessTTLSec access token 有效期：2h（config access_ttl_sec 默认值）。
	AccessTTLSec = 7200
	// RefreshTTLSec refresh token 有效期：7d（config refresh_ttl_sec 默认值）。
	RefreshTTLSec = 604800
	// BcryptCost bcrypt 生产成本（config bcrypt_cost 生产默认，gotify 佐证）。
	BcryptCost = 10
	// BcryptTestCost bcrypt 测试成本（离线单元测试低 cost）。
	BcryptTestCost = 4
	// PasswordMaxBytes 密码长度上限（字节）；bcrypt 仅取前 72 字节，
	// 注册/改密需按此预校验防截断。
	PasswordMaxBytes = 72
	// MessageContentMaxBytes 消息 content 长度上限（字节，W2-M03）：
	// 32KB，须 ≤ ws.max_message_bytes（64KB 单帧上限）——content 只是帧
	// payload 的一部分，留余量给协议其他字段。
	MessageContentMaxBytes = 32768
	// PullLimitDefault 拉取/历史单次返回条数默认与上限（W2-M05 裁决 D3）：
	// limit∈[1,50] 有效，≤0 或 >50 归一 50（proto PullRequest.limit 注释同源）。
	PullLimitDefault = 50
	// PendingMarkerTTL seq 在途标记 TTL（W2-M05-C2 裁决 D10）：NextPending
	// 登记的 conv:{id}:pend:{seq} 键的存活上界。不变式：必须 > 最坏提交延迟
	// （NextPending 至 MySQL Create 提交的可观测间隔）——标记先于提交过期
	// 会使迟到提交在窗口外被拉取侧按永久空洞跳过而丢失。M07 实测写路径
	// p99=20.5ms、同相位突发档 221.9ms，余量 ~45×（按突发档）；W3 观测项
	// （提交延迟若逼近 TTL 须上调本值）。
	PendingMarkerTTL = 10 * time.Second
	// GroupNameMaxRunes 群名长度上限（字符，W3-M01 D8）：对齐 DDL
	// group_info.name VARCHAR(64) 字符口径，服务端预校验防 DB 截断。
	GroupNameMaxRunes = 64
	// GroupAnnouncementMaxRunes 群公告长度上限（字符，W3-M01 D8）：对齐
	// DDL group_info.announcement VARCHAR(512)；空串=清空公告（合法输入）。
	GroupAnnouncementMaxRunes = 512
	// GroupMuteMaxDurationSec 禁言时长上限（秒，W3-M01 D8）：30 天封顶，
	// 0=解禁；不提供永久禁言（用 30 天上限替代，到期自动解禁零清理任务）。
	GroupMuteMaxDurationSec = 2592000
)
