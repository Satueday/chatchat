// Package config 是全项目唯一配置面（W1-M02）：Config 结构体各段对齐
// W1-M00-plan.md §6.4 字段表（零漂移）及后续模块冻结表（W2-M02 [ws]、
// W2-M04 [deliver]、W2-M06 [logic]/[pushpull]、W3-M01 [group]、
// W3-M02 [friend]、W3-M03 [kafka]、W3-M06 [inbox]、W4-M01 [hotdetect] 扩展），
// 提供 viper 加载（toml 覆盖内置默认值，CHATCHAT_ 前缀环境变量覆盖文件）
// 与字段级校验（校验失败即退出，错误含字段路径）。
//
// 本包零业务依赖：不 import gin/gorm/net；无 init()；无包级可变全局。
//
// 安全约定：敏感项（MySQL.Password、Redis.Password、JWT.Secret）一律使用
// SecretString 类型，其 String() 返回 "[REDACTED]"，因此 zap.Any 整体 dump
// 也不会泄漏明文；但**禁止将整个配置 dump 入日志**——本约定为类型保险之外的
// 第二道防线，新代码 review 时须双确认。
package config

import (
	"errors"
	"fmt"
	"os"
	"strings"

	"github.com/spf13/viper"
)

// SecretString 承载敏感配置值：String() 恒返回掩码，防止 fmt/%v/zap.Any 泄漏；
// Value() 返回明文，仅供连接构造（W1-M05 MySQL/Redis、W1-M06 JWT）使用，
// 禁止将 Value() 的返回值写入任何日志或错误信息。
type SecretString string

// String 实现 fmt.Stringer：恒返回掩码，任何日志路径只见 "[REDACTED]"。
func (s SecretString) String() string { return "[REDACTED]" }

// Value 返回明文。仅供连接构造（M05/M06）使用，禁止入日志。
func (s SecretString) Value() string { return string(s) }

// MarshalText 双保险：序列化（JSON/yaml 等 encoding.TextMarshaler 路径）同样只输出掩码。
func (s SecretString) MarshalText() ([]byte, error) { return []byte("[REDACTED]"), nil }

// Server 段：HTTP 监听与运行模式。
type Server struct {
	Addr string `mapstructure:"addr"`
	Mode string `mapstructure:"mode"` // debug|release|test
}

// Log 段：结构化日志构造参数（pkg/log 消费）。
type Log struct {
	Level   string `mapstructure:"level"`   // debug|info|warn|error
	Encoder string `mapstructure:"encoder"` // json|console
	Output  string `mapstructure:"output"`  // stdout|file
}

// MySQL 段：连接与连接池参数。
type MySQL struct {
	Host               string       `mapstructure:"host"`
	Port               int          `mapstructure:"port"`
	User               string       `mapstructure:"user"`
	Password           SecretString `mapstructure:"password"`
	Database           string       `mapstructure:"database"`
	Charset            string       `mapstructure:"charset"`
	ParseTime          bool         `mapstructure:"parse_time"`
	Loc                string       `mapstructure:"loc"`
	MaxOpenConns       int          `mapstructure:"max_open_conns"`
	MaxIdleConns       int          `mapstructure:"max_idle_conns"`
	ConnMaxLifetimeSec int          `mapstructure:"conn_max_lifetime_sec"`
}

// Redis 段：连接与池参数。
type Redis struct {
	Addr     string       `mapstructure:"addr"`
	Password SecretString `mapstructure:"password"`
	DB       int          `mapstructure:"db"`
	PoolSize int          `mapstructure:"pool_size"`
}

// Kafka 段：broker 地址与通道命名（W1 占位 brokers；W3-M03 增 topic/
// consumer_group——命名冻结于 W3-M03-plan D2 / ADR-0007：普通投递 topic
// chatchat-message（分区键 conversationId 同会话保序）、消费组
// chatchat-pushpull（pushpull 服务按组水平扩展；hot topic W4 预留不入配置）；
// W3-M05 增 push_topic——在线推送落地通道（pushpull→gateway，ADR-0009：
// 分区键 conversationId，gateway 侧按 per-gateway 广播组消费）；
// W4-M02 增 hot_topic/hot_consumer_group——热点群专属通道（spec §7.2
// 冻结名 chatchat-hot-group；独立消费组 chatchat-pushpull-hot 与普通组
// rebalance/lag 域分离，ADR-0010 D1）。
type Kafka struct {
	Brokers          []string `mapstructure:"brokers"`
	Topic            string   `mapstructure:"topic"`              // 生产/消费的普通投递 topic
	ConsumerGroup    string   `mapstructure:"consumer_group"`     // pushpull 消费组名
	PushTopic        string   `mapstructure:"push_topic"`         // 在线推送落地 topic（pushpull 生产、gateway 消费）
	HotTopic         string   `mapstructure:"hot_topic"`          // 热点群专属 topic（gateway 生产、pushpull hot 消费者消费）
	HotConsumerGroup string   `mapstructure:"hot_consumer_group"` // hot 消费组名（与 ConsumerGroup 分离）
}

// JWT 段：签名密钥（仅 env 注入）与 TTL。
type JWT struct {
	Secret        SecretString `mapstructure:"secret"` // 仅 env：CHATCHAT_JWT_SECRET
	AccessTTLSec  int          `mapstructure:"access_ttl_sec"`
	RefreshTTLSec int          `mapstructure:"refresh_ttl_sec"`
}

// User 段：用户模块开关与默认值。
type User struct {
	BcryptCost        int    `mapstructure:"bcrypt_cost"`
	VerifyCodeEnabled bool   `mapstructure:"verify_code_enabled"`
	VerifyCodeMock    string `mapstructure:"verify_code_mock"`
}

// WS 段：WS 网关连接参数（W2-M02，spec.md §8.5；对齐 W2-M02-plan §6.2 冻结表）。
type WS struct {
	Addr                string `mapstructure:"addr"`                  // 网关监听地址（与 api :8080 分离）
	HeartbeatTimeoutSec int    `mapstructure:"heartbeat_timeout_sec"` // 读空闲超时=心跳超时
	WriteTimeoutSec     int    `mapstructure:"write_timeout_sec"`     // 单次写超时
	SendChannelSize     int    `mapstructure:"send_channel_size"`     // send channel 缓冲
	MaxMessageBytes     int64  `mapstructure:"max_message_bytes"`     // 单帧上限
	OnlineTTLSec        int    `mapstructure:"online_ttl_sec"`        // 在线键 TTL（须 > 心跳超时）
	GatewayID           string `mapstructure:"gateway_id"`            // 空=os.Hostname() 推导
}

// Deliver 段：投递驱动与重推参数（W2-M04，spec.md §6.2；W3-M04 增 driver
// 开关）。退避倍数固定 2（D10：少旋钮，公式 base×2^n 内建于实现）。
// driver=kafka（默认，生产真实形态：mq.Producer + KafkaDeliverer，先落库
// 后写 MQ，ADR-0008）|memory（降级开关：MemoryDeliverer 在线推+重推）。
// retry_* 两参数仅 memory driver 生效（kafka 路径无重推——至少一次语义
// 由消费侧承担，M05）。
type Deliver struct {
	Driver      string `mapstructure:"driver"`        // kafka|memory，env: CHATCHAT_DELIVER_DRIVER
	RetryBaseMS int    `mapstructure:"retry_base_ms"` // 首次重推等待与退避基数（plan §7.4「1s 起步」），仅 memory 生效
	RetryMax    int    `mapstructure:"retry_max"`     // 重推次数上限（D3：另含首发共最多 1+RetryMax 次 Push），仅 memory 生效
}

// Logic 段：logic 服务健康端点监听（W2-M06；W2 无业务监听，W3 Kafka 消费
// 参数届时增补，plan §4 裁决 D3「不加多余旋钮」）。
type Logic struct {
	Addr string `mapstructure:"addr"` // env: CHATCHAT_LOGIC_ADDR
}

// PushPull 段：pushpull 服务参数（W2-M06 健康端点；W4-M02 增 hot 旁路
// 协程池参数——hot_notify_workers/hot_notify_queue，W4-M02-plan D5/D10：
// 池是「可丢弃旁路」（轻通知/未来异步面，M02 阶段池内无真实业务任务，
// 机制测试锁定 C8 门禁；Timeline 写入永不入池）。
type PushPull struct {
	Addr             string `mapstructure:"addr"`               // env: CHATCHAT_PUSHPULL_ADDR
	HotNotifyWorkers int    `mapstructure:"hot_notify_workers"` // 旁路 worker 数 [1,64]，env: CHATCHAT_PUSHPULL_HOT_NOTIFY_WORKERS
	HotNotifyQueue   int    `mapstructure:"hot_notify_queue"`   // 旁路队列容量 [1,65536]（满即丢+计数），env: CHATCHAT_PUSHPULL_HOT_NOTIFY_QUEUE
}

// Group 段：群组模块参数（W3-M01，W3-M01-plan §6.4 冻结表）。
type Group struct {
	ReconcileIntervalSec int `mapstructure:"reconcile_interval_sec"` // 成员数对账周期（秒），env: CHATCHAT_GROUP_RECONCILE_INTERVAL_SEC
}

// Friend 段：好友模块参数（W3-M02，W3-M02-plan §6.4 冻结表）。
type Friend struct {
	RequireFriendForChat bool `mapstructure:"require_friend_for_chat"` // 单聊好友关系门槛开关（plan §8.4 默认策略：默认开、配置可放开），env: CHATCHAT_FRIEND_REQUIRE_FOR_CHAT
}

// Inbox 段：信箱读侧参数（W3-M06，W3-M06-plan §2 CAP-005 冻结表）。
type Inbox struct {
	RoamDays int `mapstructure:"roam_days"` // 漫游窗口天数（清单过滤：窗口外投递记录不出现在补齐发现面；拉取/历史不受限），env: CHATCHAT_INBOX_ROAM_DAYS
}

// HotDetect 段：热点群打标参数（W4-M01，W4-M01-plan §4 D10 冻结表）。
// 对齐约束（validate 强制）：window_min*60 与 aggregate_interval_sec 均须
// 为 bucket_sec 整数倍（桶边界对齐，D1/D9），且窗口桶数 ≤3600（MGET 键数
// 护栏）。验收注入组合：threshold=5、interval=10、window=1、bucket=1、N=2。
type HotDetect struct {
	ThresholdMsgPerMin   int `mapstructure:"threshold_msg_per_min"`  // 打标阈值（msg/min，窗口均值 ≥ 阈值判 hot，含等号），env: CHATCHAT_HOTDETECT_THRESHOLD_MSG_PER_MIN
	AggregateIntervalSec int `mapstructure:"aggregate_interval_sec"` // 聚合周期（秒，api 进程 Detector.Run ticker；亦为 hot_group_stat 窗口粒度），env: CHATCHAT_HOTDETECT_AGGREGATE_INTERVAL_SEC
	WindowMin            int `mapstructure:"window_min"`             // 滑动窗口（分钟，rate 分母），env: CHATCHAT_HOTDETECT_WINDOW_MIN
	BucketSec            int `mapstructure:"bucket_sec"`             // 计数桶宽（秒，[1,60]，桶=unixSec/bucket_sec 全局对齐），env: CHATCHAT_HOTDETECT_BUCKET_SEC
	LowPeriodsToUnmark   int `mapstructure:"low_periods_to_unmark"`  // 摘标所需连续低周期数 N（streak ≥ N 摘标；遇高周期清零），env: CHATCHAT_HOTDETECT_LOW_PERIODS_TO_UNMARK
}

// Signal 段：WebRTC 信令参数（W4-M04，W4-M04-plan §4 D10 冻结表；ADR-0011）。
// addr 强制 loopback（形态 C 前不对外——validate 拒绝非 127.0.0.1/localhost/
// [::1] 绑定，CAP-307「对外监听」不支持项）。
type Signal struct {
	Addr          string `mapstructure:"addr"`            // gRPC Exchange 契约面监听（loopback），env: CHATCHAT_SIGNAL_ADDR
	SessionTTLSec int    `mapstructure:"session_ttl_sec"` // 未应答会话 TTL（秒，[10,3600]，sweep 兜底回收），env: CHATCHAT_SIGNAL_SESSION_TTL_SEC
}

// Config 为十六段配置顶层结构（字段表唯一事实源：W1-M00-plan.md §6.4 +
// W2-M02-plan §6.2 + W2-M04-plan §6.3 + W2-M06-plan §6.1 + W3-M01-plan §6.4 +
// W3-M02-plan §6.4 + W3-M03-plan §6.4 [kafka] 扩展 + W3-M06-plan §2 [inbox] +
// W4-M01-plan §4 D10 [hotdetect] + W4-M04-plan §4 D10 [signal]）。
type Config struct {
	Server    Server
	Log       Log
	MySQL     MySQL
	Redis     Redis
	Kafka     Kafka
	JWT       JWT
	User      User
	WS        WS
	Deliver   Deliver
	Logic     Logic
	PushPull  PushPull
	Group     Group
	Friend    Friend
	Inbox     Inbox
	HotDetect HotDetect
	Signal    Signal
}

// defaults 返回全部内置默认值（与 W1-M00-plan.md §6.4 toml 逐一对应）。
func defaults() map[string]any {
	return map[string]any{
		"server.addr":                      ":8080",
		"server.mode":                      "release",
		"log.level":                        "info",
		"log.encoder":                      "json",
		"log.output":                       "stdout",
		"mysql.host":                       "127.0.0.1",
		"mysql.port":                       3306,
		"mysql.user":                       "root",
		"mysql.password":                   "",
		"mysql.database":                   "chatchat",
		"mysql.charset":                    "utf8mb4",
		"mysql.parse_time":                 true,
		"mysql.loc":                        "UTC",
		"mysql.max_open_conns":             20,
		"mysql.max_idle_conns":             10,
		"mysql.conn_max_lifetime_sec":      3600,
		"redis.addr":                       "127.0.0.1:6379",
		"redis.password":                   "",
		"redis.db":                         0,
		"redis.pool_size":                  50,
		"kafka.brokers":                    []string{"127.0.0.1:9092"},
		"kafka.topic":                      "chatchat-message",
		"kafka.consumer_group":             "chatchat-pushpull",
		"kafka.push_topic":                 "chatchat-push",
		"kafka.hot_topic":                  "chatchat-hot-group",
		"kafka.hot_consumer_group":         "chatchat-pushpull-hot",
		"jwt.access_ttl_sec":               7200,
		"jwt.refresh_ttl_sec":              604800,
		"user.bcrypt_cost":                 10,
		"user.verify_code_enabled":         false,
		"user.verify_code_mock":            "000000",
		"ws.addr":                          ":8081",
		"ws.heartbeat_timeout_sec":         60,
		"ws.write_timeout_sec":             10,
		"ws.send_channel_size":             256,
		"ws.max_message_bytes":             65536,
		"ws.online_ttl_sec":                120,
		"ws.gateway_id":                    "",
		"deliver.driver":                   "kafka",
		"deliver.retry_base_ms":            1000,
		"deliver.retry_max":                3,
		"logic.addr":                       ":8082",
		"pushpull.addr":                    ":8083",
		"pushpull.hot_notify_workers":      4,
		"pushpull.hot_notify_queue":        1024,
		"group.reconcile_interval_sec":     300,
		"friend.require_friend_for_chat":   true,
		"inbox.roam_days":                  7,
		"hotdetect.threshold_msg_per_min":  50,
		"hotdetect.aggregate_interval_sec": 60,
		"hotdetect.window_min":             60,
		"hotdetect.bucket_sec":             60,
		"hotdetect.low_periods_to_unmark":  3,
		"signal.addr":                      "127.0.0.1:8084",
		"signal.session_ttl_sec":           60,
	}
}

// envKeys 为全叶子键 → CHATCHAT_ 环境变量键的显式绑定表
// （杜绝 AutomaticEnv+Unmarshal 失效模式，go-chat 教训）。
// kafka.brokers 不在此表：切片的 env 解析单独处理（逗号分隔）。
var envKeys = []struct{ key, env string }{
	{"server.addr", "CHATCHAT_SERVER_ADDR"},
	{"server.mode", "CHATCHAT_SERVER_MODE"},
	{"log.level", "CHATCHAT_LOG_LEVEL"},
	{"log.encoder", "CHATCHAT_LOG_ENCODER"},
	{"log.output", "CHATCHAT_LOG_OUTPUT"},
	{"mysql.host", "CHATCHAT_MYSQL_HOST"},
	{"mysql.port", "CHATCHAT_MYSQL_PORT"},
	{"mysql.user", "CHATCHAT_MYSQL_USER"},
	{"mysql.password", "CHATCHAT_MYSQL_PASSWORD"},
	{"mysql.database", "CHATCHAT_MYSQL_DATABASE"},
	{"mysql.charset", "CHATCHAT_MYSQL_CHARSET"},
	{"mysql.parse_time", "CHATCHAT_MYSQL_PARSE_TIME"},
	{"mysql.loc", "CHATCHAT_MYSQL_LOC"},
	{"mysql.max_open_conns", "CHATCHAT_MYSQL_MAX_OPEN_CONNS"},
	{"mysql.max_idle_conns", "CHATCHAT_MYSQL_MAX_IDLE_CONNS"},
	{"mysql.conn_max_lifetime_sec", "CHATCHAT_MYSQL_CONN_MAX_LIFETIME_SEC"},
	{"redis.addr", "CHATCHAT_REDIS_ADDR"},
	{"redis.password", "CHATCHAT_REDIS_PASSWORD"},
	{"redis.db", "CHATCHAT_REDIS_DB"},
	{"redis.pool_size", "CHATCHAT_REDIS_POOL_SIZE"},
	{"kafka.topic", "CHATCHAT_KAFKA_TOPIC"},
	{"kafka.consumer_group", "CHATCHAT_KAFKA_CONSUMER_GROUP"},
	{"kafka.push_topic", "CHATCHAT_KAFKA_PUSH_TOPIC"},
	{"kafka.hot_topic", "CHATCHAT_KAFKA_HOT_TOPIC"},
	{"kafka.hot_consumer_group", "CHATCHAT_KAFKA_HOT_CONSUMER_GROUP"},
	{"jwt.access_ttl_sec", "CHATCHAT_JWT_ACCESS_TTL_SEC"},
	{"jwt.refresh_ttl_sec", "CHATCHAT_JWT_REFRESH_TTL_SEC"},
	{"user.bcrypt_cost", "CHATCHAT_USER_BCRYPT_COST"},
	{"user.verify_code_enabled", "CHATCHAT_USER_VERIFY_CODE_ENABLED"},
	{"user.verify_code_mock", "CHATCHAT_USER_VERIFY_CODE_MOCK"},
	{"ws.addr", "CHATCHAT_WS_ADDR"},
	{"ws.heartbeat_timeout_sec", "CHATCHAT_WS_HEARTBEAT_TIMEOUT_SEC"},
	{"ws.write_timeout_sec", "CHATCHAT_WS_WRITE_TIMEOUT_SEC"},
	{"ws.send_channel_size", "CHATCHAT_WS_SEND_CHANNEL_SIZE"},
	{"ws.max_message_bytes", "CHATCHAT_WS_MAX_MESSAGE_BYTES"},
	{"ws.online_ttl_sec", "CHATCHAT_WS_ONLINE_TTL_SEC"},
	{"ws.gateway_id", "CHATCHAT_WS_GATEWAY_ID"},
	{"deliver.driver", "CHATCHAT_DELIVER_DRIVER"},
	{"deliver.retry_base_ms", "CHATCHAT_DELIVER_RETRY_BASE_MS"},
	{"deliver.retry_max", "CHATCHAT_DELIVER_RETRY_MAX"},
	{"logic.addr", "CHATCHAT_LOGIC_ADDR"},
	{"pushpull.addr", "CHATCHAT_PUSHPULL_ADDR"},
	{"pushpull.hot_notify_workers", "CHATCHAT_PUSHPULL_HOT_NOTIFY_WORKERS"},
	{"pushpull.hot_notify_queue", "CHATCHAT_PUSHPULL_HOT_NOTIFY_QUEUE"},
	{"group.reconcile_interval_sec", "CHATCHAT_GROUP_RECONCILE_INTERVAL_SEC"},
	{"friend.require_friend_for_chat", "CHATCHAT_FRIEND_REQUIRE_FOR_CHAT"},
	{"inbox.roam_days", "CHATCHAT_INBOX_ROAM_DAYS"},
	{"hotdetect.threshold_msg_per_min", "CHATCHAT_HOTDETECT_THRESHOLD_MSG_PER_MIN"},
	{"hotdetect.aggregate_interval_sec", "CHATCHAT_HOTDETECT_AGGREGATE_INTERVAL_SEC"},
	{"hotdetect.window_min", "CHATCHAT_HOTDETECT_WINDOW_MIN"},
	{"hotdetect.bucket_sec", "CHATCHAT_HOTDETECT_BUCKET_SEC"},
	{"hotdetect.low_periods_to_unmark", "CHATCHAT_HOTDETECT_LOW_PERIODS_TO_UNMARK"},
	{"signal.addr", "CHATCHAT_SIGNAL_ADDR"},
	{"signal.session_ttl_sec", "CHATCHAT_SIGNAL_SESSION_TTL_SEC"},
}

// Load 加载配置：内置默认值 → 可选 toml 覆盖 → CHATCHAT_ env 覆盖 → 字段级校验。
// path 为空时取 "config.toml"；文件缺失不算错误（全默认值继续），
// 解析失败与校验失败返回错误（校验错误为 errors.Join 聚合的多条字段级错误）。
// jwt.secret 不经文件路径：Load 内用 env CHATCHAT_JWT_SECRET 覆写。
func Load(path string) (*Config, error) {
	if path == "" {
		path = "config.toml"
	}

	v := viper.New()

	for k, d := range defaults() {
		v.SetDefault(k, d)
	}

	// 文件存在才读；缺失属预期（默认值继续），解析失败则报错含路径。
	if _, err := os.Stat(path); err == nil {
		v.SetConfigFile(path)
		v.SetConfigType("toml")
		if err := v.ReadInConfig(); err != nil {
			return nil, fmt.Errorf("读取配置文件 %s 失败: %w", path, err)
		}
	}

	// 表驱动显式 BindEnv：env > 文件 > 默认。
	for _, b := range envKeys {
		_ = v.BindEnv(b.key, b.env)
	}

	var cfg Config
	if err := v.Unmarshal(&cfg); err != nil {
		return nil, fmt.Errorf("解析配置失败: %w", err)
	}

	// kafka.brokers：env（逗号分隔）优先，其次 toml，再默认值。
	if raw := strings.TrimSpace(os.Getenv("CHATCHAT_KAFKA_BROKERS")); raw != "" {
		cfg.Kafka.Brokers = strings.Split(raw, ",")
	}

	// jwt.secret 不走文件路径：记录文件值（用于判「文件值无效」），再用 env 覆写。
	fileSecret := cfg.JWT.Secret
	cfg.JWT.Secret = SecretString(os.Getenv("CHATCHAT_JWT_SECRET"))

	if err := validate(&cfg, fileSecret); err != nil {
		return nil, err
	}
	return &cfg, nil
}

// validate 执行全部字段级校验规则，多错误以 errors.Join 聚合，
// 单条格式 "<段>.<字段>: <原因>"（可附 env 键提示）。
// fileSecret 为 toml 文件中的 jwt.secret 原值：非空即无效（secret 仅认 env）。
func validate(cfg *Config, fileSecret SecretString) error {
	var errs []error
	add := func(format string, args ...any) {
		errs = append(errs, fmt.Errorf(format, args...))
	}

	switch cfg.Server.Mode {
	case "debug", "release", "test":
	default:
		add("server.mode: 非法值 %q，允许 debug|release|test (env: CHATCHAT_SERVER_MODE)", cfg.Server.Mode)
	}

	switch cfg.Log.Level {
	case "debug", "info", "warn", "error":
	default:
		add("log.level: 非法值 %q，允许 debug|info|warn|error (env: CHATCHAT_LOG_LEVEL)", cfg.Log.Level)
	}

	switch cfg.Log.Encoder {
	case "json", "console":
	default:
		add("log.encoder: 非法值 %q，允许 json|console (env: CHATCHAT_LOG_ENCODER)", cfg.Log.Encoder)
	}

	switch cfg.Log.Output {
	case "stdout", "file":
	default:
		add("log.output: 非法值 %q，允许 stdout|file (env: CHATCHAT_LOG_OUTPUT)", cfg.Log.Output)
	}

	if cfg.MySQL.Port < 1 || cfg.MySQL.Port > 65535 {
		add("mysql.port: %d 超出范围 [1,65535] (env: CHATCHAT_MYSQL_PORT)", cfg.MySQL.Port)
	}
	if cfg.MySQL.MaxOpenConns <= 0 {
		add("mysql.max_open_conns: %d 必须为正 (env: CHATCHAT_MYSQL_MAX_OPEN_CONNS)", cfg.MySQL.MaxOpenConns)
	}
	if cfg.MySQL.MaxIdleConns <= 0 {
		add("mysql.max_idle_conns: %d 必须为正 (env: CHATCHAT_MYSQL_MAX_IDLE_CONNS)", cfg.MySQL.MaxIdleConns)
	}
	if cfg.MySQL.MaxIdleConns > cfg.MySQL.MaxOpenConns {
		add("mysql.max_idle_conns: %d 不得大于 max_open_conns %d", cfg.MySQL.MaxIdleConns, cfg.MySQL.MaxOpenConns)
	}
	if cfg.MySQL.ConnMaxLifetimeSec <= 0 {
		add("mysql.conn_max_lifetime_sec: %d 必须为正 (env: CHATCHAT_MYSQL_CONN_MAX_LIFETIME_SEC)", cfg.MySQL.ConnMaxLifetimeSec)
	}

	if cfg.Redis.Addr == "" || !strings.Contains(cfg.Redis.Addr, ":") {
		add("redis.addr: %q 非空且须含 host:port (env: CHATCHAT_REDIS_ADDR)", cfg.Redis.Addr)
	}
	if cfg.Redis.PoolSize <= 0 {
		add("redis.pool_size: %d 必须为正 (env: CHATCHAT_REDIS_POOL_SIZE)", cfg.Redis.PoolSize)
	}

	if len(cfg.Kafka.Brokers) == 0 {
		add("kafka.brokers: 不得为空 (env: CHATCHAT_KAFKA_BROKERS，逗号分隔)")
	}
	if cfg.Kafka.Topic == "" {
		add("kafka.topic: 不得为空 (env: CHATCHAT_KAFKA_TOPIC)")
	}
	if cfg.Kafka.ConsumerGroup == "" {
		add("kafka.consumer_group: 不得为空 (env: CHATCHAT_KAFKA_CONSUMER_GROUP)")
	}
	if cfg.Kafka.PushTopic == "" {
		add("kafka.push_topic: 不得为空 (env: CHATCHAT_KAFKA_PUSH_TOPIC)")
	}
	if cfg.Kafka.HotTopic == "" {
		add("kafka.hot_topic: 不得为空 (env: CHATCHAT_KAFKA_HOT_TOPIC)")
	}
	if cfg.Kafka.HotConsumerGroup == "" {
		add("kafka.hot_consumer_group: 不得为空 (env: CHATCHAT_KAFKA_HOT_CONSUMER_GROUP)")
	}

	if cfg.JWT.AccessTTLSec <= 0 {
		add("jwt.access_ttl_sec: %d 必须为正 (env: CHATCHAT_JWT_ACCESS_TTL_SEC)", cfg.JWT.AccessTTLSec)
	}
	if cfg.JWT.RefreshTTLSec <= 0 {
		add("jwt.refresh_ttl_sec: %d 必须为正 (env: CHATCHAT_JWT_REFRESH_TTL_SEC)", cfg.JWT.RefreshTTLSec)
	}

	if cfg.User.BcryptCost < 4 || cfg.User.BcryptCost > 31 {
		add("user.bcrypt_cost: %d 超出范围 [4,31] (env: CHATCHAT_USER_BCRYPT_COST)", cfg.User.BcryptCost)
	}

	if cfg.WS.Addr == "" || !strings.Contains(cfg.WS.Addr, ":") {
		add("ws.addr: %q 非空且须含 host:port (env: CHATCHAT_WS_ADDR)", cfg.WS.Addr)
	}
	if cfg.WS.HeartbeatTimeoutSec < 1 || cfg.WS.HeartbeatTimeoutSec > 600 {
		add("ws.heartbeat_timeout_sec: %d 超出范围 [1,600] (env: CHATCHAT_WS_HEARTBEAT_TIMEOUT_SEC)", cfg.WS.HeartbeatTimeoutSec)
	}
	if cfg.WS.WriteTimeoutSec <= 0 {
		add("ws.write_timeout_sec: %d 必须为正 (env: CHATCHAT_WS_WRITE_TIMEOUT_SEC)", cfg.WS.WriteTimeoutSec)
	}
	if cfg.WS.SendChannelSize < 1 || cfg.WS.SendChannelSize > 65536 {
		add("ws.send_channel_size: %d 超出范围 [1,65536] (env: CHATCHAT_WS_SEND_CHANNEL_SIZE)", cfg.WS.SendChannelSize)
	}
	if cfg.WS.MaxMessageBytes < 1024 || cfg.WS.MaxMessageBytes > 16777216 {
		add("ws.max_message_bytes: %d 超出范围 [1024,16777216] (env: CHATCHAT_WS_MAX_MESSAGE_BYTES)", cfg.WS.MaxMessageBytes)
	}
	if cfg.WS.OnlineTTLSec <= cfg.WS.HeartbeatTimeoutSec {
		add("ws.online_ttl_sec: %d 必须大于 heartbeat_timeout_sec %d（TTL 由心跳续期兜底）",
			cfg.WS.OnlineTTLSec, cfg.WS.HeartbeatTimeoutSec)
	}

	switch cfg.Deliver.Driver {
	case "kafka", "memory":
	default:
		add("deliver.driver: 非法值 %q，允许 kafka|memory (env: CHATCHAT_DELIVER_DRIVER)", cfg.Deliver.Driver)
	}

	if cfg.Deliver.RetryBaseMS < 10 || cfg.Deliver.RetryBaseMS > 600000 {
		add("deliver.retry_base_ms: %d 超出范围 [10,600000] (env: CHATCHAT_DELIVER_RETRY_BASE_MS)",
			cfg.Deliver.RetryBaseMS)
	}
	if cfg.Deliver.RetryMax < 1 || cfg.Deliver.RetryMax > 10 {
		add("deliver.retry_max: %d 超出范围 [1,10] (env: CHATCHAT_DELIVER_RETRY_MAX)",
			cfg.Deliver.RetryMax)
	}

	if cfg.Logic.Addr == "" || !strings.Contains(cfg.Logic.Addr, ":") {
		add("logic.addr: %q 非空且须含 host:port (env: CHATCHAT_LOGIC_ADDR)", cfg.Logic.Addr)
	}

	if cfg.PushPull.Addr == "" || !strings.Contains(cfg.PushPull.Addr, ":") {
		add("pushpull.addr: %q 非空且须含 host:port (env: CHATCHAT_PUSHPULL_ADDR)", cfg.PushPull.Addr)
	}

	// [pushpull] hot 旁路池两键（W4-M02-plan D5/D10 冻结表）。
	if cfg.PushPull.HotNotifyWorkers < 1 || cfg.PushPull.HotNotifyWorkers > 64 {
		add("pushpull.hot_notify_workers: %d 超出范围 [1,64] (env: CHATCHAT_PUSHPULL_HOT_NOTIFY_WORKERS)",
			cfg.PushPull.HotNotifyWorkers)
	}
	if cfg.PushPull.HotNotifyQueue < 1 || cfg.PushPull.HotNotifyQueue > 65536 {
		add("pushpull.hot_notify_queue: %d 超出范围 [1,65536] (env: CHATCHAT_PUSHPULL_HOT_NOTIFY_QUEUE)",
			cfg.PushPull.HotNotifyQueue)
	}

	if cfg.Group.ReconcileIntervalSec < 10 || cfg.Group.ReconcileIntervalSec > 86400 {
		add("group.reconcile_interval_sec: %d 超出范围 [10,86400] (env: CHATCHAT_GROUP_RECONCILE_INTERVAL_SEC)",
			cfg.Group.ReconcileIntervalSec)
	}

	if cfg.Inbox.RoamDays < 1 || cfg.Inbox.RoamDays > 365 {
		add("inbox.roam_days: %d 超出范围 [1,365] (env: CHATCHAT_INBOX_ROAM_DAYS)",
			cfg.Inbox.RoamDays)
	}

	// [hotdetect] 段（W4-M01-plan §4 D10 冻结表）：范围 + 对齐三约束
	//（窗口/周期均为桶宽整数倍——桶边界对齐保证周期跨桶不相交（D9）与
	// 窗口桶序列确定（D1）；窗口桶数 ≤3600 为 MGET 键数护栏）。
	if cfg.HotDetect.ThresholdMsgPerMin < 1 || cfg.HotDetect.ThresholdMsgPerMin > 100000 {
		add("hotdetect.threshold_msg_per_min: %d 超出范围 [1,100000] (env: CHATCHAT_HOTDETECT_THRESHOLD_MSG_PER_MIN)",
			cfg.HotDetect.ThresholdMsgPerMin)
	}
	if cfg.HotDetect.AggregateIntervalSec < 1 || cfg.HotDetect.AggregateIntervalSec > 3600 {
		add("hotdetect.aggregate_interval_sec: %d 超出范围 [1,3600] (env: CHATCHAT_HOTDETECT_AGGREGATE_INTERVAL_SEC)",
			cfg.HotDetect.AggregateIntervalSec)
	}
	if cfg.HotDetect.WindowMin < 1 || cfg.HotDetect.WindowMin > 1440 {
		add("hotdetect.window_min: %d 超出范围 [1,1440] (env: CHATCHAT_HOTDETECT_WINDOW_MIN)",
			cfg.HotDetect.WindowMin)
	}
	if cfg.HotDetect.BucketSec < 1 || cfg.HotDetect.BucketSec > 60 {
		add("hotdetect.bucket_sec: %d 超出范围 [1,60] (env: CHATCHAT_HOTDETECT_BUCKET_SEC)",
			cfg.HotDetect.BucketSec)
	}
	if cfg.HotDetect.LowPeriodsToUnmark < 1 || cfg.HotDetect.LowPeriodsToUnmark > 100 {
		add("hotdetect.low_periods_to_unmark: %d 超出范围 [1,100] (env: CHATCHAT_HOTDETECT_LOW_PERIODS_TO_UNMARK)",
			cfg.HotDetect.LowPeriodsToUnmark)
	}
	// 对齐三约束仅在校验 BucketSec 范围有效（[1,60]，上方 add 已覆盖非法值
	// 并附字段路径与 env 提示）后执行——W4-M01 移交 P2-1 修复（W4-M02
	// plan D8）：bucket_sec=0 时下方三条取模/除法会整数除零 panic，短路后
	// validate 正常返回含 hotdetect.bucket_sec 字段路径的聚合错误。
	if cfg.HotDetect.BucketSec >= 1 {
		if windowSec := cfg.HotDetect.WindowMin * 60; windowSec%cfg.HotDetect.BucketSec != 0 {
			add("hotdetect.window_min: %d 分钟（%d 秒）必须为 bucket_sec %d 的整数倍（桶边界对齐）",
				cfg.HotDetect.WindowMin, windowSec, cfg.HotDetect.BucketSec)
		}
		if cfg.HotDetect.AggregateIntervalSec%cfg.HotDetect.BucketSec != 0 {
			add("hotdetect.aggregate_interval_sec: %d 必须为 bucket_sec %d 的整数倍（周期桶对齐，跨周期不相交）",
				cfg.HotDetect.AggregateIntervalSec, cfg.HotDetect.BucketSec)
		}
		if cfg.HotDetect.WindowMin*60/cfg.HotDetect.BucketSec > 3600 {
			add("hotdetect.window_min: 窗口桶数 %d 超出 MGET 键数护栏 3600 (bucket_sec=%d)",
				cfg.HotDetect.WindowMin*60/cfg.HotDetect.BucketSec, cfg.HotDetect.BucketSec)
		}
	}

	// [signal] 段（W4-M04-plan §4 D10 冻结表）：loopback 强制（禁止事项
	//「禁 gRPC 监听非 loopback」的 fail-fast 面——CAP-307）+ TTL 范围。
	// host 提取自 addr 末个冒号前（IPv6 [::1]:port 形态去方括号）；空 host
	//（":8084" 全接口绑定）与非 loopback 主机名均拒绝。
	if cfg.Signal.Addr == "" || !strings.Contains(cfg.Signal.Addr, ":") {
		add("signal.addr: %q 非空且须含 host:port (env: CHATCHAT_SIGNAL_ADDR)", cfg.Signal.Addr)
	} else if !isLoopbackHost(cfg.Signal.Addr) {
		add("signal.addr: %q 必须绑定 loopback（127.0.0.1/localhost/[::1]；形态 C 前不对外，ADR-0011）(env: CHATCHAT_SIGNAL_ADDR)",
			cfg.Signal.Addr)
	}
	if cfg.Signal.SessionTTLSec < 10 || cfg.Signal.SessionTTLSec > 3600 {
		add("signal.session_ttl_sec: %d 超出范围 [10,3600] (env: CHATCHAT_SIGNAL_SESSION_TTL_SEC)",
			cfg.Signal.SessionTTLSec)
	}

	// jwt.secret：文件值非空即无效（仅 env 注入）；env 非空且 ≥32 字节才通过。
	if fileSecret != "" {
		add("jwt.secret: 必须经环境变量 CHATCHAT_JWT_SECRET 注入")
	}
	if s := os.Getenv("CHATCHAT_JWT_SECRET"); len(s) < 32 {
		add("jwt.secret: 缺失或不足 32 字节 (env: CHATCHAT_JWT_SECRET)")
	}

	return errors.Join(errs...)
}

// isLoopbackHost 判定 addr 的 host 部分是否 loopback（W4-M04 [signal].
// addr 校验用）：末个冒号前取 host，IPv6 方括号形态去壳；允许
// 127.0.0.1 / localhost / ::1，其余（含空 host 全接口绑定）false。
func isLoopbackHost(addr string) bool {
	host := addr
	if i := strings.LastIndex(addr, ":"); i >= 0 {
		host = addr[:i]
	}
	host = strings.Trim(host, "[]")
	switch host {
	case "127.0.0.1", "localhost", "::1":
		return true
	}
	return false
}
