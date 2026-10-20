package log

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"chatchat/config"
)

// Logger 包装 *zap.Logger 并持有 file 模式下的文件句柄，
// 保证资源有所有者与关闭路径：调用方 defer Logger.Close()（Sync + 关 fd）。
type Logger struct {
	*zap.Logger
	closer io.Closer
}

// Close 同步缓冲并释放 file 模式的文件句柄（stdout 模式 closer 为 nil）。
func (l *Logger) Close() error {
	err := l.Sync()
	if l.closer != nil {
		if cerr := l.closer.Close(); err == nil {
			err = cerr
		}
	}
	return err
}

type options struct {
	service    string
	outputPath string // file 模式输出路径（默认 logs/chatchat.log，仅供测试注入）
}

// Option 定制日志构造行为。
type Option func(*options)

// WithService 设置固定字段 service 的值（默认 "chatchat"）。
func WithService(name string) Option {
	return func(o *options) { o.service = name }
}

// withOutputPath 覆盖 file 模式输出路径，仅供测试注入临时目录（非导出，保持导出面最小）。
func withOutputPath(path string) Option {
	return func(o *options) { o.outputPath = path }
}

// productionEncoderConfig：ISO8601 UTC 时间戳、大写字段名。
func productionEncoderConfig() zapcore.EncoderConfig {
	return zapcore.EncoderConfig{
		TimeKey:        "TS",
		LevelKey:       "LEVEL",
		NameKey:        "LOGGER",
		CallerKey:      "CALLER",
		FunctionKey:    zapcore.OmitKey,
		MessageKey:     "MSG",
		StacktraceKey:  "STACKTRACE",
		LineEnding:     zapcore.DefaultLineEnding,
		EncodeLevel:    zapcore.CapitalLevelEncoder,
		EncodeTime:     iso8601UTCTimeEncoder,
		EncodeDuration: zapcore.SecondsDurationEncoder,
		EncodeCaller:   zapcore.ShortCallerEncoder,
	}
}

// iso8601UTCTimeEncoder 以 ISO8601 输出 UTC 时间戳（zapcore.ISO8601TimeEncoder
// 使用本地时区，与日志时间统一 UTC 的约定不符，故显式转换）。
func iso8601UTCTimeEncoder(t time.Time, enc zapcore.PrimitiveArrayEncoder) {
	enc.AppendString(t.UTC().Format("2006-01-02T15:04:05.000Z0700"))
}

// New 按 cfg.Log 构造 zap Logger：
// level 经 zapcore.ParseLevel 解析（失败返回含原值的错误）；
// encoder json（默认）或 console；output stdout（默认）或 file（logs/chatchat.log，
// 追加写；轮转为扩展点，本期不实现）。
// 注意：config 校验已保证 level/encoder/output 合法，但 config.Log 可被直接构造
// （绕过 config.Load），New 仍须自行防御。
func New(cfg config.Log, opts ...Option) (*Logger, error) {
	o := &options{service: "chatchat", outputPath: filepath.Join("logs", "chatchat.log")}
	for _, opt := range opts {
		opt(o)
	}

	level, err := zapcore.ParseLevel(cfg.Level)
	if err != nil {
		return nil, fmt.Errorf("log.level: 非法值 %q: %w", cfg.Level, err)
	}

	var enc zapcore.Encoder
	switch cfg.Encoder {
	case "json", "": // json 为默认
		enc = zapcore.NewJSONEncoder(productionEncoderConfig())
	case "console":
		enc = zapcore.NewConsoleEncoder(productionEncoderConfig())
	default:
		// config 校验后不应到达此处；防御性保留。
		return nil, fmt.Errorf("log.encoder: 非法值 %q，允许 json|console", cfg.Encoder)
	}

	var sink zapcore.WriteSyncer
	var closer io.Closer
	switch cfg.Output {
	case "stdout", "": // stdout 为默认
		sink = os.Stdout
	case "file":
		// 目录固定相对 logs/；轮转为扩展点，本期不实现。
		if err := os.MkdirAll(filepath.Dir(o.outputPath), 0o755); err != nil {
			return nil, fmt.Errorf("log.output: 创建日志目录失败: %w", err)
		}
		f, err := os.OpenFile(o.outputPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
		if err != nil {
			return nil, fmt.Errorf("log.output: 打开日志文件失败: %w", err)
		}
		sink = zapcore.AddSync(f)
		closer = f
	default:
		return nil, fmt.Errorf("log.output: 非法值 %q，允许 stdout|file", cfg.Output)
	}

	core := zapcore.NewCore(enc, sink, level)
	l := zap.New(core,
		zap.AddCaller(),
		zap.Fields(zap.String("service", o.service)),
	)
	return &Logger{Logger: l, closer: closer}, nil
}

// WithTraceID 为 logger 附加 trace_id 字段（Week 2 链路接通，当前预留）。
func WithTraceID(l *zap.Logger, traceID string) *zap.Logger {
	return l.With(zap.String("trace_id", traceID))
}
