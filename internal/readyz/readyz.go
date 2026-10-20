// Package readyz 依赖就绪探针（W4-M05 plan D8）：/readyz 返回 200 当且仅当
// 全部依赖探活通过——与静态存活的 /healthz 分级（healthz=进程活着；readyz=
// 依赖就绪可服务）。logic 无依赖面不设 readyz（CAP-505「明确不支持」）。
//
// 语义：任一 Checker 失败 → 503 + 失败项名清单（text/plain，不泄漏错误
// 细节——内部信息不出探针面）；全过 → 200 "ok"。单项探活超时 2s（探针
// 不得拖垮服务：超时视为该项失败）。探针路径由装配层挂载；本包自带零
// 日志（探针高频轮询，成功路径静默——gotify 健康探针日志豁免先例）。
package readyz

import (
	"context"
	"net/http"
	"strings"
	"time"
)

// probeTimeout 单项依赖探活超时上限（plan D8：探针 2s）。
const probeTimeout = 2 * time.Second

// Checker 单项依赖探活：Name 为失败清单展示名（如 "mysql"），Check 为
// 探活函数（Ping 语义，nil=健康）。Check 为 nil 视为恒过（占位登记）。
type Checker struct {
	Name  string
	Check func(ctx context.Context) error
}

// New 构造 readyz HTTP handler：逐项以 2s 超时探活；全过 200 "ok"，
// 任一失败 503 + "readyz: not ready (mysql, redis)" 形态的失败项清单。
// 零 Checker 时恒 200（退化语义：无依赖面即就绪——调用方不该这么装配，
// 但行为确定不 panic）。
func New(checkers ...Checker) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		failed := make([]string, 0, len(checkers))
		for _, c := range checkers {
			if c.Check == nil {
				continue
			}
			cctx, cancel := context.WithTimeout(ctx, probeTimeout)
			err := c.Check(cctx)
			cancel()
			if err != nil {
				failed = append(failed, c.Name)
			}
		}
		if len(failed) > 0 {
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = w.Write([]byte("not ready (" + strings.Join(failed, ", ") + ")"))
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
}
