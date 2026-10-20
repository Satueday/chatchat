// Package log 提供基于 zap 的结构化日志构造（责任模块 W1-M02）。
//
// 零业务依赖基础设施：仅依赖 config.Log 段类型与 zap，不 import gin/gorm，
// 不持有业务状态。level/encoder/output 全部来自 config.Log；
// JSON 编码为默认，service 字段固定，trace_id 经 WithTraceID 预留（Week 2 接通）。
package log
