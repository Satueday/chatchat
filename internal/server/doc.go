// Package server 实现 WS 网关连接管理（W2-M02，spec.md §8.5）：握手 JWT
// 鉴权、每连接读写协程 + 带缓冲 send channel + 写超时断连、心跳 cmd 帧
// 与超时踢出、ConnManager 注册表与 Redis 在线状态。见 server.go 包总注。
package server
