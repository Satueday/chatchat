// Package signal 实现 WebRTC 1v1 信令服务核心（W4-M04，ADR-0011）：
// 五型 Signal 帧（invite/offer/answer/ICE/hangup）的会话校验与转发路由。
//
// 形态（ADR-0011 D1）：进程内数据面（生产 WS 路径——wire 分派 → WSAdapter
// → Service → PeerRouter 回投对端连接）+ gRPC stream 契约面（spec §8.4
// 字面——Exchange 双向流，loopback 监听，与 WS 面共享同一 Service 核心）。
//
// 分层约束：本包不 import gin/gorm/sarama/internal/server——对连接注册表
// 的依赖经 PeerRouter/ConnManagerView 窄接口结构化满足（ConnManager 先例，
// 装配层连接）；SDP/ICE 载荷透传不解析（不读不验）；信令帧不排序不聚合
// （trickle ICE 无序事实）；isGroup 不校验不实现（仅预留透传）。
package signal
