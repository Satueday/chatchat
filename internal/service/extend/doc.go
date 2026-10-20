// Package extend 扩展模块插件位（W4-M06 契约预留，ADR-0013）：
// WebRTC 信令已落地（signal 子包）；media/bot/feed/pay 四模块契约草案
// 见 docs/extend-contracts.md（状态 Draft——实现模块启动时须复核）。
//
// 插件位规约摘要（全文 docs/extend-contracts.md §1，八条）：
// 接入形态=独立服务+MQ/RPC 挂接（不取进程内 .so，核心链路零侵入）；
// 能力自报 interface 草案与元信息规约；宿主窄接口白名单=MQ 信封+gRPC/REST
// 契约面（禁直连核心表、禁直发 WS）；生命周期与降级=不可达不阻塞核心
// （best-effort 摘除）；消息必经宿主管道（seq/落库/投递链，禁扩展自行
// 编号直推）；ext 命名空间=模块前缀（宿主保留 chatchat.）；本包内子包
// +doc.go 三段式（职责/形态/分层约束——signal/doc.go 先例）。
//
// 责任模块 W4；四模块填实周期未排（契约 Draft）。
package extend
