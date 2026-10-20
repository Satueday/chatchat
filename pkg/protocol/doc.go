// Package protocol 定义 ChatChat 的 protobuf 通信协议：Frame 信封 + cmd 命令字
// 分发 + ChatMessage/PullRequest/PullResponse/Signal 消息体（spec.md §5）。
// 本包为纯编解码契约层：仅依赖 protobuf runtime，不 import 业务框架、不建立
// 网络连接；编解码 API 即 google.golang.org/protobuf/proto 的 Marshal/Unmarshal。
// 生成物 protocol.pb.go 由 make proto（docker 化 protoc）产出，禁止手改。
package protocol
