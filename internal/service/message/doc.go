// Package message 消息业务与投递（W2-M03 落实）：单聊 Logic 服务
// （校验/服务端覆写/seq/落库链路，见 service.go）与 Deliverer 投递契约
// （plan.md §4：W2 noop / M04 内存实现 / W3 Kafka 实现，接口不变）。
// proto 类型止步本包入口（解码 ChatMessage 后转 model.Message 进 DAO）。
package message
