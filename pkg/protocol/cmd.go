package protocol

// Cmd 命令字（Header.cmd，int32）。分配段：1-99 平台保留；>=100 业务扩展预留。
// 值分配冻结于 W2-M01-plan.md §6.2，禁止挪用；语义合法性校验归消费方（网关/Logic）。
const (
	CmdHeartbeat    int32 = 1  // 心跳 ping（客户端 -> 服务端）
	CmdHeartbeatAck int32 = 2  // 心跳 pong（服务端 -> 客户端）
	CmdChat         int32 = 3  // 单聊消息（payload = ChatMessage）
	CmdGroupChat    int32 = 4  // 群聊消息（payload = ChatMessage）
	CmdAck          int32 = 5  // 消息回执（payload = PullRequest{conversationId, afterSeq}，afterSeq=已确认接收 seq 累计值；Header.ack 回显同值仅供协议对称，服务端一律以 payload 为准，W2-M04 裁决 D1）
	CmdSignal       int32 = 6  // WebRTC 信令（payload = Signal，W4 实现）
	CmdPullRequest  int32 = 7  // 拉取请求（payload = PullRequest）
	CmdPullResponse int32 = 8  // 拉取响应（payload = PullResponse）
	CmdNotify       int32 = 9  // 轻通知（热点群降级：conversationId + maxSeq，W4 实现）
	CmdError        int32 = 10 // 服务端错误帧（错误码复用 pkg/errors 语义，M02 消费）
)

// ContentType 消息内容类型（ChatMessage.contentType）。值冻结于 W2-M01-plan.md §6.2。
const (
	ContentTypeText      int32 = 1 // 文本
	ContentTypeFile      int32 = 2 // 文件
	ContentTypeImage     int32 = 3 // 图片
	ContentTypeAudio     int32 = 4 // 音频
	ContentTypeVideo     int32 = 5 // 视频
	ContentTypeVoiceCall int32 = 6 // 语音通话
	ContentTypeVideoCall int32 = 7 // 视频通话
	ContentTypeCustom    int32 = 8 // 自定义
)

// SignalType WebRTC 信令类型（Signal.signalType）。值冻结于 W2-M01-plan.md §6.2。
const (
	SignalTypeOffer        int32 = 1 // offer
	SignalTypeAnswer       int32 = 2 // answer
	SignalTypeICECandidate int32 = 3 // ice-candidate
	SignalTypeHangup       int32 = 4 // 挂断
	SignalTypeInvite       int32 = 5 // 呼叫邀请
)
