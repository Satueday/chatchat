package pushpull

import (
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"

	"go.uber.org/zap"
)

// NotifyDispatcher 为热点通道旁路可丢弃任务池（W4-M02 plan D5，spec §7.2
// 「专属协程池背压」字面结构；CAP-105）：有界 chan + N worker。
//
// 定位冻结：池是「可丢弃旁路」——轻通知/未来异步面（CmdNotify 归 M03），
// Timeline 写入永不入池（同步于 HotService.Handle 内，commit 语义绑定）。
// 「lag 超限丢轻通知保 Timeline」的结构表达：丢弃面只可能作用于 Submit
// 的旁路任务（队列满→丢弃+Warn+计数；lag 全局判定接线归 M03/M05）。
//
// 语义：
//   - Submit 永不阻塞：队列满 → 丢弃 + Warn + dropped 计数（调用方不受
//     背压牵连——丢通知不断连，go-chat 反例规避）；
//   - Close 优雅退出：关闭后不再接单（Submit 返回 false），在途任务执行
//     完毕（drain 语义）后 worker 退出；
//   - Submit 的 func 不得捕获不可回收资源（plan §6）；Close 后 Submit
//     返回 false 由调用方 Warn。
//
// M02 阶段池内无真实业务任务（M03 填入轻通知实现即生效），机制由单测
// 锁定 C8 门禁（有界/丢弃不阻塞/优雅退出/无泄漏）。
//
// 并发约定：closed 与 queue 关闭由 mu 保护，Submit 的 select-send 亦在
// mu 临界区内（W4-M03 TGT-201 修复：Submit 与 Close 经 mu 全序化，
// send-on-closed-channel 不可达；server-master stream.go:82-93 双通道
// 范式同构）；dropped 为原子计数。
//
// W4-M05（plan D10）：Close 排空加 3s 上界（closeDrainTimeout）——超时
// 放弃等待在途旁路任务并 Error 登记数量后返回（旁路任务单次 I/O 已有
// notifyTimeout 5s 预算，worker 终将排空，Close 不再无限等待；Timeline
// 路径不受影响——本就同步于 HotService.Handle，不入池）。running 原子
// 计数供放弃登记的 N 值（队列积压 + 在执行）。
type NotifyDispatcher struct {
	queue   chan func()
	workers int
	logger  *zap.Logger
	wg      sync.WaitGroup

	mu      sync.Mutex
	closed  bool
	dropped atomic.Uint64
	running atomic.Int64 // 正在执行的旁路任务数（Close 放弃登记用）
}

// NewNotifyDispatcher 构造旁路池并启动 workers 个 worker goroutine
// （属主本实例，Close 回收；workers/queue 由 config [pushpull] 两键注入，
// 装配层负责范围校验前置——config.validate [1,64]/[1,65536]）。workers≤0
// 归一 1、queue≤0 归一 1（config 为第一防线，此处第二道，server.Options
// normalize 先例）；logger 为 nil 时 zap.NewNop 兜底。
func NewNotifyDispatcher(workers, queue int, logger *zap.Logger) *NotifyDispatcher {
	if logger == nil {
		logger = zap.NewNop()
	}
	if workers <= 0 {
		workers = 1
	}
	if queue <= 0 {
		queue = 1
	}
	d := &NotifyDispatcher{
		queue:   make(chan func(), queue),
		workers: workers,
		logger:  logger,
	}
	d.wg.Add(workers)
	for i := 0; i < workers; i++ {
		go d.run()
	}
	return d
}

// run 单 worker 循环：取任务执行（任务 panic 由 recover 吸收——单个旁路
// 任务缺陷不得击穿 worker 池，plan §10 风险面）；queue 关闭且排空后退出
// （drain 语义：Close 时已在队列中的任务仍被执行）。
func (d *NotifyDispatcher) run() {
	defer d.wg.Done()
	for task := range d.queue {
		d.safeRun(task)
	}
}

// safeRun 执行单个旁路任务，panic 转 Error 日志（含 panic 值与调用堆栈
// debug.Stack——展平的栈仍含 task 帧；worker 存活优先——旁路任务非正确性
// 数据，丢一条优于死一个 worker）。running 计数在 task 前后维护（Close
// 放弃登记的在执行数量源）。
func (d *NotifyDispatcher) safeRun(task func()) {
	d.running.Add(1)
	defer d.running.Add(-1)
	defer func() {
		if p := recover(); p != nil {
			d.logger.Error("pushpull notify: 旁路任务 panic（已吸收）",
				zap.Any("panic", p),
				zap.String("stack", string(debug.Stack())))
		}
	}()
	task()
}

// Submit 提交旁路任务：永不阻塞（有界 chan select 双分支——满即丢/已关
// 即拒）。返回 true=已入队；false=被拒（池已关闭，调用方 Warn）或被丢弃
// （队列满，本方法已 Warn+计数——聚合字段为 M05 指标预留）。
//
// P2 竞态修复（W4-M03 TGT-201，M02 移交首项）：select-send 在 mu 临界区
// 内执行——发送方要么先于 Close 完成入队、要么见 closed 返 false，
// send-on-closed-channel panic 不可达（Close 同持 mu 关 queue，两动作
// 经 mu 全序化；server-master stream.go:82-93 双通道范式同构）。select
// 带 default 永不阻塞，临界区内发送无死锁面（worker 持续排水）。
func (d *NotifyDispatcher) Submit(task func()) bool {
	if task == nil {
		return false // 空任务无执行语义，直接拒绝（不占丢弃计数）
	}
	d.mu.Lock()
	if d.closed {
		d.mu.Unlock()
		return false
	}
	dropped := false
	select {
	case d.queue <- task:
	default:
		dropped = true
	}
	d.mu.Unlock()
	if dropped {
		n := d.dropped.Add(1)
		d.logger.Warn("pushpull notify: 旁路队列已满，任务被丢弃（可丢弃旁路语义）",
			zap.Uint64("droppedTotal", n))
		return false
	}
	return true
}

// closeDrainTimeout Close 排空等待上界（plan D10：3s——超时放弃等待在途
// 旁路任务并 Error 登记；旁路任务自身有 notifyTimeout 5s 预算，worker 终
// 将排空，但 Close 不阻塞退出链超过 3s）。
const closeDrainTimeout = 3 * time.Second

// Close 有界优雅退出（W4-M05 plan D10）：标记关闭（后续 Submit 返回
// false）→ 关闭 queue → 等待 worker 排空在途任务，至多 closeDrainTimeout；
// 超时 Error 登记放弃数量（队列积压 + 在执行）后返回——worker 在后台继续
// 排空（任务有 5s 预算，不泄漏），调用方退出链不被无限拖住。幂等（closed
// 标记与 chan close 在 mu 内单次执行；重复 Close 同样有界等待）。由
// cmd/pushpull 装配层入优雅退出链。
func (d *NotifyDispatcher) Close() {
	d.mu.Lock()
	if d.closed {
		d.mu.Unlock()
		d.waitDrained() // 已关闭：同样有界等待（幂等）
		return
	}
	d.closed = true
	close(d.queue)
	d.mu.Unlock()
	d.waitDrained()
}

// waitDrained 有界等待全 worker 退出：done 辅助 goroutine 在排空完成时
// 关闭（若超时先返回，该 goroutine 随 worker 终将退出而结束——不泄漏，
// 任务自带 5s 预算上界）；超时分支 Error 登记放弃数量。
func (d *NotifyDispatcher) waitDrained() {
	done := make(chan struct{})
	go func() {
		d.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(closeDrainTimeout):
		abandoned := int64(len(d.queue)) + d.running.Load()
		d.logger.Error("pushpull notify: 排空超时，放弃等待在途旁路任务（worker 后台继续排空）",
			zap.Int64("abandoned", abandoned),
			zap.Duration("timeout", closeDrainTimeout))
	}
}

// Dropped 返回累计丢弃任务数（M05 指标预留的观测面）。
func (d *NotifyDispatcher) Dropped() uint64 {
	return d.dropped.Load()
}
