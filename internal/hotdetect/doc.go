// Package hotdetect 热点群打标（W4-M01，spec §7.1 闭环）：Redis 时间桶
// 计数（Recorder.RecordGroupMsg，gateway/logic 埋点经 message 包窄接口
// 结构化消费）→ api 进程周期聚合（Detector.Run：候选发现（ZSET recent ∪
// MySQL hot 集）→ 窗口 MGET → 阈值判定 → 双落打标/摘标）→ IsHot 双源
// 查询（Redis 优先、miss/错误回源 MySQL hot_level、双败 6003）。
//
// W4-M02 增摘标切换协议（UnmarkSwitcher，ADR-0010 D6）：M01「判定即摘标」
// 升级为 P1 Timeline 补齐 → P2 摘标 → P3 回写扩散三阶段状态机，由
// Detector 低周期分支逐轮驱动（Advance）；状态键 Redis Hash
// group:{id}:hot:backfill + 全局索引 SET group:hot:backfilling。
// W4-M03 起 IsHot 为拉取侧热点分支与清单增补的共用消费面（plan D4/D6）。
//
// 对外面（temp-w4 §5 C2）：HotDetector/Recorder 接口、NewRecorder/
// NewDetector/NewUnmarkSwitcher 构造器、Detector.Run 聚合循环；桶键构造、
// 窗口 MGET、streak 状态机、打标/摘标编排全私有——M02 仅依赖 IsHot 语义
// 与 hot_level 冻结值（0=普通，1=热点；>1 预留，D6），不感知计数实现。
package hotdetect
