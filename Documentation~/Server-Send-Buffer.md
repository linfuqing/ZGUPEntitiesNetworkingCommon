# Server 发送缓冲与跨 Tick 背压

## 目标

`NetworkServerSendBuffer` 面向大量长连接，必须同时满足：

- All、Channel、Identity 三类路由统一走同一条发送管线；
- 生产者只写实际产生的消息，不按“连接数 × 历史频道数”预分配矩阵；
- 目标发送并行，单个慢连接不能拖住其他连接；
- UTP 发送队列本 Tick 满时，未发送数据保留到后续 Tick；
- Tick 工作量有界，但上限不是丢弃策略。

## 数据流

1. 每个在线源连接拥有一个 `SourceOutbox`，按源内 FIFO 保存消息正文和路由目标。
2. Planner 为当前在线频道构建稀疏成员索引，并从各源 Outbox 轮转选择完整消息。
3. 选择使用 `maxPlannedDeliveryWorkPerTick` 工作预算。每条消息至少消耗 1 个工作量，实际扇出更高时按扇出消耗；预算永远不小于当前在线连接数，所以一条 All 消息可以完整进入计划。
4. 计划按目标连接整理为连续区间；各目标的 Sender Job 并行走 UTP 直发快路径。只有 UTP queue full 时，失败数据包和其后的未发送后缀才复制到该目标的持久重试队列。
5. 所有 Sender Job 完成后，`CompleteDeliveryPlan` 才消费各源本 Tick 已计划的 FIFO 前缀；此时消息已经交给 UTP，或已进入目标重试队列。预算外消息继续留在 Outbox，下一 Tick 从原位置续投。

`planStatus` 的含义：

- `0`：当前 Outbox 已全部纳入本 Tick 计划；
- `1`：本 Tick 正常发送，但仍有消息留待后续 Tick；
- `2`：Planner 内部计数不一致；本 Tick 不发送也不消费，以便保留现场并 fail-safe。

## 顺序与身份

- 单个源连接内严格保持 FIFO，不拆分单条消息的目标集合。
- 多个源之间采用跨 Tick 轮转游标，避免一个高流量源长期独占预算。
- Identity 路由在 Outbox 中保存稳定的 user ID，不保存易受连接删除/槽位压缩影响的 connection index。延后期间发生槽位移动不会把消息发给错误用户。
- 延后期间若 Identity 目标已离线，该消息按 0 扇出退休；若同一 user ID 已重连，则解析到新连接。
- All/Channel 延后消息在实际计划的 Tick 按当时在线成员解析，这是过载续投语义，不保存庞大的目标快照。

## 背压与内存边界

每个目标连接的重试队列受以下两个阈值共同限制：

- `maxPendingSendMessageCountPerConnection`，默认 4096；
- `maxPendingSendBytesPerConnection`，默认 256 KiB。

超过任一阈值只断开该慢目标，并交给正常断连清理流程；不会全局停止发送或清空其他连接的数据。目标恢复并排空持久重试队列后会释放其峰值容量；正常直发不会在每个连接上永久保留一次大广播的容量。

Planner 的 `maxPlannedDeliveryWorkPerTick` 默认 262,144。它限制单 Tick 的路由与临时 delivery 数量；达到预算时消息仍在源 Outbox 中，因此不会改变“本轮满、下轮继续”的原语义。设置为 0 不代表无限制，构造时会钳制到至少 1，运行时还会提升到至少在线连接数。

复杂度从旧的稠密矩阵降为：

- 源积压：`O(实际消息数 + 实际正文)`；
- 活跃频道成员：`O(本 Tick 活跃成员关系数)`；
- 临时投递计划：`O(min(本 Tick 工作预算, 实际扇出))`；
- 目标重试：只按实际未发送数据增长，并受单连接阈值约束。

需要明确：为了保持“绝不因全局预算丢消息”，源 Outbox 不做静默丢弃或全局断连。如果长期输入速率持续高于排空速率，`deferredMessageCount` 和 `deferredPayloadByteCount` 仍会增长；任何有限内存系统都无法同时保证无限积压与绝不拒绝。生产环境应通过 `NetworkRelayServer.GetSendDiagnostics()` 监控这两个指标，以及 `pendingRetryMessageCount`、`pendingRetryByteCount` 和 `retainedRetryByteCapacity`（主线程读取前须完成 Server System dependency；`NetworkRelayServerManager.server` 访问器已完成）。若需要严格的全局内存上限，应在业务入口增加明确的 admission control 或源级限流策略，而不是在发送层悄悄丢包。

## 大型 ECS 组件访问约束

当前 Unity 6.5/UTP 组合下，`NetworkRelayServer` 的 unmanaged size 约为 10 KiB。Mono 对大型结构体参数存在限制；`AddComponentData(systemHandle, server)`、`GetComponentData<NetworkRelayServer>`、`SystemAPI.GetSingleton<NetworkRelayServer>`、`TryGetSingleton(..., out server)` 或 `SetSingleton(server)` 都可能在业务代码执行前抛出 `InvalidProgramException: Passing an argument of size ...`。

因此以下规则是硬门禁：

- 创建时先 `AddComponent<NetworkRelayServer>(systemHandle)`，再通过 `GetComponentDataRW(...).ValueRW` 原位构造；
- Server System 的 Update/Destroy 只通过 `GetSingletonRW` / `TryGetSingletonRW` 取得引用，不复制再写回；
- 只读 Job 通过 singleton ref 调用 `readonly AsReadOnly()`，只传递紧凑投影视图；
- 尚未创建组件时，外部探测统一走非抛异常的 `NetworkRelayServerManager.IsServerReady()` / `GetServerStatus()`；
- `NetworkRelayServerLifecycleTests.Manager_LargeServerComponentInitializesInPlaceAndReportsStatus` 必须保持通过。

## 配置入口

`NetworkRelayServerManager` 暴露：

- `_maxPendingSendMessageCountPerConnection`
- `_maxPendingSendBytesPerConnection`
- `_maxPlannedDeliveryWorkPerTick`

构造 `NetworkRelayServer` 时也可直接传入相同的三个参数。默认值位于 `NetworkServerSendBuffer` 常量中，避免 Manager、Relay 与 Buffer 各自维护一套数字。

## 回归门禁

`NetworkServerSparseSendBufferTests` 覆盖：

- Outbox FIFO、正文压缩与前缀消费；
- 目标重试队列条数/字节上限和已发送前缀压缩；
- All、Channel、Identity 通过真实 IPC UTP 链路跨 3 个 Tick 续投；
- 客户端最终收到每条消息且只收到一次；
- Identity 目标离线后的 0 扇出积压仍受 Tick 工作预算约束。
