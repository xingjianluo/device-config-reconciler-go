# device-config-reconciler-go（本地设备配置协调组件）

一个纯本地、零外部依赖的设备配置协调（reconciliation）组件：用内存存储模拟设备端，
把“期望配置”持续收敛到“设备实际状态”，并在重复事件、并发处理、漂移、删除清理以及
瞬时/永久两类失败下给出可预期的行为。

## 目录结构

```
internal/
  model/       领域类型：期望配置、设备状态、事件、动作、账本、结果、错误分类
  classify/    错误分类：TRANSIENT（瞬时，可重试） vs PERMANENT（永久，不重试）
  storage/     并发安全的内存存储：期望配置 + 按对象的代际（generation）账本 + 最近结果
  device/      设备模拟器：本地 map 存实际状态，可注入瞬时/永久故障与测试钩子
  reconciler/  协调核心：逐对象加锁、代际闸门、比对期望与实际、下发/纠正/清理、幂等记账
  worker/      有界合并队列与工作器：有限并发、同对象合并、背压、退避重试、积压统计
cmd/demo/      端到端演示
```

## 新增能力一览（本次）

1. **高频乱序更新收敛**：事件携带单调 `generation`；迟到的旧代际变更只会被确认
   （结果标记 `Stale=true`），不能回滚设备状态、不能覆盖最新结果、不能把对象卡在中间值；
   同一变更重复送达效果与送达一次完全相同。
2. **删除与重建交错**：删除意图也按代际排序，删除墓碑（tombstone）防止旧 PUT 在删除后
   复活对象；删除未确认完成期间到达的更新/重建以新代际为准，删除完成前绝对外不宣称结束。
3. **批量、背压与合并**：运行队列有界（`MaxQueue`），满时 `Submit` 阻塞背压、
   `TrySubmit` 返回 `ErrQueueFull`、`SubmitContext` 可被 context 取消；同一对象
   未处理完的多次变更**按最高代际**合并成一个槽位；`Stats()` 实时暴露排队/处理中/
   退避重试/合并积压，方便发现堆积。

## 协调语义（Reconciliation Semantics）

对象身份为 `deviceID/feature`，每个对象有一个期望配置（带单调递增的 `generation`）
和一份设备实际状态。协调器对事件的处理：

- **PUT（创建/更新）**：先持久化期望配置，再让设备收敛到该期望。
  - 设备无状态 → 下发 `CREATE`；设备有状态但与期望不一致（漂移）→ 下发 `UPDATE`。
  - 目标代际已是账本最后一条且设备状态与期望一致 → 返回 `NONE` 空操作，不下发。
- **DELETE（删除）**：删除必须**等清理动作真正完成**才算结束。
  - 先移除期望配置并记下删除代际墓碑；设备上仍有状态时下发 `CLEANUP`。
  - `CLEANUP` 成功后才清除设备状态与代际账本并报告 `Converged=true`。
  - 清理失败时删除保持“未完成”，重复投递 DELETE 会继续重试清理；完成后的重复
    DELETE 是幂等空操作。
  - DELETE 建议与 PUT 共用同一单调 generation 序列，以启用乱序/交错保护（见下文）。
  - 迟到且代际更老的 DELETE（对象已被更新的 PUT 重建）返回 `Stale`，不产生清理。
- **RESYNC（周期/触发再对账）**：重新读取期望与实际，被手动改动过的设备会在下一轮
  被纠正回期望值（漂移纠正）；无漂移时不产生任何下发，也不重复记账。

### 幂等（Idempotency）

幂等由两层保证，重复投递的结果与“只投递一次”完全一致：

1. **工作器层（事件去重 + 同对象合并）**：相同 `EventID` 只被接受一次（`seen` 集合）；
   同一对象已有 claim（排队/处理中/退避重试中）时，后续事件折叠进该对象唯一的
   parked 槽，绝不占第二个队列槽。折叠按**最高 generation** 保留最新意图，
   claim 处理前取最高代际事件执行，因此乱序到达不会让新提交被旧提交覆盖；
   claim 结束时若仍有更新的折叠事件，会就地延续（re-arm）直到最新意图处理完。
2. **协调器层（代际记账 + 代际闸门）**：每个对象一把互斥锁，同一对象串行处理，
   不同对象并行；成功下发后该 `generation` 在账本中只记录一次。重复处理同代际且设备已
   一致时直接空操作；代际更早的 PUT/DELETE 直接返回 `Stale` 确认，不动设备、不记账、
   不覆盖该对象最新的对外结果，因此不会重复下发、重复记账或互相覆盖。

## 乱序收敛语义（Out-of-Order Convergence）

控制面对同一对象在短时间内连续下发多次变更时，无论到达顺序如何、中间夹杂多少迟到或
重复旧变更，最终设备状态只取决于**最后一次提交（generation 最大）的期望**：

- PUT 到达时若存储中已有更大 generation 的期望，或对象存在 generation ≥ 本次的删除墓碑，
  本次事件直接返回 `Converged=true, Stale=true`，不触碰设备；事件被显式确认，不会被
  悄悄丢弃，但也绝不会把状态往回带。
- 删除墓碑记录最近一次 DELETE 的 generation：删除完成后重放的旧 PUT 会被判定 stale，
  不能复活已删除对象；比墓碑更新的 PUT 正常清除墓碑并重建。
- 同代 PUT 重复处理走原有“账本一致即空操作”路径，结果等价于只投递一次。
- 工作器合并保留最高 generation，因此即使高频事件在 claim 处理期间乱序折叠，
  claim 永远朝最新意图收敛；`Drain()` 返回后设备必在最新期望上。

> 兼容性：不携带 generation（generation=0）的事件沿用旧行为——按到达顺序、last-write-wins
> 处理，代际闸门与墓碑均不介入。要获得乱序保护，PUT/DELETE 都应带上单调递增 generation。

## 删除与重建交错（Delete/Recreate Interleaving）

- DELETE 本身可以带 generation。删除仍坚持原有硬语义：**只有 CLEANUP 在设备上真正成功**
  才清除设备状态与账本、报告 `Converged=true`；瞬时/永久失败时删除保持未完成，
  对外结果是失败而不是结束，重复 DELETE 继续重试清理。
- 删除处理期间到达的新 PUT（generation 更大）：新期望先持久化，设备恢复后收敛到新期望；
  此时重放/重试的旧 DELETE 判为 `Stale`，不会清掉新期望，也不会对已重建状态做清理。
- “创建后紧跟删除”：DELETE 的 generation 大于刚创建的 PUT 时正常删除；若乱序导致
  更老的 DELETE 晚于更新的 PUT 到达，老 DELETE 判为 stale，对象保留。
- 最终结果只取决于最后一次提交的意图：最后是删除则设备不留任何状态；最后是创建/更新
  则设备按最新期望收敛，账本与结果均属于最新 generation，不残留中间态。

## 重试与错误分类规则

设备错误先经 `internal/classify` 分类，两类失败在对外结果 `ActionResult.ErrClass`
中明确区分：

| 类别 | 触发场景（示例） | 处理方式 | 对外表现 |
|------|------------------|----------|----------|
| `TRANSIENT` | 设备临时锁定、短暂不可用、超时、未知错误（fail-closed） | 事件保留，按指数退避重试，直到成功或达到 `MaxRetries` | 重试期间不丢事件；成功后收敛。重试耗尽仍是 `TRANSIENT`，不会被误判成永久失败 |
| `PERMANENT` | 字段非法（mode 非 auto/manual）、参数越界（level 不在 [0,100]）、设备明确拒绝载荷 | 第 1 次即停止重试 | `Converged=false`、`ErrClass=PERMANENT`、`Reason` 写明原因；非法载荷不会下发到设备 |

- 未知/未分类错误默认按 `TRANSIENT` 处理（fail-closed），避免意外错误静默丢事件。
- 退避：`BaseBackoff * 2^(attempt-1)`，上限 `MaxBackoff`；重试次数上限 `MaxRetries`。
- 非法字段在协调器本地即被判为 PERMANENT，不会产生任何设备操作。

## 批量、容量与背压（Bounded Queue, Coalescing & Backpressure）

工作器的运行队列有界，高并发对账与高频写入都不会让内存无上限增长：

- **有限并发**：`Config.Workers` 个 goroutine 并行处理，同一对象由 per-object 锁串行化，
  不同对象互不阻塞。
- **有界运行队列**：`Config.MaxQueue`（默认 1024）限制排队槽位。注意正在设备上执行的
  claim 已出队，不占排队槽；`Stats().Queued + InFlight + Retrying` 是真正占用中的工作量。
  - `Submit`：满时阻塞等待空位（背压，保持旧的无返回值签名）。
  - `TrySubmit`：满且该对象无 in-flight claim 时立即返回 `ErrQueueFull`，由调用方决定
    丢弃/降速/稍后重试；累计拒绝数见 `Stats().Dropped`。
  - `SubmitContext`：阻塞但可被 context 取消/超时打断，返回 context 错误。
- **同对象有界合并**：对象已有 claim 时，无论再堆多少事件都只折叠进一个 parked 槽，
  折叠保留最高 generation。合并只压缩处理次数，不改变最终收敛结果；事件在 claim
  每次出队（含退避重试后）时取最新，`Drain()` 保证所有被接受事件的最新意图处理完。
- **积压可观测**：`Stats()` 返回 `QueueStats` 快照：

  | 字段 | 含义 |
  |------|------|
  | `Pending` | 已接受未终结的总工作量（排队 + 处理中 + 退避中 + 合并槽） |
  | `Queued` | 等待 worker goroutine 的可运行 claim 数 |
  | `InFlight` | 正在设备上处理的 claim 数 |
  | `Retrying` | 正在退避等待重试的 claim 数 |
  | `Parked` | 折叠在 in-flight claim 之后、等待接棒的对象数 |
  | `InflightObjects` | 当前持有 claim 的不同对象数 |
  | `MergedSubmissions` | 累计被合并（未占新槽）的提交数 |
  | `Dropped` | 累计因队列满被 `TrySubmit` 拒绝的次数 |

> 兼容性：旧代码无需改动即可工作——`MaxQueue` 有默认值，`Submit` 仍是无返回值的阻塞调用；
> 旧的“同对象已有事件在队则丢弃后续提交”行为升级为“折叠最高代际并接棒处理”，最终收敛
> 语义更强且不回归。

## 本地验证方法

要求 Go 1.25+（开发环境为 Go 1.26）。

```bash
# 1. 构建
go build ./...

# 2. 静态检查
go vet ./...

# 3. 全部单元测试（建议带竞态检测）
go test -race ./...

# 4. 覆盖率
go test -cover ./...

# 5. 端到端演示：创建/更新/重复事件/漂移纠正/瞬时重试/永久失败/删除清理
go run ./cmd/demo
```

### 测试覆盖的场景

- **乱序收敛**：对同一对象按“偶数代降序 + 奇数代升序 + 旧代重复回放”的乱序方式连投 200 个
  变更，断言设备最终落在最高代际、设备 Apply 次数远小于 200（合并生效）、Drain 后积压为 0；
  用设备钩子卡住第一次 Apply，在处理中折叠新代际并重放旧代，断言最终只有最新代际上设备且
  Apply 恰为 2 次（重复变更不重复下发）。
- **删除/重建交错**：PUT1→DELETE2→PUT3 背靠背提交，断言最终收敛到 gen3；用钩子卡住 CLEANUP，
  在清理确认前提交重建，断言删除从未被当作完成、最终设备是新期望、重放的旧 DELETE 为 stale；
  清理永久失败期间到达的新 PUT 被持久化，设备恢复后收敛，且删除墓碑不让旧 PUT 复活对象。
- **批量与背压**：单 worker + `MaxQueue=8` 且设备调用被钩子卡住时，填满 8 个排队槽后
  `TrySubmit` 返回 `ErrQueueFull`，`Stats` 精确反映 queued/in-flight/pending/dropped；
  队列满时对同一已排队对象再投 200 次全部被合并（parked=1、merged=200、队列不增长），
  放开后仍收敛到最后一次提交；`SubmitContext` 在满载时阻塞并能被超时取消。
- **并发协调**：64 个 goroutine 同时处理同一对象，断言恰有 1 次 CREATE、账本长度为 1、
  所有结果收敛；同一 EventID 从 128 个 goroutine 投递断言设备恰 1 次 Apply；
  50/100 个不同对象并行收敛互不串扰（`-race` 下运行）。
- **重复事件**：同一 `EventID` 从多 goroutine 投递 100 次，断言只有一个收敛结果、
  账本只有一条；同一事件在协调器层直接处理 5 次结果等价于 1 次。
- **漂移纠正**：先收敛，再在设备上“手动改坏”，RESYNC 后断言设备被纠正回期望；
  无漂移的再次 RESYNC 不下发、不记账。
- **删除清理**：清理受永久/瞬时故障阻断时删除不算完成、设备状态保留；设备恢复或
  重试成功后才清除状态与账本；完成后重复 DELETE 幂等。
- **正常与异常路径**：瞬时故障退避重试后成功（attempts 计数）、重试耗尽仍标记
  TRANSIENT；非法字段一次即停并带明确 PERMANENT 原因。

单测日志会打印每条断言的**触发条件（trigger）**与**判定依据（verdict）**，
可用 `go test -v` 查看，例如：

```
trigger=device locked for the first 3 attempts | verdict: backoff retries, converged success, attempts>=4
verdict: converged after 4 attempts with exponential backoff -> PASS
```
