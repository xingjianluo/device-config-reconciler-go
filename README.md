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
  device/      设备模拟器：本地 map 存实际状态，可注入瞬时/永久故障
  reconciler/  协调核心：逐对象加锁、比对期望与实际、下发/纠正/清理、幂等记账
  worker/      事件队列与工作器：并发执行、事件去重、指数退避重试
cmd/demo/      端到端演示
```

## 协调语义（Reconciliation Semantics）

对象身份为 `deviceID/feature`，每个对象有一个期望配置（带单调递增的 `generation`）
和一份设备实际状态。协调器对事件的处理：

- **PUT（创建/更新）**：先持久化期望配置，再让设备收敛到该期望。
  - 设备无状态 → 下发 `CREATE`；设备有状态但与期望不一致（漂移）→ 下发 `UPDATE`。
  - 目标代际已是账本最后一条且设备状态与期望一致 → 返回 `NONE` 空操作，不下发。
- **DELETE（删除）**：删除必须**等清理动作真正完成**才算结束。
  - 先移除期望配置；设备上仍有状态时下发 `CLEANUP`。
  - `CLEANUP` 成功后才清除设备状态与代际账本并报告 `Converged=true`。
  - 清理失败时删除保持“未完成”，重复投递 DELETE 会继续重试清理；完成后的重复
    DELETE 是幂等空操作。
- **RESYNC（周期/触发再对账）**：重新读取期望与实际，被手动改动过的设备会在下一轮
  被纠正回期望值（漂移纠正）；无漂移时不产生任何下发，也不重复记账。

### 幂等（Idempotency）

幂等由两层保证，重复投递的结果与“只投递一次”完全一致：

1. **工作器层（事件去重）**：相同 `EventID` 只被接受一次（`seen` 集合）；同一对象已有
   事件在队列/处理中时，新事件不再叠加（处理本身会重新读取最新期望，天然幂等）。
2. **协调器层（代际记账）**：每个对象一把互斥锁，同一对象串行处理，不同对象并行；
   成功下发后该 `generation` 在账本中只记录一次。重复处理同代际且设备已一致时直接
   空操作，因此不会重复下发、重复记账或互相覆盖。

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

- **并发协调**：64 个 goroutine 同时处理同一对象，断言恰有 1 次 CREATE、账本长度为 1、
  所有结果收敛；50 个不同对象并行收敛互不串扰（`-race` 下运行）。
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
