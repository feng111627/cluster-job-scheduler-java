# cluster-job-scheduler-java

一个用内存数据模拟的本地集群作业调度与队列管理服务（Spring Boot 3 / Java 21）。
演示从 **作业提交 → 排队 → 调度落位 → 完成/失败/节点掉线释放** 的完整闭环，
并保证不同操作路径下的资源账目与排队结论一致、可解释。

## 1. 调度模型

- **节点 `WorkerNode`**：每个节点有固定二维资源容量 `capacity = (cpu, memoryMb)`，
  维护已分配 `used`，`available = capacity - used`。启动时由 `SchedulerConfig`
  注册 3 个节点 `node-a/b/c`，每个 `8 CPU / 16384 MB`，集群总量 `24 CPU / 49152 MB`。
- **作业 `Job`**：一个作业包含 `taskCount` 个资源需求完全相同的任务（`taskResource`），
  并携带 `priority`（数值越大越优先）。状态机：
  `PENDING → RUNNING → COMPLETED | FAILED`；提交阶段被拒为 `REJECTED`（不进队列）。
- **调度器 `ClusterScheduler`**：单例内存组件，持有节点表、作业表与等待队列。
  **提交与调度分离**：`submitJob` 只负责校验与排队，必须显式调用
  `schedulePendingJobs` 才会尝试落位。
- **整批原子（all-or-nothing）**：调度一个多任务作业时，先在“影子账目”上依次为全部
  任务试摆（在线节点上按首次适应装箱），**所有任务都能放下才把计划一次性提交**；
  任意一个任务放不下，整批放弃，节点真实账目一行都不改，因此不会出现
  “只调度了一部分任务却已占住资源”。
- 注意一个刻意保留的约束：**任务不可拆分到多个节点**，单任务必须落在单个节点内。
  因此“总量够”不等于“装得下”（例如 4 个 6CPU 任务总量 24，但每节点 8CPU
  最多各放 1 个，3 个节点只能容纳 3 个），这与容量拒绝是两层不同的判断。

## 2. 优先级与排队规则（稳定、可解释）

每轮调度对等待队列做一次确定性排序：

1. **优先级降序**：`priority` 数值大的作业先被考虑；
2. **同优先级按提交序号升序（FIFO）**：提交序号在入队临界区内单调分配，先到先得；
3. **节点顺序固定为注册顺序**（`node-a → node-b → node-c`），装箱采用首次适应。

因此：

- 排序键完全确定（优先级 + 全局提交序号 + 固定节点顺序），**相同集群状态下相邻
  调度的结论必然逐位一致**，不会出现莫名抖动；没有用到任何不确定的集合迭代顺序。
- 某作业本轮放不下时只是跳过、留在队列，调度器会继续尝试其后能放下的作业
  （队头阻塞不会阻断后面的作业）；待资源释放后再次调用调度即可接手。

## 3. 提交校验与拒绝原因

提交时做两道**只取决于集群拓扑、与当前占用无关**的结构性校验，拒绝原因可区分：

| 拒绝原因 | 触发条件 |
| --- | --- |
| `TASK_EXCEEDS_LARGEST_NODE` | 单任务需求在任一维度超过集群最大单节点容量（任何时刻都放不下一个任务） |
| `TOTAL_EXCEEDS_CLUSTER_CAPACITY` | 全部任务的总需求超过集群总容量（即使集群全空也放不下整批） |

被拒作业标记为 `REJECTED` 并保留记录，但**不进入等待队列、不占用任何资源**。
边界值“恰好相等”视为可接受（单任务恰好等于节点容量、总需求恰好等于集群总量）。
其余“暂时装不下”的情况不是拒绝，而是正常排队。

## 4. 资源账目如何保证一致

- **不可变资源向量 `Resource`**：加减都返回新对象；`subtract` 一旦会使任一维度变成
  负数就抛异常——这是“可用资源不能为负、重复释放不能越算越多”的底层防线。
- **单一临界区**：`ClusterScheduler` 的所有公共方法都在同一把锁下串行执行，
  提交/调度/完成/失败/节点掉线的任意并发交错都不会把同一份资源分配两次。
  试摆与提交在同一临界区内完成，保证整批原子。
- **释放幂等**：只有 `RUNNING` 作业才会真正归还资源；`COMPLETED/FAILED` 作业再次
  `complete/fail` 直接返回 `false`，账目不变。
- **释放顺序无关**：每个任务只从其落点节点扣减它当初申请的那一份资源，
  加法满足交换律；先放哪个任务、后放哪个任务，最终账目相同。
- **节点掉线**：节点置 `OFFLINE` 且不再参与装箱；其上所有作业整体 `FAILED`，
  这些作业分布在**其他节点**上的任务资源也一并归还。重复标记掉线返回 0。
  节点重新上线时占用天然为 0。
- 恒等关系：每个节点恒有 `0 <= used <= capacity`、`available = capacity - used >= 0`；
  集群已分配资源 = 所有 RUNNING 作业全部任务需求之和。

## 5. HTTP 接口

基址 `/api/scheduler`：

| 方法与路径 | 说明 |
| --- | --- |
| `POST /jobs/{jobId}` | 提交作业，body：`{"priority":9,"taskCount":3,"cpuPerTask":6,"memoryMbPerTask":1024}`；返回 `accepted` 与 `rejectReason` |
| `POST /schedule` | 执行一轮调度，返回本轮落位与仍排队的作业 id |
| `POST /jobs/{jobId}/complete` | 正常完成并释放（幂等） |
| `POST /jobs/{jobId}/fail` | 任务失败并释放（幂等） |
| `POST /nodes/{nodeId}/offline` | 节点掉线，返回受影响失败作业数 |
| `POST /nodes/{nodeId}/online` | 节点恢复上线 |
| `GET /snapshot` | 队列、运行中作业与每个节点的资源账目快照 |

快速体验：

```bash
mvn spring-boot:run            # 默认端口见启动日志（随机端口）
curl -s -XPOST localhost:<port>/api/scheduler/jobs/j1 \
  -H 'Content-Type: application/json' \
  -d '{"priority":9,"taskCount":3,"cpuPerTask":6,"memoryMbPerTask":1024}'
curl -s -XPOST localhost:<port>/api/scheduler/schedule
curl -s localhost:<port>/api/scheduler/snapshot
```

## 6. 如何验证

### 单元测试

```bash
mvn test
```

共 13 个用例，按业务分成 4 个测试类：

| 测试类 | 覆盖业务 |
| --- | --- |
| `SubmitRejectionTest` | 两类拒绝原因区分、边界值接受、被拒作业不入队不占资源 |
| `BatchSchedulingTest` | 优先级+FIFO 顺序、调度确定性与不动点、整批 all-or-nothing、跨节点装箱与账目守恒、剩余资源非负 |
| `ConcurrentAccessTest` | 16 线程并发提交+调度不重复分配、12 线程混合并发释放的幂等性与顺序无关、提交/调度/完成持续交错下账目恒成立 |
| `NodeFailureTest` | 节点掉线导致跨节点作业整体失败并全量归还、掉线节点不参与装箱、节点恢复、单作业失败不影响同节点其他作业 |

**日志可读性**：每个用例通过统一的 `report(业务, 用例, 结果, 明细)` 方法输出一行，
形如：

```
【业务】多任务整批原子调度 | 【用例】整批失败时不允许部分占用资源 | 【结果】通过 | 调度前已用CPU=20, 调度后已用CPU=20
```

调度器自身也会用 INFO 日志记录入队、拒绝原因、每轮落位/排队、释放与节点掉线明细，
因此跑测试时可以直接从日志看到“覆盖了哪些业务、每条业务的用例与结果”。
也可只跑某一类业务：

```bash
mvn -Dtest=NodeFailureTest test
```
