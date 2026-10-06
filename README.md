# 本地集群作业调度与队列管理（内存模拟）

一个 Spring Boot 后端服务，用内存中的节点 / 作业数据模拟作业从
**提交 → 排队 → 调度 → 运行 → 释放（或故障失败）** 的完整过程。
核心逻辑在纯 Java 的 `SchedulerService` 中，不依赖数据库，可用单测复现所有结论。

## 1. 调度模型

### 1.1 核心对象（`scheduler` 包）

| 对象 | 含义 |
| --- | --- |
| `Resources(cpu, memoryMb, diskMb)` | 不可变资源向量，支持 `canFit / plus / minus / max`；`minus` 结果为负会直接抛异常，从根上阻止负账目 |
| `Node` | 集群节点，持有 `capacity`（总容量）、`allocated`（已分配）、`online`（在线状态） |
| `Task` | 作业中的一个任务，携带资源需求，调度后记录所在节点 id |
| `Job` | 由 1~N 个任务组成，携带优先级和单调提交序号 `sequence`，状态机为 `PENDING → RUNNING → COMPLETED/FAILED`（或提交即 `REJECTED`） |
| `SchedulerService` | 队列管理 + 调度 + 资源账目的唯一入口，线程安全 |

### 1.2 状态流转

```
提交 ──校验失败──> REJECTED（不进系统、不占资源，带 RejectReason）
  │
  └──校验通过──> PENDING（入队）
                    │  一轮调度中所有任务同时放得下
                    v
                 RUNNING ──releaseJob──> COMPLETED
                    │
                    ├──taskFailed(任一任务)──> FAILED（整作业释放）
                    └──nodeOffline(所在节点)──> FAILED（整作业释放，跨节点任务一并回收）
```

每次提交、释放、任务失败、节点掉线 / 上线之后都会**自动再跑一轮调度**，
因此排队作业在资源空出时会自动补位；也可通过 `POST /api/cluster/schedule` 手动触发。

## 2. 优先级与排队规则（稳定、可解释）

队列排序键为：

1. **优先级 `priority` 数值大的在前**；
2. 优先级相同时，**先提交的在前**（提交时分配单调递增的 `sequence`，FIFO）。

节点按 `nodeId` 字典序遍历，每个任务放到**第一台剩余容量放得下的在线节点**（first-fit）。
因此只要节点集合和队列内容一致，相邻两次调度的结果必然一致，不会莫名变化。

队首作业暂时放不下时，调度器会**跳过它继续尝试后面的作业**（避免碎片浪费），
被跳过的作业保留在队列原位置，下一轮资源变化后仍按原顺序参与。

## 3. 资源账目处理方式

### 3.1 提交时的三道校验（拒绝原因可区分）

| `RejectReason` | 触发条件 |
| --- | --- |
| `INVALID_REQUEST` | 作业 id 为空 / 重复、任务列表为空、任务 id 重复等 |
| `TASK_EXCEEDS_LARGEST_NODE` | 任一任务的需求超过**最大单台在线节点**容量（任何时刻都不可能调度） |
| `JOB_EXCEEDS_CLUSTER_CAPACITY` | 作业全部任务需求之和超过**在线集群总容量**（逐维比较 CPU/内存/磁盘） |

后两者是必要条件检查：资源不足但理论可调度的作业会被接受并排队，而不是直接拒绝。

### 3.2 多任务整批调度（all-or-nothing）

- 逐个任务试探分配，任一任务找不到放得下的节点：
  **立即把本作业此前已试探占用的资源逆序全部归还**，作业留在队列；
- 只有全部任务都占位成功，才提交分配（任务绑定节点、作业转 RUNNING）。
- 因此不会出现"只调度了一部分任务却已经占住资源"的中间态。

### 3.3 账目不变量

所有账目变更都在 `SchedulerService` 的同一把 `ReentrantLock` 内串行完成：

- 节点已分配量每一维恒满足 `0 ≤ allocated ≤ capacity`，**剩余资源永不为负**
  （`Resources.minus` 在不够减时抛异常，`Node.tryAllocate` 失败不改账）；
- 并发提交 / 释放被锁串行化，**同一份资源不可能被两个作业重复分配**；
- 释放（作业完成、任务失败、节点掉线）全部**幂等**：只归还当前确有分配的任务，
  对同一对象重复调用第二次时无资源可还，**可用资源不会越算越多**；
- 在队列中没有等待者的前提下，释放的先后顺序不影响最终账目（有单测验证）；
- 节点掉线时：该节点上的所有作业整体 FAILED，**跨节点作业在其它节点上的份额一并回收**，
  节点占用清零并置为离线，随后触发补位调度。

## 4. HTTP 接口（演示用）

基础路径 `/api/cluster`：

| 方法与路径 | 说明 |
| --- | --- |
| `POST /nodes/{nodeId}` | 注册节点，body：`{"cpu":4,"memoryMb":4,"diskMb":0}` |
| `POST /jobs` | 提交作业：`{"jobId":"j1","priority":5,"tasks":[{"taskId":"t1","requirement":{"cpu":3,...}}]}`，返回是否接受及拒绝原因 |
| `POST /jobs/{jobId}/release` | 作业完成并释放资源 |
| `POST /tasks/{taskId}/fail` | 上报任务失败，整作业失败释放 |
| `POST /nodes/{nodeId}/offline` / `online` | 节点掉线 / 重新上线 |
| `POST /schedule` | 手动触发一轮调度 |
| `GET /jobs/{jobId}` | 查询作业状态与 task→node 分配 |
| `GET /snapshot` | 查看队列与全部节点资源账目快照 |

快速演示（服务启动后）：

```bash
curl -s -X POST localhost:8080/api/cluster/nodes/n1 -H 'Content-Type: application/json' -d '{"cpu":4,"memoryMb":4,"diskMb":0}'
curl -s -X POST localhost:8080/api/cluster/nodes/n2 -H 'Content-Type: application/json' -d '{"cpu":4,"memoryMb":4,"diskMb":0}'
curl -s -X POST localhost:8080/api/cluster/jobs -H 'Content-Type: application/json' \
  -d '{"jobId":"j1","priority":0,"tasks":[{"taskId":"a","requirement":{"cpu":3,"memoryMb":3,"diskMb":0}},{"taskId":"b","requirement":{"cpu":3,"memoryMb":3,"diskMb":0}}]}'
curl -s localhost:8080/api/cluster/snapshot
```

## 5. 如何验证

### 5.1 运行单元测试

```bash
mvn test
```

测试覆盖四个业务场景（均位于 `src/test/.../tests/scheduler`），
用 JUnit 5 动态测试组织，每条用例都有编号、业务名称，并在控制台打印
`[用例 xx] 名称` 与 `-> 结果：PASS/FAIL`，业务日志（SLF4J）同时输出提交、
排队、整批试探/回滚、资源归还、故障处理等明细：

| 测试类 | 覆盖业务 |
| --- | --- |
| `SubmissionRejectionTest` | **提交被拒绝**：超单节点、超集群总量（CPU/内存逐维）、非法请求、重复 id；被拒作业不占资源 |
| `BatchSchedulingTest` | **整批调度**：多任务拆分到多节点、放不下时零部分占用与账目回滚、优先级 + FIFO 顺序、相邻调度结论稳定、释放后自动补位 |
| `ConcurrentSubmitTest` | **并发提交 / 释放**：20 作业并发提交恰好 4 运行 16 排队；释放与再提交混合并发下运行数守恒、无超卖 |
| `FailureReleaseTest` | **节点故障释放**：掉线导致跨节点作业整体失败并补位、重复掉线 / 重复失败上报幂等、释放顺序无关、并发混合故障后无负资源 |

想从日志重点观察某个业务：

```bash
mvn test -Dtest=SubmissionRejectionTest   # 只跑拒绝场景
mvn test -Dtest=BatchSchedulingTest       # 只跑整批调度与排队
mvn test -Dtest=ConcurrentSubmitTest      # 只跑并发
mvn test -Dtest=FailureReleaseTest        # 只跑故障释放
```

### 5.2 建议重点核对的结论

- 被拒作业查不到状态、节点账目为 0，不同拒绝路径返回不同 `RejectReason`；
- 放不下的多任务作业：所有任务都未绑定节点、两台节点 allocated 仍为 0；
- 并发结束后：`运行数 + 排队数 == 接受数`，各节点 allocated ≤ capacity 且总量守恒；
- 故障 / 释放（含重复、乱序、并发）后：在线节点 allocated 归零或等于仍在运行作业的占用，
  任意一维 available 永远 ≥ 0。
