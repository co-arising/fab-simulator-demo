# Fab durable workflow：独立本地样例

## 边界与完整流程

本样例是 additive 接入，不替换旧 Fab 页面、actor、projection 或启动模块。控制器只把输入交给公共 `WorkflowHost`，并查询其已提交的 `BatchProtocol.View`；没有另一套业务 FSM、内存进度表或自动完成设备的循环。

刻意限定 **一个 item = 一个 FOUP = 一片 wafer**，每次设备执行仅处理一个 item，不宣称支持多 wafer 批次、动态选端口、拆分或合并。

| 当前执行及权威结果 | 下一步 | 资源处置 |
|---|---|---|
| transport-in / SUCCESS | litho | 释放 AMHS，携带该 FOUP 的 litho-port1 占用 |
| litho / SUCCESS | transport-out | 释放光刻设备，`retainCarried=true`，保留光刻端口 |
| transport-out / SUCCESS | measure | 释放旧光刻端口与 AMHS，携带 metrology-port1 占用 |
| measure / REWORK | rework | 释放量测设备，保留量测端口 |
| rework / SUCCESS | measure，execution=1 | 释放返工设备，保留同一量测端口 |
| measure / SUCCESS | unload | 释放量测设备，仍保留量测端口 |
| unload / SUCCESS | Completed | 释放量测端口与 AMHS，不再携带占用 |

默认 `count=2`；独立 fixture 对以 `1` 结尾的 item 首次量测给出 REWORK，其余首次量测可成功。第二次量测不能再请求返工。只有 unload 的权威 SUCCESS 才返回 `ItemDisposition.Completed`。`retainCarried` 是公共 `BatchCompletion` 字段，不是控制器自行保留资源。

`WF2:<area>:<base64url-itemId>~<execution>` 是业务请求，`WR2:<area>:<base64url-itemId>~<outcome>` 是外部结果；二者封装在有长度上限的二进制字符串帧内，不是 HTTP 服务。每次 QUERY 必须使用外部存储的 key/digest，并校验 epoch、taskId、结果与 evidence 一致性、完整且不重复的 item 集合。

## 启动前提与隔离

以下命令沿用 JDK11，构建/运行必须指定明确候选版本与独占路径。当前接入的编译/测试及实跑结果见末节。

1. `monarch-r2.sbt` 仅在显式指定 `monarch.r2.version` 时override公共artifact；`project/MonarchR2Profile.scala`作用于Play项目，未指定时排除本次新增源码/测试/路由，保留原依赖。新增路由只生成forward router，避免覆盖旧`controllers.routes`。不加入sibling产品源码。
   已准备候选 `0.3.0-dev.r2-20261106-dc168f96fa62`，JAR SHA-256 `2dea09f3c97b4832fd78552f546a1d5fb4f68d5b92c42cb3d4466101c59d21b0`，core固定0.2.1。
2. `application-durable-fab.conf` **不 include application.conf/projection.conf**，并按名字禁用旧 `FabRuntimeModule` 和 Play DB/evolutions 模块；不会实例化旧自定义 JDBC/bootstrap。旧代码仍在同一个web工程编译，本次编译已通过；这不等于旧配置下的全部功能和测试已验收。
3. 使用独占的绝对存储路径；profile 设置 `akka.actor.provider=local`、LevelDB `native=off` 和本地 snapshot。无需 Docker、Mongo、MySQL、H2 或生产配置。不要把路径指向旧 demo 的 journal，也不要并发打开同一存储目录。
4. 外部 fixture 必须是父进程管理的独立 JVM，绑定 **127.0.0.1:19112**，使用另外一个独占目录。它不是生产设备或传感器证书服务，不模拟物理端口容量；容量由运行时资源协调器负责。

在 `/Users/zhongdj/Workspaces/mad-polaris/fab-simulator-demo` 内准备环境（路径末尾按本次独占演示调整；恢复时保持不变）：

```bash
export FAB_DURABLE_PLAY_SECRET="$(openssl rand -hex 32)"
export FAB_DURABLE_STORAGE_ROOT="/Users/zhongdj/Workspaces/mad-polaris/fab-simulator-demo/target/durable-fab-r2-demo-a"
export MONARCH_R2_VERSION="0.3.0-dev.r2-20261106-dc168f96fa62"
export MONARCH_CORE_VERSION="0.2.1"

sbt -Dmonarch.r2.version="$MONARCH_R2_VERSION" \
    -Dmonarch.core.version="$MONARCH_CORE_VERSION" \
    -Dmonarch.r2.repository=file:///Users/zhongdj/Workspaces/mad-polaris/monarch-persistence/target/r2-candidate-20261106-dc168f96fa62 \
    -Dmonarch.core.repository=file:///Users/zhongdj/Workspaces/mad-polaris/monarch-persistence/target/demo-candidate-20261021 \
    -Dconfig.resource=application-durable-fab.conf \
    -Dhttp.address=127.0.0.1 -Dhttp.port=9012 \
    -Dfab.durable.fixture-controls=true \
    "web / run 9012"
```

需要独立 Maven repository 时另传 `-Dmonarch.r2.repository=<URL>`、`-Dmonarch.core.repository=<URL>`。不应发布或修改 runtime 来迎合本样例。

父会话准备好的 fixture test classpath 可按以下入口启动，**不要由 web 进程启动或编译 fixture**：

```bash
java -cp "$FIXTURE_TEST_CLASSPATH" \
  net.imadz.monarch.persistence.ExternalTaskServiceFixture \
  /absolute/exclusively-owned/fab-external-ledger-demo-a 19112
```

该入口在 runtime 的 test sources 中，不保证包含在生产 JAR；classpath 必须由父会话实际提供。首次目录应不存在或为空，fixture 会创建 ownership marker。观察到 `EXTERNAL_READY 19112` 后再使用页面。

浏览器打开 **http://127.0.0.1:9012/fab/durable**。页面自包含，无 CDN；GET 页面生成 CSRF token，所有按钮 POST 带 `Csrf-Token`。保留 Play 默认安全过滤器，Host 只允许 loopback，控制器也检查远端地址。不要通过公网或反向代理暴露。secret 无可工作的源码默认值，未设置环境变量则拒绝启动。

## HTTP 合同

| 方法与路径 | 行为 |
|---|---|
| GET `/fab/durable` | 页面；开始、查询、有限轮询、可选外部任务完成按钮 |
| POST `/fab/durable/jobs/:jobId/start?count=2` | 逐项 Offer，收到全部接纳回复后显式发送 CloseInput；不等业务完成 |
| GET `/fab/durable/jobs/:jobId` | 直接询问 WorkflowHost，返回已提交 view 的 JSON，不拼接外部状态 |
| POST `/fab/durable/jobs/:jobId/reconcile?batchId=...` | 显式、有限查证原Operation/Inbox/Batch，验证该batch属于已提交job；不重置预算或强制成功 |
| GET `/fab/durable/jobs/:jobId/tasks` | 单次 TASKS 调用，只返回解析后属于该 job 的 operationKey |
| POST `/fab/durable/jobs/:jobId/complete?operationKey=...` | 校验 job/key 与 active committed batch 后，单次调用外部 COMPLETE |

`jobId` 必须匹配 `[A-Za-z0-9][A-Za-z0-9_-]{0,47}`，`count` 为 1..10。sequence 固定为 0..count-1，itemId 为 `<jobId>-item-<sequence>`，输入 payload 冻结 count 与 FOUP/wafer 身份。同 jobId、同 count 可重放，包括 CloseInput 后与恢复后：由 journal 的 offer digest 去重。改变 count 会冲突，不会扩容或新建尝试。中途断线可能已接纳部分输入；保持同 jobId/count 重放以补齐，再显式关输入，不能假定 HTTP 失败代表事务回滚。

每个 host 最多 16 个 job 身份、每类 child cache 最多 2048 个；GET 未见过的身份也会激活一个 job actor，因此不能用随机 jobId 探测。没有无限流、自动清空历史或开放式入场。

外部控制默认 `false`，关闭时 tasks/complete 返回 403；打开仅适用于这个本地演示。COMPLETE 不接受自填 outcome，不改 host 状态，也不立即标记工作流完成。fixture 根据自己的 durable item 历史选择结果；host 后续 QUERY 校验并提交，再推进下一步。已不在 active batch 的 key 返回冲突，即使 fixture 自身支持幂等 COMPLETE。

每个 HTTP 请求最多 10 次 Offer + 一次 CloseInput，actor ask 各有 5 秒超时；外部端点每次最多一个 fixture 请求，没有服务端无限重试。客户端有 4 个 I/O worker、16 个排队槽位、2 秒 connect/read 超时及覆盖排队时间的 4 秒 socket 截止时刻，超时后不会让尚未发送的排队任务再发出请求。Play 生命周期关闭 socket 与线程池。页面轮询每次至多 300 轮，可手动停止或继续；不会自动 START 或 COMPLETE。

## 失败与恢复的含义

- 缺失、重复、外来 item，错误 area/key/digest/epoch/taskId 或不一致 payload 均 fail closed。
- `RETRY` 虽在 fixture 线协议词汇中，本 Fab 策略不授权业务 retry。
- 即使 `KnownFailed` 的关联 evidence 正确，本 fixture 也没有失败后的物理释放证明；program 返回 Left，不伪装成 SUCCESS、不自动重试、不凭失败释放端口。
- NotFound、空闲、断线、经过一段时间、ledger epoch 改变，都不是安全重发证据。保持原 request/operationKey，不通过换 jobId 掩盖未知副作用。
- 同目录、同 namespace、同制品与同配置重启，并 GET/重放原 jobId，才会激活对应持久化恢复。没有启动扫描器或内存 job 注册表；外部 ledger 必须同时保留原目录与 epoch。不得 FORGET、删除 journal 或并发启动两个 namespace owner 来演示恢复。
- 路由包含显式有限reconcile，默认观察120次、人工30次/轮，Inbox300次/轮；预算在首次接管持久冻结，重启不重置。按钮只查证原身份，不强制成功、人工释放、重置epoch或盲重发。即使新宿主缓存为空，也根据已提交job激活原operation查证。

## 合同测试与已执行验收

`web/test/net/imadz/application/durable/FabDurableWorkflowProgramSpec.scala` 的10项策略/codec测试已通过。它覆盖稳定输入、manifest/request、缺失/重复/错误evidence、完整返工链的占位保留、移动/卸载释放授权、KnownFailed不得自动重试，以及严格wire framing。

本地命令示例（每仓重型构建串行执行）：

```bash
sbt -Dmonarch.r2.version="$MONARCH_R2_VERSION" \
    -Dmonarch.core.version="$MONARCH_CORE_VERSION" \
    -Dmonarch.r2.repository=file:///Users/zhongdj/Workspaces/mad-polaris/monarch-persistence/target/r2-candidate-20261106-dc168f96fa62 \
    -Dmonarch.core.repository=file:///Users/zhongdj/Workspaces/mad-polaris/monarch-persistence/target/demo-candidate-20261021 \
    "web / Test / compile" \
    "web / testOnly net.imadz.application.durable.FabDurableWorkflowProgramSpec"
```

2026-11-06 已执行：

- core/web完整源码使用JDK11与冻结公共JAR在独立target编译；10/10策略测试通过。stage为`web/target/r2-stage-dc168f96fa62`，未覆盖原`target/universal/stage`。
- `monarch-persistence/examples/verify_http_consumers.py --kind fab`通过真实HTTP：页面JS语法、CSRF、两项完整完成/重复输入/计数冲突、预算Blocked后杀整个PlayJVM并查证原任务、终态再次重启。
- 两作业共4FOUP均accepted并unloaded回stocker；指定item-1量测两次/返工一次，其余一次量测；24个外部任务、24次submit、重复业务效果0。
- 两次重启恢复同一namespace/journal；三个不同Play PID全部记录退出，独立ledger持续存在。实际CodeSource/JAR SHA与上文候选一致。
- 证据：`monarch-persistence/target/r2-http-evidence-20261106/r2-http-fab-8fhho6pj/`，含report/HTML/JS/LevelDB/WAL和processes.jsonl。所有该门创建的进程已停止。
- 编译中发现通配import与类成员重名、新路由覆盖旧controllers.routes两个接线问题，修正为明确import与AutoPlugin下forward-only新路由；旧页面/dirty文件不改。
- 未指定R2版本时实际检查sources、routes tasks、dependencyOverrides，均无本次新增项；这是配置隔离验证，不是旧页面功能的全量测试。
- 这是HTTP契约与JVM恢复证据，不是浏览器视觉、真实设备、物理断电或脑裂认证。
