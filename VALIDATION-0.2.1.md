# Fab Monarch 0.2.1 candidate validation

Date: 2026-10-21. **The recorded compilation and serializer-enabled actor
integration checks passed (9/9). This is not a complete compatibility or recovery
acceptance: subsequent source review confirmed unresolved parent/child crash
windows and acknowledgement gaps, listed below. A fresh isolated packaged-app
launch was BLOCKED before HTTP by a missing ClusterSharding Guice binding.**

## Scope and preserved work

Only `/Users/zhongdj/Workspaces/mad-polaris/fab-simulator-demo` was modified or
built. Branch remains `monarch-0.2.1-compat`; HEAD remains `d1be967`.
The interrupted agent's `build.sbt` changes were preserved. On resumption that
was the only changed file, no untracked test files existed, and no Fab build
process was running. No full agent JSONL was read. Bank was not operated on.

The existing `target/monarch-0.2.1-compile.log` recorded successful core/web
compilation and both candidate jars on the resolved classpath (lines 25,
296, 306–308). It did not establish parent/child persistent integration.

Changes completed here

- Only Monarch runtime pins change from `0.2` to `0.2.1`; `common-core` and
  `saga-core` remain `0.2`. The opt-in `-Dmonarch.repository=<Maven repository URL>`
  resolver does not build/publish sibling projects. Actor and persistence
  testkit dependencies are Test-only.
- Parent progress now uses `DurableRunObserver.onCheckpointCommitted`, not the
  speculative `RunObserver.onStageComplete` callback. Inline OCAP remains in the
  existing interpreter, preserving `StageFailed` followed by `OcapHandled`.
  The obsolete comment claiming Monarch's 0.2 resolution gap still requires
  inline placement was corrected; business rules were not rewritten.
- Added a narrow parent event/state protobuf contract and serializer, with
  converters for all existing Scala event/state variants and application
  bindings only for those two ADTs. Existing Fab state/error protobuf converters
  are reused; their wire fields and the Scala business models are unchanged.
  No generic command/context/ActorRef/closure serialization binding was added.
- Added actual `FabPipelineExecutionActor` + `PersistentMonarch` child integration
  tests. The existing lot gate suite now explicitly uses a local actor provider,
  rather than inheriting the demo's cluster/fixed-port configuration.

## Exact candidate and toolchain

Validation consumed the already packaged Maven-layout repository

```text
file:///Users/zhongdj/Workspaces/mad-polaris/monarch-persistence/target/demo-candidate-20261021
```

Both compile and test dependency classpaths contain these exact files, with no
Monarch `0.2` jar in those classpaths (`target/monarch-0.2.1-resume-03.log:450–451`
and `:588–589`)

```text
/Users/zhongdj/Workspaces/mad-polaris/monarch-persistence/target/demo-candidate-20261021/net/imadz/monarch-core_2.13/0.2.1/monarch-core_2.13-0.2.1.jar
SHA-256 66223d37ab979b59cde557500f3da7aafaf532ddfe3268401c5f786093e0f971

/Users/zhongdj/Workspaces/mad-polaris/monarch-persistence/target/demo-candidate-20261021/net/imadz/monarch-persistence_2.13/0.2.1/monarch-persistence_2.13-0.2.1.jar
SHA-256 ba144dbc528e1257a2e9569290b31ec1d35cfb0624bce6ea5d59ef6a7df1b05c
```

The packaged persistence POM declares `monarch-core_2.13` version `0.2.1`
(lines 50–51). The runtime test prints the code-source locations of classes from
both jars, not just the requested dependency versions (`resume-03.log:42`,
`resume-04.log:38`). It asserts that the child event and run-state serializer is
`net.imadz.monarch.persistence.MonarchSerializer`; candidate serializer ID is
`77102121`. Parent serializer ID is `77102122`, with versioned event/state
manifests. No remote availability or publication is claimed.

Actual sbt banner: **sbt 1.4.9 / AdoptOpenJDK Java 11.0.11**. Repository Scala
version: **2.13.14**. JDK selected by `/usr/libexec/java_home -v 11`, resolving to
`/Library/Java/JavaVirtualMachines/adoptopenjdk-11.jdk/Contents/Home`.

## Reproduction command and build results

Executed from the Fab repository, twice successfully after test fixes

```bash
cd /Users/zhongdj/Workspaces/mad-polaris/fab-simulator-demo
JAVA_HOME="$(/usr/libexec/java_home -v 11)" sbt \
  -Dmonarch.repository=file:///Users/zhongdj/Workspaces/mad-polaris/monarch-persistence/target/demo-candidate-20261021 \
  -Dsbt.log.noformat=true \
  "core / compile" \
  "web / compile" \
  "web / Test / compile" \
  "web / testOnly net.imadz.application.chain.LotAckGateSpec net.imadz.application.chain.FabPipelineExecutionActorSpec"
```

The first successful run additionally executed
`"show web / Compile / dependencyClasspath"` and
`"show web / Test / dependencyClasspath"`.

| Check | First successful run | Independent repeat |
|---|---|---|
| `core / compile` | PASS, `resume-03.log:7` | PASS, `resume-04.log:7` |
| `web / compile` | PASS, `resume-03.log:8` | PASS, `resume-04.log:8` |
| `web / Test / compile` | PASS, `resume-03.log:13` | PASS, `resume-04.log:9` |
| Both named suites | 9/9 PASS, `resume-03.log:435–440` | 9/9 PASS, `resume-04.log:431–436` |

Final repeat completed at 07:48:04 Asia/Shanghai. Exact summary

```text
Total number of tests run: 9
Suites: completed 2, aborted 0
Tests: succeeded 9, failed 0, canceled 0, ignored 0, pending 0
All tests passed.
```

Suite breakdown: `LotAckGateSpec` **4 passed**;
`FabPipelineExecutionActorSpec` **5 passed**. Machine-readable latest receipts
are `web/target/test-reports/TEST-net.imadz.application.chain.LotAckGateSpec.xml`
and `web/target/test-reports/TEST-net.imadz.application.chain.FabPipelineExecutionActorSpec.xml`.
Both report zero errors/failures/skips. Logs/receipts are local ignored target
artifacts, not committed evidence files.

Earlier attempts are retained, not hidden

- `target/monarch-0.2.1-resume-01.log`: application compilation passed; new tests
  needed an explicit invariant `ActorSystem[Nothing]` type and could not directly
  instantiate the library's package-private `DurableState`. Fixed test code; the
  serializer assertion uses the public child run-state value instead.
- `target/monarch-0.2.1-resume-02.log`: 7/8 tests passed; happy-path test mistakenly
  expected eight lot notifications instead of the actual seven. Corrected the
  count; OCAP and recovery already passed. The rejection regression case was
  then added before both final 9/9 runs.
- Existing compiler warnings (including deprecated `-target`) are not treated
  as clean-warning certification. No unrelated warning cleanup was attempted.

## What the running actors demonstrated

The new suite spawns the production parent behavior and its real Monarch child.
It uses serializer-enabled journal and snapshot testkits, Java serialization
**disabled**, a local actor provider, no cluster seed nodes, and port `0`.
Equipment replies and lot confirmations are controlled external boundaries;
Saga calls fail the test if unexpectedly invoked. Play/bootstrap, projection
workers, MongoDB/JDBC, containers, and listeners on `9000`/`2551` were not started.

1. **Wire contract and runtime artifacts.** All eight parent event variants and
   four parent state variants round-trip, including absent/present state fields,
   rich wafer/equipment/routing state and recursive OCAP plans. Unknown manifests,
   empty event payloads and a runtime command are rejected by the host serializer.
   Real child event/run-state values select the new packaged Monarch serializer.
2. **Happy path.** Real measurement → classification → sealing records child
   `StageDone` cursors `Measure_CDSEM-01#0`, `Classify#1`, `SealComplete#2`.
   The final decoded child checkpoint retains CD `32.0`, classification `PASS`,
   and completed count `3`; the parent stores a serialized `Executing` snapshot
   and ends with `AllCompleted` counts `(total=1, passed=1, reworked=0, scrapped=0)`.
   The confirming lot boundary receives the expected seven notifications,
   including `CompleteProcess(passCount=1)` (`resume-03.log:116–117`).
3. **Inline OCAP.** A real equipment `JobFailed(SENSOR_ANOMALY)` leads to structured
   parent `StageFailed` followed by `OcapHandled`, not a fake `StageCompleted` for
   measurement. The first child checkpoint decodes with count `1` and
   `OcapResolved(error)`; later stages complete (`resume-03.log:196–197`).
4. **Rejected child checkpoint.** Deliberate testkit rejection of `StageDone`
   yields no child checkpoint, no parent `StageCompleted`/`AllCompleted`, and no
   downstream equipment request. The `WriteMessageRejected` / ERROR diagnostic
   in `resume-03.log:227–228` is the intentional fault stimulus, not a failed
   suite (`:319–320`). This specifically checks committed observer semantics.
5. **Actor recovery and stale reply.** Stop the production parent with a committed
   measurement checkpoint/snapshot and an in-flight recipe request; respawn the
   production parent with the same persistence IDs. Both actors replay serialized
   data, skip measurement, retry the uncommitted recipe, rebuild context once,
   preserve wafer measurements/classification, and publish `RECOVERED` with one
   phase skipped. Delivering the old actor incarnation's equipment reply produces
   no new journal records or equipment requests (`resume-03.log:430–431`).
   The fallback state factory intentionally throws, proving this path uses the
   persisted state rather than the production empty-state fallback.

## Fresh public application observation — BLOCKED

After the interrupted session, the actual packaged Play application was launched
with JDK 11 and the two candidate jars above, private loopback HTTP/remoting ports
53376/53377, and fresh target-local LevelDB/snapshot directories. No workflow
components were disabled and no shared database or container was started.

Injector creation stopped the application before HTTP, exit code 255:

```text
[Guice/MissingImplementation]:
No implementation for ClusterSharding was bound.
```

The dependency chain was `router.Routes → FabDemoController → FabDemoService →
ClusterSharding`. A public `POST /api/fab-demo/m35/start` attempt returned HTTP 000,
curl exit 7 (connection refused). No UI interaction, pipeline checkpoint or
recovery was observed through the running web application. This is BLOCKED, not
a passing web-workflow verification.

Runtime classpath and packaged jar hashes matched the candidate artifacts, but
no Monarch class was observed loaded before the injector failure. All 314
application source fingerprints were unchanged. Both owned process groups exited
and the private ports were closed afterward.

Captures: `target/runtime-verify-20261021T092551/application.log`, `report.json`,
`runtime-jar-proof.json`, and before/after source fingerprints. No tests or
standalone typechecks were run during this runtime investigation. Supply the
intended sharding binding/bootstrap, then repeat this isolated public-surface
verification; do not disable the application flow to obtain a startup pass.

## Unresolved acceptance blockers found after the recorded runs

These are source-confirmed integration gaps, mostly predating the upgrade. The
9 passing tests do not establish these fault-window contracts, and the runtime
failures below have not yet been reproduced through the application's public
interface.

- **Accepted initial state can be lost.** Parent `Started` stores identifiers and
  stage count, not the submitted wafer state. A crash before child `RunStarted`
  commits makes recovery use the production empty-wafer factory.
- **Child-to-parent progress delivery is not replayable.** The child commits
  `StageDone` before sending a one-way parent notification. Recovery reports only
  a completed count; it does not replay missing committed notifications. A crash
  before parent `StageCompleted`/`OcapHandled` commits can permanently omit that
  domain progress. Parent event application also lacks cursor deduplication.
- **Terminal child state is not reconciled.** If child `RunCompleted` or
  `RunFailedEvt` commits before the parent terminal event, the recovered parent
  remains `Executing`. The child does not repeat terminal callbacks on recovery,
  rejects another `StartRun`, and the parent discards that recovery reply.
- **Inline OCAP does not drain its lot acknowledgements.** The clean interpreter
  seals the lot acknowledgement gate; the resolved branch advances its count
  without doing so. The passing Notify-only OCAP case does not cover unconfirmed
  `FailLot` or scrap/seal writes. The gate also does not inspect confirmation
  errors.
- **Lot acknowledgement timeout loses its classification.** A Fab exception
  created by the downstream gate drain reaches a converter that recognizes only
  Monarch exceptions, changing `LOT_ACK_TIMEOUT` into `UNEXPECTED`. The copied
  gate-composition test does not exercise that production conversion.
- **Infrastructure failure can leave a misleading executing parent.** The child
  deliberately stops without a business-failure callback after checkpoint
  infrastructure failure; the parent neither watches its termination nor offers
  a reconciliation channel. The rejection test checks no advancement but does
  not require a completed acknowledgement or controlled-recovery outcome.
- **Recovery publishing can interrupt persistence.** A throwing UX publisher is
  called before some parent persistence effects are returned; it can prevent
  parent progress or terminal state from being recorded.

Closing these gaps requires a coordinated start-payload/schema change, an
explicit authoritative-history and terminal-reconciliation contract, lot-ack
handling, and infrastructure lifecycle reporting. They are not fixed by changing
version pins or enabling Protobuf. A separate implementation plan and fault-window
acceptance are needed; no such changes have been made in this report.

## Other known limitations

- **Status/fault APIs are placeholders.** `FabDemoService.scala:1612–1627` returns
  `IDLE`, zero recovery counters and empty fault history. The controller consumes
  those results. These APIs cannot certify execution/recovery success.
- **Adapter/cache recovery is process-memory only.** `FabDemoService.scala:58–62`
  holds the shared adapter in a volatile `Option`; simulator registrations are
  in-memory maps (`core/.../ActorEquipmentAdapter.scala:28–51`). Bootstrap falls
  back to a new adapter when it is absent (`ApplicationBootstrap.scala:111–113`).
  This does not reconstruct simulator registrations after a JVM restart.
- **Empty production state fallback.** `ApplicationBootstrap.scala:150–151`
  supplies `FabDemoState(wafers = Map.empty)`; a similar service factory exists at
  `FabDemoService.scala:1346–1348`. The test proves replay from an available
  checkpoint/snapshot, not reconstruction of missing initial domain state in the
  parent-Started/child-RunStarted crash window.
- **LevelDB/Mongo read-source mismatch remains.** Active write journal/snapshot
  configuration is LevelDB/local (`web/conf/application.conf:33–38`), while
  `WorkOrderProjection.scala:25–34` and `WorkOrderCompletionProjection.scala:30–39`
  explicitly query `MongoReadJournal` and use JDBC projection sessions. Merely
  starting MongoDB does not connect those projections to LevelDB or initialize
  required JDBC pools/schemas. DB/projection startup and effects were not tested.
- **Projection caches are not durable business state.** View-handler maps and the
  lot→work-order completion associations remain in-memory
  (`FabDemoViewHandler.scala:32–41`, `WorkOrderCompletionHandler.scala:37–55`).
  Committed projection offsets alone do not prove reconstruction of those maps.
- **Two journals are not one transaction.** A committed child checkpoint and its
  parent domain/projection notification have separate persistence boundaries.
  The suite does not certify the crash window between child commit and parent
  callback journaling, missing-callback reconciliation, or cross-journal
  exactly-once progress. Uncommitted stages can be retried; external business
  side-effect idempotency remains the host's responsibility.
- **No full JVM/business E2E claim.** Recovery was same-system actor stop/respawn
  with testkit data, not JVM termination/restart, production LevelDB/Mongo replay,
  sharding restart, HTTP/UI acceptance, real equipment recovery, lot aggregate
  persistence, Saga transactions, or projection-driven work-order completion.
  Inline OCAP and normal checkpoint recovery are separate scenarios; recovery
  immediately after an OCAP checkpoint was not independently exercised.
- **No legacy parent journal migration certification.** Existing Scala domain
  schemas were preserved, but historical parent records serialized by external
  overrides/older serializers were not supplied or tested. New parent protobuf
  bindings do not themselves migrate existing records.

`git diff --check` passed. No commit, push, tag, remote publication, deletion,
service/container lifecycle operation, or other-repository build/edit was done.
