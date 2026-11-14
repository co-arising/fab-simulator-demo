# fab-simulator-demo

Semiconductor fab **equipment simulators** + **demo web UI** — Akka Typed actors
that emulate fab tools (AMHS, stocker, lithography scanner, CD-SEM, generic
equipment), with a Play web frontend for the fab demo and M3.5 self-healing demo.

Part of the [MadPolaris](https://github.com/MadPolaris) engine family.
Extracted from [MadPolaris/playAkkaCQRS](https://github.com/MadPolaris/playAkkaCQRS)
(`fab-simulation/` module + fab web UI). [中文版](README-zh.md).

## Modules

| Module | Description |
|--------|-------------|
| `core/` | Equipment simulators: `AmhsSimulator`, `StockerSimulator`, `LithographySimulator`, `CdSemSimulator`, `GenericEquipmentSimulator` (+ protocol, model) |
| `web/` | Play 2.8 demo: fab simulation dashboard, M3.5 self-healing demo (3D view, fault injection, OCAP rules), WebSocket event streams |

The `web` module contains the full CQRS stack extracted from playAkkaCQRS
(`net.imadz.domain` / `net.imadz.application` / `net.imadz.infrastructure` —
133 files): aggregates, projections, sagas, routing, repositories.

## Run it

```bash
sbt "web / run"
```

- Fab demo: http://localhost:9000/fab-demo
- Fab designer: http://localhost:9000/fab-designer
- M3.5 self-healing: http://localhost:9000/fab-demo/m35
- M3.5 3D view: http://localhost:9000/fab-demo/m35-3d

The checked-in write journal is **local LevelDB**, with local snapshots; several
projections instead query MongoDB at `127.0.0.1:27017`. Starting MongoDB alone does
not reconcile those sources or initialize JDBC projection storage. Review
`web/conf/application.conf` and the [validation limitations](VALIDATION-0.2.1.md)
before treating the UI as an end-to-end recovery demonstration.

## Monarch 0.2.1 candidate validation

Only `monarch-core` and `monarch-persistence` are pinned to `0.2.1`;
`common-core` and `saga-core` remain at `0.2`. No remote publication was performed.
Until these Monarch artifacts are available from your normal repositories, opt
in to an existing Maven-layout candidate repository with `-Dmonarch.repository`.
The resolver is not enabled unless the property is supplied; it does not build
or publish sibling projects.

Validated local candidate (replace this URL on another machine)

```bash
cd /Users/zhongdj/Workspaces/mad-polaris/fab-simulator-demo
export JAVA_HOME="$(/usr/libexec/java_home -v 11)"
MONARCH_REPOSITORY="file:///Users/zhongdj/Workspaces/mad-polaris/monarch-persistence/target/demo-candidate-20261021"
sbt -Dmonarch.repository="$MONARCH_REPOSITORY" \
  "core / compile" "web / compile" "web / Test / compile" \
  "web / testOnly net.imadz.application.chain.LotAckGateSpec net.imadz.application.chain.FabPipelineExecutionActorSpec"
```

These tests run the actual persistent pipeline parent and Monarch child with
serializer-enabled persistence testkits, controlled equipment/lot boundaries,
and a local actor provider. They do not launch Play, MongoDB, containers, a
cluster, or listeners on `9000`/`2551`. Actor replay is not a true JVM restart or
complete business/Saga/projection E2E. See [VALIDATION-0.2.1.md](VALIDATION-0.2.1.md)
for exact artifact hashes, test evidence and remaining gaps. In particular,
child checkpoints and parent progress/terminal events are not reconciled across
crashes, and inline OCAP acknowledgement handling is incomplete. The recorded
9/9 result is scoped test evidence, not acceptance of those recovery windows.
A fresh isolated packaged-web launch also stopped before HTTP with a missing
`ClusterSharding` Guice binding; actual web workflow verification remains blocked.

For a separately configured application run, pass the same repository property
to `sbt ... "web / run"`; the default HTTP/remoting ports overlap with Bank, so
do not run both demos unchanged at the same time.

## Build

```bash
sbt compile
sbt "web / Test / compile"
```

## Note

This is a demo application, not a published artifact: it is never published
to Maven Central (`publish / skip := true`).

License: Apache-2.0.
