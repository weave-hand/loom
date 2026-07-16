# Worker deployment path Design

> **Status:** design (direction). This spec makes `road-deploy-worker` build-ready. A separate
> work agent claims the item, writes the implementation plan from this spec, and builds it.
> **Slice 1 of 2** — see *Out of scope* for the deferred transport slice.

## Problem

**No shipped loom runs the worker.** `worker-bin` is the only thing that constructs the job loop
outside tests, and it has no deployment path on either shipped surface.

Evidence:

- `control_plane_worker` is depended on by exactly two BUCK files: `src/control-plane/worker/BUCK`
  (itself) and `src/services/worker/BUCK`. The only `Worker::new` outside a `tests/` dir is
  `src/services/worker/src/main.rs:81`.
- `deploy/images/` contains `ingest/`, `query-api/`, `engine/` — **no worker image**.
- `deploy/chart/chart/templates/` contains `ingest.yaml`, `query-api.yaml`, `migrations-job.yaml`,
  `postgres-cnpg.yaml`, `networkpolicy.yaml`, `objectstore-pvc.yaml`, `httproute.yaml`,
  `serviceaccount.yaml`, `ui-configmap.yaml` — **no worker template**.
- `src/services/standalone/BUCK`'s `standalone` library deps are core/engine/ingest/query-api/
  runtime/managed-postgres — **no worker**, and `src/services/standalone/src/lib.rs` contains no
  worker reference. The single `loom` binary drains nothing either.

Consequence: every queue-driven job kind — `flush_table`, `gc_table`, `compact_table`,
`sweep_orphans`, `transform`, `typed-transform`, `stream_consolidate`, `stream_mv`,
`build_vector_index` — is enqueued by the control plane and **never drained**. Jobs accumulate in
`queue` indefinitely. This is not a degradation of an optional feature: transform, compaction, GC,
and micro-batch MVs are advertised capabilities that cannot run in any deployed configuration.

The gap was anticipated but never tracked. `fut-worker-lazy-compact-ctx`
(`docs/FUTURE.md`) records: *"no flush-only worker deployment exists yet … when the worker Helm
manifest is authored either set `LOOM_WAREHOUSE_URI` there or make `CompactCtx` construction
lazy/per-job"* — a prior session saw the manifest coming and left a note instead of a register item.

## Scope

Give the worker a deployment path on **both** shipped surfaces, **UDS-only**, with **no transport
change** and **no change to `store-config`**. Two halves, one acceptance question: *do queued jobs
actually drain?*

## Half A — standalone (`loom` single binary)

**Deps.** Add `//src/services/worker:worker` and `//src/control-plane/worker:worker` to the
`standalone` `rust_library` in `src/services/standalone/BUCK`.

**The zero-pool guard is not violated.** `src/services/worker/BUCK` asserts "NO
`//src/control-plane/postgres` in this dep list" on the **`worker-bin` binary**, not on the
`worker` **library** — the library's deps are core/loom-config/datafusion-io/engine-wire/
store-config/third-party, with no postgres. `standalone` already depends on postgres transitively
via `engine`, so composing the library in changes nothing about that guard. The guard's structural
uquery check still targets `worker-bin`.

**Insertion point.** `serve_composite` (`src/services/standalone/src/lib.rs:68`) already:

1. binds the engine UDS synchronously and spawns `engine::run` into a
   `JoinSet<(&'static str, Result<(), BoxErr>)>` (`lib.rs:107-125`);
2. **awaits `eng_ready_rx`** and surfaces the engine's real error if it dies before ready
   (`lib.rs:129-136`);
3. then binds/spawns ingest and query-api — where query-api dials `addrs.engine_socket`
   (`lib.rs:163-175`).

The worker task spawns **alongside query-api, after the `eng_ready_rx` gate**, dialing the same
`addrs.engine_socket`. Joining the same `JoinSet` gives it the existing error-cascade and shutdown
fan-out for free; no new lifecycle machinery.

**Object store — reuse `cfg`, do not re-parse env.** `worker-bin` calls
`store_config::ObjectStoreConfig::parse_from_env(&env)` (`main.rs:49`), which **requires**
`LOOM_WAREHOUSE_URI` and errors `Missing` without it. That is deliberate, not a bug —
`store-config/src/lib.rs:151-153` documents it: *"Parse from env without a `data_path` fallback:
`LOOM_WAREHOUSE_URI` is required (postgres-free callers like the zero-pool worker have no data
dir)."* Pooled services instead go through `service_runtime::Config::from_map`, which calls
`ObjectStoreConfig::parse(vars, &data_path)` (`src/services/runtime/src/lib.rs:293`) — the
data_path fallback.

In-process, `serve_composite` **already holds a resolved `cfg.object_store`**. The standalone worker
must build its write store from `cfg.object_store` via `service_runtime::build_write_store`, **not**
via `parse_from_env`. This sidesteps the `LOOM_WAREHOUSE_URI` requirement entirely — which matters,
because standalone's embedded mode sets only `LOOM_DATA_PATH`, so a `parse_from_env` call here would
fail on exactly the default single-binary configuration.

**Shutdown.** The composite fans shutdown out over a `tokio::sync::watch` consumed by the `sub(sd_rx)`
helper (`lib.rs:91-99`); the worker loop consumes a `tokio_util::sync::CancellationToken`
(`worker/src/main.rs`). Bridge them: await `sub(sd_rx)`, then `token.cancel()`. The worker task must
return from its `JoinSet` entry on cancellation so the composite's join logic behaves as it does for
the other three.

**Tuning.** `worker-bin` composes `datafusion_io::JobConfig` via `loom_config::load(&env)` and reads
`LOOM_COMPACT_THRESHOLD_BYTES` (default `128 * 1024 * 1024`). `StandaloneTuning`
(`standalone::StandaloneTuning::from_map`) gains the equivalent fields so the in-process worker is
tuned the same way, through the same defaults < file < env layering. Match `worker-bin`'s strict
parse semantics — a malformed value fails startup rather than silently defaulting.

**Contexts.** `CompactCtx`, `TransformCtx`, and `StreamMvCtx` are constructed as in
`worker-bin`'s `main`, with the clients (`GrpcQueueClient`, `FlightTableClient`, `FlightSqlClient`)
connected to `addrs.engine_socket`.

## Half B — Helm chart

**Image.** New `deploy/images/worker/{apko.yaml,apko.lock.json,BUCK}`, mirroring
`deploy/images/engine/` exactly: Wolfi base with `ca-certificates-bundle` + `glibc` + `libgcc`,
nonroot uid/gid 65532, `archs: [x86_64]`, entrypoint `/usr/local/bin/worker`; BUCK stages
`root//src/services/worker:worker-bin` via `tar_layer` at that path and publishes
`ghcr.io/weave-hand/loom-worker`. Refresh the lockfile with `apko lock apko.yaml`.

**Template.** New `deploy/chart/chart/templates/worker.yaml`: a Deployment carrying **`worker-bin`
+ an engine sidecar**, sharing an `engine-sock` emptyDir — structurally the query-api pod minus the
HTTP surface. Why its own pod rather than a container in the query-api pod: it gives
`worker.replicas` independent of serving with no transport work, keeps transform/compaction
DataFusion compute out of the serving pod, and multi-engine is already de facto real (any
`queryApi.replicas > 1` already yields multiple engine sidecars).

Required specifics:

- **The worker container gets no `loom.dbEnv`.** It is zero-pool; only its engine sidecar carries
  DB credentials. Assert this — it is a posture property, not an oversight.
- **`LOOM_WAREHOUSE_URI` must be set on the worker container in *both* modes.** `loom.objectStoreEnv`
  (`_helpers.tpl`) emits `LOOM_WAREHOUSE_URI` **only when `objectStore.s3.enabled`**; the default
  file:// path emits `LOOM_DATA_PATH` alone. Since `worker-bin` calls `parse_from_env` eagerly in
  `main`, a worker wired to the bare helper **crash-loops on a default install**. Emit
  `file://<objectStore.mountPath>` in the non-S3 case via a **small dedicated helper** (e.g.
  `loom.workerWarehouseEnv`) used by the worker container only. Do **not** mutate the shared
  `loom.objectStoreEnv` — the three existing services' rendered env must stay byte-identical.
- **`LOOM_WORKER_ID` from `fieldRef: metadata.name`**, giving a stable per-pod queue-lease identity;
  the binary otherwise mints a fresh uuid per process restart.
- **No probes.** Same rationale as the engine sidecar (see the comment in `query-api.yaml`): the
  image is binary-only, so an exec probe has nothing to run (#356), and the worker exposes no port
  to `tcpSocket`-probe. It fails loud and restarts if the engine socket is absent.
- **`data` volume mount and the co-scheduling podAffinity only when `!objectStore.s3.enabled`**,
  mirroring `query-api.yaml`'s default (co-schedule onto ingest's node so both can mount the
  ReadWriteOnce PVC). With S3, neither applies.
- Engine sidecar env mirrors query-api's: placeholder `LOOM_BIND_ADDR`, `loom.objectStoreEnv`,
  `LOOM_LOCK_TIMEOUT_MS`, `HOME`/`TMPDIR` = `/tmp`, `LOOM_ENGINE_SOCKET`, `loom.dbEnv`,
  `loom.migrateOnBootEnv`.
- Pod carries `loom.labels` / `loom.selectorLabels` like every other loom workload.

**NetworkPolicy needs no change.** Every policy in `networkpolicy.yaml` selects on
`loom.selectorLabels`, which the worker pod carries — so it inherits default-deny plus the DNS,
Postgres, and S3 egress allowances automatically. (The worker container itself needs no egress; its
engine sidecar shares the pod's network identity.)

**Values.** A `worker:` block in `values.yaml` mirroring `queryApi:` — `image.{repository,tag,digest,
pullPolicy}`, `replicas` (default `1`), `resources`, `nodeSelector`, `tolerations`, `affinity` —
documented in the same comment idiom as its neighbours.

## Release wiring

Easy to miss, and without it the chart references an image that is never built or pushed:

- `.github/workflows/release.yml`: add `WORKER_IMAGE: ghcr.io/weave-hand/loom-worker` alongside the
  three existing image envs (~lines 40-42) and a `buck2 run deploy//images/worker:image.push` line
  to **each** of the three push blocks (~86-88, ~161-163, ~213-215).
- `deploy/chart/BUCK`: add `"worker.image": "//images/worker:image.info"` to `helm_chart`'s `images`
  map so the worker image is digest-pinned into `values.yaml` on the release path. Update the
  rule's header comment, which still says "the two service images".

## Testing

**Half A is the acceptance surface, and it is the half CI actually guards.**
`src/services/standalone/tests/composite_e2e.rs` already stands up the full composite (a
`loom_fixture_test` target `composite-e2e`). Extend it: enqueue a job against the running composite
and assert it **drains** — the direct, honest proof that jobs no longer pile up. This test fails on
`main` today (nothing drains), which is what makes it a real regression guard rather than a
tautology. Prefer a job kind whose completion is observable without heavy setup; a `flush_table` on
a landed table is the natural candidate.

**Half B** extends `deploy/chart/tests/render_assertions.sh` (golden `helm template` assertions):

- the worker Deployment renders, with both containers;
- the worker container has **no `LOOM_DB_`** env (zero-pool posture);
- `LOOM_WAREHOUSE_URI` is present on the worker container in **both** `s3.enabled=false` (as
  `file://…`) and `s3.enabled=true` (as `s3://…`);
- `data` mount + podAffinity present when `!s3`, absent when `s3.enabled=true`;
- the three existing services' rendered env is unchanged by the new helper.

**Be explicit about the limit:** the `deploy//` cell is off the `//src` CI sweep and needs the
`homelab` external cell, and `render_assertions.sh` requires `helm` on PATH and is not run in CI
(its own header says so). Half B is therefore verified locally / on the release path only. This is
the established pattern for this chart, not a new concession — but it means Half A's e2e is the only
automated guard, and the implementation plan should not claim otherwise.

## Out of scope

**Slice 2 — configurable TCP transport + distributed topology.** Already tracked by
`fut-engine-wire-multi-tls` ("Multiple engines / pooling / TLS / auth on the engine socket"), whose
own text defers this *"until loom runs the engine/worker/query-api across a trust boundary"* —
precisely what a networked engine introduces. That slice makes the transport selectable (UDS today,
TCP added) so components can be deployed as independent, independently-scaled Deployments against a
shared engine Service, and it must decide the auth/TLS posture for a socket that currently assumes
local trust. It builds on this slice; the seam is narrow on both sides (client: `uds_channel`,
`engine-wire/src/lib.rs:20`; server: `engine::run` already takes a bound listener, so
`engine/src/main.rs` binds and passes it). A later planning pass specs it and promotes
`fut-engine-wire-multi-tls`.

**`fut-worker-lazy-compact-ctx` stays open.** This spec takes its sanctioned "set
`LOOM_WAREHOUSE_URI` in the manifest" branch, discharging the manifest note; lazy/per-job
`CompactCtx` construction remains a live idea for a flush-only worker that should not need warehouse
config or a reachable Flight endpoint at startup. The work agent should update that entry's prose to
record that the manifest now exists, rather than closing it.

**Not touched:** `store-config` (the `parse_from_env` contract is correct as written), the
`worker-bin` zero-pool guard, ingest/query-api/engine templates, and the NetworkPolicy.
