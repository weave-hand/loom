# Deploy hardening — schema migrations + S3 object store in the Helm chart

- **Date:** 2026-06-30
- **Area:** deploy
- **Register items:** promotes [[fut-deploy-followups]] → mints [[road-deploy-hardening]]; records [[fut-deploy-s3-credentials-identity]]
- **Status:** spec (ready for a work agent to plan + build)

## North star

A fresh `helm install` produces a **working, scalable** loom: the control-plane schema is
applied automatically (no out-of-band step), and an operator can point loom at S3/MinIO
instead of a single-node `ReadWriteOnce` PVC — removing the co-scheduling constraint that
keeps the two services pinned to one node. This closes the demo→deployable gap.

## Current state (reconciled)

[[road-deploy-mvp]] shipped the apko/Wolfi images + the umbrella Helm chart
(`deploy/chart/chart`). Reconciling [[fut-deploy-followups]]'s three named gaps against the
chart as it stands **today**:

- **Engine sidecar — already shipped.** `templates/query-api.yaml` runs the engine as a
  sidecar container in the query-api pod, sharing a unix socket (`engine.socketPath` on an
  emptyDir), with `LOOM_ENGINE_SOCKET` wired. The `engine` apko image
  (`deploy/images/engine`) and `values.yaml` `engine:` block exist. **This gap is closed**;
  the fut entry is stale on it.
- **S3 object store — the binary already supports it; only the chart doesn't.**
  `datafusion-io::object_store_url_for` resolves `s3://bucket/...`, and
  `service_runtime::{build_storage_factory, build_serving_object_store}` build an S3 store
  from the object-store config (`cfg.object_store.warehouse_uri`, …) — the engine main
  already wires them. But the **chart** sets only `LOOM_DATA_PATH` to a PVC mountPath
  (a `file://` store) on a `ReadWriteOnce` PVC, co-scheduling query-api onto ingest's node
  (`values.yaml` objectStore + the query-api affinity note). **No code change needed — this
  is chart wiring.**
- **Schema migrations — genuinely open.** `control_plane_postgres::run_embedded_migrations`
  (the `sqlx::migrate!("./migrations")` migrator) is invoked **only in the embedded-PG
  branch** of `build_pool_managed` (`runtime/src/lib.rs:277`). The **managed/external branch
  — the path the chart's CloudNativePG deployment uses — never migrates**, so
  `templates/NOTES.txt` and `postgres-cnpg.yaml` both tell the operator to "apply migrations
  out of band." A `helm install` therefore yields services that cannot serve until a human
  runs migrations.

So the remaining work is **two chart-level concerns** (one needing a small code seam):
applying migrations on the managed path, and wiring S3 as an object-store option.

## Design

### Migrations — both mechanisms, chart-selectable

Provide **two** ways to migrate the managed Postgres, selected by a chart value, plus the
existing out-of-band escape hatch:

**Code seam (small, shared by both):**

- Expose migration application for the **managed** pool — `run_embedded_migrations`
  (already used for embedded) called against the managed pool. Two entry points reuse it:
  - a **migrate-and-exit entrypoint** (a `LOOM_MIGRATE=apply` mode / subcommand on the
    service binary, or a tiny dedicated mode) that builds the managed pool, runs the
    migrator, and exits 0 — for the Job; and
  - an **on-boot flag** (`LOOM_DB_MIGRATE_ON_BOOT`, default off) that, when set, makes
    `build_pool_managed`'s managed (`None`) branch run the migrator after building the pool,
    mirroring the embedded branch.

**Chart (`migrations.mode`):**

- **`job`** (default): a Helm `pre-install,pre-upgrade` **hook Job** that runs the
  migrate-and-exit entrypoint against the (CNPG or external) Postgres before the Deployments
  roll. Least-privilege — DDL credentials live on the one-shot Job, not the long-running
  services — and makes the chart's current "out of band" note a first-class, automatic step.
- **`onBoot`**: set `LOOM_DB_MIGRATE_ON_BOOT=true` on the ingest + query-api Deployments;
  no Job. Fewest moving parts; concurrent replicas race on the **sqlx migrator's advisory
  lock** (safe — the first wins, the rest no-op), at the cost of every pod needing DDL
  rights.
- **`external`**: neither (today's behaviour) — for operators who manage schema separately.

`NOTES.txt` / `postgres-cnpg.yaml` notes are updated to reflect the selected mode.

### S3 object store — optional, alongside the PVC

Add an `objectStore.s3` values block and wire it without forcing it:

- **Values:** `objectStore.s3.{enabled, endpoint (MinIO), bucket, region, pathStyle,
  credentialsSecret}` (the secret carries access-key/secret-key).
- **When `enabled`:** set loom's object-store config env (the `warehouse_uri =
  s3://bucket/…`, endpoint, region, and credentials-from-secret that `build_storage_factory`
  already consumes) on **all three** containers (ingest, query-api, engine); **skip** the
  shared PVC mount + `LOOM_DATA_PATH` local path; **relax** the query-api→ingest
  co-scheduling affinity (the RWX/single-node reason is gone — each pod reaches S3 over
  egress); and open a **NetworkPolicy egress** rule to the S3/MinIO endpoint (the chart is
  default-deny-egress).
- **When disabled (default):** the current `file://` PVC path is unchanged.

The binary needs **no change** — it already builds an S3 store from this config.

### Decided (not open)

- **Engine sidecar is out of scope — already shipped** (reconciliation correction).
- **Both migration mechanisms built**, `migrations.mode` (job default / onBoot / external) —
  per the "both configurable" call; `job` is the recommended default for the least-privilege
  posture.
- **S3 optional alongside the PVC** (default PVC) — no zero-dependency single-node install is
  taken away; S3 is opt-in and removes the multi-node constraint.
- **Credentials via a referenced K8s secret** this slice; cloud workload-identity (IRSA /
  GKE WI, secretless) is [[fut-deploy-s3-credentials-identity]].

## Scope

In scope:

- Code seam: managed-pool migration application — a migrate-and-exit entrypoint
  (`LOOM_MIGRATE`) **and** an on-boot flag (`LOOM_DB_MIGRATE_ON_BOOT`) in the managed branch,
  both reusing `run_embedded_migrations`.
- Chart `migrations.mode` (`job` hook Job / `onBoot` env / `external`) + updated NOTES.
- Chart `objectStore.s3` block: S3 config env on all three containers, PVC-skip when enabled,
  affinity relax, NetworkPolicy egress to the endpoint; PVC path unchanged when disabled.
- `helm lint` + `helm template` golden assertions (below) and a fixture test for the
  managed-branch migrate path.

Out of scope:

- **Binary S3 support** — already done; **end-to-end S3 serving test**
  ([[fut-iceberg-s3-serving-e2e]]) and **S3 multipart upload** ([[fut-iceberg-s3-multipart]])
  are separate iceberg-area items.
- **Cloud workload-identity / secretless S3** ([[fut-deploy-s3-credentials-identity]]).
- **Down/rollback migrations** (loom migrations are forward-only `sqlx::migrate!`); migration
  **observability**/locking changes; the engine sidecar (done).
- Replacing CNPG, multi-region, autoscaling (orthogonal deploy concerns).

## Testing

Deploy artifacts are validated by chart rendering + a focused code test (no cluster in the
buck2 harness):

1. **`helm lint`** stays green (the `helm_chart(lint = True)` rule).
2. **`helm template` golden assertions:**
   - `migrations.mode=job` (default) renders a `pre-install,pre-upgrade` hook **Job** running
     the migrate entrypoint, and the services have no on-boot flag.
   - `migrations.mode=onBoot` sets `LOOM_DB_MIGRATE_ON_BOOT=true` on both Deployments and
     renders **no** Job.
   - `migrations.mode=external` renders neither.
   - `objectStore.s3.enabled=true` renders the S3 config env on **all three** containers, no
     PVC mount / no `LOOM_DATA_PATH` local path, the relaxed affinity, and a NetworkPolicy
     egress rule to the endpoint.
   - default (no S3) still renders the PVC mount + the co-scheduling affinity.
3. **Managed-branch migrate code test** (`loom_fixture_test`): build a **managed** pool with
   `LOOM_DB_MIGRATE_ON_BOOT` set against a fresh database → the loom schema is present
   afterward (the embedded path is already covered; this pins the managed wiring + the
   migrate entrypoint).

## Risk

- **Migration mechanism touches startup/DDL.** Mitigated: both paths reuse the **already-
  proven** `run_embedded_migrations`/`embedded_migrator` (the embedded path + every fixture
  test exercise it); the on-boot race is covered by sqlx's advisory lock; `external` keeps
  the current behaviour for cautious operators. Default `job` isolates DDL rights to a
  one-shot pod.
- **S3 wiring is chart-only** (binary unchanged), so its blast radius is template rendering;
  the default (PVC) path is untouched, and the golden tests pin both branches. A
  misconfigured NetworkPolicy egress would block S3 — pinned by the egress-rule assertion.
- **Reconciliation risk** (speccing already-done work) is the one this spec most guards
  against: the engine sidecar was verified shipped and excluded, so the slice is exactly the
  two open gaps, not three.
