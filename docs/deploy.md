# Deploying loom (images + Helm chart)

This is loom's packaging: reproducible OCI images for the three service
binaries and a Helm chart that runs them on Kubernetes, plus a standalone
single-binary `loom` for environments without a cluster (see *Single-binary
`loom`* below). The image/Helm build
rules come from [`jomcgi/homelab`](https://github.com/jomcgi/homelab/tree/main/buck2),
consumed as a **git external cell** (`homelab`) rather than vendored or
submoduled — buck2 fetches the pinned commit automatically (see *Build rules*
below). The deployable targets live in their own `deploy//` buck2 cell
(deliberately off the normal `//src` CI sweep — see below).

_Capabilities as of 649b69f1._

## What ships

| Component | buck2 target | Image / artifact |
| --- | --- | --- |
| ingest service | `deploy//images/ingest:image` | `ghcr.io/weave-hand/loom-ingest` |
| query-api service | `deploy//images/query-api:image` | `ghcr.io/weave-hand/loom-query-api` |
| engine service | `deploy//images/engine:image` | `ghcr.io/weave-hand/loom-engine` |
| worker | `deploy//images/worker:image` | `ghcr.io/weave-hand/loom-worker` |
| Helm chart | `deploy//chart:chart` | `oci://ghcr.io/weave-hand/charts/loom` |
| standalone binary | `//src/services/standalone:loom` | not published (build locally) |

Each image is the buck2-built Rust binary (`x86_64-unknown-linux-gnu`, glibc)
layered onto a minimal apko/Wolfi base carrying just `glibc` + `libgcc` +
CA certs. query-api is a pure wire client over the engine's Arrow Flight
service, so it bundles no analytics engine and needs no extra runtime libs.
Its image also carries the wasm UI bundle at `/usr/share/loom/ui`; the chart
serves it from the query-api container by default (`ui.enabled`, `LOOM_UI_DIR`
SPA fallback), and `ui.apiBase` mounts a config.js override for path-prefix or
detached serving;
the **engine** ships as its own image and runs as a **sidecar container in the
query-api pod**, sharing a Unix socket (`engine.socketPath` on an emptyDir,
wired via `LOOM_ENGINE_SOCKET`). All containers run as
non-root (uid 65532) with the binary at the image entrypoint. Package versions
are pinned in the committed `apko.lock.json`; refresh with `apko lock apko.yaml`
when an `apko.yaml` changes (buck2 can fetch the pinned tool:
`buck2 run homelab//buck2/bin:apko -- lock apko.yaml`). A lockfile embeds a
checksum of its own `apko.yaml`, so locks are **not** interchangeable between
images even when the package sets match — generate, never copy.

### The worker (#449)

The **worker** drains the job queue — `flush_table`, `gc_table`, `compact_table`,
`sweep_orphans`, `transform`, `typed-transform`, `stream_consolidate`,
`stream_mv`, `build_vector_index`. Nothing else does: without one running, those
jobs are enqueued and accumulate forever, so transform, compaction, GC and
micro-batch MVs never happen. It reaches the engine **only over a pod-local Unix
socket**, so it must always be co-located with an engine.

Both deploy paths run one:

- **Chart** — a `loom-worker` Deployment carrying its **own engine sidecar** over
  a shared `engine-sock` emptyDir; structurally the query-api pod minus the HTTP
  surface. `worker.replicas` is therefore independent of `queryApi.replicas`, and
  transform/compaction compute stays out of the serving pod. Like query-api it is
  co-scheduled onto ingest's node by default so it can mount the ReadWriteOnce
  object-store PVC (override `worker.affinity` with an RWX class, or use S3).
- **Standalone `loom`** — composed **in-process** as a task in the same runtime,
  dialing the composite's internal engine socket after engine-ready.

The worker container carries **no database credentials**: it is a zero-pool wire
client and the engine owns Postgres (only the sidecar gets `LOOM_DB_*`). It does
need `LOOM_WAREHOUSE_URI` explicitly even in the default file:// mode — unlike the
pooled services it has no `Config`/`LOOM_DATA_PATH` fallback — which the chart
supplies via the worker-only `loom.workerWarehouseEnv` helper. It has no probes:
the image is binary-only and it exposes no port, so it fails loud and restarts if
the socket is absent, exactly like the engine sidecar.

## Images: build & push manually

```sh
# Build an image tar locally (no registry):
buck2 build deploy//images/ingest:image

# Push to ghcr at one or more runtime tags (crane must be authed: see below):
buck2 run deploy//images/ingest:image.push -- 0.1.0 latest
```

`:image.info` exposes the content digest (`OciImageInfo`) so the chart can
digest-pin the deployed image rather than chase a mutable tag.

Auth for `crane`/`helm` push (CI does this automatically):

```sh
echo "$GHCR_TOKEN" | buck2 run homelab//buck2/bin:crane -- auth login ghcr.io -u <user> --password-stdin
echo "$GHCR_TOKEN" | buck2 run homelab//buck2/bin:helm  -- registry login ghcr.io -u <user> --password-stdin
```

## Helm chart

```sh
# Lint / render with the standalone helm (or via buck2):
buck2 build deploy//chart:chart.lint
helm template loom deploy/chart/chart

# Package + push (version comes from Chart.yaml; images digest-pinned in values):
buck2 run deploy//chart:chart.push
```

### Prerequisites in the target cluster

- **CloudNativePG operator + CRDs** (`postgresql.cnpg.io/v1`) when
  `postgres.enabled=true` (default). The chart creates a CNPG `Cluster`; CNPG
  generates the `<release>-loom-pg-app` secret the services read.
- **Gateway API CRDs** (`gateway.networking.k8s.io/v1`) only if you enable the
  optional `HTTPRoute` (`gateway.enabled=true`).
- A **StorageClass** for the shared object-store PVC. loom does not choose one
  (`objectStore.storageClassName: ""` uses the cluster default).

### Object store and schema migrations (#273)

Both concerns landed with the deploy-hardening slice (#273), which closed the
demo→deployable gap: a fresh `helm install` yields a working loom with no
out-of-band schema step, and the warehouse can live on S3/MinIO instead of a
single-node PVC.

- **Schema migrations** are applied automatically per `migrations.mode`:
  `job` (default) runs a hook Job (the ingest image with `LOOM_MIGRATE=apply`),
  isolating DDL rights to a one-shot pod. On a **fresh install** it runs
  `post-install` — the bundled CNPG cluster and its secret are created in the main
  phase, so the migrate Job runs after them and its entrypoint retries the connect
  (~2 min) until Postgres is accepting connections; `helm install` blocks on the
  Job, so it returns only once the schema is applied. On an **upgrade** it runs
  `pre-upgrade` — the database already exists, so migrations land before the new
  Deployment pods roll (clean ordering for additive changes). `onBoot` sets
  `LOOM_DB_MIGRATE_ON_BOOT=true` on the service containers so each pod migrates at
  startup (sqlx's advisory lock serialises concurrent replicas); `external` applies
  neither, for operators who manage the schema themselves. The migrations are
  **baked into every binary** at compile time (`sqlx::migrate!` — no migrations
  on disk to mount), so any loom image, including the standalone `loom` binary,
  can act as the one-shot migrator via `LOOM_MIGRATE=apply`.
- **Object store — local PVC (default) or S3/MinIO.** By default the services use a
  `LocalFileSystem` warehouse at `LOOM_DATA_PATH`, backed by one PVC that ingest
  writes and query-api reads; with the default `ReadWriteOnce` the chart
  co-schedules query-api onto ingest's node (a default `podAffinity`) so both can
  mount it. For multi-node spread either supply an RWX `objectStore.storageClassName`
  and override `queryApi.affinity`, or set `objectStore.s3.enabled=true` to point
  the warehouse at S3/MinIO — that drops the PVC and the co-scheduling affinity, sets
  the S3 config env on all three containers, and opens a NetworkPolicy egress to the
  endpoint (`objectStore.s3.port`). Credentials come from a referenced secret
  (`objectStore.s3.credentialsSecret`).

### Security posture (defaults)

- **Default-deny NetworkPolicy** over all loom pods: no ingress at all; egress
  only to DNS and the in-cluster Postgres. Extend via `networkPolicy.extra*`.
- **No external ingress** until you opt in with `gateway.enabled=true`, which
  attaches an `HTTPRoute` to an existing parent Gateway *and* opens the single
  matching ingress NetworkPolicy rule. The `gatewayClassName` lives on the
  Gateway object (cluster-managed), not the route.
- **Hardened containers**: `runAsNonRoot`, `readOnlyRootFilesystem`, all
  capabilities dropped, `seccompProfile: RuntimeDefault`,
  `automountServiceAccountToken: false`. A writable `emptyDir` is mounted at
  `/tmp` (with `workingDir`/`HOME`/`TMPDIR` pointed there) as the only writable
  scratch space.

### Key values

See [`deploy/chart/chart/values.yaml`](../deploy/chart/chart/values.yaml) for
the full set; the notable knobs:

| Value | Default | Purpose |
| --- | --- | --- |
| `postgres.enabled` | `true` | Bundle CNPG, or use `postgres.external.existingSecret`. |
| `objectStore.storageClassName` | `""` | StorageClass for the data PVC (deployer's choice). |
| `objectStore.accessModes` | `[ReadWriteOnce]` | Set `[ReadWriteMany]` for multi-pod. |
| `networkPolicy.enabled` | `true` | Default-deny on/off. |
| `gateway.enabled` | `false` | Attach an HTTPRoute to a parent Gateway. |
| `gateway.parentRef.name` | `""` | Existing Gateway to route through. |
| `ui.enabled` | `true` | Serve the bundled web UI from query-api (`LOOM_UI_DIR`). |
| `ui.apiBase` | `""` | config.js override: API prefix when served behind a rewriting route. |

## Configuration model

All binaries (chart-deployed and standalone) share one typed, validated config
seam (#202): operational tuning knobs (inline/flush byte limits, Parquet write
tuning, worker poll/backoff, serving page limit, DB pool `max_connections`)
live in per-domain structs and resolve as three overlaid layers, **defaults <
file < env**:

1. **Defaults** — each struct's `Default` (the documented constants).
2. **Config file** — if `LOOM_CONFIG_FILE` points at a path, the **JSON**
   document there (e.g. a mounted ConfigMap value) is deserialized over the
   defaults; partial documents are valid, omitted keys keep their defaults.
   YAML is deliberately not supported yet.
3. **Environment** — flat `LOOM_*` vars override individual keys last, so
   secrets and per-pod tuning need no file edit.

Configuration is **fail-loud**: a present-but-malformed value from either the
file or an env var aborts startup with a `ConfigError` naming the offending
key — it never silently falls back to a default (#202). The remaining ad-hoc
readers that violated this (auth session/token TTLs, engine write-path tuning,
`LOOM_ENGINE_SOCKET`, query-api's UI/CORS/Flight/export-cap vars) were swept
onto the same fail-loud env-snapshot readers (#309); the mains now boot through
one shared `service_runtime::bootstrap` path. Safety guardrails (recursion/
graph-depth caps) and domain invariants (namespaces, job kinds) stay `const`
and are deliberately not configurable.

Surfacing the tuning knobs as first-class chart `values.yaml` fields is still
open (`fut-deploy-config-values-wiring`) — today an operator sets the raw
`LOOM_*` env vars or mounts a `LOOM_CONFIG_FILE` document by hand.

## Release pipeline (`.github/workflows/release.yml`)

Three independent flows, by event — there is **no** auto-increment; versioned
releases are intentional.

**Merge to `main` → `edge`.** Builds both images and pushes **immutable
`sha-<short>` tags only** — there is no moving image tag. The moving channel is
the **chart**: it's published as `bleeding-edge` (a SemVer `0.0.0-edge` that
`crane` also tags `bleeding-edge`), digest-pinned to those `sha-<short>` images,
with `appVersion: sha-<short>`. So "deploy latest main" = pull the
`bleeding-edge` chart, which references exact, immutable image digests. The job
**also** publishes a versioned chart at the committed `Chart.yaml` `version` when
that version isn't already in ghcr — bump it in a PR to cut a chart release.

**`workflow_dispatch` (version `X.Y.Z`) → `release`.** Run it from the Actions
tab with a version: it validates the format, refuses if `vX.Y.Z` already exists
(no clobber), tags the current main images `X.Y.Z` + `latest`, and cuts a
`vX.Y.Z` git tag + GitHub Release.

**Pull request → `dev-image`** (same-repo branches; fork PRs are skipped for
lack of push creds). On every commit it pushes dev preview images + chart:
`…/loom-ingest:0.0.0-pr<N>.sha<short>` (plus a moving `pr-<N>` tag) and
`charts/loom:0.0.0-pr<N>.sha<short>`. These SemVer prereleases sort below real
releases. The pushed refs are written to the job summary.

At a glance — **images** are immutable: `sha-<short>` (every main commit),
`X.Y.Z` + `latest` (releases), `0.0.0-pr<N>.sha<short>` / `pr-<N>` (PR previews).
The **chart** is the moving deployable: `bleeding-edge` (latest main), `X.Y.Z`
(`Chart.yaml`-versioned releases), `0.0.0-pr<N>.sha<short>` (PR previews); its
`appVersion` records the app snapshot it pins.

> The `deploy//` cell is kept off the `//src` CI sweep on purpose: apko
> fetches packages over the network and builds local-only, so building it on
> every PR would force local materialization. It is exercised only by this
> release workflow (Rust binaries still compile on BuildBuddy RE).

## Single-binary `loom`

For local development, CI environments, and edge deployments where Kubernetes is
unavailable, loom ships as a self-contained single binary that boots all three
services (engine, ingest, query-api) in one process with an embedded Postgres.

### What it is

`loom` (buck2 target `//src/services/standalone:loom`, shipped in #276) runs
the engine gRPC service (tonic over a Unix-domain socket), the ingest HTTP
service, and the query-api HTTP service as concurrent tasks in one tokio
runtime, via the same library serve seams (`engine::run` / `ingest::serve` /
`query_api::serve`) the lean per-service binaries use — the wiring exists once.
In embedded mode it boots the bundled Postgres distribution exactly once,
waits for the engine to be ready (an explicit readiness gate: listeners are
bound synchronously before the serve loops spawn), then opens the two HTTP
listeners. A single SIGINT or SIGTERM fans out graceful shutdown to all three
services, and the embedded Postgres is stopped cleanly (`pg_ctl stop -m fast`)
last, after its clients have drained. If any one serve task exits with an
error, the composite shuts the others down too and reports the originating
service; the embedded Postgres is stopped cleanly on **every** exit path —
clean shutdown, serve-task error, or startup failure (#278).

The binary is built the same way as the lean service images:

```sh
buck2 build //src/services/standalone:loom
```

### Embedded Postgres lifecycle

The embedded control plane is a real Postgres that loom owns end to end — the
same postgres adapter, `.sqlx` cache, and Iceberg mirror catalog as the
external-DB deploy, with no second backend:

- **Persistent cluster.** Data lives at `<LOOM_DATA_PATH>/pgdata`, the Unix
  socket at `<LOOM_DATA_PATH>/pgrun` (socket-only; no TCP port). First boot
  runs `initdb`; subsequent boots adopt the cluster in place (no re-init, data
  survives restarts). A single-owner guard fails fast if another loom already
  holds the data dir. Hardening (#247) added `0700` perms on `pgdata`/`pgrun`
  and fast-fail readiness (a dead `postgres` child surfaces its real error
  instead of burning the readiness timeout).
- **Self-extracting distribution.** The PG binaries ride inside `loom`
  (`include_bytes!` of the pinned distribution tarball) and self-extract on
  first boot to `<LOOM_DATA_PATH>/cache/pg-<version>/` (lock-free temp-dir +
  atomic rename; reused across restarts). No `LOOM_PG_BIN_DIR` needs to be
  pre-staged. The embed lives in a gated crate, so lean service binaries do
  not carry the ~12 MB.
- **Migrations baked in.** Schema migrations are embedded at compile time via
  `sqlx::migrate!` and applied on boot — nothing to mount, and the same
  embedded migrator backs `LOOM_MIGRATE=apply` and the chart's migration Job.

### Host prerequisites

Embedded mode runs loom's own bundled Postgres, but loom embeds **only its own
artifacts** — system libraries are the deployment environment's responsibility.
The bundled `postgres`/`initdb` dynamically link `libxml2.so.2`, so the host must
provide a system libxml2 exposing that soname (package `libxml2` on
Wolfi/Debian/Ubuntu/Fedora). If it is missing, `loom` fails fast at cluster start
with a named error —

```
embedded Postgres cannot start: missing shared library libxml2.so.2. loom bundles
only its own Postgres artifacts, not system libraries — install it on the host …
```

— rather than a cryptic loader failure. Some distros ship a newer soname (Arch
provides `libxml2.so.16`, not `.so.2`); `tools/dev-up.sh` carries a dev-only shim
that symlinks the newest system `libxml2.so.*` as `libxml2.so.2`, but that is a
local bridge, not a deployment posture.

**Image posture:** no standalone `loom` OCI image exists yet (the `deploy/`
images are the external-Postgres services, which do not need libxml2). When a
`deploy//images/loom` standalone image is added, its `apko.yaml` MUST include the
Wolfi `libxml2` package so the embedded cluster can boot.

### First-admin bootstrap (`loom create-admin`)

There is **no env-driven admin auto-bootstrap** — the old
`LOOM_BOOTSTRAP_ADMIN_*` variables are gone from all binaries. The sole
admin-creation path is the out-of-band CLI subcommand:

```sh
loom create-admin --username <name>   # password read from stdin (prompt on stderr)
```

It seeds the first admin (a normal identity holding the reserved `admin`
role, not an ACL-bypass superuser) and **seals** the instance in one sequence;
a second `create-admin` against the same database is refused. Run it against
the running instance's database (`tools/dev-up.sh` does this automatically
after boot). In embedded mode `create-admin` is a client-only connection: it
needs neither the PG-binary vars (`LOOM_PG_BIN_DIR`/`LOOM_PG_LD_LIBRARY_PATH`)
nor the `LOOM_DB_*` vars — config defaults them to the embedded socket, so
`LOOM_PG_MODE=embedded` + `LOOM_DATA_PATH` is enough. A chart-level one-shot
`create-admin` Job does not exist yet (`fut-helm-create-admin-job`).

### Environment variables

| Variable | Required | Default | Purpose |
| --- | --- | --- | --- |
| `LOOM_DATA_PATH` | yes | — | Root for Parquet/Iceberg data **and** the embedded-Postgres cache (`<path>/cache/pg-<version>/`). |
| `LOOM_ENGINE_SOCKET` | yes | — | Path to the Unix-domain socket the engine listens on and the other two services connect to. |
| `LOOM_BIND_ADDR` | yes | — | Required by config parsing but **unused by `loom`** (the composite binds the two dedicated addrs below); set any value. |
| `LOOM_PG_MODE` | no | `external` | Set to `embedded` to start the bundled Postgres automatically. |
| `LOOM_QUERY_API_BIND_ADDR` | no | `0.0.0.0:8080` | TCP address for the query-api HTTP listener. |
| `LOOM_INGEST_BIND_ADDR` | no | `0.0.0.0:8081` | TCP address for the ingest HTTP listener. |
| `LOOM_FLIGHT_BIND_ADDR` | no | — | If set, also exposes the engine's Arrow Flight SQL endpoint on this address. |
| `LOOM_PG_BIN_DIR` | no | — | Path to an external `pg_ctl`/`postgres` install. **Optional in embedded mode** — the binary self-extracts its baked-in Postgres distribution to `<LOOM_DATA_PATH>/cache/pg-<version>/` and wires it automatically. |
| `LOOM_MIGRATE` | no | — | Set to `apply` to run schema migrations and exit immediately (useful with an external/managed Postgres before starting the full process). |
| `LOOM_DB_HOST`, `LOOM_DB_PORT`, `LOOM_DB_USER`, `LOOM_DB_PASSWORD`, `LOOM_DB_NAME` | external only | — | Control-plane database connection vars. In **external** mode all five are required and are the real connection settings. In **embedded** mode they are **optional** — they default to the bundled cluster's socket (`LOOM_DB_HOST` → `<LOOM_DATA_PATH>/pgrun`, `LOOM_DB_PORT` → `5432`, `LOOM_DB_USER` → `postgres`, trust password, `LOOM_DB_NAME` → `loom`); set `LOOM_DB_NAME` to rename the database, otherwise omit them. |
| `LOOM_WAREHOUSE_URI` | no | — | Object-store warehouse URI (e.g. `s3://bucket/prefix` or a local `file://` path under `LOOM_DATA_PATH`). |

### Minimal quick-start

```sh
mkdir -p /tmp/loom-data

LOOM_PG_MODE=embedded \
LOOM_DATA_PATH=/tmp/loom-data \
LOOM_ENGINE_SOCKET=/tmp/loom-data/engine.sock \
LOOM_BIND_ADDR=0.0.0.0:0 \
./loom
```

The binary self-extracts Postgres on first run, applies schema migrations, and
starts accepting traffic on ports 8080 (query-api) and 8081 (ingest). The
extracted Postgres distribution is cached under
`/tmp/loom-data/cache/pg-<version>/` and reused on subsequent starts.

In embedded mode the `LOOM_DB_*` connection vars are optional — config defaults
them to the bundled Postgres socket (see the table above), so they are omitted
here. `LOOM_BIND_ADDR` is still required by config parsing but unused by the
composite (it binds the two dedicated HTTP addrs and talks to Postgres over its
Unix socket).

### Using an external Postgres

Set `LOOM_PG_MODE=external` (or omit it) and supply the `LOOM_DB_*` connection
variables. To apply schema migrations before starting the services:

```sh
LOOM_MIGRATE=apply \
LOOM_DB_HOST=db.example.com \
LOOM_DB_NAME=loom \
... \
./loom
```

When `LOOM_MIGRATE=apply` the process exits after migrations complete — it does
not start the HTTP listeners or the engine, making it suitable as a migration
init container.

## Known gaps

Open deploy-area register items (see `docs/ISSUES.md` / `docs/FUTURE.md` for
full context):

- `#fut-embedded-pg-cache-gc` — stale `pg-<version>/` extract caches are never
  swept after a version-pin bump (bounded disk leak).
- `#fut-embedded-postgres-pg-upgrade` — no `pg_upgrade` story when a PG major
  bump meets an existing `pgdata` (it errors clearly; migration is manual).
- `#fut-helm-create-admin-job` — the chart has no one-shot `create-admin` Job;
  first-admin bootstrap in a Helm deploy is a manual CLI run.
- `#fut-deploy-config-values-wiring` — the typed tuning knobs are not surfaced
  as `values.yaml` fields; operators set raw env vars.
- `#fut-deploy-data-path-optional-s3` — `LOOM_DATA_PATH` is required even under
  an `s3://` warehouse (the chart's S3 path sets an inert value).
- `#fut-deploy-s3-credentials-identity` — S3 credentials are static-secret
  only; no cloud workload identity (IRSA / GKE WI) yet.
- `#fut-graceful-shutdown-tls` — the lean per-service binaries have no graceful
  shutdown/signal handling and no TLS (the standalone composite has the signal
  wiring; TLS is open everywhere).
- `#fut-config-yaml-format` — `LOOM_CONFIG_FILE` is JSON-only; YAML authoring
  awaits a maintained YAML crate.

## Build rules (the `homelab` external cell)

The apko/oci/helm rules the `deploy//` cell loads (`@homelab//buck2/...`) are
**not** vendored or submoduled. They are consumed as a buck2 **git external
cell**: the `.buckconfig` declares

```ini
[cells]
  deploy = deploy
  homelab = homelab
[external_cells]
  homelab = git
[external_cell_homelab]
  git_origin = https://github.com/jomcgi/homelab.git
  commit_hash = <sha1>
```

and buck2 fetches that commit's tree into `buck-out` on demand. There is no
checkout to manage; `git_origin`'s repo just has to be reachable.

Two consumer-side details make the cross-cell consumption work:

- **`load()` needs the `@` prefix** — `load("@homelab//buck2/...")`, the same
  form loom uses for `@prelude//...`. A bare `homelab//...` in `load()` fails to
  parse.
- **`deploy/buck2/bin/` re-exports the rules' CLIs.** The homelab rules
  reference their tools as `//buck2/bin:<tool>`; a macro-emitted target label
  resolves in the *consuming* cell, so that becomes `deploy//buck2/bin:<tool>`.
  `deploy/buck2/bin/BUCK` aliases those names to `@homelab//buck2/bin:<tool>`. If
  the homelab rules are ever updated to reference `@homelab//buck2/bin:<tool>`
  directly, that shim can be deleted.

**Publishing a new rules version** is just pointing `commit_hash` at a newer
homelab commit (ideally a tagged release sha1 — buck2 requires a sha1, not a
branch/tag name). homelab needs no special packaging step.

Notes / alternatives:

- A git external cell fetches the whole `git_origin` repo tree at that commit.
  To keep that small, homelab can split the `buck2/` rules into a dedicated,
  public repo (keeping the top-level `buck2/` layout so the `//buck2/...` refs
  still resolve) and point `git_origin` there.
- **OCI is not an option here**: buck2 external cells are only `git` or
  `bundled` — there is no OCI-registry cell provider, so an "publish the rules to
  ghcr as an OCI artifact" flow would require an out-of-band `oras pull`/extract
  before every build, which is non-hermetic; the git external cell is the
  buck2-native path.
- `buck2 expand-external-cell homelab` materializes an editable local copy if you
  need to hack on the rules.
- **CI note:** the PR `affected` job uses `btd`/`supertd`, which parse the
  `root//...` graph themselves and cannot read external cells. Putting the
  deploy targets in their own `deploy//` cell keeps them (and the `homelab//`
  loads) out of `root//...`, so btd never has to resolve the external cell — only
  buck2 proper (the release job) builds `deploy//...`. (`.buckconfig` also sets
  `[project] ignore = _base` so a config change doesn't make btd descend into the
  job's base-commit checkout, whose nested prelude has latent errors.)
