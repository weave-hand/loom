# Deploying loom (images + Helm chart)

This is loom's MVP packaging: reproducible OCI images for the two service
binaries and a Helm chart that runs them on Kubernetes. The image/Helm build
rules come from [`jomcgi/homelab`](https://github.com/jomcgi/homelab/tree/main/buck2),
consumed as a **git external cell** (`homelab`) rather than vendored or
submoduled — buck2 fetches the pinned commit automatically (see *Build rules*
below). The deployable targets live in their own `deploy//` buck2 cell
(deliberately off the normal `//src` CI sweep — see below).

## What ships

| Component | buck2 target | Image / artifact |
| --- | --- | --- |
| ingest service | `deploy//images/ingest:image` | `ghcr.io/weave-hand/loom-ingest` |
| query-api service | `deploy//images/query-api:image` | `ghcr.io/weave-hand/loom-query-api` |
| Helm chart | `deploy//chart:chart` | `oci://ghcr.io/weave-hand/charts/loom` |

Each image is the buck2-built Rust binary (`x86_64-unknown-linux-gnu`, glibc)
layered onto a minimal apko/Wolfi base. The base carries `glibc` + `libgcc` +
CA certs; query-api additionally carries `libstdc++` for its embedded DuckDB
(the `duckdb` crate compiles bundled C++) **and** layers the vendored
`ducklake`/`postgres_scanner` DuckDB extensions at `/opt/duckdb/extensions`
(`DUCKDB_EXTENSION_DIR`), which the binary `LOAD`s at attach time. Both run as
non-root (uid 65532) with the binary at the image entrypoint. Package versions
are pinned in the committed `apko.lock.json`; refresh with `apko lock apko.yaml`
when an `apko.yaml` changes.

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

### Known limitations (MVP)

- **Schema migrations** are applied automatically per `migrations.mode`:
  `job` (default) runs a `pre-install`/`pre-upgrade` hook Job (the ingest image
  with `LOOM_MIGRATE=apply`) before the Deployments roll, isolating DDL rights to
  a one-shot pod; `onBoot` sets `LOOM_DB_MIGRATE_ON_BOOT=true` on the service
  containers so each pod migrates at startup (sqlx's advisory lock serialises
  concurrent replicas); `external` applies neither, for operators who manage the
  schema themselves. `mode=job` assumes the Postgres endpoint is reachable at hook
  time (the CNPG operator prerequisite already covers this).
- **Shared local object store.** The services currently use a `LocalFileSystem`
  object store at `LOOM_DATA_PATH`, backed by one PVC that ingest writes and
  query-api reads. With the default `ReadWriteOnce` the chart co-schedules
  query-api onto ingest's node (a default `podAffinity`) so both can mount it;
  for multi-node spread, supply an RWX `objectStore.storageClassName` and
  override `queryApi.affinity`. (A real S3/MinIO object store is on the roadmap
  and will remove this constraint.)

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
  `/tmp` (with `HOME`/`TMPDIR` pointed there) for DuckDB scratch.

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
