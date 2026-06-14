# Deploying loom (images + Helm chart)

This is loom's MVP packaging: reproducible OCI images for the two service
binaries and a Helm chart that runs them on Kubernetes. The image/Helm build
rules come from [`jomcgi/homelab`](https://github.com/jomcgi/homelab/tree/main/buck2),
consumed as a **git external cell** (`homelab`) rather than vendored or
submoduled — buck2 fetches the pinned commit automatically (see *Build rules*
below). The deployable targets live under `//deploy` in the root cell
(deliberately off the normal `//src` CI sweep — see below).

## What ships

| Component | buck2 target | Image / artifact |
| --- | --- | --- |
| ingest service | `//deploy/images/ingest:image` | `ghcr.io/weave-hand/loom-ingest` |
| query-api service | `//deploy/images/query-api:image` | `ghcr.io/weave-hand/loom-query-api` |
| Helm chart | `//deploy/chart:chart` | `oci://ghcr.io/weave-hand/charts/loom` |

Each image is the buck2-built Rust binary (`x86_64-unknown-linux-gnu`, glibc)
layered onto a minimal apko/Wolfi base. The base carries `glibc` + `libgcc` +
CA certs; query-api additionally carries `libstdc++` for its embedded DuckDB
(the `duckdb` crate compiles bundled C++). Both run as non-root (uid 65532) with
the binary at the image entrypoint. Package versions are pinned in the committed
`apko.lock.json`; refresh with `apko lock apko.yaml` when an `apko.yaml` changes.

## Images: build & push manually

```sh
# Build an image tar locally (no registry):
buck2 build //deploy/images/ingest:image

# Push to ghcr at one or more runtime tags (crane must be authed: see below):
buck2 run //deploy/images/ingest:image.push -- 0.1.0 latest
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
buck2 build //deploy/chart:chart.lint
helm template loom deploy/chart/chart

# Package + push (version comes from Chart.yaml; images digest-pinned in values):
buck2 run //deploy/chart:chart.push
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

- **Schema migrations are not applied by the chart.** CNPG provisions Postgres
  but loom's migrations (`src/control-plane/postgres/migrations`) must be applied
  out of band before the services can serve. A migration Job is future work.
- **Shared local object store.** The services currently use a `LocalFileSystem`
  object store at `LOOM_DATA_PATH`, backed by one PVC that ingest writes and
  query-api reads. Across pods this requires `ReadWriteMany`; with the default
  `ReadWriteOnce`, keep one replica each and co-schedule them, or supply an RWX
  `objectStore.storageClassName`. (A real S3/MinIO object store is on the
  roadmap and will remove this constraint.)

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

On every merge to `main` the `release` job:

1. Reads the previous published version (highest semver tag of the ingest image
   in ghcr).
2. Derives the next version from the Conventional Commits since that release's
   `vX.Y.Z` git tag: `feat` → minor, `fix`/`perf` → patch, `!`/`BREAKING CHANGE`
   → major. No release-worthy commits ⇒ no-op.
3. Builds and pushes both images to ghcr at `X.Y.Z` + `latest`.
4. Stamps the chart `version`/`appVersion`, digest-pins the freshly built images
   into the chart's `values.yaml`, and pushes the chart to ghcr.
5. Cuts a `vX.Y.Z` git tag and a GitHub Release with the packaged chart attached.

`workflow_dispatch` accepts an explicit `version` to bypass commit analysis (use
it to cut the first release before any `vX.Y.Z` tag exists).

On **pull requests** (same-repo branches; fork PRs are skipped for lack of push
creds), the `dev-image` job builds and pushes dev-tagged preview images + chart
to ghcr on every commit: `…/loom-ingest:0.0.0-pr<N>.<sha>` (plus a moving
`pr-<N>` tag) and `charts/loom:0.0.0-pr<N>.<sha>`. These SemVer prereleases sort
below real releases and are ignored by the version detection, so they never
affect the auto-increment on `main`. The pushed refs are written to the job
summary.

> The `//deploy` targets are kept off the `//src` CI sweep on purpose: apko
> fetches packages over the network and builds local-only, so building them on
> every PR would force local materialization. They are exercised only by this
> release workflow (Rust binaries still compile on BuildBuddy RE).

## Build rules (the `homelab` external cell)

The apko/oci/helm rules `//deploy` loads (`homelab//buck2/...`) are **not**
vendored or submoduled. They are consumed as a buck2 **git external cell**: the
`.buckconfig` declares

```ini
[cells]
  homelab = homelab
[external_cells]
  homelab = git
[external_cell_homelab]
  git_origin = https://github.com/jomcgi/homelab.git
  commit_hash = <sha1>
```

External cells resolve only from the **root** cell, so `//deploy` lives in the
root cell (not its own cell) — that's how `load("homelab//buck2/...")` resolves
under `buck2 run`.

and buck2 fetches that commit's tree into `buck-out` on demand. There is no
checkout to manage; `git_origin`'s repo just has to be reachable and the rules
keep using only cell-relative `//buck2/...` + `prelude//...` refs (so they
resolve the same in homelab or here).

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
  `root//...` graph themselves and cannot read external cells. Since `//deploy`
  loads `homelab//...`, that job appends `deploy` to `[project] ignore` at
  runtime (supertd honors it, just like `_base`) so btd skips `//deploy`
  entirely. buck2 proper (the dev-image and release jobs) uses the committed
  config and builds `//deploy` normally — it resolves the external cell from the
  root cell. (`[project] ignore = _base` likewise keeps btd out of the job's
  base-commit checkout, whose nested prelude has latent errors.)
