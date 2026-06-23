# BuildBuddy Workflows CI — design

**Status:** approved (brainstorming)
**Date:** 2026-06-23
**Topic:** Replace the GitHub Actions CI with a BuildBuddy-native `buildbuddy.yaml`.

## Problem & goal

loom's CI runs on GitHub Actions (`.github/workflows/ci.yml`) but already offloads
all build/test compute to BuildBuddy remote execution (`[buck2_re_client]` in
`.buckconfig`). The GitHub runner is therefore a thin orchestrator that, on every
fresh run, re-materializes inputs and leans on GitHub-specific machinery
(composite actions, `actions/cache`, `github.event.pull_request.base.sha`, a
Discord GitHub Action).

**Goal:** make a repo-root `buildbuddy.yaml` the single source of truth for CI,
running the same build/test/lint coverage as BuildBuddy-native Workflows. Workflow
VMs are co-located with the RE/cache and are snapshotted/reused, so warm runs skip
both toolchain materialization and software installs.

This is a **replace**, not a parallel/complement: GitHub Actions is removed once the
BuildBuddy workflow is proven green (see *Cutover* below).

## Decisions (from brainstorming)

1. **Role:** Replace GitHub Actions. `buildbuddy.yaml` becomes the source of truth.
2. **PR strategy:** Faithfully port the `btd` *affected* job (not a full build), so PRs
   build/test only impacted first-party targets — a 1:1 mirror of today's behavior.
3. **Discord:** Dropped. Rely on BuildBuddy's own notification integrations + GitHub
   commit statuses. No webhook plumbing in the repo.

## Background: how BuildBuddy Workflows differ from GitHub Actions

- Config is a single `buildbuddy.yaml` at the repo root, holding a list of **actions**
  (named command groups), each with its own `triggers`.
- Triggers: `push` (with `branches`/`tags`), `pull_request` (matches the **base**
  branch; `merge_with_base: true` by default merges PR head with base before running),
  and `schedule`. Pattern matching supports `*` and `!`-negation, last-match-wins.
- Steps are **arbitrary bash** (`- run: ...`), so `buck2` is fully supported. Each
  step is a fresh bash process; only `env:` values persist across steps.
- **No composite actions, no `actions/*` marketplace, no `actions/cache`.** Software is
  installed with `apt-get`, guarded by existence checks because **workflow VMs are
  snapshotted and reused** between runs, so installs persist.
- Built-in images: `ubuntu-{18.04,20.04,22.04,24.04}`. Per-action `os`/`arch`
  (`linux`/`amd64` defaults), `container_image`, `resource_requests`
  (`cpu`/`memory`/`disk`), `env`, and `timeout`.
- **Secrets** configured in the BuildBuddy org/repo settings are auto-injected as env
  vars into trusted runs.
- Actions are **independent runs**: there is no GitHub-style `needs:`/job-aggregation
  step. Each action reports its own status to BuildBuddy and to GitHub commit statuses.

### Verified runner facts (from docs + `ci_runner` source)

These were checked against `enterprise/server/cmd/ci_runner/main.go` and the
workflows-config / secrets docs, and they shape the design below:

- **No git-context env vars.** The runner injects only `BUILDBUDDY_*` vars
  (`BUILDBUDDY_CI_RUNNER_ROOT_DIR`, `BUILDBUDDY_ARTIFACTS_DIRECTORY`,
  `BUILDBUDDY_RUN_ID`, `BUILDBUDDY_CI_RUNNER_ABSPATH`) plus `HOME`, `USER`
  (=`buildbuddy`), `PATH`, `TERM`, `GIT_TERMINAL_PROMPT=0`, `DEBIAN_FRONTEND`. There is
  **no `GIT_BASE_BRANCH` / `GIT_BRANCH` / `GIT_COMMIT_SHA`** exposed to steps. The
  affected job therefore must not depend on such a var (see below — it derives the base
  from the trigger instead).
- **PR HEAD is a merge commit by default.** With `merge_with_base: true` (the default)
  the runner does `git merge --no-edit <base>` and that merged commit becomes HEAD.
  Setting **`merge_with_base: false`** leaves HEAD at the clean PR branch head — which
  btd's base→head diff requires.
- **Shallow fetch by default** (depth 1). `git_fetch_depth` (int) and
  `git_fetch_filters` (default `["blob:none"]`) are per-action config. The runner
  deepens to full history for PRs that need a merge base, but we don't rely on that
  implicit behavior — the affected action sets `git_fetch_depth: 0` explicitly.
- **Submodules are not checked out by the runner.** `git submodule update --init
  --recursive` is load-bearing (prelude) and must also run inside the base worktree.
- **Secrets are env vars for trusted runs**, but `BUILDBUDDY_API_KEY` is **not**
  auto-present for non-bazel tools; it must be added as an org secret named exactly
  `BUILDBUDDY_API_KEY` (see *Secrets & RE wiring*).
- **Default resources: 3 CPU / 8 GB / 20 GB** — ample here, since build and test both
  run on RE and the runner only orchestrates (no `resource_requests` override).

## Files

| File | Change |
|------|--------|
| `buildbuddy.yaml` | **Add** — root CI config: `build-test`, `affected`, `lint` actions. |
| `tools/ci/buildbuddy-setup.sh` | **Add** — shared, idempotent per-action setup (zstd + bsdtar + pinned buck2 + submodule init). |
| `.github/workflows/ci.yml` | **Remove** (gated Step B) — superseded by `buildbuddy.yaml`. |
| `.github/actions/install-bsdtar/` | **Remove** (gated Step B) — `ci.yml` was its only user. |
| `.github/actions/setup-buck2/` | **Keep** — still used by `release.yml` (image/Helm publishing stays on GitHub Actions). |
| `.github/workflows/release.yml`, `claude.yml` | **Keep** — out of scope; no clean BuildBuddy equivalent (`workflow_dispatch` releases, GitHub Releases, the Claude bot). |

## Component 1: shared setup script (`tools/ci/buildbuddy-setup.sh`)

**Purpose:** the DRY equivalent of the `setup-buck2` + `install-bsdtar` composite
actions plus submodule init. Sourced/run at the top of every action. Idempotent and
existence-guarded so warm (snapshotted) runs are near-instant. Mirrors the proven
buck2-install logic in `tools/cloud-setup.sh`.

**Responsibilities (each guarded — skip if already satisfied):**

1. **Packages:** `apt-get install -y zstd libarchive-tools` (bsdtar) only when the
   binaries are absent. `zstd` is required to decompress the buck2 release; `bsdtar`
   is required by the control-plane-postgres libxml2 genrule (runs locally on the
   runner).
2. **buck2:** install the pinned release to a **version-stamped path**
   (`~/.cache/loom-buck2/<BUCK2_RELEASE>/buck2`) and expose it on `PATH` (symlink into
   `/usr/local/bin` or prepend to `PATH`). Version-stamping means a `BUCK2_RELEASE`
   bump auto-reinstalls while the snapshot keeps prior versions warm. Download:
   `https://github.com/facebook/buck2/releases/download/<BUCK2_RELEASE>/buck2-x86_64-unknown-linux-gnu.zst`,
   `zstd -d`, `chmod +x`.
3. **Prelude submodule:** `git submodule update --init --recursive` (pinned alongside
   the buck2 release — keep `BUCK2_RELEASE` aligned with the prelude pin).

**The single buck2 pin** (`BUCK2_RELEASE`) lives in this script, carrying the existing
"keep aligned with the prelude submodule pin" comment. (Today the pin is duplicated in
`ci.yml` and `cloud-setup.sh`; this script becomes CI's copy.)

**Verify:** the script ends by printing `buck2 --version` so a broken install fails loudly.

## Component 2: `buildbuddy.yaml` actions

Common to all actions:

- `os: linux`, `arch: amd64`, `container_image: ubuntu-24.04` (matches GitHub's
  `ubuntu-latest`; the prebuilt `//tools:supertd`/`//tools:btd` fork binaries are
  linked against GLIBC_2.39, which `ubuntu-22.04`'s glibc 2.35 lacks — and `buck2 run`
  executes them locally on the runner).
- **No `resource_requests`** — inherit the runner default (3 CPU / 8 GB / 20 GB). Both
  build and test run on RE (`-M none`, `BUCK_PREFER_REMOTE`; the RE workers now run as
  non-root, so the fixture tests that boot `initdb`/`postgres`/`duckdb` run remotely
  too). The workflow runner is a thin orchestrator — its only local work is a few light
  genrules (e.g. the control-plane-postgres libxml2/bsdtar extract), so the default is
  ample. Add `resource_requests` only if a real run shows pressure.
- `env: { BUCK_PREFER_REMOTE: "true" }` — keep everything that can run on RE on RE
  (same rationale as today: avoid materializing the multi-GiB toolchain locally).
- First step runs the shared setup script.
- `BUILDBUDDY_API_KEY` reaches the action as an injected secret env var (see
  *Secrets & RE wiring*), satisfying `.buckconfig`'s `[buck2_re_client]` headers.

### Action `build-test`

- **Trigger:** `push.branches: [main]`. Mirrors today's "main must always be fully green".
- **Steps:** setup → `buck2 build -M none //src/...` → `buck2 test //src/...`.
- `-M none` validates the build on RE without downloading final artifacts. Tests
  (including the postgres/duckdb fixtures) run on RE; no `--local-only` needed.

### Action `affected` (PR btd port)

- **Trigger:** `pull_request.branches: [main]` with **`merge_with_base: false`** and
  **`git_fetch_depth: 0`**. Faithful port of `ci.yml`'s `affected`.
  `merge_with_base: false` is deliberate: btd diffs base→head, so HEAD must be the clean
  PR head, not a base+head merge commit. `git_fetch_depth: 0` guarantees the base
  history needed for `git merge-base`.
- **Deriving the base without an env var:** the runner exposes no `GIT_BASE_BRANCH`, but
  this action only fires on PRs whose base branch matches `[main]`, so the base is
  definitionally `main`. The step fetches it explicitly rather than assuming a remote
  ref is present: `git fetch --no-tags origin main` →
  `BASE_SHA="$(git merge-base FETCH_HEAD HEAD)"`.
- **Steps (after setup):**
  1. **Changes file:** `git fetch --no-tags origin main`; `BASE_SHA=$(git merge-base
     FETCH_HEAD HEAD)`; then `git diff --name-status --no-renames "$BASE_SHA" HEAD` piped
     through the existing `awk 'NF >= 2 { print substr($1,1,1) " " $2 }'` into
     `changes.txt` (the sapling `hg status` format btd expects).
  2. **Base-state graph:** materialize the base commit in a `git worktree`
     (`git worktree add "$BB_ROOT/_base" "$BASE_SHA"`, where `$BB_ROOT` =
     `$BUILDBUDDY_CI_RUNNER_ROOT_DIR`) — the BuildBuddy equivalent of the GitHub second
     checkout. Because the runner doesn't populate submodules and worktrees don't
     inherit them, run `git -C "$BB_ROOT/_base" submodule update --init --recursive`
     (prelude is needed for graph evaluation), then `buck2 run //tools:supertd --
     targets root//... --output "$PWD/base.jsonl"` from inside the worktree.
  3. **Impacted targets:** in the head checkout,
     `buck2 run //tools:btd -- --changes changes.txt --base base.jsonl --universe
     root//... --json-lines | jq -r 'select(.target | startswith("root//src/")) |
     .target' | sort -u > impacted.txt`.
  4. **Build & test impacted:** if `impacted.txt` is non-empty,
     `mapfile -t targets < impacted.txt` → `buck2 build -M none "${targets[@]}"` →
     `buck2 test "${targets[@]}"`. Empty ⇒ log "nothing impacted" and exit 0.
- **Env vars used:** none for git context — base is `main` by construction (the trigger
  filter). Only `BUILDBUDDY_CI_RUNNER_ROOT_DIR` (runner-provided) is referenced, for a
  stable worktree path.

### Action `lint`

- **Trigger:** both `push.branches: [main]` and `pull_request.branches: [main]` (lint
  runs on all events today).
- **Steps:** setup → `buck2 run //tools:prek -- run --all-files --show-diff-on-failure`.
- Fully hermetic via buck2 (the `reindeer-check` hook uses loom's own toolchain cargo);
  bsdtar from setup is harmless if unused here.

## Secrets & RE wiring

`.buckconfig`'s `[buck2_re_client]` authenticates via
`http_headers = x-buildbuddy-api-key:$BUILDBUDDY_API_KEY`. BuildBuddy auto-injects
**secrets** as env vars into trusted runs (no `env:`/`--test_env` plumbing needed for a
bash step) — but only secrets that exist. The runner does **not** expose its internal
BES key to non-bazel tools, so this is a hard prerequisite:

> Add an org secret named exactly **`BUILDBUDDY_API_KEY`** (BuildBuddy UI → org
> settings → Secrets), set to an RE-capable API key. Once present, every action's
> `buck2` invocation authenticates to RE/cache with no per-action wiring.

This is out-of-repo setup the implementation can't encode; the plan lists it as a Step A
prerequisite and adds a one-line note to `CLAUDE.md`'s CI section.

## Cutover (two gated steps)

Deleting `ci.yml` in the same change would leave `main` with **no CI** until the
BuildBuddy app is connected to the repo and the workflow proven green — a UI step
outside this PR. Therefore:

- **Step A (this work):** land `buildbuddy.yaml` + `tools/ci/buildbuddy-setup.sh`.
  Connect the repo in the BuildBuddy UI, add the `BUILDBUDDY_API_KEY` secret, and
  confirm all three actions run green on a real push and a real PR.
- **Step B (gated follow-up):** once Step A is proven green, delete **only the
  superseded CI**: `.github/workflows/ci.yml` and the now-orphaned
  `.github/actions/install-bsdtar`. **Keep `.github/actions/setup-buck2`** — it is still
  consumed by `release.yml`. `release.yml` (image/Helm publishing) and `claude.yml` (the
  Claude bot) stay on GitHub Actions; only the build/test/lint CI moves to BuildBuddy.

The implementation plan produces both steps but marks B as gated on a green BuildBuddy
run, so CI coverage is never dropped on `main`.

## What is intentionally dropped

- **Discord notification** (`notify-discord` job + `sarisia/actions-status-discord`):
  removed. BuildBuddy notifications + GitHub commit statuses cover it.
- **`actions/cache` steps:** unnecessary — snapshotted VMs persist installs, and
  BuildBuddy's remote cache covers build artifacts.
- **Composite actions:** replaced by `tools/ci/buildbuddy-setup.sh`.

## Testing & verification

- **Static:** `buildbuddy.yaml` is YAML-validated; `tools/ci/buildbuddy-setup.sh`
  passes `shellcheck` and `bash -n`, and is idempotent (running twice is a no-op on the
  second pass).
- **Live (manual, part of Step A):** confirm on the BuildBuddy UI that
  `build-test` runs green on a push to a throwaway branch / `main`, `affected` runs green
  and selects a sane impacted set on a PR, and `lint` runs green. These can't be
  asserted in buck2 tests — they're release-gate checks recorded in the PR.

## Resolved from docs / source (was "open items")

1. **Base branch/SHA:** no env var exists; derived as `main` from the trigger filter +
   explicit `git fetch origin main` + `git merge-base` (see the affected action). The
   action sets `git_fetch_depth: 0` for history and `merge_with_base: false` for a clean
   HEAD.
2. **Submodules / history:** the runner checks out neither submodules nor (by default)
   full history. Handled: setup runs `git submodule update --init --recursive`; the base
   worktree re-inits submodules; `git_fetch_depth: 0` supplies history.
3. **Secrets:** `BUILDBUDDY_API_KEY` must be added as an org secret (Step A prerequisite).
4. **Resources:** runner default (3 CPU / 8 GB / 20 GB) is kept — build and test both
   run on RE, so the runner only orchestrates; no override.

## Residual checks (only confirmable on a live run, recorded in the PR)

- That `git_fetch_depth: 0` + `git fetch origin main` reliably yields a merge-base on a
  real PR (the `affected` action selecting a sane impacted set is the green-light).
- Default resources hold — add `resource_requests` only if a real run shows pressure.
- Worktree-based `submodule update` succeeds under the runner's git version
  (ubuntu-24.04 ships git ≥ 2.43, which supports worktree submodules).
