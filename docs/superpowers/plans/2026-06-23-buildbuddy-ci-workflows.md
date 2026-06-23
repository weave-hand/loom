# BuildBuddy Workflows CI Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace loom's GitHub Actions CI with a BuildBuddy-native `buildbuddy.yaml` that mirrors the existing `build-test` / `affected` / `lint` jobs.

**Architecture:** A repo-root `buildbuddy.yaml` defines three actions. Each begins by running a shared, idempotent `tools/ci/buildbuddy-setup.sh` (the DRY replacement for the old `setup-buck2` + `install-bsdtar` composite actions plus prelude submodule init), then runs the same `buck2` build/test/lint commands the GitHub jobs ran. The GitHub workflow + composite actions are removed only after the BuildBuddy workflow is proven green (gated Step B).

**Tech Stack:** BuildBuddy Workflows (`buildbuddy.yaml`), bash, buck2, BuildBuddy remote execution/cache.

**Spec:** `docs/superpowers/specs/2026-06-23-buildbuddy-ci-workflows-design.md`

## Global Constraints

- **buck2 pin:** `BUCK2_RELEASE="2026-05-18"` — must stay aligned with the prelude submodule pin (see CLAUDE.md / `.gitmodules`). This single copy lives in `tools/ci/buildbuddy-setup.sh`.
- **buck2 download URL:** `https://github.com/facebook/buck2/releases/download/<BUCK2_RELEASE>/buck2-x86_64-unknown-linux-gnu.zst` (x86_64 linux only).
- **Container image:** `ubuntu-24.04` for every action (matches GitHub's `ubuntu-latest`; needed because the prebuilt `//tools:supertd`/`//tools:btd` fork binaries require GLIBC_2.39, absent on `ubuntu-22.04`, and `buck2 run` executes them locally on the runner; git ≥ 2.43 → worktree submodules work).
- **No `resource_requests`:** inherit the runner default (3 CPU / 8 GB / 20 GB). Build and test both run on RE (the RE workers run as non-root, so the postgres/duckdb fixture tests run remotely too); the runner only orchestrates plus a few light local genrules (libxml2/bsdtar extract). Add `resource_requests` only if a real run shows pressure.
- **`env: { BUCK_PREFER_REMOTE: "true" }`** on every action; builds use `-M none`.
- **Secret prerequisite (out-of-repo, already done):** an org secret named exactly `BUILDBUDDY_API_KEY` must exist — `.buckconfig`'s `[buck2_re_client]` reads `$BUILDBUDDY_API_KEY`. The user confirmed this is added.
- **Shell scripts are committed mode `100755`** (match `tools/cloud-setup.sh`).
- **No git-context env vars exist** in the runner; the affected action derives base = `main` from its trigger filter.
- **Local validation gate:** `bash -n` for shell (PyYAML/shellcheck are not available locally; the live BuildBuddy run is the authoritative config validator).

---

### Task 1: Shared setup script (`tools/ci/buildbuddy-setup.sh`)

**Files:**
- Create: `tools/ci/buildbuddy-setup.sh`

**Interfaces:**
- Produces: an executable script that, when run from the repo root, leaves `buck2 <BUCK2_RELEASE>` on `PATH` (via `/usr/local/bin/buck2`), `zstd`/`bsdtar`/`jq` installed, and the prelude submodule initialized. Consumed by every action in `buildbuddy.yaml` (Task 2) as `./tools/ci/buildbuddy-setup.sh`.

- [ ] **Step 1: Write the script**

Create `tools/ci/buildbuddy-setup.sh` with exactly:

```bash
#!/bin/bash
# Shared per-action setup for loom's BuildBuddy Workflows CI (buildbuddy.yaml).
#
# Runs at the top of every action. BuildBuddy snapshots/reuses workflow VMs, so
# every install is existence-guarded — warm runs are near-instant. This is the DRY
# replacement for the old .github/actions/{setup-buck2,install-bsdtar} composite
# actions plus prelude submodule init: the runner checks out neither submodules nor
# the buck2 binary nor the apt packages we need.
#
# Mirrors the buck2-install logic in tools/cloud-setup.sh. Keep BUCK2_RELEASE
# aligned with the prelude submodule pin (see CLAUDE.md / .gitmodules).
set -euo pipefail

BUCK2_RELEASE="2026-05-18"   # keep aligned with the prelude submodule pin
BUCK2_DIR="$HOME/.cache/loom-buck2/$BUCK2_RELEASE"
BUCK2_BIN="$BUCK2_DIR/buck2"

# 1. Packages. zstd decompresses the buck2 release; bsdtar (libarchive-tools) is
#    used by the control-plane-postgres libxml2 genrule (runs locally on the
#    runner); jq parses btd output in the affected action. Guarded — a snapshotted
#    VM that already has them skips apt entirely.
need_pkg=()
command -v zstd   >/dev/null 2>&1 || need_pkg+=(zstd)
command -v bsdtar >/dev/null 2>&1 || need_pkg+=(libarchive-tools)
command -v jq     >/dev/null 2>&1 || need_pkg+=(jq)
if [ "${#need_pkg[@]}" -gt 0 ]; then
  sudo apt-get update -y
  sudo apt-get install -y --no-install-recommends "${need_pkg[@]}"
fi

# 2. buck2 -> a version-stamped cache dir, symlinked onto the default PATH. The
#    filesystem persists across an action's steps (only the shell env does not), and
#    /usr/local/bin is on the default PATH, so later steps find buck2 without re-
#    exporting PATH. A BUCK2_RELEASE bump lands in a fresh dir; the snapshot keeps
#    prior versions warm.
if [ ! -x "$BUCK2_BIN" ]; then
  mkdir -p "$BUCK2_DIR"
  url="https://github.com/facebook/buck2/releases/download/$BUCK2_RELEASE/buck2-x86_64-unknown-linux-gnu.zst"
  curl -fsSL "$url" -o "$BUCK2_DIR/buck2.zst"
  zstd -d -f "$BUCK2_DIR/buck2.zst" -o "$BUCK2_BIN"
  chmod +x "$BUCK2_BIN"
fi
sudo ln -sf "$BUCK2_BIN" /usr/local/bin/buck2

# 3. Prelude submodule (the runner does not check it out). Idempotent.
git submodule update --init --recursive

buck2 --version
```

- [ ] **Step 2: Make it executable and verify syntax**

Run:
```bash
chmod +x tools/ci/buildbuddy-setup.sh
bash -n tools/ci/buildbuddy-setup.sh && echo "SYNTAX OK"
```
Expected: prints `SYNTAX OK` with no other output (no syntax errors).

- [ ] **Step 3: Confirm the committed mode is 100755**

Run:
```bash
git add tools/ci/buildbuddy-setup.sh && git ls-files -s tools/ci/buildbuddy-setup.sh
```
Expected: the line starts with `100755` (executable), e.g. `100755 <hash> 0	tools/ci/buildbuddy-setup.sh`.

- [ ] **Step 4: Commit**

```bash
git commit -m "ci(buildbuddy): add shared per-action setup script"
```

---

### Task 2: `buildbuddy.yaml` with the three actions

**Files:**
- Create: `buildbuddy.yaml`

**Interfaces:**
- Consumes: `tools/ci/buildbuddy-setup.sh` (Task 1) as the first step of each action.
- Produces: the CI config BuildBuddy reads on push/PR. No downstream code consumes it.

- [ ] **Step 1: Write the config**

Create `buildbuddy.yaml` at the repo root with exactly:

```yaml
# loom CI — BuildBuddy Workflows. Source of truth for build/test/lint, replacing
# .github/workflows/ci.yml. Runs on BuildBuddy runners co-located with the RE/cache;
# workflow VMs are snapshotted/reused, so the per-action setup
# (tools/ci/buildbuddy-setup.sh) is near-instant on warm runs.
#
# PREREQUISITE: an org secret named BUILDBUDDY_API_KEY must exist (BuildBuddy UI →
# org settings → Secrets). .buckconfig's [buck2_re_client] reads $BUILDBUDDY_API_KEY
# to authenticate RE/cache, and the runner does not expose its internal key to
# non-bazel tools. Secrets are auto-injected as env vars for trusted runs.
actions:
  # Full build + test on pushes to main (main must always be fully green). PRs use
  # the cheaper `affected` action below.
  - name: build-test
    triggers:
      push:
        branches: [main]
    os: linux
    arch: amd64
    container_image: ubuntu-24.04
    env:
      BUCK_PREFER_REMOTE: "true"
    steps:
      - run: ./tools/ci/buildbuddy-setup.sh
      # -M none: validate the build on RE without downloading final artifacts.
      - run: buck2 build -M none //src/...
      # Build and test both run on RE (incl. postgres/duckdb fixtures); no --local-only.
      - run: buck2 test //src/...

  # On PRs (base = main), build & test only the first-party targets the diff impacts,
  # computed by btd. merge_with_base: false keeps HEAD at the clean PR head (btd diffs
  # base->head, not a base+head merge commit); git_fetch_depth: 0 supplies the history
  # `git merge-base` needs.
  - name: affected
    triggers:
      pull_request:
        branches: [main]
        merge_with_base: false
    git_fetch_depth: 0
    os: linux
    arch: amd64
    container_image: ubuntu-24.04
    env:
      BUCK_PREFER_REMOTE: "true"
    steps:
      - run: ./tools/ci/buildbuddy-setup.sh
      - run: |
          set -euo pipefail
          head_root="$PWD"

          # Base is `main` by construction (this action only fires on PRs based on
          # main). The runner exposes no base-branch env var, so fetch it explicitly.
          git fetch --no-tags origin main
          base_sha="$(git merge-base FETCH_HEAD HEAD)"

          # Changed files in the sapling/`hg status` format btd expects:
          # one line per file, "<M|A|D> <project-relative-path>".
          git diff --name-status --no-renames "$base_sha" HEAD \
            | awk 'NF >= 2 { print substr($1, 1, 1) " " $2 }' > changes.txt
          echo "=== changed files ==="; cat changes.txt

          # Base-state target graph, from a worktree at the base commit (the
          # BuildBuddy equivalent of the GitHub second checkout). The runner checks
          # out no submodules and worktrees don't inherit them, so re-init prelude.
          base_dir="${BUILDBUDDY_CI_RUNNER_ROOT_DIR:-$head_root}/_base"
          rm -rf "$base_dir"
          git worktree add --detach "$base_dir" "$base_sha"
          git -C "$base_dir" submodule update --init --recursive
          ( cd "$base_dir" && buck2 run //tools:supertd -- \
              targets root//... --output "$head_root/base.jsonl" )

          # Impacted first-party targets. btd computes the after-state itself via
          # --universe in this (head) checkout. Scope to //src — that's what we
          # build/test; third-party deps come along transitively.
          buck2 run //tools:btd -- \
            --changes changes.txt --base base.jsonl --universe root//... --json-lines \
            | jq -r 'select(.target | startswith("root//src/")) | .target' \
            | sort -u > impacted.txt
          echo "=== impacted targets ==="; cat impacted.txt

          if [ -s impacted.txt ]; then
            mapfile -t targets < impacted.txt
            echo "Building/testing: ${targets[*]}"
            buck2 build -M none "${targets[@]}"
            buck2 test "${targets[@]}"
          else
            echo "No first-party targets impacted by this diff — nothing to build/test."
          fi

  # prek hooks (rustfmt, clippy, file checks, reindeer-in-sync) on all events, so CI
  # enforces exactly what local pre-commit does. Fully hermetic via buck2.
  - name: lint
    triggers:
      push:
        branches: [main]
      pull_request:
        branches: [main]
    os: linux
    arch: amd64
    container_image: ubuntu-24.04
    env:
      BUCK_PREFER_REMOTE: "true"
    steps:
      - run: ./tools/ci/buildbuddy-setup.sh
      - run: buck2 run //tools:prek -- run --all-files --show-diff-on-failure
```

- [ ] **Step 2: Sanity-check structure**

Run:
```bash
grep -cE '^  - name:' buildbuddy.yaml
```
Expected: `3` (three actions: build-test, affected, lint).

Then, if PyYAML happens to be available, validate parse (best-effort — skip if it errors with `ModuleNotFoundError`):
```bash
python3 -c "import yaml; d=yaml.safe_load(open('buildbuddy.yaml')); print('actions:', [a['name'] for a in d['actions']])" 2>&1 | head -1
```
Expected (if PyYAML present): `actions: ['build-test', 'affected', 'lint']`. If it prints `ModuleNotFoundError`, that's acceptable — authoritative validation is the live BuildBuddy run (Step A verification below).

- [ ] **Step 3: Commit**

```bash
git add buildbuddy.yaml
git commit -m "ci(buildbuddy): add build-test, affected, and lint actions"
```

---

### Task 3: Document the BuildBuddy CI in CLAUDE.md (Step A docs)

**Files:**
- Modify: `CLAUDE.md` — the "Continuous integration" section.

**Interfaces:** none (documentation).

- [ ] **Step 1: Read the current CI section**

Run:
```bash
grep -n "Continuous integration" CLAUDE.md
```
Note the line; read that section so the new text matches its style.

- [ ] **Step 2: Add a BuildBuddy-CI paragraph**

Insert the following paragraph at the **top** of the "Continuous integration" section body (immediately after the `## Continuous integration` heading line, before the existing GitHub Actions description). This documents the new source of truth while `ci.yml` is still present (it is removed in the gated Task 4):

```markdown
**BuildBuddy Workflows are the source of truth for CI.** `buildbuddy.yaml` at the
repo root defines three actions mirroring the jobs below — `build-test` (push to
`main`, full `buck2 build`/`test //src/...`), `affected` (PRs, btd-driven impacted
build/test), and `lint` (prek hooks on all events). They run on BuildBuddy runners
co-located with the RE/cache, with VMs snapshotted/reused, so the shared per-action
setup (`tools/ci/buildbuddy-setup.sh`: pinned buck2 + zstd/bsdtar/jq + prelude
submodule init) is near-instant on warm runs. **Prerequisite:** an org secret named
`BUILDBUDDY_API_KEY` (BuildBuddy UI → Secrets) — `.buckconfig`'s `[buck2_re_client]`
reads `$BUILDBUDDY_API_KEY`, which the runner does not otherwise expose to buck2. The
GitHub Actions workflow below is being retired once the BuildBuddy workflow is proven
green (see the plan's gated Step B).
```

- [ ] **Step 3: Verify and commit**

Run:
```bash
grep -q "BuildBuddy Workflows are the source of truth" CLAUDE.md && echo "DOC ADDED"
git add CLAUDE.md
git commit -m "docs(ci): document BuildBuddy Workflows as CI source of truth"
```
Expected: prints `DOC ADDED`.

---

### Task 4 (GATED — Step B): Remove the GitHub Actions CI

> **DO NOT EXECUTE until the BuildBuddy workflow has run green** on a real push to `main` and a real PR (the `affected` action selecting a sane impacted set is the green-light). This is the cutover that removes GitHub CI; running it early leaves `main` with no CI. The reviewer must confirm the BuildBuddy runs are green before this task starts.

**Files:**
- Delete: `.github/workflows/ci.yml`
- Delete: `.github/actions/setup-buck2/` (directory)
- Delete: `.github/actions/install-bsdtar/` (directory)
- Modify: `CLAUDE.md` — trim the now-removed GitHub Actions description.

**Interfaces:** none.

- [ ] **Step 1: Confirm the gate**

Verify (with the human reviewer) that BuildBuddy's `build-test`, `affected`, and `lint` actions have all run green. Do not proceed otherwise.

- [ ] **Step 2: Delete the GitHub workflow and orphaned composite actions**

Run:
```bash
git rm .github/workflows/ci.yml
git rm -r .github/actions/setup-buck2 .github/actions/install-bsdtar
```

- [ ] **Step 3: Verify no remaining references**

Run:
```bash
grep -rn "setup-buck2\|install-bsdtar\|workflows/ci.yml" .github CLAUDE.md docs 2>/dev/null || echo "NO REFS"
```
Expected: `NO REFS` (or only references inside this plan/spec, which are historical and fine to leave).

- [ ] **Step 4: Trim the GitHub Actions prose in CLAUDE.md**

In `CLAUDE.md`'s "Continuous integration" section, edit the retired sentence added in Task 3 — change:

```markdown
The GitHub Actions workflow below is being retired once the BuildBuddy workflow is proven
green (see the plan's gated Step B).
```

to:

```markdown
(The previous GitHub Actions workflow and its composite actions were removed once the
BuildBuddy workflow was proven green.)
```

Leave the per-job descriptions (`build-test`/`affected`/`lint` behavior, RE-vs-local placement) — they still describe the BuildBuddy actions accurately. Remove any sentences that are GitHub-Actions-specific and now false (e.g. references to `actions/cache`, the `setup-buck2` composite action, or the Discord `notify-discord` job), if present in that section.

- [ ] **Step 5: Commit**

```bash
git add -A
git commit -m "ci: remove GitHub Actions CI, superseded by BuildBuddy Workflows"
```

---

## Post-implementation

- **Live verification (Step A green-light, before Task 4):** push the branch and confirm in the BuildBuddy UI that `build-test`, `affected`, and `lint` run green; confirm `affected` selects a plausible impacted set on a PR. Record this in the PR description.
- **Registers:** at completion, run the `loom-docs-update` skill — this branch touches a spec + plan, and the Stop hook will nudge for it. The CI migration itself is unlikely to need a ROADMAP/ISSUES entry, but record any newly-deferred follow-on (e.g. resource-sizing tuning) if one emerges.

## Self-review notes

- **Spec coverage:** setup script (Task 1) ↔ spec Component 1; three actions incl. `merge_with_base: false` + `git_fetch_depth: 0` + base-from-trigger derivation + worktree submodule re-init (Task 2) ↔ spec Component 2 + affected action; `BUILDBUDDY_API_KEY` prerequisite + CLAUDE.md note (Task 3) ↔ spec Secrets/Cutover Step A; gated removal (Task 4) ↔ spec Cutover Step B; Discord/actions-cache drop ↔ "What is intentionally dropped" (no task needed — they're simply absent from `buildbuddy.yaml`).
- **jq:** added to the setup package guard because it is not guaranteed on the BuildBuddy `ubuntu-24.04` image (the affected action pipes btd output through it).
- **No placeholders:** all file contents are given in full; verification commands have concrete expected output.
