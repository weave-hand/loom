# UI e2e Hermetic-RE Browser Image — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make `//src/ui/e2e:login` run for real on the RE-routed CI sweep (instead of auto-skipping) by giving the RE worker the browser's runtime libraries via a custom RBE container image published to public GHCR.

**Architecture:** A custom OCI image — `FROM` the flame-public BuildBuddy RBE base + one `apt-get install` layer of the ~24 `chrome-headless-shell` runtime libs — published to public `ghcr.io/weave-hand/loom-rbe-browser`. loom's single global RE execution platform (`platforms/defs.bzl`) is repointed at the new image by digest. The browser *binary* still comes from buck's `//third-party/browser` vendored archive at test time; the image supplies only the libs. Because the image is a strict superset of the base, all other RE actions are unaffected.

**Tech Stack:** Docker/OCI (Dockerfile, `docker buildx`), GitHub Actions (`workflow_dispatch` publish), GHCR (public package), buck2 execution platforms, BuildBuddy RE.

## Global Constraints

- **Base image, digest-pinned:** the Dockerfile's `FROM` is `gcr.io/flame-public/rbe-ubuntu24-04@sha256:<digest>` (resolve the current digest of `:latest`), NOT a moving tag — reproducibility.
- **Published image is PUBLIC:** `ghcr.io/weave-hand/loom-rbe-browser`, anonymous pull, no BuildBuddy registry credentials.
- **`platforms/defs.bzl` pin is by digest:** `docker://ghcr.io/weave-hand/loom-rbe-browser@sha256:<digest>`, never a tag.
- **The image is a strict superset of the base:** only ADD an apt layer — never change `USER`, entrypoint, `WORKDIR`, or the buildbuddy toolchain layout.
- **Chrome for Testing version is the single source of truth** in `third-party/browser/BUCK`: `CHROME_FOR_TESTING_VERSION = "150.0.7871.46"`. The smoke test reads this value; it is never hardcoded elsewhere.
- **Publish is manual (`workflow_dispatch` only)** — the image changes ~never (Chrome-major or base bump). No auto-increment, no per-PR registry churn.
- **The smoke test is the gate:** a missing lib fails the *publish*, never the test sweep. `LOOM_UI_E2E=1` is NOT baked into the test target env (it would break local soft-skip).
- **This is a separate PR** — it does NOT touch PR #311 (the login e2e itself). Branch: `feature/ui-e2e-hermetic-rbe` (already created off `origin/main`).

---

## File Structure

- `tools/ci/rbe-browser/Dockerfile` (new) — the custom RBE image definition.
- `tools/ci/rbe-browser/smoke.sh` (new) — build-and-smoke helper: builds the image, runs the vendored `chrome-headless-shell --version` against its libs. Used locally to iterate the deb list AND invoked by the workflow. One responsibility: prove the lib closure is complete.
- `.github/workflows/rbe-image.yml` (new) — `workflow_dispatch` job: build → smoke → push public → print digest.
- `platforms/defs.bzl` (modify, line 12) — repoint `container-image` to the new digest.
- `third-party/browser/BUCK` (modify, header comment) — rewrite the "NOT run on RE" caveat.
- `src/ui/CLAUDE.md` (modify, the login-e2e bullet) — reflect hermetic-on-RE.
- `docs/FUTURE.md` (modify) — close `fut-ui-e2e-hermetic-browser` (via `loom-docs-update`).

---

## Task 1: Dockerfile + smoke helper (iterate the deb closure locally)

**Files:**
- Create: `tools/ci/rbe-browser/Dockerfile`
- Create: `tools/ci/rbe-browser/smoke.sh`

**Interfaces:**
- Consumes: `CHROME_FOR_TESTING_VERSION` from `third-party/browser/BUCK` (currently `"150.0.7871.46"`); the Chrome for Testing download URL shape `https://storage.googleapis.com/chrome-for-testing-public/{ver}/linux64/chrome-headless-shell-linux64.zip`.
- Produces: a buildable image tagged `loom-rbe-browser:local` whose libs satisfy `chrome-headless-shell`. `smoke.sh` exits 0 iff `chrome-headless-shell --version` prints a version from inside the image. Later tasks (workflow) reuse `smoke.sh`.

**Context:** `docker` is available on the dev box (`/usr/bin/docker`). The base `gcr.io/flame-public/rbe-ubuntu24-04` is Ubuntu 24.04, so package names use the `t64` (time_t-transition) variants. The smoke downloads the browser on the HOST (which has `curl`/`unzip`) and mounts it read-only into a container run FROM the image, so the container needs no `curl`/`unzip` — only the libs. `chrome-headless-shell --version` load-links the same libs a real page render needs, so a clean `--version` proves the closure.

- [ ] **Step 1: Resolve the base image digest**

Run:
```bash
docker buildx imagetools inspect gcr.io/flame-public/rbe-ubuntu24-04:latest --format '{{.Manifest.Digest}}'
```
Expected: a line like `sha256:abc123…`. Record it — it goes in the `FROM` line. (If `imagetools` is unavailable, `docker pull gcr.io/flame-public/rbe-ubuntu24-04:latest && docker inspect --format='{{index .RepoDigests 0}}' gcr.io/flame-public/rbe-ubuntu24-04:latest` yields `…@sha256:…`.)

- [ ] **Step 2: Write the Dockerfile**

Create `tools/ci/rbe-browser/Dockerfile` (substitute the digest from Step 1 for `<BASE_DIGEST>`):

```dockerfile
# Custom BuildBuddy RBE image for loom: the flame-public rbe-ubuntu24-04 base
# plus the chrome-headless-shell runtime libraries, so //src/ui/e2e:login can
# start its vendored browser on the RE worker (see
# docs/superpowers/specs/2026-07-02-ui-e2e-hermetic-rbe-image-design.md).
#
# STRICT SUPERSET of the base: this ONLY adds an apt layer. Do not change USER,
# ENTRYPOINT, WORKDIR, or the buildbuddy toolchain layout — every loom RE action
# runs on this image once platforms/defs.bzl is repointed at it.
#
# The browser BINARY is NOT in this image; buck vendors it (//third-party/browser).
# This image supplies only the shared libraries the binary loads at runtime.
#
# Base pinned by digest for reproducibility. To refresh the base, re-resolve:
#   docker buildx imagetools inspect gcr.io/flame-public/rbe-ubuntu24-04:latest \
#     --format '{{.Manifest.Digest}}'
FROM gcr.io/flame-public/rbe-ubuntu24-04@sha256:<BASE_DIGEST>

# chrome-headless-shell (Chrome for Testing) runtime deps on Ubuntu 24.04.
# The publish-time smoke (tools/ci/rbe-browser/smoke.sh) is the ground truth for
# this list; add any lib the smoke reports as "error while loading shared
# libraries" and re-run.
RUN apt-get update && apt-get install -y --no-install-recommends \
      ca-certificates \
      fonts-liberation \
      libasound2t64 \
      libatk-bridge2.0-0t64 \
      libatk1.0-0t64 \
      libatspi2.0-0t64 \
      libcairo2 \
      libcups2t64 \
      libdbus-1-3 \
      libdrm2 \
      libexpat1 \
      libgbm1 \
      libglib2.0-0t64 \
      libnspr4 \
      libnss3 \
      libpango-1.0-0 \
      libx11-6 \
      libxcb1 \
      libxcomposite1 \
      libxdamage1 \
      libxext6 \
      libxfixes3 \
      libxkbcommon0 \
      libxrandr2 \
    && rm -rf /var/lib/apt/lists/*
```

- [ ] **Step 3: Write the smoke helper**

Create `tools/ci/rbe-browser/smoke.sh`:

```bash
#!/usr/bin/env bash
# Build the custom RBE image and prove its libs satisfy chrome-headless-shell.
# Downloads the browser on the HOST (needs curl+unzip here, not in the image),
# mounts it read-only into a container run FROM the image, and runs --version.
# Exit 0 iff the version prints — i.e. the lib closure is complete.
#
# Usage: tools/ci/rbe-browser/smoke.sh [IMAGE_TAG]
#   IMAGE_TAG defaults to loom-rbe-browser:local (built from ./Dockerfile).
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd "$here/../../.." && pwd)"
image="${1:-loom-rbe-browser:local}"

# Single source of truth for the browser version.
ver="$(grep -oP 'CHROME_FOR_TESTING_VERSION = "\K[^"]+' "$repo_root/third-party/browser/BUCK")"
echo "chrome-for-testing version: $ver"

# Build the image if the caller didn't pre-build a tag that exists.
if ! docker image inspect "$image" >/dev/null 2>&1; then
  echo "building $image from $here/Dockerfile"
  docker build -t "$image" "$here"
fi

# Fetch the pinned browser on the host.
work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT
url="https://storage.googleapis.com/chrome-for-testing-public/$ver/linux64/chrome-headless-shell-linux64.zip"
echo "downloading $url"
curl -fsSL -o "$work/chs.zip" "$url"
unzip -q "$work/chs.zip" -d "$work"
chs_dir="$work/chrome-headless-shell-linux64"

# Run --version inside the image, using the image's libs.
echo "=== chrome-headless-shell --version (inside $image) ==="
docker run --rm -v "$chs_dir:/chs:ro" "$image" \
  /chs/chrome-headless-shell --version
echo "=== smoke OK: lib closure is complete ==="
```

Make it executable: `chmod +x tools/ci/rbe-browser/smoke.sh`.

- [ ] **Step 4: Run the smoke — expect it to prove the closure (iterate if not)**

Run:
```bash
tools/ci/rbe-browser/smoke.sh
```
Expected on success: a final line like `Google Chrome for Testing 150.0.7871.46` followed by `=== smoke OK: lib closure is complete ===`, exit 0.

If instead it prints `error while loading shared libraries: libFOO.so.N: cannot open shared object file`, add the owning package to the Dockerfile's `apt-get install` list (map lib→package with `apt-file search libFOO.so.N` on an Ubuntu 24.04 host, or by known mapping — e.g. `libgtk-3.so` → `libgtk-3-0t64`, `libcairo-gobject.so` → `libcairo2`, `libXrender.so` → `libxrender1`), rebuild (`docker build -t loom-rbe-browser:local tools/ci/rbe-browser`), and re-run the smoke. Repeat until it prints the version. This loop IS the test for this task.

- [ ] **Step 5: Commit**

```bash
git add tools/ci/rbe-browser/Dockerfile tools/ci/rbe-browser/smoke.sh
git commit -m "ci(rbe): browser-lib RBE image Dockerfile + smoke helper"
```

---

## Task 2: Publish workflow

**Files:**
- Create: `.github/workflows/rbe-image.yml`

**Interfaces:**
- Consumes: `tools/ci/rbe-browser/Dockerfile` and `tools/ci/rbe-browser/smoke.sh` from Task 1; the `GITHUB_TOKEN` secret with `packages: write`.
- Produces: on manual dispatch, a public image at `ghcr.io/weave-hand/loom-rbe-browser` (tags `latest` + `cft-<ver>`) and its digest printed in the job log/summary for Task 3 to pin.

**Context:** Model the ghcr/token pattern on `.github/workflows/release.yml` (`permissions: packages: write`, login with `${{ github.actor }}` + `GITHUB_TOKEN`). GitHub `ubuntu-latest` ships `docker`/`buildx` natively — no third-party build actions needed. This is a GitHub Actions workflow (like `release.yml`/`claude.yml`), NOT a BuildBuddy action — it does not touch `buildbuddy.yaml`, RE, or buck2. `smoke.sh` builds the image tag it's given if not present, so the workflow builds explicitly first, then smokes that exact tag.

- [ ] **Step 1: Write the workflow**

Create `.github/workflows/rbe-image.yml`:

```yaml
name: rbe-image

# Custom BuildBuddy RBE image for loom (flame-public base + chrome-headless-shell
# runtime libs), so //src/ui/e2e:login can start its vendored browser on the RE
# worker. Manual only — the image changes ~never (Chrome-major or base bump).
# After a successful run, copy the printed digest into platforms/defs.bzl's
# container-image line. See
# docs/superpowers/specs/2026-07-02-ui-e2e-hermetic-rbe-image-design.md.
on:
  workflow_dispatch:

env:
  IMAGE: ghcr.io/weave-hand/loom-rbe-browser

permissions:
  contents: read
  packages: write   # push the image to ghcr

jobs:
  publish:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4

      - name: Read Chrome for Testing version
        id: ver
        run: |
          set -euo pipefail
          v="$(grep -oP 'CHROME_FOR_TESTING_VERSION = "\K[^"]+' third-party/browser/BUCK)"
          echo "cft=$v" >> "$GITHUB_OUTPUT"
          echo "chrome-for-testing version: $v"

      - name: Build image
        run: docker build -t loom-rbe-browser:ci tools/ci/rbe-browser

      - name: Smoke-test the lib closure (fails the publish if a lib is missing)
        run: tools/ci/rbe-browser/smoke.sh loom-rbe-browser:ci

      - name: Log in to ghcr
        run: echo "${{ secrets.GITHUB_TOKEN }}" | docker login ghcr.io -u '${{ github.actor }}' --password-stdin

      - name: Tag and push
        id: push
        run: |
          set -euo pipefail
          cft="${{ steps.ver.outputs.cft }}"
          docker tag loom-rbe-browser:ci "$IMAGE:latest"
          docker tag loom-rbe-browser:ci "$IMAGE:cft-$cft"
          docker push "$IMAGE:latest"
          docker push "$IMAGE:cft-$cft"
          digest="$(docker buildx imagetools inspect "$IMAGE:latest" --format '{{.Manifest.Digest}}')"
          echo "digest=$digest" >> "$GITHUB_OUTPUT"

      - name: Report the digest to pin
        run: |
          echo "### RBE browser image published" >> "$GITHUB_STEP_SUMMARY"
          echo "" >> "$GITHUB_STEP_SUMMARY"
          echo "Pin this in \`platforms/defs.bzl\`:" >> "$GITHUB_STEP_SUMMARY"
          echo '```' >> "$GITHUB_STEP_SUMMARY"
          echo "docker://${IMAGE}@${{ steps.push.outputs.digest }}" >> "$GITHUB_STEP_SUMMARY"
          echo '```' >> "$GITHUB_STEP_SUMMARY"
```

- [ ] **Step 2: Validate the workflow YAML is well-formed**

Run (Python is always available via the hermetic toolchain; this checks parse-ability, not semantics):
```bash
python3 -c "import yaml,sys; yaml.safe_load(open('.github/workflows/rbe-image.yml')); print('yaml ok')"
```
Expected: `yaml ok`. (If `actionlint` is on PATH, also run `actionlint .github/workflows/rbe-image.yml` and expect no output.)

- [ ] **Step 3: Commit**

```bash
git add .github/workflows/rbe-image.yml
git commit -m "ci(rbe): workflow_dispatch to publish the browser RBE image to ghcr"
```

---

## Task 3: Publish once, pin the digest, validate on RE

**Files:**
- Modify: `platforms/defs.bzl` (line 12, the `container-image` value)

**Interfaces:**
- Consumes: the `rbe-image` workflow from Task 2 (run it to obtain a digest); the running BuildBuddy CI on this PR.
- Produces: `platforms/defs.bzl` pinned to `docker://ghcr.io/weave-hand/loom-rbe-browser@sha256:<digest>`, validated by an RE run of `//src/ui/e2e:login` that no longer skips.

**Context:** This task has an operator step that cannot run headlessly from the dev box — it requires triggering the GitHub Actions workflow (GHCR push perms) and reading its digest. Do the operator steps, paste the digest, then make the one-line edit. The de-risk for "don't break everything": the PR that flips this line is itself built/tested on the new image, so a bad image reddens THIS PR before it can reach main. Because btd's `affected` PR action may not treat a `.bzl` config change as impacting targets (it's loaded via buckconfig, not a BUCK dep edge), do NOT trust `affected` alone — run the explicit full-RE validation in Step 4.

- [ ] **Step 1: Confirm the GHCR package will be public (pre-flight risk)**

The existing loom images establish the precedent. Check that `ghcr.io/weave-hand/loom-ingest` is publicly pullable (anonymous):
```bash
docker logout ghcr.io 2>/dev/null || true
docker manifest inspect ghcr.io/weave-hand/loom-ingest:latest >/dev/null 2>&1 && echo "public precedent OK" || echo "NOT anonymously pullable — check org policy / package visibility"
```
Expected: `public precedent OK` (or a known-good tag). If the org forbids public packages, STOP and escalate — the fallback is a private package + BuildBuddy registry-pull credentials (heavier; out of this plan's scope).

- [ ] **Step 2: Push the branch and run the publish workflow**

```bash
git push -u origin feature/ui-e2e-hermetic-rbe
```
Then trigger the workflow on this branch (either `gh workflow run rbe-image.yml --ref feature/ui-e2e-hermetic-rbe` if `gh` is authed, or the Actions tab → "rbe-image" → Run workflow → branch `feature/ui-e2e-hermetic-rbe`). Wait for it to succeed. After the first successful push, set the `loom-rbe-browser` package's visibility to **Public** in the GHCR package settings (one-time; new packages default to private and inherit no visibility from the repo).

- [ ] **Step 3: Pin the digest in `platforms/defs.bzl`**

Copy the digest from the workflow's job summary ("Pin this in `platforms/defs.bzl`"). Edit `platforms/defs.bzl` line 12, replacing:
```python
                "container-image": "docker://gcr.io/flame-public/rbe-ubuntu24-04:latest",
```
with (substitute the real digest):
```python
                "container-image": "docker://ghcr.io/weave-hand/loom-rbe-browser@sha256:<digest>",
```
Leave `OSFamily` and `dockerUser` unchanged.

- [ ] **Step 4: Validate on RE — the login e2e must RUN, not skip**

This requires `BUILDBUDDY_API_KEY` in the env (RE). Run the browser test on RE and confirm it did not auto-skip:
```bash
BUCK_PREFER_REMOTE=true buck2 test //src/ui/e2e:login --unstable-allow-all-tests-on-re > /tmp/rbe-login.log 2>&1
grep -E "Tests finished|Pass|FAIL" /tmp/rbe-login.log | tail -3
grep -c "skipping login_flow" /tmp/rbe-login.log
```
Expected: `Pass 1. Fail 0.` AND the skip-line count is `0` (the test ran for real on the new image). If the skip count is non-zero, the image lacks a lib the browser needs at *session-create* time (beyond `--version`) — return to Task 1, add the lib, re-publish (Task 3 Step 2), re-pin, and re-validate.

Then confirm the swap did not break the broader sweep (the "don't break everything" guard — do NOT rely on btd `affected`):
```bash
BUCK_PREFER_REMOTE=true buck2 build -M none //src/... > /tmp/rbe-build.log 2>&1; tail -3 /tmp/rbe-build.log
```
Expected: `BUILD SUCCEEDED`. (If `BUILDBUDDY_API_KEY` is not available in this environment, this validation must be performed by CI on the pushed PR before merge — note it explicitly in the PR description and do not merge until the BuildBuddy invocation shows `login_flow` running with 0 skips.)

- [ ] **Step 5: Commit**

```bash
git add platforms/defs.bzl
git commit -m "ci(rbe): pin global RE platform to the browser image digest

//src/ui/e2e:login now runs for real on RE instead of auto-skipping."
```

---

## Task 4: Docs + close the register item

**Files:**
- Modify: `third-party/browser/BUCK` (header comment, lines 1–16)
- Modify: `src/ui/CLAUDE.md` (the "Login e2e" bullet)
- Modify: `docs/FUTURE.md` (`fut-ui-e2e-hermetic-browser`)

**Interfaces:**
- Consumes: nothing new (documentation of Tasks 1–3).
- Produces: docs that reflect hermetic-on-RE; the register item closed with `status: promoted` + `spec:` set. `bash tools/docs.sh validate` passes.

**Context:** The current `third-party/browser/BUCK` header and the `src/ui/CLAUDE.md` login-e2e bullet both assert the test runs only on the local executor and never on RE. After Task 3 that is false. Keep the wording precise: the browser *binary* is still vendored; the *libs* now come from the custom RBE image on RE, and from the host locally.

- [ ] **Step 1: Rewrite the `third-party/browser/BUCK` header caveat**

In `third-party/browser/BUCK`, replace the second paragraph of the header comment (the block starting "We vendor only the browser BINARY" through "see fut-ui-e2e-hermetic-browser.") with:

```
# We vendor only the browser BINARY (a reproducible, pinned version). Its ~24
# runtime libs (libnspr4/libnss3, cairo/pango, X11, alsa, dbus, gbm/drm, …) come
# from the EXECUTOR image, not vendored: locally from the host loader; on the RE
# workers from the custom RBE image (tools/ci/rbe-browser/, published to
# ghcr.io/weave-hand/loom-rbe-browser and pinned in platforms/defs.bzl). So
# //src/ui/e2e:login runs for real on RE (hermetic) and on a dev box with the
# libs present; it auto-skips only where the browser can't start (LOOM_UI_E2E=1
# turns that skip into a hard error). x86_64-linux only — Chrome for Testing
# publishes no linux-arm64 build; the aarch64 arm is a documented follow-up
# (fut-ui-e2e-hermetic-browser retains the arm64 note).
```

- [ ] **Step 2: Update the `src/ui/CLAUDE.md` login-e2e bullet**

In `src/ui/CLAUDE.md`, in the "Login e2e (`//src/ui/e2e:login`)" bullet, replace the sentence:

```
The Postgres side is
  hermetic; the **browser takes its libs from the host**, so it runs on the local executor
  and **auto-skips when no browser is present** (set `LOOM_UI_E2E=1` to turn a
  browser-start failure into a hard error).
```

with:

```
The Postgres side is
  hermetic; the **browser binary is vendored and its libs come from the executor
  image** — from the host locally, and from the custom RBE image
  (`tools/ci/rbe-browser/`, pinned in `platforms/defs.bzl`) on the RE workers — so it
  runs for real on the RE-routed CI sweep and on a dev box with the libs, and
  **auto-skips only where the browser can't start** (set `LOOM_UI_E2E=1` to turn a
  browser-start failure into a hard error).
```

Also update the trailing line `Hermetic-RE hardening: fut-ui-e2e-hermetic-browser.` to `Hermetic-RE image: tools/ci/rbe-browser/ (spec 2026-07-02-ui-e2e-hermetic-rbe-image).`

- [ ] **Step 3: Close the register item**

Invoke the `loom-docs-update` skill to close `fut-ui-e2e-hermetic-browser`. The intended end state of its entry in `docs/FUTURE.md`: checkbox `[x]`, `status:promoted`, `spec:2026-07-02-ui-e2e-hermetic-rbe-image-design`, and a closing note that the custom RBE image landed and the login e2e now runs on RE — while explicitly retaining the arm64 gap as the remaining deferred work (Chrome for Testing has no linux-arm64 build). If `loom-docs-update` is unavailable, edit the entry directly to that end state.

- [ ] **Step 4: Validate the registers**

Run:
```bash
bash tools/docs.sh validate
```
Expected: no errors (exit 0). This confirms the `spec:` slug resolves to the on-disk spec file and the grammar/links are intact.

- [ ] **Step 5: Commit**

```bash
git add third-party/browser/BUCK src/ui/CLAUDE.md docs/FUTURE.md
git commit -m "docs(rbe): reflect hermetic-on-RE browser; promote fut-ui-e2e-hermetic-browser"
```

---

## Final verification (whole-branch)

- `python3 -c "import yaml; yaml.safe_load(open('.github/workflows/rbe-image.yml'))"` → `yaml ok`.
- `tools/ci/rbe-browser/smoke.sh` → prints `Google Chrome for Testing 150.0.7871.46` + `smoke OK`.
- `bash tools/docs.sh validate` → clean.
- On RE (CI or a keyed session): `buck2 test //src/ui/e2e:login --unstable-allow-all-tests-on-re` → `Pass 1. Fail 0.` with `0` "skipping login_flow" lines; `buck2 build -M none //src/...` → `BUILD SUCCEEDED`.
- `buck2 run //tools:prek -- run --all-files` → all hooks Pass (rustfmt/clippy skip — no Rust changed; file checks + docs-validate pass).

**Do not merge** until the RE validation shows `login_flow` running with 0 skips — that is the whole point of the change, and the guard against a global-image swap that silently breaks the sweep.
