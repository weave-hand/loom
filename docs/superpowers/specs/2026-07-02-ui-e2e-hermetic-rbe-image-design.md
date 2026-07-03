# UI e2e: hermetic browser on RE via a custom RBE image — design

**Status:** approved (2026-07-02)
**Register item:** `fut-ui-e2e-hermetic-browser` (to be promoted on landing)
**Predecessor:** `docs/superpowers/specs/2026-07-02-ui-login-e2e-fantoccini-design.md` (built the login e2e; deferred hermetic-RE)

## Problem

`//src/ui/e2e:login` is a `fantoccini` (Rust WebDriver) test that boots the loom
composite and drives a **vendored** headless Chromium
(`//third-party/browser`, Chrome for Testing, x86_64-linux) through the login
flow. The browser **binary** is vendored and pinned, but its ~36 runtime
libraries (nspr/nss, GTK/X11, alsa, dbus, gbm/drm, …) come from the **host
loader path**, not from buck.

CI routes test *runs* to Remote Execution (`buck2 test … --unstable-allow-all-tests-on-re`)
so the Postgres/DuckDB fixtures run as the non-root `buildbuddy` worker rather
than the runner user. The RE workers use the `gcr.io/flame-public/rbe-ubuntu24-04`
**build** image, which does **not** carry the browser libs. So on RE,
`start_browser()` fails, and the test's auto-skip gate turns that into a trivial
pass. Net result today:

- **Local** (dev box / `ubuntu-24.04` runner with the deb.deps): the test can run
  for real (gated behind `LOOM_UI_E2E=1` for a hard failure).
- **CI sweep (RE):** the test **auto-skips** — CI gets *compile* coverage (an API
  or build break is caught) but not *behavior* coverage (a login regression would
  not redden CI).

The "apt-install the libs on the runner" trick does **not** help, because the
test never executes on the runner — it executes on the RE worker.

## Goal

Make `//src/ui/e2e:login` run **for real** in the RE-routed CI sweep — no
auto-skip — so a genuine login regression reddens CI. Do it hermetically, by
giving the RE worker the browser libs, without regressing local ergonomics or
touching the green login-e2e feature (PR #311).

Non-goals: arm64 support (loom's vendored browser and CI RE are both amd64-only,
so arm64 has no consumer here; Chrome for Testing *does* now ship a linux-arm64
build as of 2026-03-12, so an arm64 arm is a matter of vendoring that archive +
an arm64 RBE image once RE runs arm64 — a separate deferred follow-up);
vendoring the glibc-coupled lib closure as buck archives (rejected as fragile —
see Alternatives).

## Approach (chosen)

A **custom RBE container image**: `FROM` the flame-public base + one apt layer of
the browser libs, published to **public** GHCR, with loom's **global** RE
execution platform repointed at it. Once the libs are present on the RE worker,
`start_browser()` succeeds and `login_flow` runs for real automatically — the
auto-skip only fires on browser *failure*, which no longer happens on RE.

### Why global (not a scoped exec platform)

loom registers exactly **one** execution platform (`root//platforms:default`,
`platforms/defs.bzl`). The custom image is a strict **superset** of the base
(it only *adds* a lib layer), so making it the base for all RE actions is safe
and cheap: the extra apt layer is pulled once per executor and cached. A scoped
second execution platform (a `browser_re` platform + a constraint +
`exec_compatible_with` on the test) would tighten the blast radius but adds
real buck2 machinery for little benefit when the image is a superset. **Global.**

### Why public GHCR (not private + BuildBuddy secrets)

The image contains nothing sensitive: the stock BuildBuddy RBE base (itself
public) plus open-source Ubuntu browser libs. Public means the RE executor pulls
it **anonymously** — no registry credentials in BuildBuddy, no token rotation, no
auth-flake failure mode. On GHCR, *package* visibility is set per-package,
independent of the (private) source repo, so this needs **no new repo** — just a
public package under the existing `weave-hand` org, published by the workflow
that already has `packages: write`.

## Components

### 1. Dockerfile — `tools/ci/rbe-browser/Dockerfile`

Lives with the other CI machinery under `tools/ci/` (it is CI infrastructure,
not a shipped loom service — so **not** the apko `deploy//` image cell, which
builds product images declaratively from Wolfi).

- `FROM gcr.io/flame-public/rbe-ubuntu24-04@sha256:<pinned-digest>` — pin the
  base **by digest** so the derived image is reproducible (the current
  `platforms/defs.bzl` uses `:latest`, which is not). Bumping the base is a
  deliberate rebuild.
- Exactly one `RUN apt-get update && apt-get install -y --no-install-recommends
  <deb.deps> && rm -rf /var/lib/apt/lists/*` layer.
- **Inherits `USER`, entrypoint, WORKDIR, and the buildbuddy toolchain layout
  untouched** — it only *adds* the lib closure, keeping it a strict superset of
  the base (so every other RE action is unaffected).

**Deriving `deb.deps`:** start from Chrome for Testing / puppeteer's documented
Debian/Ubuntu dependency list for `chrome-headless-shell` (nss/nspr, the
GTK/X11/cairo/pango stack, alsa, dbus, gbm/drm, expat, fonts-liberation,
xkbcommon, …), adjusted for Ubuntu **24.04** package renames (the `t64`
time_t-transition variants, e.g. `libasound2t64`, `libatk-bridge2.0-0t64`,
`libcups2t64`). The **publish-time smoke test (component 2) is the ground truth**
that validates the closure is complete against the actual pinned binary; the
plan iterates the list against it. The exact, verified package list is captured
in the plan and the Dockerfile — not fixed in this design.

### 2. Publish workflow — `.github/workflows/rbe-image.yml` (`workflow_dispatch` only)

The image changes ~never — only on a Chrome-major bump or a base refresh — so
publishing is **intentional/manual**, matching `release.yml`'s
`workflow_dispatch` versioned-release philosophy (no auto-increment). GitHub
Actions `ubuntu-latest` ships `docker`/`buildx` natively, so this is a plain
Dockerfile build (unlike the apko `deploy//` flow).

Steps:
1. `docker buildx build` the Dockerfile.
2. **Smoke-test the lib closure** before pushing: read
   `CHROME_FOR_TESTING_VERSION` from `third-party/browser/BUCK`, download that
   exact `chrome-headless-shell` zip into a container run from the freshly built
   image, and run `chrome-headless-shell --version`. If it does not print the
   version (a missing lib), **fail the publish**. This makes a missing lib fail
   *here*, not silently in the test sweep, and keeps the lib image and the
   vendored browser version in lockstep.
3. Push to **public** `ghcr.io/weave-hand/loom-rbe-browser` (auth: the existing
   `GITHUB_TOKEN` + `permissions: packages: write`; the package's visibility is
   set to public once, in the GHCR package settings).
4. Emit the pushed **digest** in the job output/log so it can be pinned
   (component 3).

The image carries **libs only** — the browser *binary* still comes from buck's
`//third-party/browser` vendored archive at test time. That split is deliberate
and unchanged.

### 3. Pin — `platforms/defs.bzl`

Change the single `container-image` line from
`docker://gcr.io/flame-public/rbe-ubuntu24-04:latest` to
`docker://ghcr.io/weave-hand/loom-rbe-browser@sha256:<digest>`. **Digest-pinned**
for RE-cache correctness and immutability. A rebuild (Chrome/base bump) updates
this one line.

**Bootstrapping sequence** (chicken-and-egg: the digest doesn't exist until the
image is published, and publishing is manual):
1. Land the Dockerfile + workflow (components 1–2).
2. Manually run the workflow once to publish the image and capture its digest.
3. Update `platforms/defs.bzl` with that digest (component 3).
4. The RE sweep now runs `login_flow` for real.

Steps 1 and 3 can both be in this PR — publish once during the branch to obtain
the digest, then commit the pin.

### 4. No test/BUCK change to "turn it on"

Once the libs are present on RE, `start_browser()` succeeds, so `login_flow`
runs for real **automatically**. The auto-skip gate only triggers on a browser
*failure*, which no longer occurs on RE. Local ergonomics are untouched (a dev
box without browser libs still soft-skips).

**Deliberately NOT baking `LOOM_UI_E2E=1` into the target env.** Doing so would
turn every local `buck2 test //src/...` **without** browser libs into a hard
failure — regressing the very soft-skip the login-e2e design chose for local
ergonomics (and buck2 doesn't forward ambient env to test actions, so there's no
clean "CI-only" way to set it on the test). Image completeness is instead
guaranteed two ways that together close the gap without env plumbing:

- **Digest-immutability:** the pin is a content-addressed digest — the image the
  RE worker runs cannot drift after publish.
- **Publish-time smoke (component 2):** a degraded image (missing a lib) can
  never reach the pin, because the smoke test fails the publish.

So a "silently degraded browser image slips through and the test skip-passes on
RE" failure mode is structurally prevented, and no env is threaded into the test
action.

### 5. Docs & register

- Rewrite the `third-party/browser/BUCK` header caveat (currently "NOT run on RE
  … the RE build image lacks the browser libs") to describe the now-hermetic RE
  setup and point at the custom image.
- Update the `src/ui/CLAUDE.md` login-e2e note (the "browser takes its libs from
  the host … auto-skips" paragraph) to reflect hermetic-on-RE.
- Close `fut-ui-e2e-hermetic-browser` via `loom-docs-update` (`status: promoted`,
  `spec:` set, checkbox `[x]`). The **arm64** sub-note stays deferred as its own
  concern — not for lack of a browser build (Chrome for Testing ships linux-arm64
  as of 2026-03-12) but because loom's vendored browser and RE are amd64-only, so
  an arm64 arm needs the arm64 CfT archive vendored + an arm64 RBE image once RE
  runs arm64.

## Verification

This cannot be verified from the dev box — it needs the BuildBuddy RE key **and**
the published image. Acceptance is:

- The publish workflow's smoke step prints the Chrome for Testing version (lib
  closure complete).
- After pinning the digest, the CI sweep's BuildBuddy invocation shows
  `login_flow` **running on the RE worker with 0 skip lines** (no
  "skipping login_flow" in the RE test log) and passing all three assertion
  phases (render / bad-creds / success → Explorer).

Local `buck2 test //src/ui/e2e:login` continues to run-or-skip exactly as before
(no local behavior change).

## Pre-flight risk

The `weave-hand` org must permit **public** GHCR packages. Verify the existing
`ghcr.io/weave-hand/loom-ingest` / `loom-query-api` package visibility as the
precedent before relying on anonymous pull; if org policy forbids public
packages, fall back to a private package + BuildBuddy registry-pull credentials
(the only reason to take the heavier secrets path).

## Alternatives considered

- **Scoped exec platform** (browser image only for the test): tighter blast
  radius, but real buck2 machinery (new platform + constraint +
  `exec_compatible_with`) for a superset image. Rejected — global is simpler and
  safe.
- **Private image + BuildBuddy secrets:** keeps the (non-sensitive) image
  private at the cost of credential plumbing and an auth-flake failure mode.
  Rejected — public is strictly simpler here.
- **Vendor the lib closure as buck archives + `LD_LIBRARY_PATH`** (the FUTURE
  item's option b): fragile — glibc coupling, redone on every Chrome bump.
  Rejected.
- **Dedicated non-RE CI lane** (apt-install on the runner, run the test locally
  off RE): a lighter near-term option, but non-hermetic and it forks the CI
  execution model. Rejected in favor of the hermetic answer.

## Scope

This is a **separate follow-up PR**. It does **not** touch PR #311 (the login
e2e itself). Files created/modified:

- Create: `tools/ci/rbe-browser/Dockerfile`
- Create: `.github/workflows/rbe-image.yml`
- Modify: `platforms/defs.bzl` (the `container-image` line)
- Modify: `third-party/browser/BUCK` (header caveat), `src/ui/CLAUDE.md` (note)
- Modify: `docs/FUTURE.md` (close `fut-ui-e2e-hermetic-browser`)
