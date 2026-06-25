# Cloud-session cold-build reliability — design

_2026-06-25. No prior register item — surfaced while diagnosing a `buck2 test
//src/...` failure in a cloud session ("the head issue from the github proxy
again")._

## Problem

A fresh **cloud session** (web/remote, running as `root`) cannot reliably build
`//src/...` from cold. `buck2 test //src/...` fails the build, and the failure
recurs across sessions. Four independent cold-build blockers were observed and
hand-fixed this session:

1. **GitHub `download_file` HEAD → 401 ("the head issue").** buck2's `http_archive`
   lowers to a `download_file` action that **always runs on the local daemon**
   (never RE). It issues an `http_head` preflight before the GET. Through the
   egress proxy, GitHub redirects HEAD to `objects.githubusercontent.com` (401)
   while GET redirects to `release-assets.githubusercontent.com` (200/206) — so the
   preflight aborts the download. Affected targets: `toolchains//:cpython_archive`,
   `:llvm-x86_64-linux`, `postgres-bin`, `duckdb-cli`.

2. **`bsdtar` missing.** The `:libxml2` fixture genrule shells out to `bsdtar`
   (`libarchive-tools`). It was absent on the host, so the genrule failed with
   `bsdtar: command not found`.

3. **iceberg-rust git fetch → 403.** The git third-party dep
   (`apache/iceberg-rust`) is fetched by a `git_fetch` action in the buck2 daemon.
   `~/.gitconfig` rewrites **all** `https://github.com/` to the scoped git proxy
   (`127.0.0.1:<port>/git/`), which 403s any repo outside this session's scope
   (`weave-hand/loom`). So `apache/iceberg-rust` is denied.

4. **prelude submodule clone → 403.** Same root cause as (3): the prelude
   (`facebook/buck2-prelude`, the repo's only git submodule, required by every
   buck2 build) is cloned by `git submodule update --init` in the hook, gets
   rewritten to the scoped proxy, and 403s. This blocker is invisible from a
   session that already has the prelude checked out — only a truly fresh clone
   hits it — which is why it must be tested from a brand-new session.

After clearing all four, the build goes green and the pure-logic tests pass. The
fixture tests are a **separate, fifth issue** rooted in test-execution *placement*,
not the cold build — see "Fixture test placement" below.

### Fixture test placement (the fifth issue)

buck2/tpx runs each test-RUN action on the **local** executor by default; it only
dispatches to RE when told (`--unstable-allow-all-tests-on-re` — verified: the
action goes from `executor: Local` to `executor: Re`). There is **no buckconfig key**
for this and **no remote test-result cache** (verified: tests re-run every
invocation; `:page` re-executed, never cache-served). RE itself is fully working in
cloud sessions (a forced cache-miss build gave `remote: 37, local: 0`).

So on this linux-x86_64 **root** box the local executor is "capable", buck2 runs the
fixture command locally, and `initdb` rejects root:

| invocation | result |
| --- | --- |
| `buck2 test //src/...` (plain) | 88 pass / **82 fail** — fixtures run local as root |
| `buck2 test //src/... --unstable-allow-all-tests-on-re` | **170 pass / 0 fail** — fixtures on RE as `buildbuddy` |

It passes on a dev machine / CI runner only because *their* local executor is
non-root (or, on a non-linux-x86_64 host, because the binary can't run locally so
buck2 has no choice but RE). The fix is to make the *invocation's environment* route
fixtures to RE where the local executor would be root — the cloud shim injects the
flag for `buck2 test`, and CI passes it explicitly. (An earlier draft of this spec
claimed fixtures pass via a "CI-populated remote test-result cache"; that mechanism
does not exist and is corrected here.)

### Root cause of (1)

The designed fix for the HEAD issue is a `NO_PROXY` bypass for the github asset
hosts, written by `tools/cloud-session-start.sh` into `~/.bashrc`. But it is
appended **after** Ubuntu's non-interactive guard (`~/.bashrc:6`,
`[ -z "$PS1" ] && return`). Every Bash tool call is a non-interactive `bash -c`
shell (`BASH_ENV` unset), so it never executes that block — and neither does the
**buck2 daemon** those shells spawn, which is the process that actually performs
`download_file`. The base `NO_PROXY`/`HTTPS_PROXY`/`BUILDBUDDY_API_KEY` reach the
shell because the harness injects them directly into each tool process's
environment (confirmed: they are not in `/etc/environment`, `profile.d`, or
`~/.profile`); there is **no repo-editable file** that adds to that injected env.
So a `~/.bashrc`-based `NO_PROXY` fundamentally cannot reach the daemon.

A misleading comment compounded the confusion: `src/control-plane/postgres/defs.bzl`
claims "BuildBuddy RE runs as root, so these tests must run their test command
LOCALLY" via `remote_execution = "disabled"`. Both clauses are stale — the RE
platform sets `dockerUser = "buildbuddy"` (non-root) in `platforms/defs.bzl`, and
`remote_execution` appears in **no** first-party BUCK file. The fixtures default to
local execution simply because no RE test profile is configured for them, not
because of an explicit pin.

## Approach: a `buck2` shim that owns the daemon's proxy/git env and test placement

Install a wrapper at `/usr/local/sbin/buck2` (which precedes `/usr/local/bin` in the
injected PATH, so it shadows the real binary — verified) that, before `exec`-ing the
real `/usr/local/bin/buck2`:

- exports the github-host `NO_PROXY` bypass. Because the daemon inherits its env from
  whichever `buck2` invocation first spawns it, and that invocation now always goes
  through the shim, the daemon is guaranteed to have the bypass regardless of how the
  harness injects env. This is the key property the `~/.bashrc` approach cannot
  provide for non-interactive shells;
- for `buck2 test` invocations, injects `--unstable-allow-all-tests-on-re` so the
  fixture test RUNS go to RE (non-root `buildbuddy`) instead of the local root
  executor (the fifth issue above). The shim finds the subcommand (first non-option
  arg, skipping `--isolation-dir`'s value) and inserts the flag right after it, before
  any targets / `--` test-arg separator. It is **gated on `BUILDBUDDY_API_KEY`** so it
  degrades to local when RE is absent, and **cloud-only** (the shim is installed only
  in cloud sessions), so local dev and a developer's own `buck2 test` are untouched —
  placement stays an environment-level choice, not a per-target one. Net effect: a
  plain `buck2 test //src/...` in a cloud session is **170/170** (verified) instead of
  88/82.

`tools/cloud-session-start.sh` (the SessionStart hook, which runs before any tool
call) installs the shim idempotently and additionally:

- ensures `bsdtar` is present (`command -v bsdtar || apt-get install -y
  --no-install-recommends libarchive-tools`), since the setup snapshot may lack it;
- **derives** the third-party github sources from the repo's own declarations —
  `.gitmodules` (submodules → `facebook/buck2-prelude`) and `git =
  "https://github.com/…"` in `src/**/Cargo.toml` (git deps → `apache/iceberg-rust`)
  — and for each sets a longest-match-wins self-referential `insteadOf`
  (`git config --global url."https://github.com/<org>/".insteadOf
  "https://github.com/<org>/"`) so those orgs stay on their direct github URL
  instead of the scoped git proxy. Nothing hardcodes the org list, so a new
  submodule or git-dep is covered automatically. The actual egress-proxy bypass is
  per-consumer: the shim's `NO_PROXY` for the daemon's `git_fetch` (iceberg), and
  `-c http.proxy=` on the submodule init (prelude — verified to propagate to the
  per-submodule child clones). Both connect directly to github, which works for
  these public, sha-pinned sources.

Why this is "dynamic enough": there is no proxy-side runtime allow API (the
`__agentproxy` control surface exposes only `/status`; `add_repo` cannot scope in
public upstreams), but the bypass keys on *hostname* and reads `$HTTPS_PROXY` at
runtime, so it survives the egress proxy's port moving (observed `36399→41665`
mid-session). The only inputs are the repo's own dep declarations, computed at
session start.

The shim source is a committed, reviewable file (`tools/ci/buck2-proxy-shim.sh`)
that the hook copies into place, not an inline heredoc — so it can be read and
tested on its own.

### Why not the alternative (pre-warm hardening)

`tools/cloud-setup.sh` already best-effort pre-warms the fixture/toolchain downloads
into `buck-out` at snapshot time, which (if captured) makes `download_file` a cache
hit so the HEAD never runs. We keep that as-is but do **not** rely on it as the
primary fix: it depends on the snapshot actually capturing `buck-out` (provisioning
behavior, possibly outside repo control), the LLVM dist is heavy for the ~5-min
setup budget, and any newly-added github download silently reintroduces the bug. The
shim is robust to all three.

## Changes

1. **New** `tools/ci/buck2-proxy-shim.sh` — the wrapper script: exports the github
   `NO_PROXY`, injects `--unstable-allow-all-tests-on-re` for `test` invocations
   (gated on `BUILDBUDDY_API_KEY`), then `exec`s `/usr/local/bin/buck2`.
2. **Edit** `tools/cloud-session-start.sh` — install the shim to
   `/usr/local/sbin/buck2` idempotently; ensure `bsdtar`; derive + set the github
   `insteadOf` overrides from `.gitmodules` + Cargo git deps; run the submodule init
   with `-c http.proxy=` so the prelude clones direct. All guarded so local dev
   (`REMOTE_ENV` unset) is unaffected and warm runs are near-instant.
3. **Edit** `src/control-plane/postgres/defs.bzl` — replace the stale comment with an
   accurate one: fixtures run on the **local** executor by default (no result cache;
   no per-target RE profile, which would force RE everywhere and break local-dev
   without a backend), so *placement* is an invocation-level choice — root hosts must
   route the test run to RE; the RE *build* always runs as the non-root `buildbuddy`.
4. **Edit** `buildbuddy.yaml` — append `--unstable-allow-all-tests-on-re` to the
   `build-test` and `affected` `buck2 test` commands so CI runs fixtures on RE
   explicitly, instead of silently depending on the runner being non-root; fix the
   comment that claimed plain `buck2 test` already routes fixtures to RE.

## Non-goals

- **A per-target RE profile for fixtures.** Setting `remote_execution` on the
  `loom_fixture_test` macro would force RE for fixtures in *every* environment and
  break local dev without an RE backend. Placement is kept an invocation-level choice
  (the shim for cloud, the explicit flag for CI, local executor for dev machines).
- **Changing the egress proxy or harness env injection.** Out of our control;
  the shim works within them.

## Testing

- **In-session, verified:** cold `download_file` of `cpython_archive` succeeds through
  the shim; `bsdtar` present; prelude submodule deinit + re-clone via the hook's exact
  command (`git -c http.proxy=` + derived overrides) succeeds at the pinned commit; the
  shim's test-flag injection unit-tested across cases (test/build/`--`/isolation-dir/
  already-present/key-unset); and the headline — a plain **`buck2 test //src/...`
  through the shim is 170 pass / 0 fail** (vs 88/82 without it). The `defs.bzl` edit is
  comment-only and the postgres package still parses.
- **Not exercisable in-session:** the SessionStart hook + snapshot lifecycle (only
  runs at session start) and the `buildbuddy.yaml` CI runs. The hook edits are guarded
  and idempotent; correctness is by inspection plus the per-piece verifications above.
  The CI flag is the same one proven at 170/170 here.
