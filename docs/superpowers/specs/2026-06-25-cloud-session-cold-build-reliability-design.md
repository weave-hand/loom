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

After clearing all four, the build goes green and the pure-logic tests pass; the
fixture tests then pass via the **CI-populated remote test-result cache** (they are
local-run by design and need a non-root user — see Non-goals).

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

## Approach: a `buck2` shim that owns the daemon's proxy/git env

Install a wrapper at `/usr/local/sbin/buck2` (which precedes `/usr/local/bin` in the
injected PATH, so it shadows the real binary — verified) that, before `exec`-ing the
real `/usr/local/bin/buck2`, exports the github-host `NO_PROXY` bypass. Because the
daemon inherits its env from whichever `buck2` invocation first spawns it, and that
invocation now always goes through the shim, the daemon is guaranteed to have the
bypass regardless of how the harness injects env. This is the key property the
`~/.bashrc` approach cannot provide for non-interactive shells.

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

1. **New** `tools/ci/buck2-proxy-shim.sh` — the wrapper script (exports github
   `NO_PROXY`, `exec`s `/usr/local/bin/buck2 "$@"`).
2. **Edit** `tools/cloud-session-start.sh` — install the shim to
   `/usr/local/sbin/buck2` idempotently; ensure `bsdtar`; derive + set the github
   `insteadOf` overrides from `.gitmodules` + Cargo git deps; run the submodule init
   with `-c http.proxy=` so the prelude clones direct. All guarded so local dev
   (`REMOTE_ENV` unset) is unaffected and warm runs are near-instant.
3. **Edit** `src/control-plane/postgres/defs.bzl` — replace the stale comment
   (lines ~1–11) with an accurate description: fixtures run their command locally by
   default (no RE test profile), pass on a non-root host, and are cached remotely
   from CI; RE itself runs as the non-root `buildbuddy` user.

## Non-goals

- **Making fixture tests execute in this root cloud session.** They are local-run by
  design and `initdb`/`postgres` refuse root. CI (non-root BuildBuddy runners) and
  local dev cover execution; warm cloud sessions cover them via the remote cache.
  Routing fixture *test execution* onto RE (a `remote_execution` profile on the
  prelude's test toolchain) is a larger, separate change and is out of scope here.
- **Changing the egress proxy or harness env injection.** Out of our control;
  the shim works within them.

## Testing

- **In-session, verifiable now:** install the shim, `buck2 kill`, force a cold
  `download_file` (e.g. delete one cached artifact or use a fresh isolation dir) and
  confirm it succeeds through the shim; confirm `bsdtar` present; confirm
  `git ls-remote` for `apache/iceberg-rust` succeeds with the override; confirm the
  full `buck2 build //src/...` is green.
- **Not exercisable in-session:** the SessionStart hook + snapshot lifecycle (only
  runs at session start). The hook edits are guarded and idempotent; correctness is
  by inspection plus a manual `bash tools/cloud-session-start.sh` dry-run under
  `REMOTE_ENV=true` against a scratch `PROFILE`/PATH.
- No buck2 graph behavior changes, so `buck2 test //src/...` is unaffected by the
  `defs.bzl` comment edit (comment-only).
