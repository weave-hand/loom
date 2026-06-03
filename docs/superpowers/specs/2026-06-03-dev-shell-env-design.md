# Design: loom dev-shell environment

## Problem

loom's Rust toolchain and dev tools are hermetic — they live inside buck2
(`//tools:rust-host-toolchain`, `//tools:reindeer`, `//tools:prek`,
`//tools:btd`, `//tools:rustfmt`, `//tools:clippy`) and are normally invoked via
`buck2 run //tools:<x> -- …`. There is no way to drop into a shell where `cargo`,
`rustc`, the dev tools, and the project's own built binaries are simply on
`PATH` and resolve to loom's pinned, hermetic versions (no host `rustup`).

We want: enter the repo (or run one command) and have the whole loom toolchain
active in the current shell.

## Goals

- `cargo` / `rustc` / `rustdoc` / `clippy-driver` / `cargo-clippy` / `rustfmt`
  on `PATH`, resolving to loom's pinned hermetic toolchain, sysroot wired.
- Dev tools (`reindeer`, `prek`, `btd`, `supertd`, `clippy`) callable directly
  by name instead of `buck2 run //tools:<x> --`.
- First-party `//src` binaries (e.g. `hello`) on `PATH` by name, **expose-only**
  (no build on activation) with an explicit `loom-refresh` to rebuild + re-point.
- Two entry points sharing one engine: **direnv** auto-activation on `cd`, and a
  manual `eval "$(./tools/env.sh)"` for non-direnv users and CI/scripts.

## Non-goals

- Building `//src` automatically on shell entry (explicitly rejected — keeps
  activation instant).
- Replacing buck2 as the build system. `cargo` is provided for ad-hoc use; loom
  still builds via buck2.
- Cross-platform support beyond what the toolchain already supports (x86_64
  Linux for the assembled host toolchain / rustfmt / clippy wrappers).

## Approach (engine): checked-in `tools/env.sh`

Chosen over two alternatives:

- **`tools/env.sh` (chosen):** a script that resolves toolchain/tool output
  paths via `buck2 build --show-output` *at activation time* and prints
  `export …` lines. Robust to `buck2 clean` and output-hash changes; mirrors the
  existing `tools/buckify.sh` and `tools/clippy-all.sh` idiom.
- **Genrule baking exports (rejected):** writes a script with absolute
  `$PWD/$(location …)` paths baked in. Goes stale after `buck2 clean`; needs the
  `uses_local_filesystem_abspaths` label. Fragile for a long-lived shell env.
- **`buck2 run //tools:env` emitting exports (rejected):** a genrule whose output
  shells out to `buck2 build --show-output` then prints exports — just `env.sh`
  wrapped in a buck target, with extra indirection and startup cost.

## PATH layout set by the environment

```
$REPO/.loom/bin   →  dev tools + first-party binaries (symlinks; gitignored)
$TOOLCHAIN/bin    →  cargo, rustc, rustdoc, clippy-driver, cargo-clippy (real dir)
LD_LIBRARY_PATH = $TOOLCHAIN/lib
```

The Rust toolchain's **real** `bin/` goes on `PATH` (not symlinked), so rustc's
sysroot auto-detection (relative to the real binary) stays correct.
`$REPO/.loom/bin` is a managed symlink directory for everything else, prepended
ahead of the toolchain so first-party binaries and tools shadow nothing
unexpected.

## Components

### 1. `tools/env.sh` (activation engine)

Run as `eval "$(./tools/env.sh)"` or from `.envrc`. It:

1. Resolves the repo root and `cd`s there (so it works from any subdir).
2. Builds + resolves `//tools:rust-host-toolchain`; emits its `bin` onto `PATH`
   and `lib` as `LD_LIBRARY_PATH`.
3. Builds + symlinks the **dev tools** into `.loom/bin`: `reindeer`, `prek`,
   `btd`, `supertd`, and the `rustfmt` + `clippy` wrapper targets. These are
   prebuilt / cheap; the build is a one-time, cached cost.
4. Symlinks `tools/loom-refresh` into `.loom/bin`.
5. Symlinks any **already-built** `//src` binaries it finds into `.loom/bin`
   (no build — keeps activation instant). Missing ones simply aren't present
   until `loom-refresh` is run.
6. Prints the `export PATH=…` and `export LD_LIBRARY_PATH=…` lines on stdout
   (and nothing else on stdout — diagnostics go to stderr — so `eval` is clean).

Idempotent: re-running re-points the same symlinks. Safe to source repeatedly
(it prepends loom dirs without unbounded `PATH` growth — it checks for its own
marker before prepending, or de-dupes).

### 2. `tools/loom-refresh` (explicit first-party rebuild)

The only thing that builds `//src`. Runs `buck2 build //src/... --show-output`
(cache-fast) and re-points the `.loom/bin` symlinks to the fresh outputs. On
`PATH` inside the activated env. Usage:

```
$ buck2 build //src/...   # or just rely on loom-refresh to build
$ loom-refresh            # repoint .loom/bin symlinks
$ hello --name you
```

### 3. `.envrc` (direnv front door)

```sh
eval "$(./tools/env.sh)"
watch_file tools/BUCK toolchains/BUCK .buckconfig
```

`watch_file` makes direnv reload when toolchain definitions change. Activates on
`cd` into the repo and all subdirs; unloads on leave. Requires a one-time
`direnv allow`. direnv persists only environment variables (PATH,
LD_LIBRARY_PATH) — which is exactly what the engine emits — so no shell
functions are relied upon (hence `loom-refresh` is a script on PATH, not a
function).

### 4. `.gitignore`

Add `/.loom/` and `/.direnv/`.

### 5. Docs

- `DEVELOPING.md`: a short "Dev shell" section — the direnv path (`direnv allow`)
  and the manual `eval "$(./tools/env.sh)"` fallback for non-direnv users / CI;
  note `loom-refresh` for first-party binaries.
- `CLAUDE.md`: a one-line pointer in the Dev tools section.

## Risks / things to verify during implementation

- **Standalone runnability of first-party binaries:** a buck2-built
  `rust_binary` must run from a `.loom/bin` symlink (glibc / RPATH / dynamic std).
  `hello` is expected to be fine; confirm with a real invocation.
- **cargo/rustc via this PATH:** confirm `cargo --version`, `rustc --version`,
  and a trivial `rustc` compile succeed (sysroot resolves) when invoked through
  the activated `PATH`.
- **No stdout pollution:** `env.sh` must keep buck2's build chatter off stdout
  (stderr or `2>/dev/null`) so `eval` only sees `export` lines.
- **PATH idempotency:** repeated activation (re-`cd`, re-`eval`) must not grow
  `PATH` unboundedly.

## Testing

- `eval "$(./tools/env.sh)"` in a clean shell → `which cargo rustc reindeer prek
  btd rustfmt` all resolve under loom dirs; `cargo --version` / `rustc --version`
  succeed.
- `loom-refresh` after `buck2 build //src/...` → `hello --name you` prints the
  expected output.
- direnv: `direnv allow`, `cd` in/out → loads and unloads; `cd` into a subdir
  keeps it active.
- Idempotency: `eval` twice → `PATH` does not contain duplicate loom entries
  unboundedly.
