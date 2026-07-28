# Development guide

How to get a loom checkout building, and the day-to-day commands. For *why*
things are shaped the way they are, see `ARCHITECTURE.md`; for the build-system
internals (cells, toolchains, the `//tools` targets), see `CLAUDE.md`.

## One-time setup

### 1. Clone with submodules

The buck2 prelude is vendored as a git submodule. A fresh clone needs it:

```sh
git submodule update --init --recursive
```

### 2. Install buck2 (pinned)

loom pins a specific dated buck2 release that **must match the vendored prelude**
(see `.gitmodules`); mismatched versions break in obscure ways. CI installs the
same version via the `BUCK2_RELEASE` in `tools/ci/buildbuddy-setup.sh` — keep
them aligned. Requires `zstd` and `gh` on PATH.

```sh
gh release download 2026-05-18 --repo facebook/buck2 \
  --pattern 'buck2-x86_64-unknown-linux-gnu.zst' -O /tmp/buck2.zst \
  && zstd -d /tmp/buck2.zst -o /tmp/buck2 \
  && sudo install -m 755 /tmp/buck2 /usr/local/bin/buck2
```

To bump buck2: pick a new dated release, then move the prelude submodule to a
commit at/near that date (`cd prelude && git fetch && git checkout <commit>`)
and update `BUCK2_RELEASE` in CI.

### 3. Install watchman

buck2 is configured to use **watchman** as its file watcher (`[buck2]
file_watcher = watchman` in `.buckconfig`), so every `buck2` command needs the
`watchman` binary on PATH — without it the buck2 daemon refuses to start (`No
Watchman connection`). This is deliberate: the default notify watcher places one
inotify watch per directory and watches `buck-out`, which exhausts
`fs.inotify.max_user_watches` once a second daemon (rust-project's
`.rust-analyzer` isolation dir) is involved; watchman + the `buck-out` ignore in
`.watchmanconfig` bounds the watch set.

watchman is not in the Ubuntu repos. Install it from the
[facebook/watchman](https://github.com/facebook/watchman/releases) release
(macOS: `brew install watchman`):

```sh
ver=v2026.06.22.00   # keep aligned with tools/ci/install-watchman.sh
curl -fsSL "https://github.com/facebook/watchman/releases/download/$ver/watchman-$ver-linux.zip" -o /tmp/watchman.zip \
  && unzip -q /tmp/watchman.zip -d /tmp \
  && sudo cp -a /tmp/watchman-$ver-linux/bin/* /usr/local/bin/ \
  && sudo cp -a /tmp/watchman-$ver-linux/lib/* /usr/local/lib/ \
  && sudo mkdir -p /usr/local/var/run/watchman && sudo chmod 2777 /usr/local/var/run/watchman \
  && sudo ldconfig \
  && watchman version
```

CI and cloud sessions install it automatically (`tools/ci/install-watchman.sh`,
`tools/cloud-setup.sh`).

### 4. Remote execution credentials

Builds run on BuildBuddy remote execution by default (`[project] remote_enabled`
in `.buckconfig`). Export your API key:

```sh
export BUILDBUDDY_API_KEY=<your-key>   # put this in your shell profile
```

Without it, override to a pure-local build per-invocation:
`buck2 build --config project.remote_enabled= //src/...`.

### 5. Install the git hooks

```sh
buck2 run root//tools:prek -- install        # install the pre-commit/pre-push hooks
buck2 run root//tools:prek -- run --all-files # run them across the whole tree once
```

Hooks (rustfmt, clippy, file checks, reindeer-in-sync, a Conventional Commits
message check, and build/test on push) are defined in `prek.toml` and are the
same set CI runs. `prek.toml` sets `default_install_hook_types`, so this one
`install` wires up the pre-commit, commit-msg, and pre-push shims together.

## Everyday commands

```sh
buck2 build -M none //...      # build everything (see -M note below)
buck2 build -M none //src/...  # build first-party code + its deps
buck2 run //src/hello:hello -- --name you
buck2 test //src/...           # run tests

# Format / lint (hermetic — go through the pinned toolchain, no rustup needed)
buck2 run //tools:rustfmt -- --check src/hello/src/main.rs
./tools/clippy-all.sh          # clippy over every first-party target
```

**`-M none` (`--materializations=none`) on builds you only want validated:**
builds run on the remote executor, and without `-M none` buck2 downloads every
final artifact to your machine — a whole-tree `//src/...` build materializes
~29 GiB of debug binaries from the CAS (and re-downloads whatever relinked on
every iteration). With it, a fully-cached validation build transfers single-digit
MiB. Drop the flag only when you actually need an output on disk (`buck2 run`
does its own materialization; locally-run tests materialize their binaries
regardless). The pre-push `buck2-build` hook already passes it. See
`docs/build-execution.md` for the full cost model.

## Dev shell (toolchain on your PATH)

Instead of prefixing everything with `buck2 run //tools:<x> --`, you can activate
loom's hermetic toolchain and dev tools directly on your `PATH`. `cargo`,
`rustc`, `rustfmt`, `reindeer`, `prek`, `btd`, and `supertd` then resolve to
loom's pinned versions (no host `rustup`), and `cargo clippy` works too.

**With [direnv](https://direnv.net/) (auto):** trust the checked-in `.envrc` once —

```sh
direnv allow
```

The environment then loads whenever you `cd` into the repo (or any subdirectory)
and unloads when you leave.

**Without direnv (manual):** evaluate the engine in your current shell (run it
from the repo root) —

```sh
eval "$(./tools/env.sh)"
```

This also works for any script that needs the toolchain on PATH. Either way, the
toolchain and dev tools are built once (cached) and exposed via symlinks under
`.loom/bin` (gitignored).

**First-party binaries:** your `//src` binaries (e.g. `hello`) are exposed on
PATH by name, but are *not* built on activation. Run `loom-refresh` to build and
(re)point them:

```sh
loom-refresh
hello --name you
```

## Adding a third-party dependency

loom imports third-party crates with reindeer (non-vendored / http_archive
mode). Add the crate to the consuming crate's manifest, regenerate the buck
rules, then depend on the generated target:

```sh
cargo add <crate> -p hello     # edits src/hello/Cargo.toml + the workspace lock
./tools/buckify.sh             # regenerate third-party/BUCK (hermetic: uses loom's toolchain cargo)
```

Then add the dep to the consuming crate's `BUCK`:

```python
deps = ["//third-party:<crate>"],
```

Notes:
- Crates with build scripts need a decision in
  `third-party/fixups/<crate>/fixups.toml` (`[buildscript]\nrun = true|false`)
  or reindeer warns.
- Crates using `env!("CARGO_PKG_*")` macros (e.g. clap's `#[command(version)]`)
  need those env vars set on the consuming rule's `env = {...}` — cargo sets them
  automatically, buck2 first-party targets don't. See `src/hello/BUCK`.

## IDE / rust-analyzer integration

buck2 ships a `rust-project` integration that generates the `rust-project.json`
rust-analyzer consumes. Build it once, then regenerate whenever targets change:

```sh
rm -rf /tmp/buck2-src
git clone --depth 1 https://github.com/facebook/buck2 /tmp/buck2-src
cargo install --locked --path /tmp/buck2-src/integrations/rust-project
rust-project develop --prefer-rustup-managed-toolchain root//src/...
```

`--prefer-rustup-managed-toolchain` points rust-analyzer at your rustup rustc
(loom's hermetic rustc isn't on your PATH). Re-run `rust-project develop` after
adding crates or dependencies.

## CI

BuildBuddy Workflows (`buildbuddy.yaml`): `build-test` (full, on `main`
pushes), `affected` (btd-scoped build/test on PRs), and `lint` (the prek hooks).
See the *Continuous integration* section of `CLAUDE.md` for the job details, and
`docs/build-execution.md` for the execution model (RE-vs-local placement,
fixture-test routing, and the materialization cost model).
