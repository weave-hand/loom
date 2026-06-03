# loom Dev-Shell Environment Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Let a developer activate loom's hermetic Rust toolchain, dev tools, and built first-party binaries on `PATH` — automatically via direnv on `cd`, or manually via `eval "$(./tools/env.sh)"`.

**Architecture:** A checked-in `tools/env.sh` resolves buck2 output paths *at activation time* (`buck2 build --show-output`) and prints `export` lines. It prepends a managed symlink dir `$REPO/.loom/bin` (dev tools + first-party binaries) and the real toolchain `bin/` to `PATH`, and sets `LD_LIBRARY_PATH` to the toolchain `lib/`. A `.envrc` calls the same script for direnv users. First-party `//src` binaries are built/repointed only by an explicit `tools/loom-refresh`.

**Tech Stack:** Bash, buck2, direnv. The toolchain/tools are existing buck2 targets in `tools/BUCK` and `toolchains/BUCK`.

---

## Pre-verified facts (from probing the live repo)

- `buck2 build <target> --show-output 2>/dev/null` prints exactly `<target> <relative-path>` lines on stdout; build chatter is on stderr.
- Toolchain: `root//tools:rust-host-toolchain` → `…/out/toolchain`, with `bin/{cargo,rustc,rustdoc,cargo-clippy,clippy-driver}` and a sysroot under `lib/`. `cargo`/`rustc` on PATH compile correctly (sysroot auto-detected). `clippy` is available as `cargo clippy` — no separate binary needed.
- Dev tools must use the **concrete per-arch genrule** targets, NOT the `command_alias` targets: the `command_alias` `--show-output` is a `__command_alias_trampoline.sh` that resolves a sibling artifact via `$0` and **breaks when symlinked**. The concrete targets emit real, symlink-safe binaries:
  - `root//tools:reindeer-x86_64-linux` → `out/reindeer`
  - `root//tools:prek-x86_64-linux` → `out/prek`
  - `root//tools:btd-bin-x86_64` → `out/btd`
  - `root//tools:supertd-bin-x86_64` → `out/supertd`
  - `root//tools:rustfmt` → `out/rustfmt` (wrapper script, bakes absolute paths — symlink-safe)
- First-party: `root//src/hello:hello` → a real binary that runs standalone via a symlink (`hello --name you` → `Hello, you!`).
- `direnv` 2.37.1 is installed at `/usr/bin/direnv`. `direnv exec <dir> <cmd>` loads `.envrc` and runs a command (used for non-interactive testing).
- `.loom/bin` persists on disk between shells (gitignored), so first-party symlinks created by `loom-refresh` survive re-activation; `env.sh` need not rebuild `//src`.

## File structure

- **Create `tools/env.sh`** — activation engine. Resolves toolchain + dev tools, manages their symlinks in `.loom/bin`, prints `export` lines. One responsibility: emit the activated environment.
- **Create `tools/loom-refresh`** — builds `//src` rust_binary targets and (re)points their symlinks in `.loom/bin`. One responsibility: make first-party binaries current.
- **Create `.envrc`** — direnv front door; calls `tools/env.sh`.
- **Modify `.gitignore`** — ignore `/.loom/` and `/.direnv/`.
- **Modify `DEVELOPING.md`** — add a "Dev shell" section.
- **Modify `CLAUDE.md`** — one-line pointer in Dev tools.

---

## Task 1: Activation engine `tools/env.sh`

**Files:**
- Create: `tools/env.sh`

- [ ] **Step 1: Write `tools/env.sh`**

```bash
#!/usr/bin/env bash
# Print `export` lines that activate loom's hermetic toolchain + dev tools in the
# current shell:
#
#     eval "$(./tools/env.sh)"      # manual
#
# or let direnv run it via .envrc. Side effects (builds + symlink management)
# print to stderr; ONLY `export …` lines go to stdout so `eval` stays clean.
#
# What it does: resolves and builds //tools:rust-host-toolchain and the dev-tool
# binaries (one-time, cached), (re)points symlinks under .loom/bin, and emits
# PATH/LD_LIBRARY_PATH. It does NOT build first-party //src — run `loom-refresh`
# for those (their symlinks persist in .loom/bin between shells).
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"
BIN="$REPO_ROOT/.loom/bin"
mkdir -p "$BIN"

log() { printf 'loom-env: %s\n' "$*" >&2; }

# Resolve a target's default output to an absolute path, building if needed.
# `|| true` keeps `set -e` from killing us on a build error — we check $rel.
resolve() {
    local rel
    rel="$( { buck2 build "$1" --show-output 2>/dev/null || true; } | awk 'NR==1{print $2}')"
    if [ -z "$rel" ]; then log "failed to resolve $1"; return 1; fi
    printf '%s/%s\n' "$REPO_ROOT" "$rel"
}

log "building toolchain + dev tools (cached after first run)…"
TOOLCHAIN="$(resolve root//tools:rust-host-toolchain)"

# Dev tools: concrete per-arch genrule outputs (real, symlink-safe binaries).
# The command_alias trampolines resolve a sibling via $0 and break when
# symlinked, so we point at the genrules directly. rustfmt is its wrapper genrule.
declare -A TOOLS=(
    [reindeer]="root//tools:reindeer-x86_64-linux"
    [prek]="root//tools:prek-x86_64-linux"
    [btd]="root//tools:btd-bin-x86_64"
    [supertd]="root//tools:supertd-bin-x86_64"
    [rustfmt]="root//tools:rustfmt"
)
for name in "${!TOOLS[@]}"; do
    if p="$(resolve "${TOOLS[$name]}")"; then ln -sfn "$p" "$BIN/$name"; fi
done

# loom-refresh helper (manages first-party binary symlinks).
ln -sfn "$REPO_ROOT/tools/loom-refresh" "$BIN/loom-refresh"

# Drop any prior loom entries from a `:`-separated path var, so re-activation
# neither grows it nor leaves stale (post-`buck2 clean`) toolchain dirs behind.
_loom_strip() {
    printf '%s' "$1" | awk -v RS=: '
        $0 != "" && $0 !~ /\/\.loom\/bin$/ && $0 !~ /__rust-host-toolchain__/ {
            printf "%s%s", sep, $0; sep=":"
        }'
}

CLEAN_PATH="$(_loom_strip "${PATH}")"
CLEAN_LDLP="$(_loom_strip "${LD_LIBRARY_PATH:-}")"

# Emit ONLY export lines on stdout (%q-quoted so eval is safe).
printf 'export PATH=%q\n' "$BIN:$TOOLCHAIN/bin:$CLEAN_PATH"
if [ -n "$CLEAN_LDLP" ]; then
    printf 'export LD_LIBRARY_PATH=%q\n' "$TOOLCHAIN/lib:$CLEAN_LDLP"
else
    printf 'export LD_LIBRARY_PATH=%q\n' "$TOOLCHAIN/lib"
fi
printf 'export LOOM_ENV_ACTIVE=%q\n' "1"
log "ready — cargo/rustc/reindeer/prek/btd/supertd/rustfmt on PATH; 'cargo clippy' for clippy; run loom-refresh for //src binaries"
```

- [ ] **Step 2: Make it executable**

Run: `chmod +x tools/env.sh`

- [ ] **Step 3: Verify it emits only export lines on stdout**

Run: `./tools/env.sh 2>/dev/null`
Expected (stdout only — three lines, paths will vary):
```
export PATH=/home/.../.loom/bin:/home/.../out/toolchain/bin:/usr/bin:...
export LD_LIBRARY_PATH=/home/.../out/toolchain/lib...
export LOOM_ENV_ACTIVE=1
```

- [ ] **Step 4: Verify activation puts the toolchain + tools on PATH**

Run:
```bash
eval "$(./tools/env.sh)"
command -v cargo rustc reindeer prek btd supertd rustfmt
cargo --version && rustc --version
```
Expected: `cargo`/`rustc`/`rustdoc` resolve under `…/out/toolchain/bin/`; `reindeer`/`prek`/`btd`/`supertd`/`rustfmt` resolve under `…/.loom/bin/`; `cargo --version` and `rustc --version` print `…-nightly` versions (no error).

- [ ] **Step 5: Verify clippy works and a trivial rustc compile succeeds (sysroot)**

Run:
```bash
eval "$(./tools/env.sh)"
cargo clippy --version
printf 'fn main(){println!("ok");}\n' > /tmp/loom_t.rs && rustc /tmp/loom_t.rs -o /tmp/loom_t && /tmp/loom_t && rm -f /tmp/loom_t /tmp/loom_t.rs
```
Expected: `cargo clippy --version` prints a clippy version; the compile prints `ok`.

- [ ] **Step 6: Verify PATH idempotency (no unbounded growth)**

Run:
```bash
eval "$(./tools/env.sh)"; eval "$(./tools/env.sh)"; eval "$(./tools/env.sh)"
echo "$PATH" | tr ':' '\n' | grep -c '/\.loom/bin$'
```
Expected: `1` (exactly one `.loom/bin` entry after three activations).

- [ ] **Step 7: Commit**

```bash
git add tools/env.sh
git commit -m "feat(tools): add env.sh to activate the hermetic toolchain on PATH

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 2: First-party binary refresh `tools/loom-refresh`

**Files:**
- Create: `tools/loom-refresh`

- [ ] **Step 1: Write `tools/loom-refresh`**

```bash
#!/usr/bin/env bash
# Build loom's first-party //src rust_binary targets and (re)point symlinks under
# .loom/bin, so they're on PATH by name inside the loom dev env (see tools/env.sh).
# This is the only thing that builds //src for the dev shell. Safe to run anytime.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"
BIN="$REPO_ROOT/.loom/bin"
mkdir -p "$BIN"

mapfile -t targets < <(buck2 uquery "kind('rust_binary', set(root//src/...))" 2>/dev/null)
if [ "${#targets[@]}" -eq 0 ]; then
    echo "loom-refresh: no first-party rust_binary targets under //src" >&2
    exit 0
fi

# --show-output builds the targets (cache-fast) and prints "<target> <path>".
while read -r tgt path; do
    [ -n "${path:-}" ] || continue
    name="${tgt##*:}"          # //src/hello:hello -> hello
    ln -sfn "$REPO_ROOT/$path" "$BIN/$name"
    printf 'loom-refresh: %s -> %s\n' "$name" "$path"
done < <(buck2 build "${targets[@]}" --show-output 2>/dev/null)
```

- [ ] **Step 2: Make it executable**

Run: `chmod +x tools/loom-refresh`

- [ ] **Step 3: Verify it builds and exposes the first-party binary**

Run:
```bash
eval "$(./tools/env.sh)"
loom-refresh
command -v hello
hello --name you
```
Expected: `loom-refresh` prints `loom-refresh: hello -> buck-out/.../hello`; `command -v hello` resolves under `…/.loom/bin/hello`; `hello --name you` prints `Hello, you!`.

- [ ] **Step 4: Commit**

```bash
git add tools/loom-refresh
git commit -m "feat(tools): add loom-refresh to expose //src binaries on PATH

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 3: direnv front door + gitignore

**Files:**
- Create: `.envrc`
- Modify: `.gitignore`

- [ ] **Step 1: Write `.envrc`**

```bash
# Activate loom's hermetic toolchain + dev tools when entering the repo (and its
# subdirectories). Run `direnv allow` once to trust this file. Non-direnv users
# and CI: eval "$(./tools/env.sh)" instead.
eval "$(./tools/env.sh)"

# Reload when the toolchain/tool definitions or the engine change.
watch_file tools/env.sh tools/BUCK toolchains/BUCK .buckconfig
```

- [ ] **Step 2: Add ignore entries to `.gitignore`**

Append these two lines to `.gitignore`:
```
/.loom/
/.direnv/
```

- [ ] **Step 3: Verify the ignores apply**

Run: `git check-ignore .loom/bin .direnv`
Expected: both `.loom/bin` and `.direnv` are printed (i.e. ignored). `git status --short` must NOT list `.loom/` or `.direnv/`.

- [ ] **Step 4: Verify direnv loads the env**

Run:
```bash
direnv allow
direnv exec . bash -c 'command -v cargo reindeer hello; cargo --version'
```
Expected: `cargo` resolves under the toolchain `bin/`, `reindeer`/`hello` under `.loom/bin/`, and `cargo --version` prints a nightly version. (If `hello` is absent, run `loom-refresh` first — direnv does not build `//src`.)

- [ ] **Step 5: Commit**

```bash
git add .envrc .gitignore
git commit -m "feat(dev): auto-activate the loom toolchain via direnv (.envrc)

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 4: Documentation

**Files:**
- Modify: `DEVELOPING.md`
- Modify: `CLAUDE.md`

- [ ] **Step 1: Add a "Dev shell" section to `DEVELOPING.md`**

Insert this section immediately **after** the "## Everyday commands" section and before "## Adding a third-party dependency":

```markdown
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

**Without direnv (manual):** evaluate the engine in your current shell —

```sh
eval "$(./tools/env.sh)"
```

This is also what CI/scripts use. Either way, the toolchain and dev tools are
built once (cached) and exposed via symlinks under `.loom/bin` (gitignored).

**First-party binaries:** your `//src` binaries (e.g. `hello`) are exposed on
PATH by name, but are *not* built on activation. Run `loom-refresh` to build and
(re)point them:

```sh
loom-refresh
hello --name you
```
```

- [ ] **Step 2: Add a one-line pointer to `CLAUDE.md`**

In the "## Dev tools" section of `CLAUDE.md`, add this bullet at the end of the tool list (after the `btd`/`supertd` bullet):

```markdown
- **`tools/env.sh` / `tools/loom-refresh`** — dev-shell activation. `eval "$(./tools/env.sh)"` (or `direnv allow` for the checked-in `.envrc`) puts the hermetic Rust toolchain (`cargo`/`rustc`/`rustfmt`, `cargo clippy`) and the dev-tool binaries (`reindeer`/`prek`/`btd`/`supertd`) on `PATH` via symlinks under `.loom/bin` (gitignored). The Rust toolchain's real `bin/` goes on PATH (sysroot stays auto-detected); dev tools point at the concrete per-arch genrules, not the `command_alias` trampolines (those break when symlinked). First-party `//src` binaries are exposed by name but only built/repointed by `tools/loom-refresh`, never on activation.
```

- [ ] **Step 3: Verify docs pass the file-check hooks**

Run: `git add DEVELOPING.md CLAUDE.md && buck2 run root//tools:prek -- run --all-files 2>/dev/null | grep -E 'trailing|end of files|Passed|Failed'`
Expected: trailing-whitespace and end-of-file-fixer report `Passed` (no `Failed`).

- [ ] **Step 4: Commit**

```bash
git add DEVELOPING.md CLAUDE.md
git commit -m "docs: document the loom dev shell (env.sh / direnv / loom-refresh)

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Self-review

**Spec coverage:**
- Rust toolchain on PATH → Task 1 (toolchain `bin/`, `LD_LIBRARY_PATH`). ✓
- Dev tools on PATH → Task 1 (`reindeer`/`prek`/`btd`/`supertd`/`rustfmt`); clippy via `cargo clippy` (documented). ✓
- First-party binaries, expose-only + `loom-refresh` → Task 2. ✓
- Two entry points, one engine (direnv + manual `eval`) → Task 3 `.envrc` calls `tools/env.sh`; manual eval documented in Task 4. ✓
- `.loom/bin` managed symlink dir; toolchain real `bin/` on PATH → Task 1. ✓
- `.gitignore` `/.loom/` + `/.direnv/` → Task 3. ✓
- Docs (DEVELOPING.md + CLAUDE.md) → Task 4. ✓
- Risks: standalone binary runnability, sysroot, stdout cleanliness, PATH idempotency — all pre-verified above and re-checked in Task 1 steps 3–6 / Task 2 step 3.

**Spec refinement:** spec step 5 said env.sh "symlinks already-built //src binaries." Refined here: env.sh does **not** touch `//src` at all (no build, no symlink); first-party symlinks are owned solely by `loom-refresh` and persist in the gitignored `.loom/bin` between shells. This is simpler and still satisfies "expose-only, no build on activation."

**Placeholder scan:** none — every script and doc block is complete.

**Naming consistency:** `.loom/bin` (`$BIN`), `LOOM_ENV_ACTIVE`, `loom-refresh`, `tools/env.sh`, `resolve`, `_loom_strip` used consistently across tasks.
