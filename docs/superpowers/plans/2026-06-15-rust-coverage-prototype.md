# Rust Coverage Prototype Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Give a developer a one-command line/region/function coverage report plus an `lcov.info` for `src/control-plane/core`, proving the buck2 `-Cinstrument-coverage` → `llvm-profdata` → `llvm-cov` flow.

**Architecture:** A parse-time buckconfig gate (`--config loom.coverage=true`) appends `-Cinstrument-coverage` to the hermetic Rust toolchain's `rustc_flags`. A `genrule` under the dev-only `//tools/coverage` package (outside `//src/...`, so CI/normal builds are untouched) runs the crate's instrumented `rust_test` binaries, merges their `.profraw` files with the pinned LLVM dist's `llvm-profdata`, and emits `lcov.info` + `report.txt` as build artifacts. A thin `tools/coverage.sh` builds that target under the config, copies `lcov.info` into the gitignored `.loom/coverage/`, and prints `report.txt`.

**Tech Stack:** buck2, the vendored prelude's `genrule`/`rust_toolchain`, Rust source-based coverage (LLVM instrumentation), the pinned LLVM 22.1.2 dist (`toolchains//:llvm-x86_64-linux`) which ships `llvm-profdata`/`llvm-cov`.

**Spec:** `docs/superpowers/specs/2026-06-15-rust-coverage-prototype-design.md`

---

## File structure

- `toolchains/BUCK` — **modify**: add the `read_config` gate and conditional `-Cinstrument-coverage` flag (the toggle).
- `tools/coverage/cover.sh` — **create**: the reusable worker. Args: `<llvm_dist_dir> <out_dir> <src_filter> <test_bin>...`. Runs each binary under `LLVM_PROFILE_FILE`, merges, and writes `lcov.info` + `report.txt` into `out_dir`. Fails with an explicit message if no `.profraw` was produced (i.e. built un-instrumented).
- `tools/coverage/BUCK` — **create**: `export_file` for `cover.sh` + the `//tools/coverage:core` `genrule` wiring the crate's six test targets, the LLVM dist, and the source filter.
- `tools/coverage.sh` — **create**: the dev wrapper (build target under the config, copy `lcov.info` out, `cat` `report.txt`).
- `CLAUDE.md` — **modify**: a short "Test coverage" note under the Testing section.

Why two scripts: `tools/coverage/cover.sh` is the *worker* that runs **inside** the buck action (no buck knowledge, pure args), so it stays hermetic and reusable per-crate. `tools/coverage.sh` is the *driver* that runs **outside** buck (invokes `buck2`, touches `.loom/`). Keeping them separate keeps each file single-purpose.

---

## Task 1: Config-gated instrumentation toggle in the toolchain

**Files:**
- Modify: `toolchains/BUCK` (the `hermetic_rust_toolchain` block at `toolchains/BUCK:148-158`, and add the gate just above it)

- [ ] **Step 1: Add the gate and wire it into `rustc_flags`**

In `toolchains/BUCK`, immediately **above** the `hermetic_rust_toolchain(name = "rust", …)` call (currently at line ~148), add:

```python
# Coverage gate: `buck2 build --config loom.coverage=true //…` appends
# -Cinstrument-coverage to every Rust compile in that configuration, so
# `rust_test` binaries and their lib deps emit an LLVM coverage map. Off by
# default — normal builds are byte-for-byte unchanged. Consumed by the
# //tools/coverage genrules. See docs/.../2026-06-15-rust-coverage-prototype.
_COVERAGE = read_config("loom", "coverage", "") in ("true", "1")
_RUSTC_FLAGS = ["-Copt-level=2"] + (["-Cinstrument-coverage"] if _COVERAGE else [])
```

Then change the toolchain call's `rustc_flags` line from:

```python
    rustc_flags = ["-Copt-level=2"],
```

to:

```python
    rustc_flags = _RUSTC_FLAGS,
```

- [ ] **Step 2: Verify the default build is unchanged (flag absent)**

Run: `buck2 audit providers //src/control-plane/core:core 2>/dev/null | grep -c instrument-coverage || true`
Expected: prints `0` (no instrumentation in the default configuration).

> Note: if `audit providers` does not surface toolchain flags on your buck2 version, instead run
> `buck2 build //src/control-plane/core:page 2>&1 | tail -2` and confirm it still builds clean. The real proof of "unchanged" is Task 1 Step 3.

- [ ] **Step 3: Verify the gate flips instrumentation on**

Run:
```bash
buck2 build --config loom.coverage=true //src/control-plane/core:page --show-simple-output 2>/dev/null
```
Expected: a binary path is printed and the build succeeds. (Link success here is the first real signal that the nightly `rust-std` carries the profiler runtime — if it fails to link with undefined `__llvm_profile_*` symbols, STOP and report; see the Risks section.)

- [ ] **Step 4: Commit**

```bash
git add toolchains/BUCK
git commit -m "feat(coverage): config-gated -Cinstrument-coverage in rust toolchain

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 2: Spike — prove the end-to-end flow by hand and lock the source filter

This task writes **no committed files**. It runs the full `-Cinstrument-coverage` → `llvm-profdata` → `llvm-cov` flow manually on one test binary to (a) confirm a coverage map is produced and (b) observe the exact source-path strings buck embeds, which determines the `src_filter` argument used in Task 3.

- [ ] **Step 1: Resolve the LLVM tools and confirm they exist**

Run:
```bash
LLVM=$(buck2 build toolchains//:llvm-x86_64-linux --show-simple-output 2>/dev/null)
ls "$LLVM/bin/llvm-profdata" "$LLVM/bin/llvm-cov"
```
Expected: both paths list without error.

- [ ] **Step 2: Build the instrumented test binary**

Run:
```bash
BIN=$(buck2 build --config loom.coverage=true //src/control-plane/core:page --show-simple-output 2>/dev/null)
echo "$BIN"
```
Expected: a path under `buck-out/…/__page__/page`.

- [ ] **Step 3: Run it under LLVM_PROFILE_FILE and merge**

Run:
```bash
TMP=$(mktemp -d)
LLVM_PROFILE_FILE="$TMP/page.profraw" "$BIN" >/dev/null
ls "$TMP"/*.profraw
"$LLVM/bin/llvm-profdata" merge -sparse "$TMP"/*.profraw -o "$TMP/merged.profdata"
```
Expected: `page.profraw` exists and `merge` exits 0. (If no `.profraw` appears, the binary was not instrumented — re-check Task 1.)

- [ ] **Step 4: Observe the embedded source paths**

Run:
```bash
"$LLVM/bin/llvm-cov" report -instr-profile="$TMP/merged.profdata" "$BIN" 2>/dev/null | head -30
```
Expected: a coverage table. **Record the leading path string of the `control-plane/core` source rows** (the `Filename` column). It will be one of:
- repo-relative, e.g. `src/control-plane/core/src/page.rs` → **use `src/control-plane/core/src` as the Task 3 `src_filter`** (the common case under buck2's repo-root cwd).
- absolute under buck-out, e.g. `/…/buck-out/…/page.rs` → use the trailing substring `control-plane/core/src` as the filter and rely on llvm-cov's prefix matching.

Also confirm the table includes the crate's own files (e.g. `page.rs`, `lib.rs`) with non-zero region counts, and is not dominated by `library/std` rows (those are filtered out in Task 3 by the source filter).

- [ ] **Step 5: Confirm the filtered report is non-empty**

Using the path prefix from Step 4 (shown here as the common case):
```bash
"$LLVM/bin/llvm-cov" report -instr-profile="$TMP/merged.profdata" "$BIN" src/control-plane/core/src 2>/dev/null
```
Expected: a table listing only `src/control-plane/core/src/*` files with a non-zero TOTAL line-coverage %. Note the exact filter string that worked — it is the `src_filter` literal for Task 3 Step 1.

No commit (nothing was created).

---

## Task 3: The coverage genrule and its worker script

**Files:**
- Create: `tools/coverage/cover.sh`
- Create: `tools/coverage/BUCK`

- [ ] **Step 1: Write the worker script**

Create `tools/coverage/cover.sh` with exactly:

```bash
#!/usr/bin/env bash
# Coverage worker — runs INSIDE a buck2 genrule action (see tools/coverage/BUCK).
# Args: <llvm_dist_dir> <out_dir> <ignore_regex> <test_bin>...
# Runs each instrumented rust_test binary under LLVM_PROFILE_FILE, merges the
# .profraw files, and writes lcov.info + report.txt into <out_dir>, dropping
# files matching <ignore_regex> (third-party crates and the test files
# themselves, leaving the library's own src/). Exits non-zero with a clear
# message if no coverage data was produced (target built without
# --config loom.coverage=true).
#
# NOTE: LLVM 22's llvm-cov ignores a positional source-path filter; the only
# working restriction is -ignore-filename-regex (confirmed by the spike).
set -euo pipefail

llvm_dir="$1"; out_dir="$2"; ignore_regex="$3"; shift 3
bins=("$@")

prof_dir="$(mktemp -d)"
i=0
for bin in "${bins[@]}"; do
    LLVM_PROFILE_FILE="$prof_dir/cov-$i.profraw" "$bin" >/dev/null 2>&1 || true
    i=$((i + 1))
done

shopt -s nullglob
raws=("$prof_dir"/*.profraw)
if [ ${#raws[@]} -eq 0 ]; then
    echo "coverage: no .profraw produced — instrumented binaries are required." >&2
    echo "coverage: build with --config loom.coverage=true" >&2
    exit 1
fi

"$llvm_dir/bin/llvm-profdata" merge -sparse "${raws[@]}" -o "$prof_dir/merged.profdata"

# First binary is positional; the rest are passed via -object.
head_bin="${bins[0]}"
object_args=()
for bin in "${bins[@]:1}"; do
    object_args+=("-object" "$bin")
done

"$llvm_dir/bin/llvm-cov" export -format=lcov \
    -instr-profile="$prof_dir/merged.profdata" \
    -ignore-filename-regex="$ignore_regex" \
    "$head_bin" "${object_args[@]}" > "$out_dir/lcov.info"

"$llvm_dir/bin/llvm-cov" report \
    -instr-profile="$prof_dir/merged.profdata" \
    -ignore-filename-regex="$ignore_regex" \
    "$head_bin" "${object_args[@]}" > "$out_dir/report.txt"
```

- [ ] **Step 2: Make it executable**

Run: `chmod +x tools/coverage/cover.sh`
Expected: no output.

- [ ] **Step 3: Write the genrule BUCK**

Create `tools/coverage/BUCK` with exactly. The third arg to `cover.sh` is the `-ignore-filename-regex` value `^/|^third-party/|/tests/` — confirmed by the spike + verification: `^/` drops absolute paths (rust stdlib at `/rustc/…`, cargo registry), `^third-party/` drops third-party crates, `/tests/` drops the integration-test files, leaving the library's own repo-relative `src/control-plane/core/src/**` (which is what we want to measure). lcov `SF:` lines come out as `src/control-plane/core/src/…`.

```python
# Dev-only coverage targets. Live here (NOT under //src/...) so CI's
# `buck2 build/test //src/...` never builds them and normal builds are
# unaffected (CLAUDE.md: //tools is dev-only, not built in CI). Build with
# --config loom.coverage=true so the referenced rust_test deps compile
# instrumented; tools/coverage.sh is the convenience driver.

export_file(
    name = "cover-sh",
    src = "cover.sh",
)

_CORE_TESTS = [
    "//src/control-plane/core:page",
    "//src/control-plane/core:error-display",
    "//src/control-plane/core:identity",
    "//src/control-plane/core:logical-type",
    "//src/control-plane/core:row-filter-validation",
    "//src/control-plane/core:serde-roundtrip",
]

genrule(
    name = "core",
    outs = {
        "lcov": ["lcov.info"],
        "report": ["report.txt"],
    },
    cmd = " ".join([
        "bash",
        "$(location :cover-sh)",
        "$(location toolchains//:llvm-x86_64-linux)",
        "$OUT",
        "'^/|^third-party/|/tests/'",
    ] + ["$(location {})".format(t) for t in _CORE_TESTS]),
)
```

- [ ] **Step 4: Verify the genrule fails clearly WITHOUT the config**

Run: `buck2 build //tools/coverage:core 2>&1 | grep -i "loom.coverage" | head -1`
Expected: the line `coverage: build with --config loom.coverage=true` appears (the build fails because the test binaries are un-instrumented and `cover.sh` finds no `.profraw`).

- [ ] **Step 5: Verify the genrule produces artifacts WITH the config**

Run:
```bash
buck2 build --config loom.coverage=true //tools/coverage:core 2>/dev/null
L=$(buck2 build --config loom.coverage=true "//tools/coverage:core[lcov]" --show-simple-output 2>/dev/null)
R=$(buck2 build --config loom.coverage=true "//tools/coverage:core[report]" --show-simple-output 2>/dev/null)
test -s "$L" && echo "lcov ok: $L"
head -1 "$L"
echo "--- report ---"; cat "$R"
```
Expected: `lcov ok: …`, the first lcov line is `SF:src/control-plane/core/src/…` (or `TN:`), and the report table lists the crate's source files with a non-zero TOTAL %.

- [ ] **Step 6: Commit**

```bash
git add tools/coverage/cover.sh tools/coverage/BUCK
git commit -m "feat(coverage): genrule emitting lcov.info + report.txt for control-plane/core

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 4: The `tools/coverage.sh` driver

**Files:**
- Create: `tools/coverage.sh`

- [ ] **Step 1: Write the driver**

Create `tools/coverage.sh` with exactly:

```bash
#!/usr/bin/env bash
# Dev driver: build the coverage genrule for a crate under the coverage config,
# copy lcov.info into the gitignored .loom/coverage/, and print the report.
# Usage: tools/coverage.sh [crate]   (default: core)
set -euo pipefail

crate="${1:-core}"
target="//tools/coverage:${crate}"

buck2 build --config loom.coverage=true "$target" >/dev/null

lcov="$(buck2 build --config loom.coverage=true "${target}[lcov]" --show-simple-output 2>/dev/null)"
report="$(buck2 build --config loom.coverage=true "${target}[report]" --show-simple-output 2>/dev/null)"

mkdir -p .loom/coverage
cp "$lcov" .loom/coverage/lcov.info

cat "$report"
echo
echo "lcov written to .loom/coverage/lcov.info"
```

- [ ] **Step 2: Make it executable**

Run: `chmod +x tools/coverage.sh`
Expected: no output.

- [ ] **Step 3: Run the full dev flow**

Run: `./tools/coverage.sh`
Expected: the coverage table prints to the terminal, ends with `lcov written to .loom/coverage/lcov.info`, and `test -s .loom/coverage/lcov.info` succeeds.

- [ ] **Step 4: Confirm `.loom/coverage/` is gitignored**

Run: `git status --porcelain .loom/ ; git check-ignore .loom/coverage/lcov.info`
Expected: `git status` prints nothing for `.loom/`, and `check-ignore` echoes `.loom/coverage/lcov.info` (it is ignored via the existing `/.loom/` rule).

- [ ] **Step 5: Commit**

```bash
git add tools/coverage.sh
git commit -m "feat(coverage): tools/coverage.sh dev driver (build, copy lcov, print report)

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 5: Document the workflow

**Files:**
- Modify: `CLAUDE.md` (the `## Testing` section)

- [ ] **Step 1: Add a coverage note**

In `CLAUDE.md`, at the end of the `## Testing` section, add:

```markdown
- **Test coverage (prototype, `control-plane/core` only):** `./tools/coverage.sh`
  prints an `llvm-cov` table and writes `.loom/coverage/lcov.info` for editor
  gutter plugins. Mechanism: `--config loom.coverage=true` adds
  `-Cinstrument-coverage` in the Rust toolchain (off by default; normal/CI builds
  untouched), and the dev-only `//tools/coverage:core` genrule runs the
  instrumented `rust_test` binaries and merges their profiles with the pinned
  LLVM dist's `llvm-profdata`/`llvm-cov`. Coverage targets live under
  `//tools/coverage` (outside `//src/...`) so CI never builds them. Extending to
  fixture crates: add a target there carrying a prelude local label (the genrule
  analog of `loom_fixture_test`'s `remote_execution = "disabled"`).
```

- [ ] **Step 2: Verify markdown lint passes**

Run: `buck2 run //tools:prek -- run --files CLAUDE.md 2>&1 | tail -5`
Expected: hooks pass (or auto-fix trailing whitespace/EOF; if they edit the file, re-stage it). Per `.claude/rules/markdown-lint.md`, the file must end in exactly one newline with no trailing whitespace.

- [ ] **Step 3: Commit**

```bash
git add CLAUDE.md
git commit -m "docs(coverage): document tools/coverage.sh in the Testing section

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Final verification (whole feature)

- [ ] Normal build untouched: `buck2 build //src/...` (no `--config`) succeeds and never builds `//tools/coverage:core`.
- [ ] Gate off by default: `buck2 build //src/control-plane/core:page` links without instrumentation.
- [ ] One command works: `./tools/coverage.sh` prints a non-zero coverage table and writes `.loom/coverage/lcov.info`.
- [ ] Explicit failure: `buck2 build //tools/coverage:core` (no config) fails with the `--config loom.coverage=true` hint.
- [ ] Existing tests still green: `buck2 test //src/control-plane/core/... > /tmp/cov_t.log 2>&1; grep -E "Tests finished|FAIL" /tmp/cov_t.log` (do not pipe `buck2 test` through `tail`).

---

## Risks / things that may need adjustment during execution

- **Profiler runtime linking — RESOLVED by the spike.** `-Cinstrument-coverage` links cleanly against the pinned nightly `rust-std`; the instrumented `:page` build succeeds and emits a coverage map. No `__llvm_profile_*` blocker.
- **Source filtering — RESOLVED by the spike.** LLVM 22.1.2's `llvm-cov` **ignores a positional source-path filter** (the table comes out unfiltered). The working mechanism is `-ignore-filename-regex`. The plan uses `^/|^third-party/|/tests/` to drop absolute paths (rust stdlib / cargo registry), third-party crates, and the integration-test files, leaving `src/control-plane/core/src/**`. Embedded paths are repo-relative, so lcov `SF:` lines are `src/control-plane/core/src/…`.
- **llvm-cov flag spelling — RESOLVED by the spike.** Single-dash flags (`-format=lcov`, `-instr-profile=`, `-ignore-filename-regex=`, `-object`) all work on the pinned LLVM 22.1.2.
- **`$(location)` of a named-output genrule (`[lcov]`/`[report]`).** Expected pattern is `outs`-dict named outputs addressed as `:core[lcov]`. If `--show-simple-output` on a named subtarget misbehaves on this buck2 version, fall back to building `//tools/coverage:core` (the whole out dir) and reading `out/lcov.info` / `out/report.txt` from the printed dir path.
