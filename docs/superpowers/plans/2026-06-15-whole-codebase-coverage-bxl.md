# Whole-codebase Coverage via BXL — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A single `tools/coverage/cov.bxl` that discovers every `rust_test` under `//src`, runs each instrumented binary (locally for fixtures, on RE for pure-logic) to collect coverage, and emits per-crate + combined `lcov`/`report`/`profdata`; a thin `tools/coverage.sh` wrapper prints the combined table to stdout and renders HTML.

**Architecture:** BXL is buck2's scripting layer — it can `cquery` for targets, read their attrs, `analysis()` them to get built artifacts, and register `actions.run(...)` with per-action `local_only`/`env`. The BXL discovers tests, classifies fixtures from the `remote_execution` attr, and orchestrates two tiny checked-in shell tools (`run_one.sh` per test → a `.profraw`; `report.sh` per crate → merge + `llvm-cov`). Execution heterogeneity is per-action, so pure-logic runs on RE and fixtures local in one script.

**Tech Stack:** buck2 BXL (`buck2 bxl`), the pinned LLVM 22.1.2 dist (`toolchains//:llvm-x86_64-linux`), the config-free coverage gate (`//tools/coverage:coverage_enabled` constraint_value + root `PACKAGE` cfg-constructor; `cov.bxl` applies it via `modifiers=`), bash worker tools.

**Spec:** `docs/superpowers/specs/2026-06-15-whole-codebase-coverage-bxl-design.md`
**Prototype it builds on / will retire:** `docs/superpowers/plans/2026-06-15-rust-coverage-prototype.md`

---

## File structure

- `tools/coverage/run_one.sh` — **create**: runs ONE instrumented test binary under a given `LLVM_PROFILE_FILE`. Trivial, certain, the unit of per-test execution.
- `tools/coverage/report.sh` — **create**: merges a crate's `.profraw`s and runs `llvm-cov export`/`report` → `lcov.info`/`report.txt`/`profdata`. This is the prototype's `cover.sh` tail, placement-agnostic.
- `tools/coverage/cov.bxl` — **create**: the orchestrator (discover → classify → analysis → per-test `run_one` actions → per-crate + combined `report.sh` actions → ensure/print).
- `tools/coverage/BUCK` — **modify**: `export_file` the two scripts (so the BXL can `analysis()` them); the `:core` genrule + `:cover-sh` are removed in Task 5.
- `tools/coverage.sh` — **modify** (Task 4): becomes the thin BXL driver (config + `-j` cap, copy artifacts to `.loom/coverage/`, render combined HTML, cat the combined table).
- `CLAUDE.md` — **modify** (Task 5): update the coverage note for the BXL flow.

**Key BXL API facts (verified in `prelude/bxl/*.bxl`, `prelude/erlang/elp.bxl`):**
- `ctx.cquery().kind("rust_test", ctx.cquery().eval("//src/..."))` → configured test nodes.
- `node.attrs_lazy().get("remote_execution")` → read an attr (returns an attr object; `.value()` for the literal).
- `ctx.analysis(label).providers()[DefaultInfo].default_outputs[0]` → a target's built artifact (the test binary, or the llvm dist dir, or an `export_file`'d script).
- `ctx.bxl_actions().actions` → `.declare_output(name)`, `.run(cmd_args(...), category=, identifier=, local_only=, env=)`.
- `ctx.output.ensure(artifact)` / `ensure_multiple(...)` / `ctx.output.print(...)`.

---

## Task 1.5: Config-free gate — DONE

**Status: already implemented and committed.**

The prototype's `read_config`-based flag gate has been replaced by a
constraint-modifier mechanism:

- `tools/coverage/BUCK` defines `constraint_setting(name = "coverage")` and
  `constraint_value(name = "coverage_enabled", ...)`.
- The root `PACKAGE` file registers the prelude's cfg-constructor
  (`set_cfg_constructor(...)`) so that `modifiers = [...]` / `-m` actually
  re-configure targets (without it, modifiers are a silent no-op).
- `toolchains/BUCK` selects the flag:
  `rustc_flags = ["-Copt-level=2"] + select({"root//tools/coverage:coverage_enabled": ["-Cinstrument-coverage"], "DEFAULT": []})`.
- `cov.bxl` sets `COVERAGE_MODIFIERS = ["root//tools/coverage:coverage_enabled"]`
  and calls `ctx.configured_targets(label, modifiers = COVERAGE_MODIFIERS)`.
- Invocation: `buck2 bxl //tools/coverage:cov.bxl:cov` — **no `--config`**.
- For a direct instrumented `buck2 build` (e.g. wrapper HTML step): use
  `buck2 build -m //tools/coverage:coverage_enabled <target>` — not `--config`.

---

## Task 1: Spike — minimal `cov.bxl` proving the mechanic on `core`

Resolves O1 (running a test binary as a BXL action → profraw), O4 (instrumented actions place OK), and the report-capture detail. Pure-logic only, `local_only=True` everywhere for now (RE comes in Task 2 verification).

**Files:**
- Create: `tools/coverage/run_one.sh`, `tools/coverage/report.sh`, `tools/coverage/cov.bxl`
- Modify: `tools/coverage/BUCK` (export the scripts)

- [ ] **Step 1: Write `tools/coverage/run_one.sh` (exact)**

```bash
#!/usr/bin/env bash
# Run one instrumented rust_test binary, writing its profile to $1.
# Args: <profraw_out_path> <test_bin>
# Any extra args after the binary are passed to it (none needed today).
set -euo pipefail
out="$1"; bin="$2"; shift 2
LLVM_PROFILE_FILE="$out" "$bin" "$@" >/dev/null 2>&1 || true
# The libtest binary exits non-zero only on a FAILING test; coverage still wants
# the profile, so we don't fail the action on test failure (the test suite is
# graded by `buck2 test`, not here).
```

- [ ] **Step 2: Write `tools/coverage/report.sh` (exact)**

```bash
#!/usr/bin/env bash
# Merge a crate's .profraw files and render lcov + a text table.
# Args: <llvm_dist_dir> <out_dir> <profdata_out> <ignore_regex> \
#       <profraw>... -- <test_bin>...
# Writes <out_dir>/lcov.info and <out_dir>/report.txt, and the merged profile to
# <profdata_out>. LLVM 22's llvm-cov ignores a positional source filter, so the
# only restriction is -ignore-filename-regex.
set -euo pipefail
llvm="$1"; out_dir="$2"; profdata="$3"; ignore="$4"; shift 4

profraws=(); bins=(); mode="p"
for a in "$@"; do
    if [ "$a" = "--" ]; then mode="b"; continue; fi
    if [ "$mode" = "p" ]; then profraws+=("$a"); else bins+=("$a"); fi
done

"$llvm/bin/llvm-profdata" merge -sparse "${profraws[@]}" -o "$profdata"

head_bin="${bins[0]}"; object_args=()
for b in "${bins[@]:1}"; do object_args+=("-object" "$b"); done

"$llvm/bin/llvm-cov" export -format=lcov -instr-profile="$profdata" \
    -ignore-filename-regex="$ignore" "$head_bin" "${object_args[@]}" > "$out_dir/lcov.info"
"$llvm/bin/llvm-cov" report -instr-profile="$profdata" \
    -ignore-filename-regex="$ignore" "$head_bin" "${object_args[@]}" > "$out_dir/report.txt"
```

- [ ] **Step 3: `chmod +x tools/coverage/run_one.sh tools/coverage/report.sh`**

- [ ] **Step 4: Export the scripts in `tools/coverage/BUCK`**

Add (keep the existing `:core` genrule and `:cover-sh` for now — removed in Task 5):

```python
export_file(name = "run-one", src = "run_one.sh")
export_file(name = "report-sh", src = "report.sh")
```

- [ ] **Step 5: Write the spike `tools/coverage/cov.bxl`**

This is the best-effort starting point; Step 6 iterates it to green and you pin
the exact working API in a comment. `out_dir` for `report.sh` must be a declared
output **directory**.

```python
IGNORE = "^/|^third-party/|/tests/"

def _cov_impl(ctx):
    bxl_actions = ctx.bxl_actions()
    actions = bxl_actions.actions

    llvm = ctx.analysis("toolchains//:llvm-x86_64-linux").providers()[DefaultInfo].default_outputs[0]
    run_one = ctx.analysis("//tools/coverage:run-one").providers()[DefaultInfo].default_outputs[0]
    report_sh = ctx.analysis("//tools/coverage:report-sh").providers()[DefaultInfo].default_outputs[0]

    tests = ctx.cquery().kind("rust_test", ctx.cquery().eval("//src/control-plane/core/..."))

    bins = []
    profraws = []
    for node in tests:
        label = node.label
        bin = ctx.analysis(label).providers()[DefaultInfo].default_outputs[0]
        bins.append(bin)
        pr = actions.declare_output("prof/{}.profraw".format(label.name))
        actions.run(
            cmd_args(["bash", run_one, pr.as_output(), bin]),
            category = "coverage_run",
            identifier = label.name,
            local_only = True,
        )
        profraws.append(pr)

    out_dir = actions.declare_output("core", dir = True)
    profdata = actions.declare_output("core.profdata")
    args = cmd_args([
        "bash", report_sh,
        cmd_args(llvm),
        out_dir.as_output(),
        profdata.as_output(),
        IGNORE,
    ])
    args.add(profraws)
    args.add("--")
    args.add(bins)
    actions.run(args, category = "coverage_report", identifier = "core", local_only = True)

    ensured = ctx.output.ensure(out_dir)
    ctx.output.print(ensured)

cov = bxl_main(impl = _cov_impl, cli_args = {})
```

- [ ] **Step 6: Run it, iterate to green**

Run: `buck2 bxl //tools/coverage:cov.bxl:cov 2>&1 | tail -20`
Expected: prints a path to the `core` output dir; `cat <that>/report.txt` shows the
same table the prototype produced (TOTAL ~97.5% regions over `src/control-plane/core/src/*`).

If the BXL API differs from the draft (likely candidates and fixes):
- `report.sh` writing into `out_dir`: the action must create the dir first — if
  buck pre-creates the declared dir, fine; if not, prepend `mkdir -p "$out_dir"`
  in `report.sh` before the writes.
- If `actions.run` rejects an output **directory** for a script that writes
  multiple files, switch to two declared file outputs (`lcov`, `report`) passed
  individually instead of `out_dir`.
- If `node.label.name`/`ctx.analysis(label)` shape differs, adjust per the error
  (e.g. `ctx.analysis(node)` vs `ctx.analysis(label)`).
- **O1 fallback already applied:** binaries are run via `run_one.sh` taking the
  profraw path as an arg, so we never depend on env-referencing-an-output.

Once green, add a comment block at the top of `cov.bxl` recording the exact
working API calls you settled on.

- [ ] **Step 7: Confirm no repo-root profraw leak**

Run: `ls default_*.profraw 2>/dev/null | wc -l`
Expected: `0` (run_one.sh always sets `LLVM_PROFILE_FILE`).

- [ ] **Step 8: Commit**

```bash
git add tools/coverage/run_one.sh tools/coverage/report.sh tools/coverage/cov.bxl tools/coverage/BUCK
git commit -m "feat(coverage): BXL spike — discover+run+report for control-plane/core

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 2: Generalize to all pure-logic crates + per-crate & combined output

Extend discovery to all of `//src`, group by package, emit one `report.sh` per
crate plus a combined one, and let pure-logic tests run on RE. Fixtures are still
excluded here (Task 3 adds them) — filter them out by the `remote_execution` attr
so the run stays green before fixture env exists.

**Files:**
- Modify: `tools/coverage/cov.bxl`

- [ ] **Step 1: Add a CLI arg and crate grouping**

Replace the `cli_args = {}` with an optional crate selector and group nodes by
package:

```python
cov = bxl_main(
    impl = _cov_impl,
    cli_args = {"crate": cli_args.option(cli_args.string())},
)
```

In `_cov_impl`, choose the universe and skip fixtures (for now):

```python
    universe = "//src/{}/...".format(ctx.cli_args.crate) if ctx.cli_args.crate else "//src/..."
    tests = ctx.cquery().kind("rust_test", ctx.cquery().eval(universe))

    # group label -> crate key (the package path), skipping fixture tests
    by_crate = {}
    for node in tests:
        if node.attrs_lazy().get("remote_execution") != None and \
           node.attrs_lazy().get("remote_execution").value() == "disabled":
            continue  # fixture — handled in Task 3
        key = str(node.label.package)   # e.g. "src/control-plane/core"
        by_crate.setdefault(key, []).append(node)
```

> Note: `attrs_lazy().get(x)` returns `None` when the attr is unset. Confirm the
> exact `.value()` accessor against the Task 1 spike (some buck2 versions return
> the literal directly from `.get(...)`); adjust this predicate to match.

- [ ] **Step 2: Per-crate report actions + a combined one**

For each crate key, run the per-test `run_one` actions and a `report.sh` (as in
Task 1) writing to `actions.declare_output(<crate_name>, dir=True)`. Accumulate
ALL profraws and ALL bins across crates, then run one more `report.sh` for the
`combined` dir using the full sets. Run pure-logic actions on RE by passing
`local_only = False` (the default — simply omit it).

Concretely, factor a helper inside `_cov_impl`:

```python
    def emit(name, nodes):
        bins, prs = [], []
        for node in nodes:
            b = ctx.analysis(node.label).providers()[DefaultInfo].default_outputs[0]
            pr = actions.declare_output("prof/{}/{}.profraw".format(name, node.label.name))
            actions.run(cmd_args(["bash", run_one, pr.as_output(), b]),
                        category = "coverage_run", identifier = "{}:{}".format(name, node.label.name))
            bins.append(b); prs.append(pr)
        out = actions.declare_output(name, dir = True)
        pd = actions.declare_output("{}.profdata".format(name))
        a = cmd_args(["bash", report_sh, cmd_args(llvm), out.as_output(), pd.as_output(), IGNORE])
        a.add(prs); a.add("--"); a.add(bins)
        actions.run(a, category = "coverage_report", identifier = name)
        return out, bins, prs

    all_bins, all_prs, outs = [], [], {}
    for key, nodes in by_crate.items():
        name = key.replace("/", "_")
        out, bins, prs = emit(name, nodes)
        outs[name] = out
        all_bins += bins; all_prs += prs

    combined = actions.declare_output("combined", dir = True)
    cpd = actions.declare_output("combined.profdata")
    ca = cmd_args(["bash", report_sh, cmd_args(llvm), combined.as_output(), cpd.as_output(), IGNORE])
    ca.add(all_prs); ca.add("--"); ca.add(all_bins)
    actions.run(ca, category = "coverage_report", identifier = "combined")

    ctx.output.print(ctx.output.ensure(combined))
    for name, out in outs.items():
        ctx.output.ensure(out)
```

- [ ] **Step 3: Run on the 3 pure-logic crates**

Run: `buck2 bxl //tools/coverage:cov.bxl:cov 2>&1 | tail -20`
Expected: a combined `core`+`memory`+`runtime` run; `cat <combined>/report.txt`
lists files from all three crates' `src/`. `--crate control-plane/memory` limits
to one. (Cap with `LOOM_COVERAGE_JOBS`→`-j` only via the wrapper in Task 4; for
now add `-j 6` manually if needed.)

- [ ] **Step 4: Confirm pure-logic actions are RE-eligible**

Run the same command and check the buck2 summary line shows `remote:` > 0 for the
coverage actions (O4). If RE rejects them, set `local_only = True` on the run
actions and note it; coverage is still correct, just local.

- [ ] **Step 4b: Ensure deterministic object ordering in llvm-cov**

The per-crate object list passed to `llvm-cov` MUST be **deterministically
ordered with each crate's source-owning (primary) test binary first**. llvm-cov
attributes each function to the first object that defines it — non-deterministic
ordering caused `page.rs` coverage to be masked and the reported percentage to
wobble between 97.5% and 99.4% across runs. Sort the per-crate `bins` list by
label (or another stable key) and ensure the primary/source-owning test comes
first before passing them to `report.sh`.

- [ ] **Step 5: Commit**

```bash
git add tools/coverage/cov.bxl
git commit -m "feat(coverage): BXL covers all pure-logic crates, per-crate + combined

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 3: Fixture crates — classify, build env from source targets, run local

**Files:**
- Modify: `tools/coverage/cov.bxl`

- [ ] **Step 1: Build the fixture env from source targets**

At the top of `_cov_impl`, analyse the fixture support targets once and assemble
the env dict (mirrors `loom_fixture_test`, derived from the same targets so it
can't drift):

```python
    def out0(lbl):
        return ctx.analysis(lbl).providers()[DefaultInfo].default_outputs[0]

    pg = out0("//src/control-plane/postgres:postgres-bin")
    xml = out0("//src/control-plane/postgres:libxml2")
    migr = out0("//src/control-plane/postgres:migrations")
    duck = out0("//src/control-plane/postgres:duckdb-cli")
    duckext = out0("//src/control-plane/postgres:duckdb-extensions")

    fixture_env = {
        "POSTGRES_BIN_DIR": cmd_args(pg, format = "{}/bin"),
        "POSTGRES_LD_LIBRARY_PATH": cmd_args(cmd_args(pg, format = "{}/lib"), xml, delimiter = ":"),
        "LOOM_MIGRATIONS_DIR": cmd_args(migr, format = "{}/migrations"),
        "DUCKDB_BIN": cmd_args(duck),
        "DUCKDB_EXTENSION_DIR": cmd_args(duckext),
    }
```

> Verify these `$(location)` equivalences against `src/control-plane/postgres/defs.bzl`
> (`loom_fixture_test`) — the env keys and the `/bin`, `/lib`, `/migrations`
> suffixes must match exactly.

- [ ] **Step 2: Run fixture tests locally with that env**

In the `emit` helper, accept an `is_fixture` flag; for fixture nodes pass
`local_only = True, env = fixture_env` to the per-test `actions.run`. Stop
skipping fixtures in the grouping (Task 2 Step 1) — instead tag each crate group
as fixture or not from the `remote_execution` attr, and pass the flag through:

```python
    def emit(name, nodes, is_fixture):
        ...
        actions.run(cmd_args(["bash", run_one, pr.as_output(), b]),
                    category = "coverage_run", identifier = "{}:{}".format(name, node.label.name),
                    local_only = is_fixture,
                    env = fixture_env if is_fixture else {})
        ...
```

The `report.sh` actions stay RE-eligible (they don't boot postgres).

- [ ] **Step 3: Run one fixture crate**

Run: `buck2 bxl //tools/coverage:cov.bxl:cov -- --crate control-plane/postgres 2>&1 | tail -20`
Expected: postgres boots locally; `cat <postgres dir>/report.txt` shows
`src/control-plane/postgres/src/*` files with non-zero coverage. **Cap jobs**
(`LOOM_COVERAGE_JOBS` via Task 4, or `-j 6` here) — fixture suites are heavy.

- [ ] **Step 4: Confirm no leak and full-codebase run**

Run the no-arg command (all 7 crates). Expected: combined table spans all crates;
`ls default_*.profraw | wc -l` is `0`. This run is slow (boots postgres many
times); use a job cap.

- [ ] **Step 5: Commit**

```bash
git add tools/coverage/cov.bxl
git commit -m "feat(coverage): BXL fixture crates — local exec + postgres/duckdb env

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 4: `tools/coverage.sh` wrapper (config, jobs, copy-out, HTML, stdout)

Replace the prototype's build logic with a BXL driver. HTML is rendered here (O3:
the wrapper has the repo source tree; BXL action sandboxes don't).

**Files:**
- Modify: `tools/coverage.sh` (replace its body)

- [ ] **Step 1: Rewrite `tools/coverage.sh`**

```bash
#!/usr/bin/env bash
# Dev driver for Rust coverage (whole codebase or one crate) via BXL.
# Usage: tools/coverage.sh [crate-package]    e.g. tools/coverage.sh control-plane/core
# No arg = whole //src tree. Prints the combined llvm-cov table to STDOUT and
# writes per-crate + combined lcov/report (+ combined HTML) into .loom/coverage/.
#
# COST: the first run recompiles the dep graph under -Cinstrument-coverage
# (via the //tools/coverage:coverage_enabled modifier) and fixture crates boot
# postgres locally — cap with LOOM_COVERAGE_JOBS=<n>.
# Never `buck2 build -m //tools/coverage:coverage_enabled` a test and run it
# outside cov.bxl, and never run `buck2 test` instrumented — both leak
# default_*.profraw; this driver always captures profiles via run_one.sh.
set -euo pipefail

crate="${1:-}"
jobs="${LOOM_COVERAGE_JOBS:-}"
bxl_flags=()
[ -n "$jobs" ] && bxl_flags+=(-j "$jobs")
bxl="//tools/coverage:cov.bxl:cov"
args=()
[ -n "$crate" ] && args=(-- --crate "$crate")

out=.loom/coverage
rm -rf "$out"; mkdir -p "$out"

# The BXL prints the combined output dir path on the last stdout line and ensures
# all per-crate dirs. Capture the printed path, copy everything out of buck-out.
combined_dir="$(buck2 bxl "${bxl_flags[@]}" "$bxl" "${args[@]}" | tail -1)"
cp -r "$combined_dir" "$out/combined"
# Per-crate dirs live as siblings under the same buck-out bxl output root.
for d in "$(dirname "$combined_dir")"/*/; do
    n="$(basename "$d")"
    [ "$n" = "combined" ] && continue
    [ -f "$d/report.txt" ] && cp -r "$d" "$out/$n"
done

# Combined HTML (needs source tree, present here) from the combined profdata.
# Build the LLVM dist without the modifier (it is not a Rust target).
llvm="$(buck2 build toolchains//:llvm-x86_64-linux --show-simple-output 2>/dev/null)"
# Reconstruct the instrumented binary object list for HTML rendering.
# Use -m to apply the coverage_enabled modifier so the build matches what cov.bxl used.
scope="//src/...";  [ -n "$crate" ] && scope="//src/${crate}/..."
bins=()
while read -r t; do
    t="${t%% (*}"
    [ -n "$t" ] && bins+=("$(buck2 build -m //tools/coverage:coverage_enabled "$t" --show-simple-output 2>/dev/null)")
done < <(buck2 cquery "kind('rust_test', ${scope})" 2>/dev/null)
if [ -f "$out/combined/combined.profdata" ] || [ -f "$combined_dir/../combined.profdata" ]; then
    pd="$out/combined/combined.profdata"; [ -f "$pd" ] || pd="$combined_dir/../combined.profdata"
    head_bin="${bins[0]}"; rest=(); for b in "${bins[@]:1}"; do rest+=(-object "$b"); done
    rm -rf "$out/html"
    "$llvm/bin/llvm-cov" show -format=html -output-dir="$out/html" \
        -instr-profile="$pd" -ignore-filename-regex='^/|^third-party/|/tests/' \
        "$head_bin" "${rest[@]}" 2>/dev/null || true
fi

cat "$out/combined/report.txt"
echo
echo "per-crate + combined lcov/report : $out/"
echo "combined html                    : $out/html/index.html"
```

> The exact buck-out layout of BXL-ensured outputs (whether `combined.profdata`
> lands beside the dir or inside it) is confirmed during Task 1/2; adjust the
> `pd`/copy paths to the real layout. If a crate selector is a package path with
> a `/`, the `.loom/coverage/<n>` dir name will contain it — fine.

- [ ] **Step 2: `chmod +x tools/coverage.sh` (already +x; no-op if so)**

- [ ] **Step 3: Run one crate then the whole codebase**

Run: `LOOM_COVERAGE_JOBS=6 ./tools/coverage.sh control-plane/core`
Expected: the core table prints to stdout; `.loom/coverage/` has `combined/`,
`control-plane_core/` (or similar), and `html/index.html`.

Run: `LOOM_COVERAGE_JOBS=6 ./tools/coverage.sh`
Expected: combined table across all crates to stdout; per-crate dirs present;
`ls default_*.profraw | wc -l` is `0`.

- [ ] **Step 4: Commit**

```bash
git add tools/coverage.sh
git commit -m "feat(coverage): tools/coverage.sh drives the BXL — stdout table + lcov + HTML

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 5: Retire the prototype genrule + update docs

**Files:**
- Modify: `tools/coverage/BUCK` (remove `:core` genrule + `:cover-sh`)
- Delete: `tools/coverage/cover.sh`
- Modify: `CLAUDE.md`

- [ ] **Step 1: Remove the prototype genrule and its worker**

In `tools/coverage/BUCK`, delete the `export_file(name = "cover-sh", …)` and the
entire `genrule(name = "core", …)` block. Keep the new `run-one`/`report-sh`
exports. Then:

```bash
git rm tools/coverage/cover.sh
```

- [ ] **Step 2: Verify nothing references the removed targets**

Run: `buck2 uquery "//tools/coverage:" 2>/dev/null`
Expected: lists `run-one`, `report-sh` (and the `.bxl` is not a target) — no
`core`/`cover-sh`. Run `grep -rn "coverage:core\|cover.sh\|cover-sh" tools/ docs/ CLAUDE.md` and fix any stale references (the prototype spec/plan under `docs/superpowers/` may mention them — leave the historical docs, but fix CLAUDE.md).

- [ ] **Step 3: Update the CLAUDE.md coverage note**

Replace the existing "Test coverage (prototype…)" bullet in the `## Testing`
section with:

```markdown
- **Test coverage (`//src` via BXL):** `./tools/coverage.sh` (whole codebase) or
  `./tools/coverage.sh control-plane/core` (one crate) prints an `llvm-cov` table
  to stdout and writes per-crate + `combined/` `lcov.info`/`report.txt` and a
  combined `html/` into the gitignored `.loom/coverage/`. Mechanism:
  `tools/coverage/cov.bxl` discovers every `rust_test` under `//src`, builds them
  with the `//tools/coverage:coverage_enabled` modifier (config-free;
  `-Cinstrument-coverage` off by default — normal builds and `buck2 test` can't
  trip it), and runs each as a buck2 action — **fixtures local with
  postgres/duckdb env, pure-logic on RE** — collecting `.profraw` via
  `run_one.sh`, then merges + renders with the pinned LLVM dist. **Footguns:** the
  first run recompiles the graph instrumented and fixture crates boot postgres
  locally — cap with `LOOM_COVERAGE_JOBS=<n>`; and never run an instrumented
  binary outside `cov.bxl` (leaks `default_*.profraw`). Specs/plans:
  `docs/superpowers/{specs,plans}/2026-06-15-*coverage*`.
```

- [ ] **Step 4: Lint the markdown**

Run: `buck2 run //tools:prek -- run --files CLAUDE.md 2>&1 | tail -5`
Expected: hooks pass (re-stage if they auto-fix EOF/whitespace).

- [ ] **Step 5: Commit**

```bash
git add tools/coverage/BUCK CLAUDE.md
git commit -m "refactor(coverage): retire prototype genrule; BXL is the coverage path

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Final verification (whole feature)

- [ ] `./tools/coverage.sh control-plane/core` matches the prototype's numbers (~97.5% regions), table on stdout.
- [ ] `LOOM_COVERAGE_JOBS=6 ./tools/coverage.sh` produces a combined table across all 7 crates + per-crate dirs + `html/index.html`.
- [ ] Fixture crates boot postgres **locally**; pure-logic actions show `remote:` in the buck2 summary.
- [ ] `ls default_*.profraw` → 0 strays after any run.
- [ ] `buck2 build //src/...` (no config) neither runs the BXL nor instruments.
- [ ] `buck2 test //src/control-plane/core/... > /tmp/cov.log 2>&1; grep -E "Tests finished|FAIL" /tmp/cov.log` still green (don't pipe `buck2 test` to `tail`).

---

## Risks / adjustments expected during execution

- **BXL API drift (Task 1).** The draft `cov.bxl` is best-effort; the spike pins the exact `cquery`/`analysis`/`actions.run` shapes. The `run_one.sh` indirection means O1 (env-referencing-output) is already avoided.
- **`attrs_lazy().get("remote_execution").value()` accessor (Task 2/3).** Confirm the exact way to read the attr literal; the fixture predicate depends on it. If `remote_execution` isn't surfaced as a plain attr on configured nodes, fall back to classifying by package path (the 4 fixture packages are known: postgres, worker, ingest, query-api) — still no per-test list.
- **BXL-ensured output layout (Task 4).** Where `combined.profdata` and per-crate dirs land in buck-out determines the wrapper's copy/HTML paths; confirm in Task 1/2 and adjust.
- **RE placement of instrumented coverage actions (Task 2 Step 4 / O4).** If RE rejects them, `local_only = True` everywhere is the correct, slower fallback.
- **Cost.** Whole-codebase runs recompile instrumented once and boot postgres ~40×; `LOOM_COVERAGE_JOBS` capping is the mitigation, documented in the wrapper and CLAUDE.md.
