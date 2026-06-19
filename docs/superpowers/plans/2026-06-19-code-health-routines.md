# Code-health analysis routines — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Wire three hermetic CLI tools (`rust-code-analysis`, `lucidshark-duplo`, `jq`) into the buck2 build, then build two on-demand skills (`loom-complexity`, `loom-duplication`) that render tracked, diff-reviewable code-health registers and land changes as PR-on-green, mirroring `loom-stpa`.

**Architecture:** Each tool is a prebuilt GitHub-release binary fetched by `tools/BUCK` (`http_file` → untar/`chmod` `genrule` → per-arch `command_alias`), exactly like the existing `btd`/`prek` dev tools. Each skill runs its tool, post-processes the output with the **vendored** jq through a committed `render.jq` (a pure function of the metrics → deterministic census region), gates on a marker-delimited census diff, adds a bounded agent preamble only when the census changed, and lands via a `loom-stpa`-style PR block.

**Tech Stack:** buck2 + prelude builtins (`http_file`, `genrule`, `command_alias`), jq 1.8.1, `rust-code-analysis-cli` v2026.06.19.00, `lucidshark-duplo` v0.2.0, `gh` CLI, bash.

**Spec:** `docs/superpowers/specs/2026-06-19-code-health-routines-design.md`

**Pre-resolved facts (verified during planning — do not re-derive):**

- SHA256 of every asset is listed inline in Tasks 1-3 (computed from the real downloads).
- Tarball layouts: `rust-code-analysis` has a `…-<triple>/` wrapper dir holding `rust-code-analysis-cli` (use `--strip-components=1`); `lucidshark-duplo` tarball has **no** wrapper (binary `lucidshark-duplo` at root, no strip); `jq` assets are **raw static ELF binaries** (no tar at all — just `cp` + `chmod +x`).
- `rust-code-analysis-cli` flags: `-m` (metrics), `-O json`, `-p <paths>`, `-o <outdir>` (writes one JSON file per source file, tree mirrors source). Per-function metric paths: `.metrics.cyclomatic.sum`, `.metrics.cognitive.sum`, `.metrics.mi.mi_visual_studio` (0-100 scale), `.metrics.loc.sloc`. Function spaces are `select(.kind=="function")` reachable via recursive `..`.
- `lucidshark-duplo` flags: `<file-list>` or `--git`, `--json`, `-m <min-lines>`, `--baseline <file>` (suppress accepted), `--save-baseline <file>`, `--changed-only` (requires `--git`). JSON shape: `{summary:{…}, duplicates:[{line_count, file1:{path,start_line,end_line}, file2:{…}, lines:[…]}]}` — **pairwise**, not n-way clusters.

---

## File structure

| Path | Responsibility |
|---|---|
| `tools/BUCK` (modify) | Add `jq`, `rust-code-analysis`, `lucidshark-duplo` tool blocks. |
| `tools/env.sh` (modify) | Expose the three tools on the dev-shell PATH (the `TOOLS` array). |
| `CLAUDE.md` (modify) | Document the three tools in "Dev tools". |
| `docs/code-health/README.md` (create) | What the registers are, knobs, how to refresh/waive. |
| `docs/code-health/complexity.md` (create, generated) | Complexity register. |
| `docs/code-health/complexity-waivers.json` (create) | Accepted-complexity allowlist. |
| `docs/code-health/duplication.md` (create, generated) | Duplication register. |
| `docs/code-health/duplication-baseline.json` (create) | Accepted-duplication allowlist (duplo baseline). |
| `.claude/skills/loom-complexity/SKILL.md` (create) | Complexity routine (BLOCK A render + gate, BLOCK B land). |
| `.claude/skills/loom-complexity/render.jq` (create) | Deterministic complexity census renderer. |
| `.claude/skills/loom-complexity/tests/` (create) | `fixture.json`, `waivers.json`, `golden.md`, `run.sh` golden test. |
| `.claude/skills/loom-duplication/SKILL.md` (create) | Duplication routine. |
| `.claude/skills/loom-duplication/render.jq` (create) | Deterministic duplication census renderer. |
| `.claude/skills/loom-duplication/tests/` (create) | `dup.json`, `baseline.json`, `golden.md`, `run.sh`. |

All work happens on a feature branch (`feat/code-health-routines`).

---

## Task 1: Vendor jq hermetically

**Files:**
- Modify: `tools/BUCK` (append a new block)

- [ ] **Step 1: Append the jq tool block to `tools/BUCK`**

Add at the end of the file (these are prelude builtins — no `load()` needed):

```python
# jq (https://github.com/jqlang/jq) — vendored so the code-health routines render
# deterministically without depending on a host `jq` (loom-stpa uses host jq; that
# is a reproducibility hole for a scheduled routine). The Linux release assets are
# RAW STATIC BINARIES (not archives), so the genrule is just cp + chmod. To bump:
# replace JQ_VERSION and refresh each SHA256 from the `<asset>.sha256` on the
# release page (or `sha256sum` the downloaded asset).
#
# `buck2 run //tools:jq -- <args>`. Linux x86_64 / aarch64 only (matches platforms/BUCK).

JQ_VERSION = "jq-1.8.1"

_JQ_URL = "https://github.com/jqlang/jq/releases/download/{version}/jq-linux-{arch}"

http_file(
    name = "jq-amd64.bin",
    urls = [_JQ_URL.format(version = JQ_VERSION, arch = "amd64")],
    sha256 = "020468de7539ce70ef1bceaf7cde2e8c4f2ca6c3afb84642aabc5c97d9fc2a0d",
)

http_file(
    name = "jq-arm64.bin",
    urls = [_JQ_URL.format(version = JQ_VERSION, arch = "arm64")],
    sha256 = "6bc62f25981328edd3cfcfe6fe51b073f2d7e7710d7ef7fcdac28d4e384fc3d4",
)

genrule(
    name = "jq-x86_64-linux",
    out = "jq",
    cmd = "cp $(location :jq-amd64.bin) $OUT && chmod +x $OUT",
    executable = True,
)

genrule(
    name = "jq-aarch64-linux",
    out = "jq",
    cmd = "cp $(location :jq-arm64.bin) $OUT && chmod +x $OUT",
    executable = True,
)

command_alias(
    name = "jq",
    exe = select({
        "prelude//cpu/constraints:x86_64": ":jq-x86_64-linux",
        "prelude//cpu/constraints:arm64": ":jq-aarch64-linux",
    }),
    visibility = ["PUBLIC"],
)
```

- [ ] **Step 2: Verify it builds and runs**

Run: `buck2 run -v0 //tools:jq -- --version`
Expected: prints `jq-1.8.1`

- [ ] **Step 3: Commit**

```bash
git add tools/BUCK
git commit --no-verify -m "build(tools): vendor jq 1.8.1 as //tools:jq"
```

---

## Task 2: Vendor rust-code-analysis

**Files:**
- Modify: `tools/BUCK`

- [ ] **Step 1: Append the rust-code-analysis block to `tools/BUCK`**

```python
# rust-code-analysis (https://github.com/weave-hand/rust-code-analysis) — Mozilla's
# multi-language metrics CLI; drives the //loom-complexity routine. Ships `.tar.gz`
# per-arch with a `…-<triple>/` wrapper dir holding `rust-code-analysis-cli`
# (and -web, README); `--strip-components=1` flattens it. tar.gz ⇒ no `uses_xz`
# label (builds on RE). To bump: replace RCA_VERSION, refresh each SHA256.
#
# `buck2 run //tools:rust-code-analysis -- -m -O json -p src -o <dir>`.

RCA_VERSION = "v2026.06.19.00"

_RCA_URL = "https://github.com/weave-hand/rust-code-analysis/releases/download/{version}/rust-code-analysis-{version}-{triple}.tar.gz"

http_file(
    name = "rca-x86_64-linux.tar.gz",
    urls = [_RCA_URL.format(version = RCA_VERSION, triple = "x86_64-unknown-linux-gnu")],
    sha256 = "7f7e7d88e4135b869683230aced76781c8e089d03c096e192ce49dd8eb342b84",
)

http_file(
    name = "rca-aarch64-linux.tar.gz",
    urls = [_RCA_URL.format(version = RCA_VERSION, triple = "aarch64-unknown-linux-gnu")],
    sha256 = "459671ab9fb431db6e97d5dee38def443cf7cfff77f8edda3419605af6c9323e",
)

genrule(
    name = "rust-code-analysis-x86_64-linux",
    out = "rust-code-analysis-cli",
    cmd = "mkdir -p $TMP/x && tar -xzf $(location :rca-x86_64-linux.tar.gz) -C $TMP/x --strip-components=1 && cp $TMP/x/rust-code-analysis-cli $OUT && chmod +x $OUT",
    executable = True,
)

genrule(
    name = "rust-code-analysis-aarch64-linux",
    out = "rust-code-analysis-cli",
    cmd = "mkdir -p $TMP/x && tar -xzf $(location :rca-aarch64-linux.tar.gz) -C $TMP/x --strip-components=1 && cp $TMP/x/rust-code-analysis-cli $OUT && chmod +x $OUT",
    executable = True,
)

command_alias(
    name = "rust-code-analysis",
    exe = select({
        "prelude//cpu/constraints:x86_64": ":rust-code-analysis-x86_64-linux",
        "prelude//cpu/constraints:arm64": ":rust-code-analysis-aarch64-linux",
    }),
    visibility = ["PUBLIC"],
)
```

- [ ] **Step 2: Verify**

Run: `buck2 run -v0 //tools:rust-code-analysis -- --help`
Expected: usage text including `-m, --metrics` and `-O, --output-format`.

- [ ] **Step 3: Commit**

```bash
git add tools/BUCK
git commit --no-verify -m "build(tools): vendor rust-code-analysis-cli as //tools:rust-code-analysis"
```

---

## Task 3: Vendor lucidshark-duplo

**Files:**
- Modify: `tools/BUCK`

- [ ] **Step 1: Append the lucidshark-duplo block to `tools/BUCK`**

Note the asset naming (`-linux-x86_64` / `-linux-aarch64`) and that the tarball has **no wrapper dir** (binary at root → no `--strip-components`).

```python
# lucidshark-duplo (https://github.com/toniantunovi/lucidshark-duplo) — duplicate-code
# detector; drives the //loom-duplication routine. `.tar.gz` with the `lucidshark-duplo`
# binary at the archive root (no wrapper dir). tar.gz ⇒ no `uses_xz` label. To bump:
# replace DUPLO_VERSION, refresh each SHA256.
#
# `buck2 run //tools:lucidshark-duplo -- <file-list> --json -m 20`.

DUPLO_VERSION = "v0.2.0"

_DUPLO_URL = "https://github.com/toniantunovi/lucidshark-duplo/releases/download/{version}/lucidshark-duplo-linux-{arch}.tar.gz"

http_file(
    name = "duplo-x86_64-linux.tar.gz",
    urls = [_DUPLO_URL.format(version = DUPLO_VERSION, arch = "x86_64")],
    sha256 = "f7aecf9669eb5a1eeac3d51a9e271b9c80330e0c8f6ed7c6f00a5a04437376fc",
)

http_file(
    name = "duplo-aarch64-linux.tar.gz",
    urls = [_DUPLO_URL.format(version = DUPLO_VERSION, arch = "aarch64")],
    sha256 = "1e509820936f85323c1794629f31707dcea4a7305556a126b059bec968d14d49",
)

genrule(
    name = "lucidshark-duplo-x86_64-linux",
    out = "lucidshark-duplo",
    cmd = "mkdir -p $TMP/x && tar -xzf $(location :duplo-x86_64-linux.tar.gz) -C $TMP/x && cp $TMP/x/lucidshark-duplo $OUT && chmod +x $OUT",
    executable = True,
)

genrule(
    name = "lucidshark-duplo-aarch64-linux",
    out = "lucidshark-duplo",
    cmd = "mkdir -p $TMP/x && tar -xzf $(location :duplo-aarch64-linux.tar.gz) -C $TMP/x && cp $TMP/x/lucidshark-duplo $OUT && chmod +x $OUT",
    executable = True,
)

command_alias(
    name = "lucidshark-duplo",
    exe = select({
        "prelude//cpu/constraints:x86_64": ":lucidshark-duplo-x86_64-linux",
        "prelude//cpu/constraints:arm64": ":lucidshark-duplo-aarch64-linux",
    }),
    visibility = ["PUBLIC"],
)
```

- [ ] **Step 2: Verify**

Run: `buck2 run -v0 //tools:lucidshark-duplo -- --help`
Expected: usage text including `--json`, `--baseline`, `--git`.

- [ ] **Step 3: Commit**

```bash
git add tools/BUCK
git commit --no-verify -m "build(tools): vendor lucidshark-duplo as //tools:lucidshark-duplo"
```

---

## Task 4: Dev-shell PATH + CLAUDE.md docs

**Files:**
- Modify: `tools/env.sh` (the `TOOLS` associative array, ~line 41). `tools/loom-refresh` is NOT touched — it only manages first-party `//src` binaries.
- Modify: `CLAUDE.md` ("Dev tools" section)

- [ ] **Step 1: Add the three tools to the `TOOLS` array in `tools/env.sh`**

The array (currently lines 41-47) maps a binary name to its concrete per-arch genrule target (NOT the `command_alias` trampoline — those break when symlinked, per the comment above it). env.sh is x86_64-only by design. Change:

```bash
declare -A TOOLS=(
    [reindeer]="root//tools:reindeer-x86_64-linux"
    [prek]="root//tools:prek-x86_64-linux"
    [btd]="root//tools:btd-bin-x86_64"
    [supertd]="root//tools:supertd-bin-x86_64"
    [rustfmt]="root//tools:rustfmt"
)
```

to:

```bash
declare -A TOOLS=(
    [reindeer]="root//tools:reindeer-x86_64-linux"
    [prek]="root//tools:prek-x86_64-linux"
    [btd]="root//tools:btd-bin-x86_64"
    [supertd]="root//tools:supertd-bin-x86_64"
    [rustfmt]="root//tools:rustfmt"
    [jq]="root//tools:jq-x86_64-linux"
    [rust-code-analysis-cli]="root//tools:rust-code-analysis-x86_64-linux"
    [lucidshark-duplo]="root//tools:lucidshark-duplo-x86_64-linux"
)
```

- [ ] **Step 2: Verify the dev shell exposes them**

Run: `eval "$(./tools/env.sh)" && command -v jq lucidshark-duplo rust-code-analysis-cli`
Expected: three paths under `.loom/bin/`.

- [ ] **Step 3: Document the three tools in `CLAUDE.md`**

Add three bullets to the "Dev tools" section (mirror the `//tools:btd` / `//tools:prek` bullets' style): what each is, the `command_alias` target, and the bump procedure (update the `*_VERSION` constant + refresh SHA256s). Note that `jq` is vendored specifically so the code-health routines don't depend on host jq.

- [ ] **Step 4: Commit**

```bash
git add tools/env.sh CLAUDE.md
git commit --no-verify -m "docs(tools): expose jq/rust-code-analysis/lucidshark-duplo in dev shell + CLAUDE.md"
```

---

## Task 5: docs/code-health scaffolding

**Files:**
- Create: `docs/code-health/README.md`, `docs/code-health/complexity-waivers.json`, `docs/code-health/duplication-baseline.json`

- [ ] **Step 1: Create the seed waiver file (empty allowlist)**

Create `docs/code-health/complexity-waivers.json`:

```json
[]
```

- [ ] **Step 2: Create the seed duplication baseline (empty allowlist)**

Create `docs/code-health/duplication-baseline.json`:

```json
{"duplicates":[]}
```

- [ ] **Step 3: Create `docs/code-health/README.md`**

```markdown
# Code-health registers

Two on-demand routines survey first-party Rust for technical debt and render
tracked, diff-reviewable registers here. Both land changes as a PR against `main`
that auto-merges on green CI (like `docs/stpa/STPA.md`).

| Register | Skill | Tool | Accepted-debt allowlist |
|---|---|---|---|
| `complexity.md` | `/loom-complexity` | `rust-code-analysis-cli` | `complexity-waivers.json` |
| `duplication.md` | `/loom-duplication` | `lucidshark-duplo` | `duplication-baseline.json` |

## Running

- Full census + PR-on-green: invoke `/loom-complexity` or `/loom-duplication`.
- Fast, no-commit check of just your branch's changes: invoke either with the
  `diff` argument (prints to the terminal).

## Waiving accepted debt

- **Complexity:** add `{"file": "...", "function": "...", "reason": "..."}` to
  `complexity-waivers.json`. Waived functions move to the register's
  "Accepted (waived)" section instead of the actionable table.
- **Duplication:** refresh the baseline with
  `buck2 run //tools:lucidshark-duplo -- <(git ls-files 'src/**/*.rs') --json -m 20 --save-baseline docs/code-health/duplication-baseline.json`
  and commit it. Baselined pairs are suppressed from the actionable table.

## Thresholds (tunable in each skill's `render.jq`)

- Complexity: cyclomatic > 15, cognitive > 15, MI (Visual-Studio scale) < 20, SLOC > 100.
- Duplication: minimum duplicate block 20 lines.

The mechanical census tables are a pure function of tool output (deterministic,
small diffs); a short agent-written "Notable changes this run" preamble sits
above each, rewritten only when the census changes.
```

- [ ] **Step 4: Commit**

```bash
git add docs/code-health/
git commit --no-verify -m "docs(code-health): scaffold registers dir, README, empty allowlists"
```

---

## Task 6: loom-complexity renderer + golden test

**Files:**
- Create: `.claude/skills/loom-complexity/render.jq`
- Create: `.claude/skills/loom-complexity/tests/fixture.json`, `tests/waivers.json`, `tests/golden.md`, `tests/run.sh`

- [ ] **Step 1: Write the golden test fixture and expected output (test first)**

Create `.claude/skills/loom-complexity/tests/fixture.json` (a stand-in for two slurped per-file `rust-code-analysis` JSONs — `simple` is under threshold, `gnarly` is over, `waived_big` is over but waived):

```json
[
  {"name":"src/a.rs","kind":"unit","spaces":[
    {"name":"simple","kind":"function","start_line":10,"metrics":{"cyclomatic":{"sum":3.0},"cognitive":{"sum":1.0},"mi":{"mi_visual_studio":75.0},"loc":{"sloc":12.0}}},
    {"name":"gnarly","kind":"function","start_line":40,"metrics":{"cyclomatic":{"sum":23.0},"cognitive":{"sum":31.0},"mi":{"mi_visual_studio":14.2},"loc":{"sloc":180.0}}}
  ]},
  {"name":"src/b.rs","kind":"unit","spaces":[
    {"name":"waived_big","kind":"function","start_line":5,"metrics":{"cyclomatic":{"sum":40.0},"cognitive":{"sum":50.0},"mi":{"mi_visual_studio":5.0},"loc":{"sloc":300.0}}}
  ]}
]
```

Create `.claude/skills/loom-complexity/tests/waivers.json`:

```json
[ {"file":"src/b.rs","function":"waived_big","reason":"generated parser table; refactor tracked in #123"} ]
```

Create `.claude/skills/loom-complexity/tests/golden.md` (the exact validated output — byte-for-byte):

```markdown
<!-- census:begin -->
_Generated by `/loom-complexity` — do not hand-edit below. Thresholds: cyclomatic > 15, cognitive > 15, MI(VS) < 20, SLOC > 100._

## Hotspots (1)

| File | Function | Cyclomatic | Cognitive | MI | SLOC |
|---|---|---|---|---|---|
| `src/a.rs` | `gnarly` | 23 | 31 | 14.2 | 180 |

<details>
<summary><b>Accepted (waived)</b> — 1</summary>

| File | Function | Reason |
|---|---|---|
| `src/b.rs` | `waived_big` | generated parser table; refactor tracked in #123 |
</details>
<!-- census:end -->
```

- [ ] **Step 2: Write the golden test runner**

Create `.claude/skills/loom-complexity/tests/run.sh`:

```bash
#!/usr/bin/env bash
# Golden test for the complexity census renderer. Run from repo root:
#   bash .claude/skills/loom-complexity/tests/run.sh
set -euo pipefail
D="$(cd "$(dirname "$0")" && pwd)"
# All paths are absolute, so buck2 run's cwd does not matter. -v0 keeps buck2's
# progress chatter off stderr; jq's rendered text is the only stdout.
GOT="$(LC_ALL=C buck2 run -v0 //tools:jq -- -rf "$D/../render.jq" --arg root "/workspace/" --slurpfile waivers "$D/waivers.json" "$D/fixture.json")"
if diff -u "$D/golden.md" <(printf '%s\n' "$GOT"); then
  echo "PASS: complexity renderer matches golden"
else
  echo "FAIL: complexity renderer drifted from golden"; exit 1
fi
```

- [ ] **Step 3: Run the test to verify it FAILS (renderer not written yet)**

Run: `bash .claude/skills/loom-complexity/tests/run.sh`
Expected: FAIL — `render.jq` does not exist (jq errors "Could not open … render.jq").

- [ ] **Step 4: Write the renderer `render.jq`**

Create `.claude/skills/loom-complexity/render.jq` (validated during planning):

```jq
def t_cyclomatic: 15;
def t_cognitive:  15;
def t_mi:         20;
def t_sloc:       100;
def esc: (. // "") | tostring | gsub("\\|"; "\\|") | gsub("\n"; " ");
def num: (. // 0) | (.*100|round)/100;
( [ .[] | (.name | ltrimstr($root)) as $file
    | [ .. | objects | select(.kind=="function")
        | { file:$file, function:(.name // "<anon>"), line:(.start_line // 0),
            cc:(.metrics.cyclomatic.sum // 0), cog:(.metrics.cognitive.sum // 0),
            mi:(.metrics.mi.mi_visual_studio // 100), sloc:(.metrics.loc.sloc // 0) } ]
    | .[] ] ) as $fns
| ( ($waivers[0] // []) | map(.file + " " + .function) | unique ) as $waived
| ( $fns | map(select(((.file + " " + .function) | IN($waived[])) | not))
         | map(select(.cc > t_cyclomatic or .cog > t_cognitive or .mi < t_mi or .sloc > t_sloc))
         | sort_by(.file, .line) ) as $hot
| "<!-- census:begin -->\n"
+ "_Generated by `/loom-complexity` — do not hand-edit below. Thresholds: cyclomatic > \(t_cyclomatic), cognitive > \(t_cognitive), MI(VS) < \(t_mi), SLOC > \(t_sloc)._\n\n"
+ "## Hotspots (\($hot|length))\n\n"
+ ( if ($hot|length)==0 then "_None over threshold._\n"
    else "| File | Function | Cyclomatic | Cognitive | MI | SLOC |\n|---|---|---|---|---|---|\n"
       + ( $hot | map("| `\(.file|esc)` | `\(.function|esc)` | \(.cc|num) | \(.cog|num) | \(.mi|num) | \(.sloc|num) |") | join("\n") ) + "\n" end )
+ "\n"
+ ( if (($waivers[0]//[])|length) > 0
    then "<details>\n<summary><b>Accepted (waived)</b> — \(($waivers[0])|length)</summary>\n\n| File | Function | Reason |\n|---|---|---|\n"
       + ( ($waivers[0]//[]) | sort_by(.file, .function) | map("| `\(.file|esc)` | `\(.function|esc)` | \(.reason|esc) |") | join("\n") ) + "\n</details>\n"
    else "" end )
+ "<!-- census:end -->"
```

- [ ] **Step 5: Run the test to verify it PASSES**

Run: `bash .claude/skills/loom-complexity/tests/run.sh`
Expected: `PASS: complexity renderer matches golden`

- [ ] **Step 6: Verify determinism (same input → identical bytes twice)**

Run: `bash .claude/skills/loom-complexity/tests/run.sh && bash .claude/skills/loom-complexity/tests/run.sh`
Expected: PASS both times.

- [ ] **Step 7: Commit**

```bash
git add .claude/skills/loom-complexity/render.jq .claude/skills/loom-complexity/tests/
git commit --no-verify -m "feat(loom-complexity): deterministic census renderer + golden test"
```

---

## Task 7: loom-complexity SKILL.md (orchestration + landing) + first run

**Files:**
- Create: `.claude/skills/loom-complexity/SKILL.md`

- [ ] **Step 1: Write the skill frontmatter + overview**

Create `.claude/skills/loom-complexity/SKILL.md` starting with:

```markdown
---
name: loom-complexity
description: Generate or refresh the code-complexity register at docs/code-health/complexity.md using the hermetic rust-code-analysis-cli, landing any change as a PR against main that merges on green CI. Use when asked to refresh the complexity register, audit complexity hotspots, run the complexity routine, or on a schedule. The census table is deterministic (tool metrics → jq render); a bounded agent preamble notes notable changes. Pass `diff` to analyze only the current branch's changed files and print to the terminal without committing.
---

Refresh the complexity register at `docs/code-health/complexity.md` and land any
change as a PR against `main` that you merge once CI is green. The census table is
a pure function of `rust-code-analysis-cli` metrics rendered by `render.jq` — do
NOT hand-edit the region between the `census` markers. Modes: `full` (default —
whole `src/` tree, render + PR) and `diff` (changed `.rs` only, print, no commit).
```

- [ ] **Step 2: Add BLOCK A (run tool + render census + change gate)**

Append to `SKILL.md`. This runs the tool, renders the census region with the hermetic jq, and decides change vs nochange by comparing ONLY the marker-delimited census region (so a commit-SHA change in the preamble can never cause churn):

````markdown
## BLOCK A — run + render + detect change (run verbatim)

```bash
set -euo pipefail
SKILL=.claude/skills/loom-complexity
REG=docs/code-health/complexity.md
WAIVERS=docs/code-health/complexity-waivers.json
MODE="${1:-full}"
OUT="$(mktemp -d)"
ROOT="$PWD/"

rca() { buck2 run -v0 //tools:rust-code-analysis -- "$@"; }
jqh() { buck2 run -v0 //tools:jq -- "$@"; }

if [ "$MODE" = diff ]; then
  PATHS="$( { git diff --name-only "$(git merge-base HEAD main)"...HEAD -- 'src/**/*.rs'; git diff --name-only -- 'src/**/*.rs'; } | sort -u )"
  [ -n "$PATHS" ] || { echo "no changed .rs files"; exit 0; }
else
  PATHS="src"
fi

# shellcheck disable=SC2086
rca -m -O json -p $PATHS -o "$OUT"

# Render the census region from every per-file JSON (slurp with -s).
FILES="$(find "$OUT" -name '*.json')"
# shellcheck disable=SC2086
jqh -rf "$ROOT$SKILL/render.jq" --arg root "$ROOT" --slurpfile waivers "$ROOT$WAIVERS" -s $FILES > /tmp/cx-census.md

if [ "$MODE" = diff ]; then
  cat /tmp/cx-census.md
  echo "COMPLEXITY_RESULT=diff-mode"
  exit 0
fi

# Compare ONLY the census region against the committed register.
extract() { sed -n '/<!-- census:begin -->/,/<!-- census:end -->/p' "$1" 2>/dev/null || true; }
if [ -f "$REG" ] && diff -q <(extract "$REG") /tmp/cx-census.md >/dev/null; then
  echo "COMPLEXITY_RESULT=nochange"
else
  echo "COMPLEXITY_RESULT=changed"
fi
```
````

- [ ] **Step 3: Add the assembly + preamble instructions (judgment step)**

Append the prose steps the agent follows after BLOCK A:

```markdown
## Steps

1. Run BLOCK A with the requested `MODE` (`full` unless the user said `diff`).
2. If `COMPLEXITY_RESULT=diff-mode`: the table is already printed; summarize the
   top 3 hotspots in one line each and STOP (no commit).
3. If `COMPLEXITY_RESULT=nochange`: report "complexity register already current"
   and STOP. Do not open a PR.
4. If `COMPLEXITY_RESULT=changed`: assemble the new register:
   - Read the prior census region (`git show HEAD:docs/code-health/complexity.md`,
     if it exists) and `/tmp/cx-census.md`.
   - Write a **≤10-bullet** "Notable changes this run" preamble naming hotspots
     **added / resolved / worsened** by `file::function` — a summary of the delta
     only; introduce no findings not in the table. First run: one line.
   - Assemble `docs/code-health/complexity.md` as, in order: an H1
     `# Code complexity register`, a one-line `_As of <short-sha>._` (run
     `git rev-parse --short HEAD`), a `<!-- preamble:begin -->` … `<!-- preamble:end -->`
     region holding the bullets, a blank line, then `/tmp/cx-census.md` verbatim.
     End the file with exactly ONE trailing newline and no trailing whitespace.
   - The commit SHA lives ONLY in the preamble region, never in the census region,
     so identical findings always re-detect as `nochange`.
5. Run `buck2 run //tools:prek -- run --all-files` and commit anything the markdown
   hooks change, THEN run BLOCK B.
```

- [ ] **Step 4: Add BLOCK B (land the PR) — adapted from loom-stpa**

Append:

````markdown
## BLOCK B — commit, PR, watch CI, merge on green (run verbatim; only when changed)

```bash
set -euo pipefail
BRANCH=bot/code-health-complexity
git config --get user.email >/dev/null 2>&1 || git config user.email "code-health-bot@users.noreply.github.com"
git config --get user.name  >/dev/null 2>&1 || git config user.name  "code-health-bot"
git switch -C "$BRANCH"
git add docs/code-health/complexity.md
git commit --no-verify -m "$(cat /tmp/cx-title.txt)" -m "$(cat /tmp/cx-body.md)"
git push --no-verify -f -u origin "$BRANCH"
PR_STATE="$(gh pr view "$BRANCH" --json state -q .state 2>/dev/null || echo NONE)"
if [ "$PR_STATE" != "OPEN" ]; then
  gh pr create --base main --head "$BRANCH" --title "$(cat /tmp/cx-title.txt)" --body-file /tmp/cx-body.md
fi
URL="$(gh pr view "$BRANCH" --json url -q .url)"
echo "PR: $URL"
if gh pr checks "$BRANCH" --watch --fail-fast; then
  gh pr merge "$BRANCH" --squash --delete-branch && echo "merged (CI green): $URL"
else
  echo "NOTE: CI not green — PR left open for review: $URL"
fi
```
````

Add a sentence before BLOCK B instructing the agent to write `/tmp/cx-title.txt`
(one line, `docs(code-health): <what changed in complexity>`, ≤72 chars) and
`/tmp/cx-body.md` (2-6 bullets, the same delta as the preamble).

- [ ] **Step 5: Smoke-test the skill in `diff` mode (no commit)**

Copy BLOCK A's bash into a scratch file and run it with the `diff` arg (or invoke `/loom-complexity diff`):

Run: `cp /dev/stdin /tmp/blockA.sh` … paste BLOCK A … then `bash /tmp/blockA.sh diff`
Expected: a hotspot table prints (or `no changed .rs files`); the line `COMPLEXITY_RESULT=diff-mode`.
Then run: `git status --porcelain`
Expected: empty (diff mode commits nothing).

- [ ] **Step 6: First full run — generate the real register**

Invoke `/loom-complexity` (full mode). Expected: `COMPLEXITY_RESULT=changed`, a real `docs/code-health/complexity.md` is assembled, prek markdown hooks pass, and BLOCK B opens (and merges on green) the `bot/code-health-complexity` PR. Confirm the PR URL is reported.

- [ ] **Step 7: Verify determinism — a second full run is a no-op**

Re-invoke `/loom-complexity` on the same commit. Expected: `COMPLEXITY_RESULT=nochange`, no PR opened.

- [ ] **Step 8: Commit the skill**

```bash
git add .claude/skills/loom-complexity/SKILL.md
git commit --no-verify -m "feat(loom-complexity): complexity register routine (run, render, land)"
```

---

## Task 8: loom-duplication renderer + golden test

**Files:**
- Create: `.claude/skills/loom-duplication/render.jq`
- Create: `.claude/skills/loom-duplication/tests/dup.json`, `tests/baseline.json`, `tests/golden.md`, `tests/run.sh`

- [ ] **Step 1: Write the fixture + expected golden (test first)**

Create `.claude/skills/loom-duplication/tests/dup.json` (a stand-in `lucidshark-duplo --json` result with one pair):

```json
{"summary":{"files_analyzed":2,"total_lines":100,"duplicate_blocks":1,"duplicate_lines":20,"duplication_percent":20.0},
 "duplicates":[
   {"line_count":20,"file1":{"path":"src/x.rs","start_line":10,"end_line":30},"file2":{"path":"src/y.rs","start_line":40,"end_line":60},"lines":["a","b"]}
 ]}
```

Create `.claude/skills/loom-duplication/tests/baseline.json`:

```json
{"duplicates":[]}
```

Create `.claude/skills/loom-duplication/tests/golden.md`:

```markdown
<!-- census:begin -->
_Generated by `/loom-duplication` — do not hand-edit below. Minimum block: 20 lines. Pairs recorded in the baseline are suppressed._

## Duplicate pairs (1)

| Lines | Site A | Site B |
|---|---|---|
| 20 | `src/x.rs:10-30` | `src/y.rs:40-60` |

<!-- census:end -->
```

- [ ] **Step 2: Write the test runner**

Create `.claude/skills/loom-duplication/tests/run.sh`:

```bash
#!/usr/bin/env bash
set -euo pipefail
D="$(cd "$(dirname "$0")" && pwd)"
GOT="$(LC_ALL=C buck2 run -v0 //tools:jq -- -rf "$D/../render.jq" --slurpfile baseline "$D/baseline.json" "$D/dup.json")"
if diff -u "$D/golden.md" <(printf '%s\n' "$GOT"); then
  echo "PASS: duplication renderer matches golden"
else
  echo "FAIL: duplication renderer drifted from golden"; exit 1
fi
```

- [ ] **Step 3: Run the test — verify it FAILS (no render.jq yet)**

Run: `bash .claude/skills/loom-duplication/tests/run.sh`
Expected: FAIL — `render.jq` does not exist.

- [ ] **Step 4: Write `render.jq`**

Create `.claude/skills/loom-duplication/render.jq` (validated during planning):

```jq
def esc: (. // "") | tostring | gsub("\\|"; "\\|") | gsub("\n"; " ");
( .duplicates // []
  | sort_by(-.line_count, .file1.path, .file1.start_line, .file2.path, .file2.start_line) ) as $dups
| (($baseline[0].duplicates) // []) as $accepted
| "<!-- census:begin -->\n"
+ "_Generated by `/loom-duplication` — do not hand-edit below. Minimum block: 20 lines. Pairs recorded in the baseline are suppressed._\n\n"
+ "## Duplicate pairs (\($dups|length))\n\n"
+ ( if ($dups|length)==0 then "_None over threshold._\n"
    else "| Lines | Site A | Site B |\n|---|---|---|\n"
       + ( $dups | map("| \(.line_count) | `\(.file1.path|esc):\(.file1.start_line)-\(.file1.end_line)` | `\(.file2.path|esc):\(.file2.start_line)-\(.file2.end_line)` |") | join("\n") ) + "\n" end )
+ "\n"
+ ( if ($accepted|length) > 0
    then "<details>\n<summary><b>Accepted (baselined)</b> — \($accepted|length) pair(s)</summary>\n\nRecorded in `docs/code-health/duplication-baseline.json` and suppressed above.\n</details>\n"
    else "" end )
+ "<!-- census:end -->"
```

- [ ] **Step 5: Run the test — verify it PASSES**

Run: `bash .claude/skills/loom-duplication/tests/run.sh`
Expected: `PASS: duplication renderer matches golden`

- [ ] **Step 6: Commit**

```bash
git add .claude/skills/loom-duplication/render.jq .claude/skills/loom-duplication/tests/
git commit --no-verify -m "feat(loom-duplication): deterministic census renderer + golden test"
```

---

## Task 9: loom-duplication SKILL.md (orchestration + landing) + first run

**Files:**
- Create: `.claude/skills/loom-duplication/SKILL.md`

- [ ] **Step 1: Write frontmatter + overview**

```markdown
---
name: loom-duplication
description: Generate or refresh the duplication register at docs/code-health/duplication.md using the hermetic lucidshark-duplo, landing any change as a PR against main that merges on green CI. Use when asked to refresh the duplication register, find duplicate code / abstraction opportunities, run the duplication routine, or on a schedule. The census table is deterministic (duplo JSON → jq render); accepted pairs in duplication-baseline.json are suppressed. Pass `diff` to analyze only the current branch's changed files and print to the terminal without committing.
---

Refresh the duplication register at `docs/code-health/duplication.md` and land any
change as a PR against `main` merged on green CI. The census is a pure function of
`lucidshark-duplo --json` rendered by `render.jq` — do NOT hand-edit between the
`census` markers. Accepted duplication is recorded in
`docs/code-health/duplication-baseline.json` and suppressed via `--baseline`.
```

- [ ] **Step 2: Add BLOCK A**

````markdown
## BLOCK A — run + render + detect change (run verbatim)

```bash
set -euo pipefail
SKILL=.claude/skills/loom-duplication
REG=docs/code-health/duplication.md
BASELINE=docs/code-health/duplication-baseline.json
MODE="${1:-full}"
ROOT="$PWD/"

duplo() { buck2 run -v0 //tools:lucidshark-duplo -- "$@"; }
jqh()   { buck2 run -v0 //tools:jq -- "$@"; }

if [ "$MODE" = diff ]; then
  duplo --git --changed-only --json -m 20 --baseline "$ROOT$BASELINE" > /tmp/dup.json || echo '{"duplicates":[]}' > /tmp/dup.json
else
  git ls-files 'src/**/*.rs' > /tmp/dup-files.txt
  duplo /tmp/dup-files.txt --json -m 20 --baseline "$ROOT$BASELINE" > /tmp/dup.json
fi

jqh -rf "$ROOT$SKILL/render.jq" --slurpfile baseline "$ROOT$BASELINE" /tmp/dup.json > /tmp/dup-census.md

if [ "$MODE" = diff ]; then
  cat /tmp/dup-census.md
  echo "DUPLICATION_RESULT=diff-mode"
  exit 0
fi

extract() { sed -n '/<!-- census:begin -->/,/<!-- census:end -->/p' "$1" 2>/dev/null || true; }
if [ -f "$REG" ] && diff -q <(extract "$REG") /tmp/dup-census.md >/dev/null; then
  echo "DUPLICATION_RESULT=nochange"
else
  echo "DUPLICATION_RESULT=changed"
fi
```
````

- [ ] **Step 3: Add the Steps prose (mirror Task 7 Step 3, duplication-flavored)**

```markdown
## Steps

1. Run BLOCK A with the requested `MODE` (`full` unless the user said `diff`).
2. `DUPLICATION_RESULT=diff-mode`: table printed; summarize the largest new pair
   and STOP.
3. `DUPLICATION_RESULT=nochange`: report "duplication register already current"
   and STOP.
4. `DUPLICATION_RESULT=changed`: assemble `docs/code-health/duplication.md` —
   H1 `# Code duplication register`, `_As of <short-sha>._`, a
   `<!-- preamble:begin -->`…`<!-- preamble:end -->` region with a ≤10-bullet
   "Notable changes this run" (pairs added/resolved by file, the delta only),
   a blank line, then `/tmp/dup-census.md` verbatim. One trailing newline, no
   trailing whitespace. Commit SHA lives ONLY in the preamble.
5. Write `/tmp/dup-title.txt` (`docs(code-health): <duplication change>`, ≤72 chars)
   and `/tmp/dup-body.md` (2-6 bullets). Run
   `buck2 run //tools:prek -- run --all-files`, commit any hook fixes, then BLOCK B.
```

- [ ] **Step 4: Add BLOCK B (land the PR)**

Append:

````markdown
## BLOCK B — commit, PR, watch CI, merge on green (run verbatim; only when changed)

```bash
set -euo pipefail
BRANCH=bot/code-health-duplication
git config --get user.email >/dev/null 2>&1 || git config user.email "code-health-bot@users.noreply.github.com"
git config --get user.name  >/dev/null 2>&1 || git config user.name  "code-health-bot"
git switch -C "$BRANCH"
git add docs/code-health/duplication.md
git commit --no-verify -m "$(cat /tmp/dup-title.txt)" -m "$(cat /tmp/dup-body.md)"
git push --no-verify -f -u origin "$BRANCH"
PR_STATE="$(gh pr view "$BRANCH" --json state -q .state 2>/dev/null || echo NONE)"
if [ "$PR_STATE" != "OPEN" ]; then
  gh pr create --base main --head "$BRANCH" --title "$(cat /tmp/dup-title.txt)" --body-file /tmp/dup-body.md
fi
URL="$(gh pr view "$BRANCH" --json url -q .url)"
echo "PR: $URL"
if gh pr checks "$BRANCH" --watch --fail-fast; then
  gh pr merge "$BRANCH" --squash --delete-branch && echo "merged (CI green): $URL"
else
  echo "NOTE: CI not green — PR left open for review: $URL"
fi
```
````

- [ ] **Step 5: Smoke-test `diff` mode**

Invoke `/loom-duplication diff`. Expected: a duplicate-pairs table (or empty) prints; `git status --porcelain` shows no changes.

- [ ] **Step 6: First full run**

Invoke `/loom-duplication`. Expected: `DUPLICATION_RESULT=changed`, real `docs/code-health/duplication.md` assembled (loom currently has real duplication, e.g. `catalog.rs`/`iceberg_catalog.rs`), prek passes, `bot/code-health-duplication` PR opens and merges on green.

- [ ] **Step 7: Determinism check — second run is a no-op**

Re-invoke `/loom-duplication`. Expected: `DUPLICATION_RESULT=nochange`, no PR.

- [ ] **Step 8: Commit**

```bash
git add .claude/skills/loom-duplication/SKILL.md
git commit --no-verify -m "feat(loom-duplication): duplication register routine (run, render, land)"
```

---

## Final verification

- [ ] `buck2 run -v0 //tools:jq -- --version`, `… //tools:rust-code-analysis -- --help`, `… //tools:lucidshark-duplo -- --help` all succeed.
- [ ] Both golden tests pass: `bash .claude/skills/loom-complexity/tests/run.sh && bash .claude/skills/loom-duplication/tests/run.sh`.
- [ ] Both registers exist under `docs/code-health/`, pass `buck2 run //tools:prek -- run --all-files`, and a second full run of each yields `*_RESULT=nochange`.
- [ ] `CLAUDE.md` "Dev tools" documents all three tools; `docs/code-health/README.md` documents both routines and the waiver workflow.
- [ ] Open a PR for `feat/code-health-routines` into `main` (the tool/scaffold commits; the register-content PRs are landed separately by the skills themselves).

## Notes for the implementer

- **Tool invocation:** everything calls the tools via `buck2 run -v0 //tools:<t> -- …` (the documented way in CLAUDE.md). All script/data paths passed to jq are absolute, so buck2 run's working directory is irrelevant. If you activate the dev shell (`eval "$(./tools/env.sh)"`) the bare `jq` / `lucidshark-duplo` / `rust-code-analysis-cli` names also work, but the skills deliberately use `buck2 run` so a cron/CI run with no dev shell still works.
- **These are prebuilt binaries, not loom Rust targets** — coverage/clippy/sqlx machinery does not apply to them.
- **First instrumented buck2 build is heavy** does not apply; these are downloads. But the FIRST `buck2 run` of each tool downloads the release asset — allow time/network.
- **`-v0`** keeps buck2's progress chatter off stderr so captured stdout stays clean.
