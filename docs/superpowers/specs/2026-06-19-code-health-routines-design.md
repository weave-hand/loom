# Code-health analysis routines — design

**Date:** 2026-06-19
**Status:** approved (brainstorming), ready for planning
**Scope of this spec:** the hermetic tooling + the two analysis skills. The
remediation process that *acts on* a register is sketched in
[§7](#7-remediation-process--out-of-scope-follow-up-spec) but specified
separately.

## 1. Goal

Stand up two on-demand routines that survey loom's first-party Rust for
technical debt and surface it as **tracked, diff-reviewable registers**:

- **`loom-complexity`** — high-complexity hotspots (cyclomatic / cognitive /
  maintainability index) via Mozilla's `rust-code-analysis-cli`.
- **`loom-duplication`** — duplicated code / missed-abstraction clusters via
  `lucidshark-duplo`.

Each routine renders a committed Markdown register and lands any change as a PR
against `main` that auto-merges on green CI — mirroring the existing
[`loom-stpa`](../../../.claude/skills/loom-stpa/SKILL.md) skill. The interpretive
work of *fixing* findings lives in a separate remediation process (§7).

## 2. Decisions (from brainstorming)

| Decision | Choice |
|---|---|
| Deliverable | Tracked register, PR-on-green (loom-stpa shape). Acting on it is a separate process. |
| Scope per run | **Both** full-census and diff-scoped modes. |
| Packaging | **Two** independent skills (complexity, duplication). |
| Cadence | On-demand now; built to be schedulable later (no cron in this spec). |
| Register generation | **Hybrid** — deterministic mechanical census table + a small agent-written "notable changes this run" preamble. |
| Accepted debt | A **committed waiver/allowlist** per skill suppresses team-accepted findings; waived items render in a separate "Accepted" section, not deleted. |

## 3. Why these tools, and why `tools/BUCK` (not reindeer)

Both tools ship prebuilt `.tar.gz` Linux binaries for `x86_64` and `aarch64` —
the two arches loom wires for remote execution — identical in shape to the
existing `btd` / `prek` / `supertd` dev tools. They are **standalone CLIs, not
Rust library crates**, so they are wired by hand in `tools/BUCK` via the
`http_file` + untar-`genrule` + `command_alias` pattern. Reindeer is explicitly
*not* used (it only buckifies third-party library crates from the workspace
`Cargo.toml`). Because both are `.tar.gz` (not bare `.zst`), the genrules need
**no** `uses_xz` label and build fine on RE.

Release assets (confirmed via `gh release view`):

- `weave-hand/rust-code-analysis` `v2026.06.19.00`:
  `rust-code-analysis-v{ver}-x86_64-unknown-linux-gnu.tar.gz`,
  `…-aarch64-unknown-linux-gnu.tar.gz`. Binary inside: `rust-code-analysis-cli`.
- `toniantunovi/lucidshark-duplo` `v0.2.0`:
  `lucidshark-duplo-linux-x86_64.tar.gz`, `lucidshark-duplo-linux-aarch64.tar.gz`.
  Binary inside: `lucidshark-duplo`.

## 4. Component 1 — hermetic tools in `tools/BUCK`

Mirror the `btd`/`supertd` block (version constant → per-arch `http_file` with
pinned `sha256` → untar `genrule` per arch → `command_alias` selecting on
`prelude//cpu/constraints`). Two new public targets:

- `//tools:rust-code-analysis` → runs `rust-code-analysis-cli`.
- `//tools:lucidshark-duplo` → runs `lucidshark-duplo`.

Implementation notes:

- `sha256` for each asset comes from the release `.sha256` sidecars; fetch at
  implementation time. The archive's internal binary path is verified with a
  `buck2 run //tools:<tool> -- --help` smoke run (adjust the `genrule` `cmd`
  `cp` path if the binary is nested under a directory inside the tarball).
- Add both to the dev shell: expose them in `tools/env.sh` / `tools/loom-refresh`
  (symlinks under `.loom/bin`) the same way `reindeer`/`prek`/`btd` are.
- **Document both in `CLAUDE.md` → "Dev tools"** — every dev tool is catalogued
  there; keep the convention (one bullet each: what it is, how to bump, the
  `command_alias` target).
- Out of CI scope by construction: `//tools/...` is not built by CI (only
  `//src/...` is), matching how the other dev tools are treated.

Version-bump procedure (documented in the CLAUDE.md bullet): update the
`*_VERSION` constant and refresh each `sha256` from the new release's sidecars.

## 5. Component 2 — `loom-complexity` skill

Location: `.claude/skills/loom-complexity/SKILL.md`. Two-block structure copied
from `loom-stpa`: **BLOCK A** renders deterministically, **BLOCK B** lands the
PR. Register: `docs/code-health/complexity.md`. Waiver file:
`docs/code-health/complexity-waivers.json`.

### 5.1 Tool invocation

```
buck2 run //tools:rust-code-analysis -- -m -O json -p <paths> -o $TMPDIR
```

`rust-code-analysis-cli` emits one JSON file per analyzed source file, each a
tree of code "spaces" (functions / impls) carrying metrics: `cyclomatic`,
`cognitive`, `halstead`, `mi` (maintainability index), `nargs`, `nexits`,
`loc`/`sloc`. (Exact flag spellings verified against `--help` during
implementation; the binary is the Mozilla CLI.)

### 5.2 Normalize + render (BLOCK A, deterministic jq)

1. Flatten every file's space tree into `{file, function, start_line,
   cyclomatic, cognitive, mi, loc}` rows (recurse nested spaces).
2. Drop rows that match a waiver entry (`{file, function}`) — collect those
   separately for the "Accepted" section.
3. Keep rows exceeding **any** threshold (tunable constants at the top of the
   jq): `cyclomatic > 15`, `cognitive > 15`, `mi < 60`, `loc > 100`.
4. Sort by `(file, start_line)` — stable semantic order, so diffs are localized.
5. Render `docs/code-health/complexity.md`:
   - Small **agent preamble** ("Notable changes this run") — see §5.4.
   - **Actionable table**: `File · Function · Cyclomatic · Cognitive · MI · LOC`.
   - **Accepted (waived)** `<details>` section listing waived rows + reason.
   - Thresholds legend.
6. **Change gate operates on the deterministic body only.** The mechanical
   table + accepted section live between fixed delimiter comments (e.g.
   `<!-- census:begin -->` … `<!-- census:end -->`); the agent preamble sits
   above, in its own delimited region. BLOCK A renders the deterministic body,
   compares **only that region** against the committed file's same region, and
   prints `COMPLEXITY_RESULT=changed|nochange` (like loom-stpa's `STPA_RESULT`).
   The preamble is (re)written and the file reassembled **only** when the census
   changed — so unchanged source ⇒ byte-identical file ⇒ `nochange`, and the
   non-deterministic prose never causes a spurious diff.

Determinism: pinned tool version + pinned source ⇒ stable numbers; jq sorting +
fixed thresholds ⇒ stable bytes in the census region. End the file with exactly one trailing newline
and no trailing whitespace (the `markdown-lint` rule — generated docs trip
`end-of-file-fixer`/`trim trailing whitespace` in the `lint` CI job otherwise).

### 5.3 Modes

- **full** (default): `-p src/` over all first-party crates → render register →
  BLOCK B (PR-on-green).
- **diff-scoped**: changed `.rs` files from `git diff --name-only` (working tree
  and/or `main...HEAD`) passed as `-p`; print the resulting table to the
  terminal; **do not** commit or open a PR. Fast PR-time check.

### 5.4 Hybrid preamble (the one non-deterministic part, bounded)

After rendering the mechanical table, the agent reads the previously committed
register (`git show HEAD:docs/code-health/complexity.md`) and the new candidate,
and writes a **≤10-bullet** "Notable changes this run" section naming hotspots
**added / resolved / regressed** (by `file::function`). It is a summary of the
deterministic delta — it introduces no findings of its own. On the first run it
is a one-line "initial census."

### 5.5 Land (BLOCK B)

Verbatim from loom-stpa, with a distinct stable branch `bot/code-health-complexity`
and a `docs(code-health):`-prefixed Conventional-Commits title. Commit/push
`--no-verify`; create-or-reuse the open PR; `gh pr checks --watch --fail-fast`;
`gh pr merge --squash --delete-branch` on green.

## 6. Component 3 — `loom-duplication` skill

Location: `.claude/skills/loom-duplication/SKILL.md`. Same two-block structure.
Register: `docs/code-health/duplication.md`. Baseline/waiver:
`docs/code-health/duplication-baseline.json`.

### 6.1 Tool invocation

Scope to first-party Rust explicitly (avoids `--git` pulling in non-`src`
tracked files):

```
git ls-files 'src/**/*.rs' > $TMPDIR/files.txt
buck2 run //tools:lucidshark-duplo -- $TMPDIR/files.txt --json -m 20
```

`-m 20` = minimum 20-line duplicate blocks. JSON output is a set of duplication
clusters, each listing its sites (`file`, start/end lines) and block size.

### 6.2 Baseline = accepted-duplication allowlist

`lucidshark-duplo` has native baseline support designed for exactly this:

- The committed `docs/code-health/duplication-baseline.json` is the team's
  **accepted-duplication allowlist**, produced/refreshed with `--save-baseline`.
- Every run passes `--baseline docs/code-health/duplication-baseline.json` so
  accepted clusters are suppressed from the actionable set.
- Adding a waiver = regenerate the baseline (a documented one-liner in the
  skill) and commit it alongside.
- The register renders accepted clusters in a separate "Accepted (baselined)"
  `<details>` section for auditability (count + a representative site), so the
  allowlist's size is visible and reviewable.

### 6.3 Normalize + render + modes + land

- Normalize JSON → clusters of `≥2` sites; sort by `(block_size desc,
  first_file, first_line)`; render `File-cluster table`: `Size (lines) · Sites ·
  Locations`. Same hybrid preamble (§5.4) and `DUPLICATION_RESULT` gate.
- **full**: scan all of `src/**/*.rs` → register → PR-on-green
  (`bot/code-health-duplication`).
- **diff-scoped**: `--changed-only --baseline …` → report only *new*
  duplication vs the accepted baseline; print to terminal, no commit.
- Land via BLOCK B, distinct branch, `docs(code-health):` title.

## 7. Remediation process — out of scope (follow-up spec)

A future `loom-codehealth-fix` skill reads a register, takes the top (or a
named) item, and performs the refactor under TDD on a per-item branch,
PR-on-green. All interpretive judgment ("is this genuinely worth fixing, and
how") lives here, not in the registers. Specified separately so this plan stays
focused on tooling + census.

## 8. Cross-cutting concerns

- **`docs/code-health/` directory** with a short `README.md`: what each register
  is, the threshold/baseline knobs, how to refresh (`/loom-complexity`,
  `/loom-duplication`), and how to waive an item in each tool.
- **No CI gate.** These are tracked registers, not build gates. Tool targets
  stay under `//tools` (out of CI's `//src/...` scope).
- **Schedulable later** with zero code change: the deterministic core + auto-PR
  means a weekly cron (`/schedule`) can be attached to either skill when wanted.
- **Markdown lint:** generated registers must end with exactly one trailing
  newline and carry no trailing whitespace, or the all-files `lint` job fails
  (see `.claude/rules/markdown-lint.md`). The jq renderers terminate with a
  single `"\n"` like loom-stpa's.

## 9. Verification

- `buck2 run //tools:rust-code-analysis -- --help` and
  `buck2 run //tools:lucidshark-duplo -- --help` succeed (binary path inside
  each tarball is correct) on x86_64; both targets resolve under the `aarch64`
  `command_alias` select.
- End-to-end dry run of each skill in **diff-scoped** mode (terminal output, no
  PR) confirms the tool → jq → table pipeline.
- A full run produces a `docs/code-health/*.md` that passes
  `buck2 run //tools:prek -- run --all-files` (markdown hooks) before push.
- Re-running with unchanged source yields `*_RESULT=nochange` (determinism of
  the mechanical table; the preamble is only written when the table changes).

## 10. Implementation order

1. Wire both hermetic tools into `tools/BUCK` (+ dev-shell + CLAUDE.md docs);
   verify with `--help`.
2. `docs/code-health/` dir + README + empty/seed waiver+baseline files.
3. `loom-complexity` skill (BLOCK A renderer, modes, BLOCK B); first full run.
4. `loom-duplication` skill (same); first full run.
