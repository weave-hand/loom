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
