---
name: loom-docs-organise
description: Bulk-consolidate loom's deferred/planned/defect docs into the three parsable registers at docs/ROADMAP.md, docs/FUTURE.md, docs/ISSUES.md, landing the change as a PR against main that merges on green CI. Use when asked to consolidate/rebuild the documentation registers, reconcile what has been deferred vs shipped, mine the codebase for lost deferred items, or on a schedule. First run also migrates TO_BE_PLANNED.md / the roadmap / ICEBERG_ROADMAP into the registers. Pass `diff` to report drift to the terminal without committing.
---

Consolidate loom's scattered deferred/planned/defect docs into the three registers
and land any change as a PR against `main` that you merge once CI is green. The
registers use the tagged-item grammar in
`docs/superpowers/specs/2026-06-20-docs-registers-consolidation-design.md` — read
it first. Markdown is the source of truth; `tools/docs.sh validate` is the gate.

Registers and their commitment level:
- `docs/ROADMAP.md` — committed/sequenced work (`status: planned|in-progress|done`)
- `docs/FUTURE.md` — deliberately-deferred ideas (`status: deferred|promoted|dropped`)
- `docs/ISSUES.md` — known defects/gaps in shipped code (`status: open|fixed|wontfix`)

Item grammar (one markdown list item, prose indented below):
`- [ ] **Title** ` + "`" + `{#id area:<a> status:<s> from:<f> pr:<p> spec:<sp>}` + "`"
`#id` prefixes `road-`/`fut-`/`iss-`; `area:` ∈
`lineage catalog ontology acl ingest query transform iceberg ui ux test quality devx build deploy cross-cutting`;
`pr:` is `-` or `#N[,#N...]`; `spec:` is `-` or spec/plan slugs; `[[id]]` cross-links.

## Steps

1. **Mine (agentic).** Use the `superpowers:dispatching-parallel-agents` skill to
   fan out read-only agents, each returning structured candidate items
   `{title, area, status, from, prs, specs, prose}`. Cover, one agent per source:
   - `docs/FUTURE.md` and `docs/TO_BE_PLANNED.md` (if present).
   - The roadmap (`docs/ROADMAP.md` if it exists, else
     `docs/superpowers/specs/2026-06-06-loom-roadmap.md`).
   - `docs/spike/ICEBERG_ROADMAP.md` (done/deferred items → `area:iceberg`).
   - `docs/superpowers/specs/` + `plans/` — "deferred" / "out of scope" /
     "not built" / "future work" sections.
   - Code markers: `grep -rnE "TODO|FIXME|unimplemented!|todo!" src` and
     known-gap panics.
   - PRs: `gh pr list --state all --limit 200 --json number,title,state,mergedAt`.
2. **Dedupe / merge.** Collapse the same item surfaced from multiple sources.
   Assign stable `#id`s — if a register already exists, reuse the existing id for
   an item (match on title+area) rather than minting a new one.
3. **Classify** each item into ROADMAP / FUTURE / ISSUES by commitment level.
4. **Reconcile.** Run `bash tools/docs.sh shipped-open` (and `--stale`); for each
   candidate, confirm via `gh pr view <n> --json state` / reading the code whether
   the work shipped. If shipped, set the item `[x]` with a terminal status and the
   `pr:`. Do NOT auto-close without confirming.
5. **Render** the three files in the grammar, grouped by `## <area>`, preserving
   the original prose. On the FIRST run also perform the migration:
   - Move `docs/TO_BE_PLANNED.md` items into ROADMAP/FUTURE, then `git rm` it.
   - Move `docs/superpowers/specs/2026-06-06-loom-roadmap.md` content into
     `docs/ROADMAP.md`; `git mv` the original to `…-loom-roadmap.md.old`.
   - Move `docs/spike/ICEBERG_ROADMAP.md` tracked items into the registers;
     leave only its "what it is" narrative as prose.
   - Restructure `docs/FUTURE.md` prose into tagged items in place.
   Each register starts with an H1, a `_As of <short-sha>._` line
   (`git rev-parse --short HEAD`), then `## <area>` sections. End each file with
   exactly ONE trailing newline and no trailing whitespace
   (`.claude/rules/markdown-lint.md`).
6. **Validate**: `bash tools/docs.sh validate` — fix every reported error before
   continuing.
7. If invoked with `diff`: print a summary of what changed vs the committed
   registers (added/closed/moved items) and STOP — no commit.
8. Run `buck2 run //tools:prek -- run --all-files` (it may fix EOF/whitespace in
   the registers; leave those fixes staged). THEN run BLOCK A.

Before BLOCK A, write `/tmp/docs-title.txt` (one line,
`docs(registers): <what changed>`, ≤72 chars) and `/tmp/docs-body.md` (2–6
bullets summarising added/closed/moved items).

## BLOCK A — commit, PR, watch CI, merge on green (run verbatim)

```bash
set -euo pipefail
BRANCH=bot/docs-registers
git config --get user.email >/dev/null 2>&1 || git config user.email "docs-bot@users.noreply.github.com"
git config --get user.name  >/dev/null 2>&1 || git config user.name  "docs-bot"
git switch -C "$BRANCH"
git add docs/ROADMAP.md docs/FUTURE.md docs/ISSUES.md docs/TO_BE_PLANNED.md \
        docs/spike/ICEBERG_ROADMAP.md docs/superpowers/specs/2026-06-06-loom-roadmap.md* 2>/dev/null || true
git add -A docs
# --no-verify: skip loom's local commit-msg/pre-push hooks (buck2-build/test would
# stall the routine); conventional style is carried by the PR title -> squash commit.
git commit --no-verify -m "$(cat /tmp/docs-title.txt)" -m "$(cat /tmp/docs-body.md)"
git push --no-verify -f -u origin "$BRANCH"
PR_STATE="$(gh pr view "$BRANCH" --json state -q .state 2>/dev/null || echo NONE)"
if [ "$PR_STATE" != "OPEN" ]; then
  gh pr create --base main --head "$BRANCH" --title "$(cat /tmp/docs-title.txt)" --body-file /tmp/docs-body.md
fi
URL="$(gh pr view "$BRANCH" --json url -q .url)"
echo "PR: $URL"
if gh pr checks "$BRANCH" --watch --fail-fast; then
  gh pr merge "$BRANCH" --squash --delete-branch && echo "merged (CI green): $URL"
else
  echo "NOTE: CI not green — PR left open for review: $URL"
fi
```
