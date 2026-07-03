# Docs Clearout — System Capabilities Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Consolidate 234 closed register items into `docs/system-capabilities/<subsystem>.md` prose docs, slim ROADMAP/ISSUES/FUTURE to open items, and delete ~320 orphaned specs/plans, landing as one reviewable PR.

**Architecture:** Mechanical input-gathering groups closed items by subsystem; parallel drafting agents synthesize one capability doc each against a fixed content contract; the orchestrator assembles, slims registers (with a user checkpoint on the FUTURE stale-cull), recomputes the keep-list, deletes, and gates on `docs.sh validate` + reference grep + prek.

**Tech Stack:** bash/grep over the register grammar, Agent fan-out, `tools/docs.sh`, prek.

**Spec:** `docs/superpowers/specs/2026-07-03-docs-clearout-system-capabilities-design.md`

## Global Constraints

- Branch: `docs/system-capabilities-clearout`; one PR; NOT auto-merged.
- Capability docs follow the content contract verbatim (H1 `# <Subsystem> capabilities`, scope paragraph, `_As of 4861433b._`, themed prose with inline `(#N)` PR refs, closing `## Known gaps` linking open `#id`s, no register grammar).
- No `src/` edits. Out of scope: `docs/spike*/`, `docs/stpa/`, `docs/build-execution.md`, `docs/error-handling-debt.md`, `docs/grimoire-kg-agenda.md`.
- Every culled FUTURE item is quoted verbatim in the PR body.
- Commits use `--no-verify` (docs-only; conventional title carried by PR squash) but prek must pass `--all-files` before the final push.

---

### Task 1: Per-subsystem input packs

**Files:**
- Create: `/home/loom/.claude/jobs/afa8c40f/tmp/packs/<subsystem>.md` (9 packs, temp — not committed)

**Interfaces:**
- Produces: one pack file per subsystem containing (a) every closed item (full register entry text) assigned to it, (b) the list of spec paths those items reference, (c) the subsystem's open item ids for the Known-gaps section.

- [ ] **Step 1: Extract closed and open items with areas**

```bash
T=/home/loom/.claude/jobs/afa8c40f/tmp/packs; mkdir -p $T
# Each register entry is one `- [x|space] ...` line (grammar guarantees one line per item).
grep -h '^- \[x\]' docs/ROADMAP.md docs/ISSUES.md docs/FUTURE.md > $T/closed-all.txt
grep -h '^- \[ \]' docs/ROADMAP.md docs/ISSUES.md docs/FUTURE.md > $T/open-all.txt
wc -l $T/closed-all.txt   # expect 234
```

- [ ] **Step 2: Assign items to subsystems by area (+ keyword overrides)**

Mapping: `catalog|ontology|acl|lineage` → control-plane; `ingest` → ingest; `query` → query-api EXCEPT lines matching `engine|serving|flight|wire` → engine; `iceberg` → engine EXCEPT lines matching `mirror|catalog` → control-plane; `transform` → transform; `deploy` → deploy; `devx|quality|test` → build-and-test; `ui` → ui; any line matching `vector` (any area) → vector-search (override, applied first); `cross-cutting` → judged individually by the orchestrator reading the title. Write each line to `$T/<subsystem>-closed.txt`. Verify no line is unassigned:

```bash
cat $T/*-closed.txt | sort | diff - <(sort $T/closed-all.txt)   # expect empty
```

- [ ] **Step 3: Attach spec paths and open ids; write packs**

For each subsystem file, extract `spec:` slugs from its lines, resolve to `docs/superpowers/specs/<slug>.md`, list existing ones; extract the subsystem's open ids from `$T/open-all.txt` with the same mapping. Concatenate into `$T/packs/<subsystem>.md` with sections `## Closed items`, `## Spec files`, `## Open ids`.

### Task 2: Draft capability docs (agent fan-out)

**Files:**
- Create: `docs/system-capabilities/{control-plane,ingest,query-api,engine,transform,vector-search,build-and-test,ui}.md`
- Create: `docs/system-capabilities/README.md`

**Interfaces:**
- Consumes: Task 1 packs.
- Produces: 8 capability docs conforming to the content contract; README index (one line per doc + the deploy pointer to `docs/deploy.md`).

- [ ] **Step 1: Dispatch 8 parallel drafting agents**, one per subsystem, each with this prompt (subsystem name, pack path substituted):

> Read `<pack path>`, then read every file under `## Spec files` (skim long ones — you need behaviour/guarantees/decisions, not task lists). Also read the relevant `src/` module docs (`lib.rs` headers) if a claim needs verifying. Write `docs/system-capabilities/<subsystem>.md`: H1 `# <Subsystem> capabilities`; one-paragraph scope; `_As of 4861433b._`; then prose sections grouped by capability THEME (not by PR) describing what exists today, how it behaves, guarantees/limits, and the key design decisions, citing PR numbers inline as `(#N)` from the items' `pr:` fields; end with `## Known gaps` linking this subsystem's open item ids as plain `` `#id` `` code spans. No task lists, no register grammar, no status tags. Do not invent capabilities not evidenced by an item, spec, or code. Return the exact file content you wrote.

- [ ] **Step 2: Review each draft against the contract** (H1/provenance/themes/PR refs/Known gaps present; no invented claims — spot-check 2-3 PR refs per doc against `$T/closed-all.txt`). Fix or re-dispatch failures.

- [ ] **Step 3: Write `docs/system-capabilities/README.md`** — scope paragraph ("what the system can do today; registers track what's planned/broken") + table linking the 8 docs and `../deploy.md` for deploy.

- [ ] **Step 4: Commit**

```bash
git add docs/system-capabilities && git commit --no-verify -m "docs: add system-capabilities subsystem docs"
```

### Task 3: Deploy merge-in

**Files:**
- Modify: `docs/deploy.md`

- [ ] **Step 1: Dispatch one agent** with the deploy pack + current `docs/deploy.md`: merge the landed deploy capabilities (standalone binary, embedded PG, chart migrator, create-admin bootstrap, per the closed deploy items) into the existing doc's structure, adding a `_Capabilities as of 4861433b._` note and inline PR refs; preserve the doc's existing operational content.
- [ ] **Step 2: Review the diff (`git diff docs/deploy.md`) — no operational content lost. Commit:**

```bash
git add docs/deploy.md && git commit --no-verify -m "docs: merge landed deploy capabilities into deploy.md"
```

### Task 4: Slim ROADMAP + ISSUES, CLAUDE.md pointer

**Files:**
- Modify: `docs/ROADMAP.md`, `docs/ISSUES.md`, `CLAUDE.md`

- [ ] **Step 1: Delete every `- [x]` line from ROADMAP.md and ISSUES.md.** Update each header's prose to state that landed capability is documented in `docs/system-capabilities/`. Keep open items byte-identical.
- [ ] **Step 2: Fix dangling `[[id]]`** — for each surviving line, check every `[[id]]` still resolves within the three registers; strip those that don't (they now refer to landed items → replace with a plain `` `#id` `` mention only if the prose needs it, else remove).
- [ ] **Step 3: CLAUDE.md** — in the *Documentation registers* section add one sentence: "Landed capability is documented per subsystem in `docs/system-capabilities/` (see its README); the registers track only open work."
- [ ] **Step 4: Validate and commit**

```bash
bash tools/docs.sh validate   # expect OK
git add docs/ROADMAP.md docs/ISSUES.md CLAUDE.md && git commit --no-verify -m "docs: slim ROADMAP/ISSUES to open items; point at system-capabilities"
```

### Task 5: FUTURE triage (user checkpoint)

**Files:**
- Modify: `docs/FUTURE.md`

- [ ] **Step 1: Delete all terminal entries** (`status: promoted|dropped`) — mechanical.
- [ ] **Step 2: Triage the 135 open deferrals** into keep / cull-candidate (superseded by landed work, absorbed, or implausible), reading `$T/closed-all.txt` + system-capabilities drafts as the evidence base. Produce `$T/future-cull.md`: each candidate quoted verbatim with a one-line reason.
- [ ] **Step 3: CHECKPOINT — present the cull-candidate list to the user** (they asked to be consulted). Apply their verdicts; genuinely-uncertain items default to keep.
- [ ] **Step 4: Delete culled lines, fix dangling `[[id]]` as in Task 4, validate, commit**

```bash
bash tools/docs.sh validate
git add docs/FUTURE.md && git commit --no-verify -m "docs: drop terminal + stale FUTURE entries"
```

### Task 6: Keep-list recompute + orphan deletion

**Files:**
- Delete: ~320 files under `docs/superpowers/{specs,plans}/`

- [ ] **Step 1: Recompute the keep-list mechanically**

```bash
T=/home/loom/.claude/jobs/afa8c40f/tmp
# (a) specs referenced by surviving open items
grep -h '^- \[ \]' docs/ROADMAP.md docs/FUTURE.md docs/ISSUES.md | grep -oP 'spec:\K[^} ]+' | grep -v '^-$' | sed 's|^|docs/superpowers/specs/|;s|$|.md|' > $T/keep.txt
# (b) externally-referenced specs/plans (outside docs/superpowers)
grep -rhoP 'docs/superpowers/(specs|plans)/[A-Za-z0-9._-]+\.md' --include='*.md' --include='*.sh' --include='*.rs' --include='*.bzl' .claude CLAUDE.md README.md tools src docs/spike docs/spikes docs/stpa docs/code-health docs/*.md docs/system-capabilities >> $T/keep.txt
# CLAUDE.md's coverage glob (spec+plan pair) + this clearout's own spec/plan
printf '%s\n' docs/superpowers/specs/2026-06-15-whole-codebase-coverage-bxl-design.md docs/superpowers/plans/2026-06-15-whole-codebase-coverage-bxl.md docs/superpowers/specs/2026-07-03-docs-clearout-system-capabilities-design.md docs/superpowers/plans/2026-07-03-docs-clearout-system-capabilities.md >> $T/keep.txt
sort -u $T/keep.txt | grep -vE '/(p|new|new2|feature-plan|deployment-plan|YYYY-MM-DD-.*|2099-01-01-present|2026-01-01-ready)\.md$' > $T/keep-final.txt
```

- [ ] **Step 2: Delete everything not on the list**

```bash
git ls-files 'docs/superpowers/specs/*.md' 'docs/superpowers/plans/*.md' | grep -vxFf $T/keep-final.txt | xargs git rm -q
git status --short | tail -3
```

- [ ] **Step 3: Prove no dangling reference** — re-run the Step 1(b) grep and check every hit exists on disk:

```bash
grep -rhoP 'docs/superpowers/(specs|plans)/[A-Za-z0-9._-]+\.md' --include='*.md' --include='*.sh' --include='*.rs' --include='*.bzl' .claude CLAUDE.md README.md tools src docs/spike docs/spikes docs/stpa docs/code-health docs/*.md docs/system-capabilities | sort -u | while read -r f; do [ -f "$f" ] || echo "DANGLING: $f"; done   # expect no output
bash tools/docs.sh validate   # expect OK (validates spec: slugs resolve)
```

- [ ] **Step 4: Commit**

```bash
git commit --no-verify -m "docs: delete specs/plans superseded by system-capabilities"
```

### Task 7: Gates + PR

- [ ] **Step 1: Full prek**

```bash
buck2 run -v0 //tools:prek -- run --all-files > /tmp/prek.log 2>&1; grep -c Failed /tmp/prek.log   # expect 0
```

If a hook rewrote files, stage and amend the last commit.

- [ ] **Step 2: Push and open the PR** (base `main`, head `docs/system-capabilities-clearout`); body: summary counts (docs added, items removed per register, files deleted) + the full FUTURE cull list from `$T/future-cull.md` with verdicts. Do NOT enable auto-merge — user reviews.

```bash
git push --no-verify -u origin docs/system-capabilities-clearout
gh pr create --base main --head docs/system-capabilities-clearout --title "docs: clearout — system-capabilities consolidation" --body-file /home/loom/.claude/jobs/afa8c40f/tmp/pr-body.md
```
