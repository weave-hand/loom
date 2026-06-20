# Documentation Registers Consolidation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Coalesce loom's scattered deferred/planned/defect docs into three parsable markdown registers (`docs/ROADMAP.md`, `docs/FUTURE.md`, `docs/ISSUES.md`) with a tagged-item grammar, plus the tooling, skills, and hook that keep them live.

**Architecture:** Markdown is the source of truth. A single `tools/docs.sh` shell script parses the tagged-item grammar and provides `validate` (a prek hook + skill gate), `query` (shell reading), and `shipped-open` (reconciliation candidates). Two skills drive the work: `loom-docs-organise` (bulk agentic mine → dedupe → render → PR-on-green) and `loom-docs-update` (single-item edits at spec/plan completion). A NO-OP-unless `Stop` hook nudges you to run `loom-docs-update`.

**Tech Stack:** POSIX-ish bash + awk (no new build target, no third-party dep), the hermetic `gh`/`prek` already in the repo, markdown registers, Claude Code skills (`.claude/skills/*/SKILL.md`) and `settings.json` hooks.

**Reference:** Design spec at `docs/superpowers/specs/2026-06-20-docs-registers-consolidation-design.md`. Read it before starting.

---

## File Structure

| Path | Responsibility | Action |
|---|---|---|
| `tools/docs.sh` | Parser + `validate`/`query`/`shipped-open` over the registers | Create |
| `tools/tests/docs_test.sh` | Black-box test harness for `docs.sh` | Create |
| `tools/tests/fixtures/*.md` | Valid + per-failure-mode fixture registers | Create |
| `prek.toml` | Add the `docs-validate` local hook | Modify |
| `.claude/skills/loom-docs-organise/SKILL.md` | Bulk mine/dedupe/render/PR skill | Create |
| `.claude/skills/loom-docs-update/SKILL.md` | Single-item completion-time edits | Create |
| `tools/docs-remind.sh` | NO-OP-unless completion reminder | Create |
| `.claude/settings.json` | Wire `docs-remind.sh` as a `Stop` hook | Modify |
| `CLAUDE.md` | Add "Documentation registers" section; repoint roadmap refs | Modify |
| `docs/ROADMAP.md`, `docs/FUTURE.md`, `docs/ISSUES.md` | The three registers | Created by running `loom-docs-organise` (Task 9) |

The grammar (from the spec), reused throughout: one markdown list item per entry —
``- [ ] **Title** `{#id area:<a> status:<s> from:<f> pr:<p> spec:<sp>}` `` with prose
indented below and `[[id]]` cross-links. `#id` prefixes: `road-`/`fut-`/`iss-`.
`area:` vocab: `lineage catalog ontology acl ingest query transform iceberg ui ux test quality devx build deploy cross-cutting`.
`status:` per register — ROADMAP `planned|in-progress|done`, FUTURE `deferred|promoted|dropped`, ISSUES `open|fixed|wontfix`.

---

## Task 1: `docs.sh` parser + `validate`

**Files:**
- Create: `tools/docs.sh`
- Create: `tools/tests/docs_test.sh`
- Create: `tools/tests/fixtures/good-ROADMAP.md`, `good-FUTURE.md`, `good-ISSUES.md`, `bad-grammar-ROADMAP.md`, `bad-dupid-FUTURE.md`, `bad-area-ISSUES.md`, `bad-status-ROADMAP.md`, `bad-link-FUTURE.md`

- [ ] **Step 1: Write the fixtures**

`tools/tests/fixtures/good-ROADMAP.md`:
```markdown
# Roadmap register

_As of test._

## ingest

- [ ] **Streaming ingest path** `{#road-streaming-ingest area:ingest status:planned from:phase-3 pr:- spec:-}`
  Incremental append path. Related: [[road-compaction-job]]

- [x] **Compaction job endpoint** `{#road-compaction-job area:catalog status:done from:phase-2 pr:#80 spec:2026-06-17-compaction}`
  Operator-triggered compaction landed.
```

`tools/tests/fixtures/good-FUTURE.md`:
```markdown
# Future work register

_As of test._

## lineage

- [ ] **Transitive provenance closure** `{#fut-lineage-closure area:lineage status:deferred from:phase-5 pr:- spec:2026-06-05-control-plane-lineage}`
  One hop today. Needs a cycle guard.
```

`tools/tests/fixtures/good-ISSUES.md`:
```markdown
# Issues register

_As of test._

## query

- [ ] **quote_ident panics on embedded quote** `{#iss-quote-ident-panic area:query status:open from:link-traversal pr:- spec:2026-06-14-query-governed-link-traversal}`
  `quote_ident` asserts no double-quote in an identifier.
```

`tools/tests/fixtures/bad-grammar-ROADMAP.md` (tag block missing the `{#...}`):
```markdown
# Roadmap register

## ingest

- [ ] **No tag block here**
  This item has no machine-readable tag block.
```

`tools/tests/fixtures/bad-dupid-FUTURE.md` (same `#id` twice):
```markdown
# Future work register

## acl

- [ ] **First** `{#fut-dup area:acl status:deferred from:x pr:- spec:-}`
  one
- [ ] **Second** `{#fut-dup area:acl status:deferred from:x pr:- spec:-}`
  two
```

`tools/tests/fixtures/bad-area-ISSUES.md` (`area:bogus` not in vocab):
```markdown
# Issues register

## bogus

- [ ] **Bad area** `{#iss-bad-area area:bogus status:open from:x pr:- spec:-}`
  nope
```

`tools/tests/fixtures/bad-status-ROADMAP.md` (`status:deferred` is a FUTURE status, illegal in ROADMAP):
```markdown
# Roadmap register

## ingest

- [ ] **Wrong status for register** `{#road-wrong-status area:ingest status:deferred from:x pr:- spec:-}`
  deferred is not a roadmap status
```

`tools/tests/fixtures/bad-link-FUTURE.md` (`[[fut-missing]]` resolves to nothing):
```markdown
# Future work register

## acl

- [ ] **Dangling link** `{#fut-has-link area:acl status:deferred from:x pr:- spec:-}`
  See [[fut-missing]].
```

`tools/tests/fixtures/bad-uppercase-ISSUES.md` (an `- [X]` item with a bad area — must NOT be silently skipped):
```markdown
# Issues register

## bogus

- [X] **Uppercase checkbox** `{#iss-uppercase area:bogus status:fixed from:x pr:- spec:-}`
  Uppercase [X] items must still be parsed and validated, not skipped.
```

`tools/tests/fixtures/bad-missing-area-FUTURE.md` (no `area:` key — must report "missing area", not a shifted column):
```markdown
# Future work register

## lineage

- [ ] **No area field** `{#fut-no-area status:deferred from:x pr:- spec:-}`
  The area: key is absent.
```

- [ ] **Step 2: Write the failing test harness**

`tools/tests/docs_test.sh`:
```bash
#!/usr/bin/env bash
# Black-box tests for tools/docs.sh. Run: bash tools/tests/docs_test.sh
set -u
DIR="$(cd "$(dirname "$0")" && pwd)"
ROOT="$(cd "$DIR/../.." && pwd)"
DOCS="$ROOT/tools/docs.sh"
FIX="$DIR/fixtures"
fail=0
rc(){ "$@" >/dev/null 2>&1; echo $?; }
check(){ # desc want_rc got_rc
  if [ "$2" = "$3" ]; then echo "ok   - $1"; else echo "FAIL - $1 (want rc=$2 got rc=$3)"; fail=1; fi
}

check "valid registers pass" 0 "$(rc bash "$DOCS" validate "$FIX/good-ROADMAP.md" "$FIX/good-FUTURE.md" "$FIX/good-ISSUES.md")"
check "malformed tag block fails" 1 "$(rc bash "$DOCS" validate "$FIX/bad-grammar-ROADMAP.md")"
check "duplicate id fails" 1 "$(rc bash "$DOCS" validate "$FIX/bad-dupid-FUTURE.md")"
check "bad area fails" 1 "$(rc bash "$DOCS" validate "$FIX/bad-area-ISSUES.md")"
check "wrong status for register fails" 1 "$(rc bash "$DOCS" validate "$FIX/bad-status-ROADMAP.md")"
check "unresolvable link fails" 1 "$(rc bash "$DOCS" validate "$FIX/bad-link-FUTURE.md")"
# Uppercase [X] must be parsed, not silently skipped: a bad area in an [X] item must still fail.
check "uppercase [X] item is validated not skipped" 1 "$(rc bash "$DOCS" validate "$FIX/bad-uppercase-ISSUES.md")"
# A missing area: key must report "missing area" (not a misleading shifted-column message).
miss="$(bash "$DOCS" validate "$FIX/bad-missing-area-FUTURE.md" 2>&1 | grep -c 'missing area' || true)"
check "missing area reported clearly" 1 "$miss"

exit $fail
```

- [ ] **Step 3: Run the test to verify it fails**

Run: `bash tools/tests/docs_test.sh`
Expected: FAIL — every check errors because `tools/docs.sh` does not exist yet (rc 127 ≠ expected).

- [ ] **Step 4: Implement `tools/docs.sh` with the extractor + `validate`**

`tools/docs.sh`:
```bash
#!/usr/bin/env bash
# loom documentation-register tool. Parses the tagged-item grammar in
# docs/{ROADMAP,FUTURE,ISSUES}.md and provides validate / query / shipped-open.
# See docs/superpowers/specs/2026-06-20-docs-registers-consolidation-design.md.
set -euo pipefail

AREAS=" lineage catalog ontology acl ingest query transform iceberg ui ux test quality devx build deploy cross-cutting "
REGISTERS=(docs/ROADMAP.md docs/FUTURE.md docs/ISSUES.md)

usage(){ echo "usage: docs.sh {validate|query|shipped-open} [args]" >&2; exit 2; }

# Emit a TSV of every well-formed item across the given files:
#   file  lineno  cb  regtype  id  area  status  from  pr  spec  title
_extract(){
  awk '
  function reg(f){ f=tolower(f);
    if (f ~ /roadmap/) return "roadmap";
    if (f ~ /future/)  return "future";
    if (f ~ /issues/)  return "issues";
    return "unknown" }
  /^- \[[ xX]\] / && index($0, "`{#") && index($0, "}`") {
    line=$0
    cb=tolower(substr(line,4,1))   # accept [X] as well as [x]; normalise to lowercase
    s=index(line,"**"); rest=substr(line,s+2); e=index(rest,"**"); title=substr(rest,1,e-1)
    b1=index(line,"`{"); b2=index(line,"}`"); block=substr(line,b1+2,b2-(b1+2))
    id=""; area=""; status=""; from="-"; pr="-"; spec="-"
    n=split(block,t," ")
    for (i=1;i<=n;i++){ x=t[i];
      if (x ~ /^#/) { id=substr(x,2) }
      else { c=index(x,":"); if (c>0){ k=substr(x,1,c-1); v=substr(x,c+1);
        if (k=="area") area=v; else if (k=="status") status=v;
        else if (k=="from") from=v; else if (k=="pr") pr=v; else if (k=="spec") spec=v } }
    }
    # Default empties to "-" so no TSV field is ever blank — blank fields collapse
    # under `IFS=$'\t' read` and shift columns. validate treats "-" as missing.
    if (id=="") id="-"; if (area=="") area="-"; if (status=="") status="-"
    printf "%s\t%d\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n",
      FILENAME, FNR, cb, reg(FILENAME), id, area, status, from, pr, spec, title
  }' "$@"
}

cmd_validate(){
  local files=("$@"); [ ${#files[@]} -gt 0 ] || files=("${REGISTERS[@]}")
  local ERR; ERR="$(mktemp)"
  local present=() f
  for f in "${files[@]}"; do
    if [ ! -f "$f" ]; then echo "$f: not found" >>"$ERR"; continue; fi
    present+=("$f")
    # Any item bullet lacking a well-formed `{# … }` tag block is malformed.
    # index() string-matching (not a brace regex) — portable across awk/mawk
    # builds, where `{`/`}` in an ERE are interval metacharacters parsed
    # inconsistently (the cause of a CI-only false "unresolved link").
    awk '/^- \[[ xX]\] / { if (index($0, "`{#") == 0 || index($0, "}`") == 0)
      printf "%s:%d: malformed item tag block\n", FILENAME, FNR }' "$f" >>"$ERR"
  done
  if [ ${#present[@]} -eq 0 ]; then sort -u "$ERR"; rm -f "$ERR"; return 1; fi

  local TSV; TSV="$(mktemp)"; _extract "${present[@]}" >"$TSV"
  local file ln cb R id area status from pr spec title want allowed term
  while IFS=$'\t' read -r file ln cb R id area status from pr spec title; do
    case "$R" in
      roadmap) want="road-"; allowed=" planned in-progress done ";  term=" done " ;;
      future)  want="fut-";  allowed=" deferred promoted dropped "; term=" promoted dropped " ;;
      issues)  want="iss-";  allowed=" open fixed wontfix ";        term=" fixed wontfix " ;;
      *) echo "$file:$ln: unknown register type (filename must contain ROADMAP/FUTURE/ISSUES)" >>"$ERR"; continue ;;
    esac
    if [ "$id" = "-" ]; then echo "$file:$ln: missing #id" >>"$ERR"
    else case "$id" in "$want"*) : ;; *) echo "$file:$ln: id '$id' must start with '$want'" >>"$ERR" ;; esac; fi
    if [ "$area" = "-" ]; then echo "$file:$ln: missing area" >>"$ERR"
    else case "$AREAS" in *" $area "*) : ;; *) echo "$file:$ln: bad area '$area'" >>"$ERR" ;; esac; fi
    if [ "$status" = "-" ]; then echo "$file:$ln: missing status" >>"$ERR"
    else case "$allowed" in *" $status "*) : ;; *) echo "$file:$ln: bad status '$status' for $R register" >>"$ERR" ;; esac; fi
    if [ "$cb" = "x" ]; then
      case "$term" in *" $status "*) : ;; *) echo "$file:$ln: checked [x] item must have a terminal status" >>"$ERR" ;; esac
    else
      case "$term" in *" $status "*) echo "$file:$ln: terminal status '$status' must be checked [x]" >>"$ERR" ;; *) : ;; esac
    fi
    if [ "$pr" != "-" ]; then
      printf '%s' "$pr" | grep -qE '^#[0-9]+(,#[0-9]+)*$' || echo "$file:$ln: bad pr '$pr' (want '-' or '#N[,#N...]')" >>"$ERR"
    fi
  done <"$TSV"

  # Duplicate ids across all present files.
  cut -f5 "$TSV" | sort | uniq -d | while read -r d; do
    [ -n "$d" ] && echo "duplicate id: $d" >>"$ERR"; done

  # Every [[link]] must resolve to a known id.
  local IDS; IDS="$(cut -f5 "$TSV" | sort -u)"
  for f in "${present[@]}"; do
    # `|| true` so a register with zero links doesn't fail the pipe under pipefail.
    { grep -oE '\[\[[a-z0-9-]+\]\]' "$f" 2>/dev/null || true; } | sed 's/\[\[//; s/\]\]//' | while read -r lid; do
      printf '%s\n' "$IDS" | grep -qx "$lid" || echo "$f: unresolved link [[$lid]]" >>"$ERR"
    done
  done

  rm -f "$TSV"
  if [ -s "$ERR" ]; then sort -u "$ERR"; rm -f "$ERR"; return 1; fi
  rm -f "$ERR"; echo "docs.sh validate: OK (${#present[@]} files)"
}

main(){
  local cmd="${1:-}"; shift || true
  case "$cmd" in
    validate) cmd_validate "$@" ;;
    *) usage ;;
  esac
}
main "$@"
```

- [ ] **Step 5: Make it executable and run the test to verify it passes**

Run:
```bash
chmod +x tools/docs.sh tools/tests/docs_test.sh
bash tools/tests/docs_test.sh
```
Expected: all six checks print `ok   - ...` and the script exits 0.

- [ ] **Step 6: Sanity-check shell syntax**

Run: `bash -n tools/docs.sh && echo SYNTAX_OK`
Expected: `SYNTAX_OK`

- [ ] **Step 7: Commit**

```bash
git add tools/docs.sh tools/tests/docs_test.sh tools/tests/fixtures
git commit -m "feat(docs): add docs.sh register parser and validate"
```

---

## Task 2: `docs.sh query`

**Files:**
- Modify: `tools/docs.sh`
- Modify: `tools/tests/docs_test.sh`

- [ ] **Step 1: Add failing tests for query**

Append to `tools/tests/docs_test.sh` *before* the final `exit $fail`:
```bash
out(){ "$@" 2>/dev/null; }
G="$FIX/good-ROADMAP.md $FIX/good-FUTURE.md $FIX/good-ISSUES.md"

# query runs against docs/{ROADMAP,FUTURE,ISSUES}.md by default, so copy the
# fixtures into a temp registers dir and run from there.
T="$(mktemp -d)"; mkdir -p "$T/docs"
cp "$FIX/good-ROADMAP.md" "$T/docs/ROADMAP.md"
cp "$FIX/good-FUTURE.md"  "$T/docs/FUTURE.md"
cp "$FIX/good-ISSUES.md"  "$T/docs/ISSUES.md"

open_ids="$(cd "$T" && out bash "$DOCS" query open | awk '{print $2}' | sort | tr '\n' ' ')"
# Compare sorted-actual against the sorted expected id list.
check "query open lists only unchecked items" "fut-lineage-closure iss-quote-ident-panic road-streaming-ingest " "$open_ids"

done_ids="$(cd "$T" && out bash "$DOCS" query done | awk '{print $2}' | tr '\n' ' ')"
check "query done lists only checked items" "road-compaction-job " "$done_ids"

acl_ids="$(cd "$T" && out bash "$DOCS" query open --area lineage | awk '{print $2}' | tr '\n' ' ')"
check "query open --area filters by area" "fut-lineage-closure " "$acl_ids"

# `query links <id>` prints the file:line of each `[[id]]` reference; assert a
# reference to road-compaction-job is found in the roadmap register.
links_hit="$(cd "$T" && out bash "$DOCS" query links road-compaction-job | grep -c 'ROADMAP.md' || true)"
check "query links finds a reference" 1 "$links_hit"

rm -rf "$T"
```

- [ ] **Step 2: Run the test to verify the new checks fail**

Run: `bash tools/tests/docs_test.sh`
Expected: the four new checks FAIL (`query` is not implemented — `usage` exits 2).

- [ ] **Step 3: Implement `cmd_query` and wire it into `main`**

In `tools/docs.sh`, add this function above `main`:
```bash
cmd_query(){
  local kind="${1:-}"; shift || true
  if [ "$kind" = links ]; then
    local id="${1:-}"; [ -n "$id" ] || { echo "usage: docs.sh query links <id>" >&2; return 2; }
    grep -nE "\[\[$id\]\]" "${REGISTERS[@]}" 2>/dev/null || echo "no references to [[$id]]"
    return 0
  fi
  local area="" status=""
  while [ $# -gt 0 ]; do
    case "$1" in
      --area)   area="${2:-}";   shift 2 ;;
      --status) status="${2:-}"; shift 2 ;;
      *) shift ;;
    esac
  done
  local present=() f
  for f in "${REGISTERS[@]}"; do [ -f "$f" ] && present+=("$f"); done
  [ ${#present[@]} -gt 0 ] || { echo "no registers found" >&2; return 1; }
  local TSV; TSV="$(mktemp)"; _extract "${present[@]}" >"$TSV"
  case "$kind" in
    open|done)
      local cb; [ "$kind" = open ] && cb=" " || cb="x"
      awk -F'\t' -v cb="$cb" -v a="$area" -v s="$status" '
        $3==cb && (a==""||$6==a) && (s==""||$7==s){ printf "%-9s %-26s %s\n", $4, $5, $11 }' "$TSV" ;;
    by-area)
      cut -f6 "$TSV" | sort | uniq -c | sort -rn ;;
    *) echo "unknown query kind: $kind (want open|done|by-area|links)" >&2; rm -f "$TSV"; return 2 ;;
  esac
  rm -f "$TSV"
}
```
And extend the `case` in `main`:
```bash
    query) cmd_query "$@" ;;
```
(insert that line directly after the `validate) cmd_validate "$@" ;;` line).

- [ ] **Step 4: Run the test to verify all checks pass**

Run: `bash tools/tests/docs_test.sh`
Expected: all checks (Task 1 + Task 2) print `ok   - ...`, exit 0.

- [ ] **Step 5: Commit**

```bash
git add tools/docs.sh tools/tests/docs_test.sh
git commit -m "feat(docs): add docs.sh query (open/done/by-area/links)"
```

---

## Task 3: `docs.sh shipped-open` (reconciliation candidates)

**Files:**
- Modify: `tools/docs.sh`
- Modify: `tools/tests/docs_test.sh`

This lists open items that carry a `pr:` ref (or, with `--stale`, open items with NO pr) as candidates for `loom-docs-organise` to verify and close. When `gh` is on PATH it annotates each PR with its merge state; the candidate selection itself is gh-independent and is what the test pins.

- [ ] **Step 1: Add failing tests**

Append to `tools/tests/docs_test.sh` *before* the final `exit $fail` (reuses a fresh temp registers dir):
```bash
T2="$(mktemp -d)"; mkdir -p "$T2/docs"
cp "$FIX/good-ROADMAP.md" "$T2/docs/ROADMAP.md"
cp "$FIX/good-FUTURE.md"  "$T2/docs/FUTURE.md"
cp "$FIX/good-ISSUES.md"  "$T2/docs/ISSUES.md"

# shipped-open: open items WITH a pr ref. good-* has no open item with a pr,
# so add one with a PR to ROADMAP.
cat >>"$T2/docs/ROADMAP.md" <<'EOF'

- [ ] **Has a PR but still open** `{#road-open-with-pr area:ingest status:planned from:x pr:#99 spec:-}`
  candidate to reconcile
EOF
cand="$(cd "$T2" && PATH="/usr/bin:/bin" bash "$DOCS" shipped-open 2>/dev/null | grep -c 'road-open-with-pr' || true)"
check "shipped-open lists open items with a pr ref" 1 "$cand"
nostale="$(cd "$T2" && PATH="/usr/bin:/bin" bash "$DOCS" shipped-open 2>/dev/null | grep -c 'road-streaming-ingest' || true)"
check "shipped-open omits open items without a pr" 0 "$nostale"
stale="$(cd "$T2" && bash "$DOCS" shipped-open --stale 2>/dev/null | grep -c 'road-streaming-ingest' || true)"
check "shipped-open --stale lists open items without a pr" 1 "$stale"

rm -rf "$T2"
```

- [ ] **Step 2: Run the test to verify the new checks fail**

Run: `bash tools/tests/docs_test.sh`
Expected: the three new checks FAIL (`shipped-open` not implemented).

- [ ] **Step 3: Implement `cmd_shipped_open` and wire it into `main`**

Add above `main` in `tools/docs.sh`:
```bash
cmd_shipped_open(){
  local stale=0
  [ "${1:-}" = "--stale" ] && stale=1
  local present=() f
  for f in "${REGISTERS[@]}"; do [ -f "$f" ] && present+=("$f"); done
  [ ${#present[@]} -gt 0 ] || { echo "no registers found" >&2; return 1; }
  local TSV; TSV="$(mktemp)"; _extract "${present[@]}" >"$TSV"
  # if-form, not `A && have_gh=1`, so a missing gh doesn't trip set -e.
  local have_gh=0; if command -v gh >/dev/null 2>&1; then have_gh=1; fi
  local file ln cb R id area status from pr spec title
  while IFS=$'\t' read -r file ln cb R id area status from pr spec title; do
    [ "$cb" = " " ] || continue                    # open items only
    if [ "$stale" = 1 ]; then
      [ "$pr" = "-" ] || continue                  # --stale: open with NO pr
      printf '%s\t%s\t(no PR linked)\n' "$id" "$title"
    else
      [ "$pr" != "-" ] || continue                 # default: open WITH a pr
      local note="$pr"
      if [ "$have_gh" = 1 ]; then
        local n="${pr%%,*}"; n="${n#\#}"
        local st; st="$(gh pr view "$n" --json state -q .state 2>/dev/null || echo '?')"
        note="$pr (#$n: $st)"
      fi
      printf '%s\t%s\t%s\n' "$id" "$title" "$note"
    fi
  done <"$TSV"
  rm -f "$TSV"
}
```
And extend `main`'s case (after the `query)` line):
```bash
    shipped-open) cmd_shipped_open "$@" ;;
```

- [ ] **Step 4: Run the full test suite**

Run: `bash tools/tests/docs_test.sh`
Expected: every check prints `ok   - ...`, exit 0.

- [ ] **Step 5: Commit**

```bash
git add tools/docs.sh tools/tests/docs_test.sh
git commit -m "feat(docs): add docs.sh shipped-open reconciliation helper"
```

---

## Task 4: Wire `docs-validate` as a prek hook

**Files:**
- Modify: `prek.toml`

- [ ] **Step 1: Add the hook**

In `prek.toml`, add this block in the `repo = "local"` section, directly after the `no-inline-tests` hook block:
```toml
# Validate the documentation registers against the tagged-item grammar
# (unique ids, controlled area/status vocab, resolvable [[links]]). See
# tools/docs.sh and docs/superpowers/specs/2026-06-20-docs-registers-consolidation-design.md.
[[repos.hooks]]
id = "docs-validate"
name = "docs registers validate"
entry = "bash tools/docs.sh validate"
language = "system"
files = "^docs/(ROADMAP|FUTURE|ISSUES)\\.md$"
pass_filenames = false
```

- [ ] **Step 2: Verify the toml parses**

Run: `buck2 run //tools:prek -- run check-toml --all-files > /tmp/docs-toml.log 2>&1; grep -Ei "fail|error" /tmp/docs-toml.log || echo TOML_OK`
Expected: `TOML_OK`.

- [ ] **Step 3: Verify the hook is recognized and passes against the fixtures**

The real registers don't exist until Task 9, so confirm the hook *runs* without the registers (no files matched ⇒ skip/pass):
```bash
buck2 run //tools:prek -- run docs-validate --all-files > /tmp/docs-hook.log 2>&1; cat /tmp/docs-hook.log
```
Expected: the `docs-validate` hook reports `Passed` or `(no files to check) Skipped` — not an error.

- [ ] **Step 4: Commit**

```bash
git add prek.toml
git commit -m "build(docs): add docs-validate prek hook"
```

---

## Task 5: `loom-docs-organise` skill

**Files:**
- Create: `.claude/skills/loom-docs-organise/SKILL.md`

This skill has no TDD loop (it is a procedural skill, like `loom-complexity`). It is validated by `bash -n` on its embedded BLOCK and by running it in Task 9.

- [ ] **Step 1: Write the skill**

`.claude/skills/loom-docs-organise/SKILL.md`:
````markdown
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
     `docs/ROADMAP.md`. **As built:** the original was kept in place with a
     supersession banner (NOT renamed to `.old`) so the ~35 historical
     specs/plans that link to it don't dangle — link preservation beats the
     archive rename here.
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
````

- [ ] **Step 2: Verify the embedded BLOCK A bash is syntactically valid**

Run:
```bash
awk '/^```bash$/{f=1;next} /^```$/{f=0} f' .claude/skills/loom-docs-organise/SKILL.md | bash -n - && echo BLOCK_OK
```
Expected: `BLOCK_OK`.

- [ ] **Step 3: Verify the frontmatter has name + description**

Run: `head -3 .claude/skills/loom-docs-organise/SKILL.md`
Expected: a `name: loom-docs-organise` line and a `description:` line within the `---` fences.

- [ ] **Step 4: Commit**

```bash
git add .claude/skills/loom-docs-organise/SKILL.md
git commit -m "feat(skill): add loom-docs-organise bulk consolidation skill"
```

---

## Task 6: `loom-docs-update` skill

**Files:**
- Create: `.claude/skills/loom-docs-update/SKILL.md`

- [ ] **Step 1: Write the skill**

`.claude/skills/loom-docs-update/SKILL.md`:
````markdown
---
name: loom-docs-update
description: Update the documentation registers (docs/ROADMAP.md, docs/FUTURE.md, docs/ISSUES.md) at the moment a spec or plan is completed — close the items the work resolved, record any newly-deferred items it introduced, and stage the edits alongside the work. Use when finishing a development branch, completing a plan or spec, after merging a PR that resolves a tracked item, or when the completion-reminder hook nudges you. For a full rebuild/reconcile use loom-docs-organise instead.
---

Keep the registers live as work lands. This is the lightweight single-item
counterpart to `loom-docs-organise` (no agentic mining). Grammar and registers
are defined in
`docs/superpowers/specs/2026-06-20-docs-registers-consolidation-design.md`.

Run this when a spec/plan is completed (or the `Stop` reminder fires). Inputs:
the spec/plan just finished and the PR number(s) for the work.

## Steps

1. Identify the completed spec/plan (the one just implemented on this branch) and
   the PR number(s). If unsure of the PR, use `gh pr view --json number -q .number`
   for the current branch.
2. **Close resolved items.** Find register items whose `spec:` references this
   spec/plan, or whose description the work satisfies:
   `bash tools/docs.sh query open | grep -i <keyword>` and
   `grep -n <spec-slug> docs/ROADMAP.md docs/FUTURE.md docs/ISSUES.md`.
   For each resolved item: change `- [ ]` → `- [x]`, set a terminal status
   (`done` / `fixed` / `promoted`), and add the PR to `pr:` (e.g. `pr:#84`).
3. **Record new deferrals.** Read the completed spec's "deferred" / "out of
   scope" / "non-goals" section. For each genuinely-deferred follow-up, add a new
   item to FUTURE (an idea) or ISSUES (a defect/gap) with `status: deferred`/`open`,
   `from:<spec-slug>`, a fresh prefixed `#id`, and a one-paragraph prose note. Add
   `[[id]]` links to related items.
4. **Promote if applicable.** If the work fulfilled a committed ROADMAP item, mark
   it `done`; if it began a `deferred` FUTURE item, set that item `promoted` and add
   the matching `road-…` item.
5. **Validate:** `bash tools/docs.sh validate` — fix every reported error.
6. **Stage** the register edits so they ride the current feature branch's commit
   (do not open a separate PR): `git add docs/ROADMAP.md docs/FUTURE.md docs/ISSUES.md`.
   Mention in the commit/PR body which items were closed/added.
````

- [ ] **Step 2: Verify the frontmatter**

Run: `head -3 .claude/skills/loom-docs-update/SKILL.md`
Expected: a `name: loom-docs-update` line and a `description:` line within the `---` fences.

- [ ] **Step 3: Commit**

```bash
git add .claude/skills/loom-docs-update/SKILL.md
git commit -m "feat(skill): add loom-docs-update completion-time skill"
```

---

## Task 7: Completion-reminder `Stop` hook

**Files:**
- Create: `tools/docs-remind.sh`
- Modify: `.claude/settings.json`

- [ ] **Step 1: Write a failing test for the NO-OP-unless logic**

Append to `tools/tests/docs_test.sh` *before* the final `exit $fail`:
```bash
REMIND="$ROOT/tools/docs-remind.sh"
# On main, always silent (rc 0, no output).
g(){ git -C "$1" "${@:2}"; }
R1="$(mktemp -d)"; g "$R1" init -q; g "$R1" config user.email t@t; g "$R1" config user.name t
mkdir -p "$R1/docs/superpowers/plans"; echo x > "$R1/docs/superpowers/plans/p.md"
g "$R1" add -A; g "$R1" commit -qm "init"
# The nudge prints to stderr, so capture 2>&1 in every assertion.
o1="$(cd "$R1" && bash "$REMIND" 2>&1)"; check "remind silent on main" "" "$o1"

# On a branch whose commit touched a plan but NOT a register: should nudge.
R2="$(mktemp -d)"; g "$R2" init -q -b main; g "$R2" config user.email t@t; g "$R2" config user.name t
mkdir -p "$R2/docs/superpowers/plans"; echo seed > "$R2/docs/seed.md"; g "$R2" add -A; g "$R2" commit -qm "seed"
g "$R2" switch -qC feature
echo plan > "$R2/docs/superpowers/plans/new.md"; g "$R2" add -A; g "$R2" commit -qm "add plan"
o2="$(cd "$R2" && bash "$REMIND" 2>&1 | grep -c 'loom-docs-update' || true)"; check "remind nudges on plan-touch branch" 1 "$o2"

# On a branch that touched a register too: silent.
g "$R2" switch -qC feature2 main
mkdir -p "$R2/docs/superpowers/plans"; echo plan > "$R2/docs/superpowers/plans/new2.md"; echo r > "$R2/docs/ROADMAP.md"
g "$R2" add -A; g "$R2" commit -qm "plan + register"
o3="$(cd "$R2" && bash "$REMIND" 2>&1)"; check "remind silent when register touched" "" "$o3"

rm -rf "$R1" "$R2"
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `bash tools/tests/docs_test.sh`
Expected: the three `remind …` checks FAIL (`tools/docs-remind.sh` does not exist).

- [ ] **Step 3: Implement the hook script**

`tools/docs-remind.sh`:
```bash
#!/usr/bin/env bash
# Stop hook: NO-OP unless the current branch added/changed a spec or plan but did
# NOT touch any of the three registers — then print a one-line advisory nudge to
# run loom-docs-update. Advisory only; always exits 0 (never blocks a stop).
# Mirrors tools/cloud-session-start.sh's "inert unless" shape.
set -euo pipefail

branch="$(git rev-parse --abbrev-ref HEAD 2>/dev/null || echo HEAD)"
if [ "$branch" = "main" ] || [ "$branch" = "HEAD" ]; then exit 0; fi

# Files changed on this branch relative to main (committed + working tree).
base="$(git merge-base HEAD main 2>/dev/null || true)"
[ -n "$base" ] || exit 0
changed="$( { git diff --name-only "$base"...HEAD; git diff --name-only; } | sort -u )"

# if-forms (not `&& exit` / `|| exit` chains) so the "should nudge" path isn't
# killed by set -e when the register grep finds no match.
if ! printf '%s\n' "$changed" | grep -qE '^docs/superpowers/(specs|plans)/'; then exit 0; fi
if printf '%s\n' "$changed" | grep -qE '^docs/(ROADMAP|FUTURE|ISSUES)\.md$'; then exit 0; fi

echo "📋 This branch touched a spec/plan but no register. Run the loom-docs-update skill to reconcile docs/{ROADMAP,FUTURE,ISSUES}.md before finishing." >&2
exit 0
```

- [ ] **Step 4: Make it executable and run the test to verify it passes**

Run:
```bash
chmod +x tools/docs-remind.sh
bash tools/tests/docs_test.sh
```
Expected: all checks (including the three `remind …`) print `ok   - ...`, exit 0.

- [ ] **Step 5: Wire the Stop hook into settings.json**

Replace the contents of `.claude/settings.json` with (adds a `Stop` array alongside the existing `SessionStart`):
```json
{
  "hooks": {
    "SessionStart": [
      {
        "hooks": [
          {
            "type": "command",
            "command": "bash \"$CLAUDE_PROJECT_DIR/tools/cloud-session-start.sh\""
          }
        ]
      }
    ],
    "Stop": [
      {
        "hooks": [
          {
            "type": "command",
            "command": "bash \"$CLAUDE_PROJECT_DIR/tools/docs-remind.sh\""
          }
        ]
      }
    ]
  }
}
```

- [ ] **Step 6: Verify settings.json is valid JSON**

Run: `python3 -m json.tool .claude/settings.json >/dev/null && echo JSON_OK || buck2 run //tools:prek -- run check-toml --files .claude/settings.json >/dev/null 2>&1; echo done`
Expected: `JSON_OK` (or, if no python3, the file visibly parses — confirm the `Stop` key is present).

- [ ] **Step 7: Commit**

```bash
git add tools/docs-remind.sh tools/tests/docs_test.sh .claude/settings.json
git commit -m "feat(docs): add completion-reminder Stop hook"
```

---

## Task 8: Document the registers in CLAUDE.md

**Files:**
- Modify: `CLAUDE.md`

- [ ] **Step 1: Add the "Documentation registers" section**

In `CLAUDE.md`, add this new section immediately before the `## Cell layout` section:
```markdown
## Documentation registers

Deferred/planned/defect work is tracked in three **parsable markdown registers**,
split by commitment level — the single source of truth for "what's deferred /
planned / broken":

- `docs/ROADMAP.md` — committed/sequenced work (`status: planned|in-progress|done`).
- `docs/FUTURE.md` — deliberately-deferred ideas (`status: deferred|promoted|dropped`).
- `docs/ISSUES.md` — known defects/gaps in shipped code (`status: open|fixed|wontfix`).

Each item is one markdown list entry with a backtick-wrapped tag block on the
title line and prose below:
`- [ ] **Title** ` + "`" + `{#id area:<a> status:<s> from:<f> pr:<p> spec:<sp>}` + "`".
`#id` prefixes `road-`/`fut-`/`iss-`; `area:` ∈ a controlled vocab; `pr:` is `-`
or `#N[,#N...]`; `[[id]]` cross-links items. The `[ ]`/`[x]` checkbox makes
"everything unfinished" a one-liner: `grep '^- \[ \]' docs/*.md`. Full grammar:
`docs/superpowers/specs/2026-06-20-docs-registers-consolidation-design.md`.

- **`tools/docs.sh`** — `validate` (grammar/ids/vocab/links; also the `docs-validate`
  prek hook), `query open|done|by-area|links` (shell reading, e.g.
  `bash tools/docs.sh query open --area acl`), and `shipped-open` (reconciliation
  candidates). Tested by `bash tools/tests/docs_test.sh`.
- **`loom-docs-organise`** skill — bulk mine/dedupe/render the registers, landed as
  a PR on green CI. Run to consolidate or reconcile (or on a schedule).
- **`loom-docs-update`** skill — at spec/plan completion, close resolved items and
  record new deferrals, staged alongside the work. The `Stop` hook
  (`tools/docs-remind.sh`) nudges you to run it when a branch touched a spec/plan
  but no register.
```

- [ ] **Step 2: Repoint the roadmap reference**

In `CLAUDE.md`, in the `## Project status` paragraph, find the sentence:
```
The slice-by-slice status of record is [`docs/superpowers/specs/2026-06-06-loom-roadmap.md`](docs/superpowers/specs/2026-06-06-loom-roadmap.md) — consult and update it as capabilities land.
```
and replace it with:
```
The slice-by-slice status of record is [`docs/ROADMAP.md`](docs/ROADMAP.md) (with deferred ideas in [`docs/FUTURE.md`](docs/FUTURE.md) and known defects in [`docs/ISSUES.md`](docs/ISSUES.md)) — consult and update them as capabilities land (see **Documentation registers** below).
```

- [ ] **Step 3: Verify markdown lint passes on CLAUDE.md**

Run: `buck2 run //tools:prek -- run trailing-whitespace end-of-file-fixer --files CLAUDE.md > /tmp/cm.log 2>&1; grep -Ei "fail" /tmp/cm.log && echo CHECK_DIFF || echo LINT_OK`
Expected: `LINT_OK` (if it printed `CHECK_DIFF`, the hook fixed whitespace/EOF — re-stage and continue).

- [ ] **Step 4: Commit**

```bash
git add CLAUDE.md
git commit -m "docs: document the documentation registers in CLAUDE.md"
```

---

## Task 9: Initial consolidation run (produces the registers + migration)

**Files:**
- Created by the skill: `docs/ROADMAP.md`, `docs/FUTURE.md`, `docs/ISSUES.md`
- Migrated/removed by the skill: `docs/TO_BE_PLANNED.md`, `docs/spike/ICEBERG_ROADMAP.md`, `docs/superpowers/specs/2026-06-06-loom-roadmap.md`

This is the one task with no code — it runs the consolidation logic built in
Task 5 to populate the registers and perform the one-time migration. It is a
real, reviewable artifact (the PR diff), not a unit test.

> **Branch coupling (important):** the `docs-validate` prek hook (Task 4) matches
> `docs/FUTURE.md`, which already exists in *old prose form*. The hook and CI
> `lint` (`prek run --all-files`) only go green once all three registers exist in
> the grammar. So this bootstrap MUST land on the **current feature branch**
> (`spec/docs-registers-consolidation`), in the same PR as Tasks 1–8 — NOT on the
> skill's separate `bot/docs-registers` branch (that branch+PR flow in the skill's
> BLOCK A is for *scheduled* runs). Run the skill's mining/dedupe/classify/
> reconcile/render/migrate/validate STEPS, but commit the result onto this branch
> and let `finishing-a-development-branch` open the single PR.

- [ ] **Step 1: Run the consolidation in dry mode first**

Invoke the `loom-docs-organise` skill with the `diff` argument. Review the drift
summary it prints: the mined candidate items, how they classify into
ROADMAP/FUTURE/ISSUES, and which existing deferred items it judges already shipped.
Expected: a sensible inventory covering the items in `docs/FUTURE.md`,
`docs/TO_BE_PLANNED.md`, the roadmap, and `ICEBERG_ROADMAP.md`, plus code/PR-mined
items. No files written.

- [ ] **Step 2: Run the full consolidation (committing on THIS branch)**

Run the `loom-docs-organise` skill's logic through Step 6 (mine → dedupe →
classify → reconcile → render the three registers → perform the first-run
migration → `bash tools/docs.sh validate` until clean). Then run
`buck2 run //tools:prek -- run --all-files` and stage the register files plus the
migration removals/renames, and commit on `spec/docs-registers-consolidation`
(do NOT switch to `bot/docs-registers`; skip the skill's BLOCK A). The whole
feature lands as ONE PR at the finishing step.

- [ ] **Step 3: Verify the registers validate**

Run: `bash tools/docs.sh validate`
Expected: `docs.sh validate: OK (3 files)`.

- [ ] **Step 4: Spot-check the registers and migration**

Run:
```bash
test -f docs/ROADMAP.md && test -f docs/FUTURE.md && test -f docs/ISSUES.md && echo REGISTERS_OK
test ! -f docs/TO_BE_PLANNED.md && echo TBP_REMOVED
grep -q "Superseded as the live status of record" docs/superpowers/specs/2026-06-06-loom-roadmap.md && echo ROADMAP_BANNERED
bash tools/docs.sh query by-area
grep -c '^- \[ \]' docs/ROADMAP.md docs/FUTURE.md docs/ISSUES.md
```
Expected: `REGISTERS_OK`, `TBP_REMOVED`, `ROADMAP_BANNERED`, a per-area count table, and non-zero open-item counts. Manually read a sample of items to confirm prose survived and `spec:`/`pr:` links look right.

- [ ] **Step 5: Confirm the local full hook run is green**

Since the bootstrap commits on this branch (not a bot-branch PR), confirm the
whole feature is green locally before finishing:
```bash
bash tools/tests/docs_test.sh
buck2 run //tools:prek -- run --all-files > /tmp/docs-prek.log 2>&1; grep -Ei "failed|error" /tmp/docs-prek.log || echo PREK_OK
```
Expected: `docs_test.sh` all green, and `PREK_OK` (the `docs-validate` hook now
passes because all three registers exist in the grammar). The feature is then
finished as a single PR via `superpowers:finishing-a-development-branch`; CI's
`lint` job re-runs these hooks on the PR.

---

## Self-Review notes

- **Spec coverage:** three registers (Task 9 output) · tagged grammar (Tasks 1, 5, 8) · `docs.sh validate`/`query`/`shipped-open` (Tasks 1–3) · prek validate hook (Task 4) · `loom-docs-organise` (Task 5) · `loom-docs-update` (Task 6) · Stop reminder hook (Task 7) · migration (Task 9, driven by Task 5's skill) · CLAUDE.md (Task 8). All spec sections map to a task.
- **Divergence from spec (intentional, minor):** `query stale` from the spec is realised as `shipped-open --stale` (open items with no linked PR); calendar-age filtering is omitted to keep the helper git-independent and testable. Noted here so the executor doesn't treat it as a gap.
- **Type/name consistency:** the TSV column order (`file lineno cb regtype id area status from pr spec title`) is fixed in `_extract` (Task 1) and consumed by the same field positions in `cmd_query`/`cmd_shipped_open` (Tasks 2–3). Subcommand names (`validate`/`query`/`shipped-open`) match between `main`, the tests, the prek hook, and both skills.
- **Markdown lint:** every generated/edited `.md` must end with exactly one trailing newline and no trailing whitespace (`.claude/rules/markdown-lint.md`); Tasks 5/8/9 call this out explicitly.
