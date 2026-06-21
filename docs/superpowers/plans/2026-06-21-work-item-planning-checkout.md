# Work-item planning & checkout Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a distributed-mutex "checkout" over the documentation registers (atomic `refs/claim/<id>` git refs) plus a `loom-work-plan` on-ramp skill, and retire the now-meaningless `in-progress` ROADMAP status.

**Architecture:** Extend `tools/docs.sh` with `claim`/`release`/`claims` subcommands that use `git push` create-if-absent of a `refs/claim/<id>` ref as a server-side mutex; an orphan commit carries claim metadata in its message. Eligibility is gated on the item being open, actionable, and referencing an on-disk spec. Two skills (`loom-work-plan`, `loom-work-checkout`) document the plan→checkout pipeline; both are composition-only. Tests run against a local bare repo used as `origin` — no GitHub.

**Tech Stack:** Bash (`tools/docs.sh`, POSIX-ish with `set -euo pipefail`), git plumbing (`mktree`/`commit-tree`/`push --force-with-lease`/`ls-remote`), GNU `date`, `gh` (behind a stubbable probe), markdown skills.

**Spec:** `docs/superpowers/specs/2026-06-21-work-item-planning-checkout-design.md`

---

## Orientation (read before starting)

- `tools/docs.sh` is the register tool. Structure: top-level vars (`AREAS`, `REGISTERS`), `usage()`, `_extract()` (awk → TSV), `cmd_validate`, `cmd_query`, `cmd_shipped_open`, `main()` (case dispatch), `main "$@"`. Add new helpers and `cmd_*` functions following that style; wire them into `usage()` and `main()`.
- The `_extract` TSV columns are (1-indexed, tab-separated): `file lineno cb regtype id area status from pr spec title`. So `$3`=checkbox char (`" "` open / `"x"` closed), `$4`=register (`roadmap`/`future`/`issues`), `$5`=id, `$7`=status, `$10`=spec.
- The whole script runs under `set -euo pipefail`. Guard commands that may "fail" as control flow: `git ls-remote` returning empty is rc 0; wrap fallible probes with `|| true`; never let an early-`exit` awk consume a long-running pipe (capture into a var first).
- Tests live in `tools/tests/docs_test.sh` (black-box, run via `bash tools/tests/docs_test.sh`). Helpers already defined there: `rc()` (runs cmd, echoes exit code, suppresses output), `out()` (runs cmd, suppresses only stderr), `check desc want_rc got_rc`. Fixtures in `tools/tests/fixtures/`.
- Verified facts you can rely on (do not re-derive):
  - `git push --force-with-lease=refs/claim/<id>: origin <sha>:refs/claim/<id>` creates the ref if absent (rc 0) and is **rejected** (rc 1, "stale info") if the ref already exists. This is the mutex.
  - `git mktree </dev/null` → the empty tree `4b825dc6…`. `printf '…' | git commit-tree <tree>` → an orphan commit whose message is stdin.
  - `git fetch -q origin <ref>` then `git show -s --format=%B FETCH_HEAD` reads a remote ref's commit message.
  - `git push origin :<ref>` deletes a remote ref.
  - GNU `date -u -d "<ISO-8601>" +%s` parses the `since:` timestamp; `date -u +%Y-%m-%dT%H:%M:%SZ` stamps it.

---

## Task 1: Retire the `in-progress` ROADMAP status

**Files:**
- Modify: `tools/docs.sh` (the `cmd_validate` roadmap status set)
- Create: `tools/tests/fixtures/bad-inprogress-ROADMAP.md`
- Modify: `tools/tests/docs_test.sh` (add regression check)
- Modify: `CLAUDE.md:118`, `docs/ROADMAP.md` (header prose), `docs/superpowers/specs/2026-06-20-docs-registers-consolidation-design.md:37,42`, `.claude/skills/loom-docs-organise/SKILL.md:13`

- [ ] **Step 1: Write the failing test fixture**

Create `tools/tests/fixtures/bad-inprogress-ROADMAP.md` with exactly this content (note: ends with one trailing newline, no trailing whitespace):

```markdown
# Roadmap register

_As of test._

## ingest

- [ ] **Was in progress** `{#road-wip area:ingest status:in-progress from:x pr:- spec:-}`
  in-progress is no longer a valid roadmap status.
```

- [ ] **Step 2: Add the regression check to the test script**

In `tools/tests/docs_test.sh`, after the existing line 19 (`check "unresolvable link fails" ...`), add:

```bash
# in-progress was retired from the roadmap status vocab (2026-06-21).
check "in-progress status now rejected" 1 "$(rc bash "$DOCS" validate "$FIX/bad-inprogress-ROADMAP.md")"
```

- [ ] **Step 3: Run the test to verify it FAILS**

Run: `bash tools/tests/docs_test.sh`
Expected: the new check prints `FAIL - in-progress status now rejected (want rc=1 got rc=0)` — because `in-progress` is still accepted by the validator.

- [ ] **Step 4: Remove `in-progress` from the validator**

In `tools/docs.sh`, `cmd_validate`, change the roadmap case line from:

```bash
      roadmap) want="road-"; allowed=" planned in-progress done ";  term=" done " ;;
```

to:

```bash
      roadmap) want="road-"; allowed=" planned done ";              term=" done " ;;
```

- [ ] **Step 5: Run the test to verify it PASSES**

Run: `bash tools/tests/docs_test.sh`
Expected: `ok   - in-progress status now rejected`, and every previously-passing check still `ok`. The script exits 0.

- [ ] **Step 6: Update the docs that advertise the old vocabulary**

`CLAUDE.md` line 118 — change:
```
- `docs/ROADMAP.md` — committed/sequenced work (`status: planned|in-progress|done`).
```
to:
```
- `docs/ROADMAP.md` — committed/sequenced work (`status: planned|done`).
```

`docs/ROADMAP.md` header — change the sentence (around line 5-7):
```
done` items are shipped (kept as the slice-by-slice history); `planned` /
`in-progress` are committed-but-unshipped. Deferred ideas live in
```
to:
```
done` items are shipped (kept as the slice-by-slice history); `planned`
items are committed-but-unshipped. Deferred ideas live in
```

`docs/superpowers/specs/2026-06-20-docs-registers-consolidation-design.md` line 37 — change:
```
| `docs/ROADMAP.md` | Committed / sequenced work — the build plan, what's next | `planned` · `in-progress` · `done` |
```
to:
```
| `docs/ROADMAP.md` | Committed / sequenced work — the build plan, what's next | `planned` · `done` |
```
and line 42 — change:
```
is `promoted` and a matching ROADMAP item appears (`planned` → `in-progress` → `done`); ISSUES
```
to:
```
is `promoted` and a matching ROADMAP item appears (`planned` → `done`); ISSUES
```

`.claude/skills/loom-docs-organise/SKILL.md` line 13 — change:
```
- `docs/ROADMAP.md` — committed/sequenced work (`status: planned|in-progress|done`)
```
to:
```
- `docs/ROADMAP.md` — committed/sequenced work (`status: planned|done`)
```

- [ ] **Step 7: Verify no stray `in-progress` references remain and registers still validate**

Run: `grep -rn "in-progress" CLAUDE.md docs/ROADMAP.md docs/FUTURE.md docs/ISSUES.md docs/superpowers/specs/2026-06-20-docs-registers-consolidation-design.md .claude/skills/`
Expected: the ONLY match is the fixture `tools/tests/fixtures/bad-inprogress-ROADMAP.md` is NOT in the searched paths, so expected output is **empty** (no matches). If `loom-docs-update/SKILL.md` shows a match, apply the same `planned|done` fix there.

Run: `bash tools/docs.sh validate`
Expected: `docs.sh validate: OK (3 files)` — the real registers still pass (none use `in-progress`).

- [ ] **Step 8: Commit**

```bash
git add tools/docs.sh tools/tests/docs_test.sh tools/tests/fixtures/bad-inprogress-ROADMAP.md \
        CLAUDE.md docs/ROADMAP.md docs/superpowers/specs/2026-06-20-docs-registers-consolidation-design.md \
        .claude/skills/loom-docs-organise/SKILL.md
git commit -m "refactor(docs): retire in-progress roadmap status"
```

---

## Task 2: `claim` and `release` primitives + eligibility gate

**Files:**
- Modify: `tools/docs.sh` (new helpers + `cmd_claim`, `cmd_release`; wire `usage()` + `main()`)
- Modify: `tools/tests/docs_test.sh` (bare-repo claim/release tests)

- [ ] **Step 1: Write the failing tests**

In `tools/tests/docs_test.sh`, just before the final `exit $fail` line, add this block:

```bash
# ---- claim / release (git-ref mutex) ----
# A working clone whose `origin` is a local bare repo. Registers + a spec live
# in the working tree; claim reads the working tree and pushes refs to origin.
CT="$(mktemp -d)"
git init -q --bare "$CT/origin.git"
git clone -q "$CT/origin.git" "$CT/wk" 2>/dev/null
WK="$CT/wk"
git -C "$WK" config user.email tester@loom
git -C "$WK" config user.name tester
mkdir -p "$WK/docs/superpowers/specs"
cat > "$WK/docs/ROADMAP.md" <<'EOF'
# Roadmap register

_As of test._

## ingest

- [ ] **Ready item** `{#road-ready area:ingest status:planned from:x pr:- spec:2026-01-01-ready}`
  has a spec on disk, open, planned — claimable.
- [ ] **No spec** `{#road-nospec area:ingest status:planned from:x pr:- spec:-}`
  direction not set — not claimable.
- [ ] **Spec missing** `{#road-specmissing area:ingest status:planned from:x pr:- spec:2099-12-31-absent}`
  spec referenced but not on disk — not claimable.
- [x] **Closed item** `{#road-closed area:ingest status:done from:x pr:#1 spec:2026-01-01-ready}`
  already done — not claimable.
EOF
touch "$WK/docs/superpowers/specs/2026-01-01-ready.md"
cp "$FIX/good-FUTURE.md" "$WK/docs/FUTURE.md"
cp "$FIX/good-ISSUES.md" "$WK/docs/ISSUES.md"

check "claim ready item succeeds"          0 "$(cd "$WK" && rc bash "$DOCS" claim road-ready)"
check "claim created the ref"              1 "$(git -C "$WK" ls-remote origin refs/claim/road-ready | wc -l | tr -d ' ')"
check "claim already-claimed fails"        1 "$(cd "$WK" && rc bash "$DOCS" claim road-ready)"
check "release succeeds"                   0 "$(cd "$WK" && rc bash "$DOCS" release road-ready)"
check "release removed the ref"            0 "$(git -C "$WK" ls-remote origin refs/claim/road-ready | wc -l | tr -d ' ')"
check "release of absent claim is ok"      0 "$(cd "$WK" && rc bash "$DOCS" release road-ready)"
check "claim unknown id fails"             1 "$(cd "$WK" && rc bash "$DOCS" claim road-bogus)"
check "claim closed item fails"            1 "$(cd "$WK" && rc bash "$DOCS" claim road-closed)"
check "claim item without spec fails"      1 "$(cd "$WK" && rc bash "$DOCS" claim road-nospec)"
check "claim item with missing spec fails" 1 "$(cd "$WK" && rc bash "$DOCS" claim road-specmissing)"
check "claim invalid id fails"             2 "$(cd "$WK" && rc bash "$DOCS" claim 'road-BAD!')"
# cleanup happens in Task 3 (CT reused there); leave CT in place for now.
```

- [ ] **Step 2: Run tests to verify they FAIL**

Run: `bash tools/tests/docs_test.sh`
Expected: the new `claim …`/`release …` checks FAIL (e.g. `claim ready item succeeds (want rc=0 got rc=2)`) because `claim`/`release` aren't implemented — `main()` hits `usage()` (rc 2).

- [ ] **Step 3: Add the claim helpers to `tools/docs.sh`**

After the `_extract()` function (before `cmd_validate`), add:

```bash
SPECDIR="docs/superpowers/specs"

_claim_ref(){ printf 'refs/claim/%s' "$1"; }

# True if $1 is a well-formed register id (prefixed + ref-safe).
_valid_id(){
  case "$1" in road-*|fut-*|iss-*) : ;; *) return 1 ;; esac
  printf '%s' "$1" | grep -qE '^[a-z0-9-]+$'
}

# Print the TSV row(s) for item id $1 from the on-disk registers (empty if none).
_item_row(){
  local id="$1" present=() f tsv
  for f in "${REGISTERS[@]}"; do [ -f "$f" ] && present+=("$f"); done
  [ ${#present[@]} -gt 0 ] || return 0
  tsv="$(_extract "${present[@]}")"
  printf '%s\n' "$tsv" | awk -F'\t' -v id="$id" '$5==id'
}

# Read claimant/since from the remote claim ref and report who holds id $1.
_print_holder(){
  local id="$1" ref body who since
  ref="$(_claim_ref "$id")"
  git fetch -q origin "$ref" 2>/dev/null || true
  body="$(git show -s --format=%B FETCH_HEAD 2>/dev/null || true)"
  who="$(printf '%s\n' "$body" | awk -F': ' '/^claimant:/{print $2}')"
  since="$(printf '%s\n' "$body" | awk -F': ' '/^since:/{print $2}')"
  echo "claim: '$id' is held by ${who:-?} since ${since:-?}" >&2
}
```

- [ ] **Step 4: Add `cmd_claim` and `cmd_release`**

After `cmd_shipped_open` (before `main()`), add:

```bash
cmd_claim(){
  local id="${1:-}"
  [ -n "$id" ] || { echo "usage: docs.sh claim <id>" >&2; return 2; }
  _valid_id "$id" || { echo "claim: invalid id '$id' (want road-/fut-/iss- + [a-z0-9-])" >&2; return 2; }
  local row cb reg status spec
  row="$(_item_row "$id")"
  row="${row%%$'\n'*}"   # first matching line only (ids are unique); no pipe → no SIGPIPE/pipefail
  [ -n "$row" ] || { echo "claim: unknown id '$id' (not in any register)" >&2; return 1; }
  cb="$(printf '%s' "$row" | cut -f3)"
  reg="$(printf '%s' "$row" | cut -f4)"
  status="$(printf '%s' "$row" | cut -f7)"
  spec="$(printf '%s' "$row" | cut -f10)"
  [ "$cb" = " " ] || { echo "claim: '$id' is closed ([x]); only open items are claimable" >&2; return 1; }
  case "$reg" in
    roadmap) [ "$status" = planned ]  || { echo "claim: '$id' status '$status' not actionable (want planned)"  >&2; return 1; } ;;
    future)  [ "$status" = deferred ] || { echo "claim: '$id' status '$status' not actionable (want deferred)" >&2; return 1; } ;;
    issues)  [ "$status" = open ]     || { echo "claim: '$id' status '$status' not actionable (want open)"     >&2; return 1; } ;;
    *) echo "claim: '$id' has unknown register '$reg'" >&2; return 1 ;;
  esac
  [ "$spec" != "-" ] || { echo "claim: '$id' has no spec (direction not set); brainstorm a spec first" >&2; return 1; }
  [ -f "$SPECDIR/$spec.md" ] || { echo "claim: spec file '$SPECDIR/$spec.md' missing for '$id'" >&2; return 1; }
  local ref; ref="$(_claim_ref "$id")"
  if [ -n "$(git ls-remote origin "$ref" 2>/dev/null)" ]; then
    echo "claim: '$id' is already claimed" >&2; _print_holder "$id"; return 1
  fi
  local who ts tree commit
  who="$(git config user.email 2>/dev/null || git config user.name 2>/dev/null || echo unknown)"
  ts="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  tree="$(git mktree </dev/null)"
  commit="$(printf 'claim: %s\n\nid: %s\nclaimant: %s\nsince: %s\nregister: %s\nspec: %s\nbranch: work/%s\n' \
    "$id" "$id" "$who" "$ts" "$reg" "$spec" "$id" | git commit-tree "$tree")"
  if git push --force-with-lease="$ref:" origin "$commit:$ref" >/dev/null 2>&1; then
    echo "claimed $id by $who at $ts"
    echo "next: git switch -c work/$id  →  implement  →  open a PR with head work/$id"
  else
    echo "claim: lost race for '$id'" >&2; _print_holder "$id"; return 1
  fi
}

cmd_release(){
  local id="${1:-}"
  [ -n "$id" ] || { echo "usage: docs.sh release <id>" >&2; return 2; }
  _valid_id "$id" || { echo "release: invalid id '$id'" >&2; return 2; }
  local ref; ref="$(_claim_ref "$id")"
  if [ -z "$(git ls-remote origin "$ref" 2>/dev/null)" ]; then
    echo "release: no live claim for '$id' (nothing to do)"; return 0
  fi
  if git push origin ":$ref" >/dev/null 2>&1; then
    git update-ref -d "$ref" 2>/dev/null || true
    echo "released $id"
  else
    echo "release: failed to delete $ref" >&2; return 1
  fi
}
```

- [ ] **Step 5: Wire `usage()` and `main()`**

Change `usage()` from:
```bash
usage(){ echo "usage: docs.sh {validate|query|shipped-open} [args]" >&2; exit 2; }
```
to:
```bash
usage(){ echo "usage: docs.sh {validate|query|shipped-open|claim|release|claims} [args]" >&2; exit 2; }
```

In `main()`, add cases (after the `shipped-open` case):
```bash
    claim) cmd_claim "$@" ;;
    release) cmd_release "$@" ;;
```

- [ ] **Step 6: Run tests to verify they PASS**

Run: `bash tools/tests/docs_test.sh`
Expected: all `claim …`/`release …` checks (except the `claims …` ones, added in Task 3) print `ok`. The `claim invalid id fails` check expects rc=2; all others rc 0/1 as written. Script exits 0 (no FAIL lines).

- [ ] **Step 7: Commit**

```bash
git add tools/docs.sh tools/tests/docs_test.sh
git commit -m "feat(docs): add claim/release work-item mutex via claim refs"
```

---

## Task 3: `claims [--reap]` listing + PR-state probe + staleness

**Files:**
- Modify: `tools/docs.sh` (`_epoch`, `_pr_state`, `cmd_claims`; wire `main()`)
- Create: `tools/tests/fixtures/pr-none.sh`, `tools/tests/fixtures/pr-open.sh`
- Modify: `tools/tests/docs_test.sh` (claims/reap tests + cleanup)

- [ ] **Step 1: Create the PR-probe stub fixtures**

Create `tools/tests/fixtures/pr-none.sh`:
```bash
#!/usr/bin/env bash
echo none
```
Create `tools/tests/fixtures/pr-open.sh`:
```bash
#!/usr/bin/env bash
echo open
```
Make both executable:
```bash
chmod +x tools/tests/fixtures/pr-none.sh tools/tests/fixtures/pr-open.sh
```

- [ ] **Step 2: Write the failing tests**

In `tools/tests/docs_test.sh`, replace the comment line `# cleanup happens in Task 3 (CT reused there); leave CT in place for now.` (added in Task 2) with this block:

```bash
# ---- claims listing + reaping (PR probe stubbed; no GitHub) ----
# Re-claim road-ready (released in the Task-2 block) so there is a live claim.
(cd "$WK" && bash "$DOCS" claim road-ready >/dev/null 2>&1)

# With no open PR (stub 'none') and default grace, a fresh claim lists as pending.
pending="$(cd "$WK" && LOOM_CLAIM_PR_PROBE="$FIX/pr-none.sh" out bash "$DOCS" claims | grep -c 'road-ready' || true)"
check "claims lists a live claim" 1 "$pending"

# With an open PR (stub 'open') the claim shows the 'PR open' state.
openst="$(cd "$WK" && LOOM_CLAIM_PR_PROBE="$FIX/pr-open.sh" out bash "$DOCS" claims | grep 'road-ready' | grep -c 'PR open' || true)"
check "claims shows PR-open state" 1 "$openst"

# A fresh claim (no PR, within grace) is NOT reaped.
notreaped="$(cd "$WK" && LOOM_CLAIM_PR_PROBE="$FIX/pr-none.sh" out bash "$DOCS" claims --reap | grep -c 'reaped' || true)"
check "fresh claim not reaped" 0 "$notreaped"

# A backdated claim (since far in the past, no PR) IS stale and gets reaped.
OLDTREE="$(git -C "$WK" mktree </dev/null)"
OLD="$(printf 'claim: iss-old\n\nid: iss-old\nclaimant: ghost\nsince: 2000-01-01T00:00:00Z\nregister: issues\nspec: -\nbranch: work/iss-old\n' | git -C "$WK" commit-tree "$OLDTREE")"
git -C "$WK" push -q origin "$OLD:refs/claim/iss-old"
reaped="$(cd "$WK" && LOOM_CLAIM_PR_PROBE="$FIX/pr-none.sh" out bash "$DOCS" claims --reap | grep -c 'reaped iss-old' || true)"
check "stale claim is reaped" 1 "$reaped"
check "reap removed the stale ref" 0 "$(git -C "$WK" ls-remote origin refs/claim/iss-old | wc -l | tr -d ' ')"

rm -rf "$CT"
```

- [ ] **Step 3: Run tests to verify they FAIL**

Run: `bash tools/tests/docs_test.sh`
Expected: the `claims …` checks FAIL — `claims` isn't implemented yet, so `out bash "$DOCS" claims` hits `usage()` and prints nothing to stdout, so every `grep -c` yields 0.

- [ ] **Step 4: Add `_epoch` and `_pr_state` helpers**

In `tools/docs.sh`, after `_print_holder` (from Task 2), add:

```bash
CLAIM_GRACE_MIN="${LOOM_CLAIM_GRACE_MIN:-60}"

# Epoch seconds for an ISO-8601 UTC timestamp (0 on parse failure).
_epoch(){ date -u -d "$1" +%s 2>/dev/null || echo 0; }

# Echo open|none|unknown for the work branch PR of id $1. Overridable for tests
# via LOOM_CLAIM_PR_PROBE (a command receiving the id and echoing the state).
_pr_state(){
  local id="$1" n
  if [ -n "${LOOM_CLAIM_PR_PROBE:-}" ]; then "$LOOM_CLAIM_PR_PROBE" "$id"; return; fi
  if command -v gh >/dev/null 2>&1; then
    n="$(gh pr list --head "work/$id" --state open --json number -q 'length' 2>/dev/null || echo 0)"
    [ "${n:-0}" -gt 0 ] && echo open || echo none
  else
    echo unknown
  fi
}
```

- [ ] **Step 5: Add `cmd_claims`**

After `cmd_release` (from Task 2), add:

```bash
cmd_claims(){
  local reap=0; [ "${1:-}" = "--reap" ] && reap=1
  local now grace_s lines
  now="$(date -u +%s)"
  grace_s=$(( CLAIM_GRACE_MIN * 60 ))
  lines="$(git ls-remote origin 'refs/claim/*' 2>/dev/null || true)"
  [ -n "$lines" ] || { echo "no live claims"; return 0; }
  local sha ref id body who since since_s age pr state left
  while read -r sha ref; do
    [ -n "$ref" ] || continue
    id="${ref#refs/claim/}"
    git fetch -q origin "$ref" 2>/dev/null || true
    body="$(git show -s --format=%B FETCH_HEAD 2>/dev/null || true)"
    who="$(printf '%s\n' "$body" | awk -F': ' '/^claimant:/{print $2}')"
    since="$(printf '%s\n' "$body" | awk -F': ' '/^since:/{print $2}')"
    since_s="$(_epoch "$since")"
    age=$(( now - since_s ))
    pr="$(_pr_state "$id")"
    if [ "$pr" = open ]; then
      state="PR open"
    elif [ "$age" -gt "$grace_s" ]; then
      state="stale"
    else
      left=$(( (grace_s - age + 59) / 60 ))
      state="PR pending (${left}m left)"
    fi
    if [ "$reap" = 1 ] && [ "$state" = stale ]; then
      git push origin ":$ref" >/dev/null 2>&1 && echo "reaped $id (stale)"
    else
      printf '%s\t%s\t%dm\t%s\n' "$id" "${who:-?}" "$(( age / 60 ))" "$state"
    fi
  done <<EOF
$lines
EOF
}
```

- [ ] **Step 6: Wire `main()`**

In `main()`, add (after the `release` case from Task 2):
```bash
    claims) cmd_claims "$@" ;;
```

- [ ] **Step 7: Run tests to verify they PASS**

Run: `bash tools/tests/docs_test.sh`
Expected: every check prints `ok` (including the five `claims …`/reap checks); script exits 0.

- [ ] **Step 8: Sanity-check against the real repo**

Run: `bash tools/docs.sh claims`
Expected: `no live claims` (no `refs/claim/*` on the real `origin`) — confirms the command runs cleanly outside the test harness.

- [ ] **Step 9: Commit**

```bash
git add tools/docs.sh tools/tests/docs_test.sh tools/tests/fixtures/pr-none.sh tools/tests/fixtures/pr-open.sh
git commit -m "feat(docs): add claims listing + stale-claim reaping"
```

---

## Task 4: `loom-work-checkout` skill + CLAUDE.md capability note

**Files:**
- Create: `.claude/skills/loom-work-checkout/SKILL.md`
- Modify: `CLAUDE.md` (Documentation registers section)

- [ ] **Step 1: Create the checkout skill**

Create `.claude/skills/loom-work-checkout/SKILL.md` with exactly this content:

```markdown
---
name: loom-work-checkout
description: Claim a documentation-register work item before building it, so two sessions/agents never work the same item. Uses an atomic refs/claim/<id> git ref as a distributed mutex. Use when starting work on a ROADMAP/FUTURE/ISSUES item, when picking up the next planned item, or in a scheduled session that builds register items. The item must already reference an on-disk spec (use loom-work-plan to get an item to that state).
---

Claim a register item, work it on a conventional branch, and let the claim
self-release when the PR lands. The claim is a server-side git mutex
(`refs/claim/<id>`); the registers and grammar are defined in
`docs/superpowers/specs/2026-06-21-work-item-planning-checkout-design.md`.

## Steps

1. **Pick** an open, ready item: `bash tools/docs.sh query open`. Ready means it
   has a `spec:` set (the claim gate rejects `spec:-`); if nothing is ready, use
   `loom-work-plan` first. Avoid items already listed by
   `bash tools/docs.sh claims`.
2. **Claim** it: `bash tools/docs.sh claim <id>`. On success it prints the
   `work/<id>` branch to use. If the claim is lost or already held, pick another.
3. **Work** it on `work/<id>` (`git switch -c work/<id>`). The spec exists by the
   gate's precondition; write a plan if needed (writing-plans), then implement
   (subagent-driven-development / executing-plans).
4. **Open a PR** whose head branch is `work/<id>` — this is what binds the claim to
   the PR. In that PR, close the register item via `loom-docs-update`
   (`- [ ]`→`- [x]`, terminal status, add `pr:#N`).
5. **Release** is automatic: once the PR merges/closes, the claim is reaped by
   `bash tools/docs.sh claims --reap` (run by routines). If you abandon before a
   PR, release explicitly: `bash tools/docs.sh release <id>`.

## Notes

- A claim with no PR older than the grace window (default 60 min,
  `LOOM_CLAIM_GRACE_MIN`) is reapable — open the PR promptly, or re-run
  `claim <id>` to refresh it.
- `claim` refuses items that are closed, non-actionable, already claimed, or whose
  `spec:` is `-` / missing on disk. A direction-less item is not claimable; give it
  a spec first via `loom-work-plan`.
```

- [ ] **Step 2: Add the capability note to CLAUDE.md**

In `CLAUDE.md`, in the "Documentation registers" section, after the `loom-docs-update` skill bullet, add a new bullet:

```markdown
- **`loom-work-checkout`** skill + `tools/docs.sh claim|release|claims` — claim a
  register item before building it. `claim <id>` pushes an atomic `refs/claim/<id>`
  ref (a server-side mutex; the item must reference an on-disk spec); `claims`
  lists live claims and `claims --reap` deletes stale ones (no open `work/<id>` PR
  past a 60-min grace). The upstream `loom-work-plan` skill gets an item to a
  claimable (spec-on-disk) state.
```

- [ ] **Step 3: Verify the skill is well-formed and registers still validate**

Run: `head -5 .claude/skills/loom-work-checkout/SKILL.md`
Expected: shows the YAML frontmatter (`---`, `name:`, `description:`, `---`).

Run: `bash tools/docs.sh validate`
Expected: `docs.sh validate: OK (3 files)`.

- [ ] **Step 4: Commit**

```bash
git add .claude/skills/loom-work-checkout/SKILL.md CLAUDE.md
git commit -m "docs(checkout): add loom-work-checkout skill"
```

---

## Task 5: `loom-work-plan` skill

**Files:**
- Create: `.claude/skills/loom-work-plan/SKILL.md`

- [ ] **Step 1: Create the planning skill**

Create `.claude/skills/loom-work-plan/SKILL.md` with exactly this content:

```markdown
---
name: loom-work-plan
description: Triage the documentation registers, promote a deferred idea into committed roadmap work, and get an item ready to build (a spec exists on disk) — the on-ramp to loom-work-checkout. Use when deciding what to work next, promoting a FUTURE idea to ROADMAP, retiring dead work, or preparing an item for checkout. For a full register rebuild use loom-docs-organise; to close items at completion use loom-docs-update.
---

Turn the backlog into a checkout-ready item — open, actionable, with a spec on
disk — then hand off to `loom-work-checkout`. This skill is composition-only (no
new tooling): it uses `tools/docs.sh` reads, `loom-docs-update` edit mechanics,
`superpowers:brainstorming` for spec authoring, and the `loom-docs-organise`
PR-on-green pattern to land. Registers + grammar:
`docs/superpowers/specs/2026-06-21-work-item-planning-checkout-design.md`.

## Steps

1. **Triage.** Survey open work and recommend what's next:
   - `bash tools/docs.sh query open` (optionally `--area <a>`), `query by-area` for
     the distribution, `shipped-open` / `shipped-open --stale` to reconcile.
   - Flag the un-actionable / in-flight: items with `spec:-` (no direction yet),
     FUTURE ideas with no `road-` promotion, items blocked by `[[links]]` to
     still-open items, and anything in `bash tools/docs.sh claims` (already
     checked out — don't plan over it).
   - Output a short ranked shortlist with one-line reasons (unblocks others, area
     balance, quick win).
2. **Promote / retire.** For a FUTURE idea being committed to: set it
   `- [x] status:promoted`, and mint a ROADMAP item `road-<slug>`,
   `- [ ] status:planned`, carrying the same `area:`, the `spec:` slug (or `-`),
   and a `[[fut-…]]` link back. The reverse is the same mechanics: drop a dead
   FUTURE idea (`- [x] status:dropped`) or demote an abandoned ROADMAP `planned`
   item back to a FUTURE `deferred` idea, recording why in the prose.
   Validate edits: `bash tools/docs.sh validate`.
3. **Ready (direction gate).** Checkout requires `docs/superpowers/specs/<spec>.md`
   to exist. If the chosen item has `spec:-` or the file is missing, invoke
   `superpowers:brainstorming` to author the spec (a human sets direction); stop at
   the committed spec (do not require the writing-plans transition here). Record the
   produced slug on the item's `spec:` tag.
4. **Land.** Bundle the promotion/retirement register edits and the new spec into a
   small `plan/<slug>` PR to `main`, merged on green (the `loom-docs-organise`
   PR-on-green pattern, scoped to one item). After merge the checkout-ready item
   and its direction are visible to every worker.

The item is now claimable: `bash tools/docs.sh claim <id>` (see
`loom-work-checkout`).
```

- [ ] **Step 2: Verify the skill is well-formed**

Run: `head -5 .claude/skills/loom-work-plan/SKILL.md`
Expected: shows the YAML frontmatter (`---`, `name: loom-work-plan`, `description:`, `---`).

- [ ] **Step 3: Commit**

```bash
git add .claude/skills/loom-work-plan/SKILL.md
git commit -m "docs(checkout): add loom-work-plan on-ramp skill"
```

---

## Task 6: Record this feature in the registers

**Files:**
- Modify: `docs/ROADMAP.md`

- [ ] **Step 1: Add a done ROADMAP item for this work**

In `docs/ROADMAP.md`, find or add a `## devx` section (after the `## deploy` section, keeping sections in their existing order is fine — add `## devx` at the end if absent). Add:

```markdown
## devx

- [x] **Work-item planning & checkout** `{#road-work-checkout area:devx status:done from:work-checkout pr:- spec:2026-06-21-work-item-planning-checkout-design}`
  `tools/docs.sh claim/release/claims` give an atomic `refs/claim/<id>` mutex over
  register items (PR-lifecycle release, spec-exists gate); `loom-work-plan` and
  `loom-work-checkout` skills document the plan→checkout pipeline. Retired the
  `in-progress` roadmap status.
```

Note: leave `pr:-` for now; the finishing step / `loom-docs-update` adds the PR number once the PR for this branch is open.

- [ ] **Step 2: Validate the registers**

Run: `bash tools/docs.sh validate`
Expected: `docs.sh validate: OK (3 files)` — the new item parses, `area:devx` is in the vocab, `status:done` matches the `[x]` checkbox, and the `spec:` slug resolves to this design doc on disk.

- [ ] **Step 3: Run the full test suite once more**

Run: `bash tools/tests/docs_test.sh`
Expected: all checks `ok`, exit 0.

- [ ] **Step 4: Commit**

```bash
git add docs/ROADMAP.md
git commit -m "docs(registers): record work-item checkout as done"
```

---

## Final verification (after all tasks)

- [ ] Run the test suite: `bash tools/tests/docs_test.sh` → all `ok`, exit 0.
- [ ] Run the validator: `bash tools/docs.sh validate` → `OK (3 files)`.
- [ ] Run `bash tools/docs.sh claims` → `no live claims`.
- [ ] Run the lint hooks (they police markdown EOF/whitespace too): `buck2 run //tools:prek -- run --all-files` and commit any in-place fixes the hooks make.
- [ ] Confirm no stray `in-progress` outside the dedicated test fixture: `grep -rn "in-progress" docs CLAUDE.md .claude/skills` → only `tools/tests/fixtures/bad-inprogress-ROADMAP.md` would match (and it's under `tools/`, not the searched paths) → expect empty.
