# Claim reap-on-merge Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Reap a work-item claim as soon as its `work/<id>` PR is merged/closed, instead of waiting out the 60-min pre-PR grace window.

**Architecture:** Extend `tools/docs.sh`'s `_pr_state` to return a fourth state `gone` (a `work/<id>` PR exists but is not open → merged/closed), and have `cmd_claims --reap` treat `gone` as immediately reapable (grace gates only the genuinely pre-PR `none` case). Stub-driven black-box tests, no GitHub.

**Tech Stack:** Bash (`tools/docs.sh`, `set -euo pipefail`), `gh` (behind the existing `LOOM_CLAIM_PR_PROBE` stub), `tools/tests/docs_test.sh`.

**Spec:** `docs/superpowers/specs/2026-06-21-work-item-planning-checkout-design.md` (PR-lifecycle release). Issue: `iss-claim-reap-on-merge`.

---

## Orientation

- `_pr_state` (in `tools/docs.sh`) currently echoes `open|none|unknown` for a claim id; it honours `LOOM_CLAIM_PR_PROBE` (a command receiving the id and echoing the state) so tests stub it.
- `cmd_claims` classifies each live claim ref into a `state` and, under `--reap`, deletes refs whose state is `stale`. Current states: `PR open`, `PR unknown (gh unavailable)`, `stale` (no PR + past grace), `PR pending (Nm left)` (no PR + within grace).
- Tests in `tools/tests/docs_test.sh` drive claims against a local bare repo (`$CT`/`$WK`), with PR-probe stub fixtures `tools/tests/fixtures/pr-{none,open,unknown}.sh`. Helpers: `rc`, `out`, `check`.

---

## Task 1: Reap claims whose PR has merged/closed

**Files:**
- Modify: `tools/docs.sh` (`_pr_state`, `cmd_claims`)
- Create: `tools/tests/fixtures/pr-gone.sh`
- Modify: `tools/tests/docs_test.sh`

- [ ] **Step 1: Create the `gone` probe stub fixture**

Create `tools/tests/fixtures/pr-gone.sh`:
```bash
#!/usr/bin/env bash
echo gone
```
Then: `chmod +x tools/tests/fixtures/pr-gone.sh`

- [ ] **Step 2: Write the failing tests**

In `tools/tests/docs_test.sh`, insert the block below **immediately before the `rm -rf "$CT"` line that ends the claims section** (i.e. after the `unknown-PR` block, lines ~166-172 — not right after the `stale claim is reaped` checks; the `unknown` block sits between them). It first releases any leftover claims from the earlier blocks (`road-ready`, `iss-old2`) so it starts from a known-clean slate and reaps exactly the ref under test:

```bash
# Clean slate: drop leftover claims from the earlier reap blocks so this sub-test
# reaps exactly the ref under test (road-ready) and nothing collateral.
(cd "$WK" && bash "$DOCS" release road-ready >/dev/null 2>&1)
(cd "$WK" && bash "$DOCS" release iss-old2 >/dev/null 2>&1)

# A claim whose PR has merged/closed (probe 'gone') is reaped IMMEDIATELY, even
# though it is a fresh claim well within the grace window.
(cd "$WK" && bash "$DOCS" claim road-ready >/dev/null 2>&1)
gonereap="$(cd "$WK" && LOOM_CLAIM_PR_PROBE="$FIX/pr-gone.sh" out bash "$DOCS" claims --reap | grep -c 'reaped road-ready' || true)"
check "merged/closed-PR claim reaped immediately" 1 "$gonereap"
check "reap removed the merged-PR ref" 0 "$(git -C "$WK" ls-remote origin refs/claim/road-ready | wc -l | tr -d ' ')"
# Listing (no --reap) shows the merged/closed state.
(cd "$WK" && bash "$DOCS" claim road-ready >/dev/null 2>&1)
gonelist="$(cd "$WK" && LOOM_CLAIM_PR_PROBE="$FIX/pr-gone.sh" out bash "$DOCS" claims | grep 'road-ready' | grep -c 'merged/closed' || true)"
check "claims shows merged/closed state" 1 "$gonelist"
(cd "$WK" && bash "$DOCS" release road-ready >/dev/null 2>&1)
```

- [ ] **Step 3: Run the tests to verify they FAIL**

Run: `bash tools/tests/docs_test.sh`
Expected: `merged/closed-PR claim reaped immediately` FAILS (got 0) — the stub returns `gone`, which the current `cmd_claims` treats as the `*` (none) branch; a fresh claim is `PR pending`, not reaped. `claims shows merged/closed state` also FAILS.

- [ ] **Step 4: Extend `_pr_state` to detect merged/closed**

In `tools/docs.sh`, replace the `_pr_state` function body's `gh` branch so it reports `gone` when a `work/<id>` PR exists but none is open:

```bash
_pr_state(){
  local id="$1" open_n all_n
  if [ -n "${LOOM_CLAIM_PR_PROBE:-}" ]; then "$LOOM_CLAIM_PR_PROBE" "$id"; return; fi
  if command -v gh >/dev/null 2>&1; then
    open_n="$(gh pr list --head "work/$id" --state open --json number -q 'length' 2>/dev/null || echo 0)"
    if [ "${open_n:-0}" -gt 0 ]; then echo open; return; fi
    # No open PR: was there ever one (merged/closed)? If so the work is done -> gone.
    all_n="$(gh pr list --head "work/$id" --state all --json number -q 'length' 2>/dev/null || echo 0)"
    [ "${all_n:-0}" -gt 0 ] && echo gone || echo none
  else
    echo unknown
  fi
}
```

- [ ] **Step 5: Reap `gone` claims immediately in `cmd_claims`**

In `tools/docs.sh`, `cmd_claims`, replace the state-classification `if/elif/else` and the reap condition. Change the classification from:

```bash
    if [ "$pr" = open ]; then
      state="PR open"
    elif [ "$pr" = unknown ]; then
      state="PR unknown (gh unavailable)"   # never reaped — can't confirm no PR
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
```

to:

```bash
    if [ "$pr" = open ]; then
      state="PR open"
    elif [ "$pr" = gone ]; then
      state="merged/closed"                  # PR done -> reapable now, ignore grace
    elif [ "$pr" = unknown ]; then
      state="PR unknown (gh unavailable)"   # never reaped — can't confirm no PR
    elif [ "$age" -gt "$grace_s" ]; then
      state="stale"
    else
      left=$(( (grace_s - age + 59) / 60 ))
      state="PR pending (${left}m left)"
    fi
    if [ "$reap" = 1 ] && { [ "$state" = stale ] || [ "$state" = "merged/closed" ]; }; then
      git push origin ":$ref" >/dev/null 2>&1 && echo "reaped $id ($state)"
    else
      printf '%s\t%s\t%dm\t%s\n' "$id" "${who:-?}" "$(( age / 60 ))" "$state"
    fi
```

- [ ] **Step 6: Run the tests to verify they PASS**

Run: `bash tools/tests/docs_test.sh`
Expected: every check `ok`, exit 0 — including `merged/closed-PR claim reaped immediately`, `reap removed the merged-PR ref`, and `claims shows merged/closed state`. The earlier `stale claim is reaped` check still reaps with message `reaped iss-old (stale)` (the test greps `reaped iss-old`, which still matches).

- [ ] **Step 7: Commit**

```bash
git add tools/docs.sh tools/tests/docs_test.sh tools/tests/fixtures/pr-gone.sh
git commit -m "fix(docs): reap a claim immediately once its PR merges/closes"
```

---

## Final verification

- [ ] `bash tools/tests/docs_test.sh` → all `ok`, exit 0.
- [ ] `bash tools/docs.sh validate` → `OK (3 files)`.
- [ ] `bash tools/docs.sh claims` → runs cleanly (`no live claims` on a clean origin).
