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
# in-progress was retired from the roadmap status vocab (2026-06-21).
check "in-progress status now rejected" 1 "$(rc bash "$DOCS" validate "$FIX/bad-inprogress-ROADMAP.md")"

# Uppercase [X] must be parsed, not silently skipped: a bad area in an [X] item must still fail.
check "uppercase [X] item is validated not skipped" 1 "$(rc bash "$DOCS" validate "$FIX/bad-uppercase-ISSUES.md")"
# A missing area: key must report "missing area" (not a misleading shifted-column message).
miss="$(bash "$DOCS" validate "$FIX/bad-missing-area-FUTURE.md" 2>&1 | grep -c 'missing area' || true)"
check "missing area reported clearly" 1 "$miss"

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

exit $fail
