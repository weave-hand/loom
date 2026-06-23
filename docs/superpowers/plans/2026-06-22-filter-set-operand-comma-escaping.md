# Literal Comma Inside `in:`/`nin:` Set Operands — Backslash Escaping Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Let a caller express a set operand (`in:`/`nin:`) that contains a literal comma — and a literal backslash — via a documented backslash-escaping rule, on both object reads and chain reads, without changing any other operator's behavior. Closes `iss-literal-comma-in-in`.

**Architecture:** A single pure-logic change in `src/services/query-api/src/filter.rs`. Replace the `r.split(',')` loop in the `In | NotIn` arm of `coerce_predicate` with a new `split_set_operands` helper that splits on **unescaped** commas and unescapes each operand (`\,` → `,`, `\\` → `\`; any other escape or a dangling trailing `\` is a hard error). All five filter call sites (`handler.rs:231,613,868,998,1190`) route through `coerce_predicate`, so the single-site fix covers every caller. No control-plane, BUCK, dependency, or lockfile change.

**Tech Stack:** Rust, pure logic (no I/O), covered by the existing `//src/services/query-api:filter-coerce` `rust_test` target (runs on RE — no hermetic fixtures).

## Global Constraints

- **Single split site.** The escaping logic lives only in the `In | NotIn` arm; scalar/null operators are untouched (they don't split, so a comma in a scalar operand like `eq:a,b` is still the literal `"a,b"`). Introducing escaping into scalar parsing would be a silent behavior change for existing callers — explicitly NOT done. (Spec: "What this does NOT change".)
- **Preserve existing error contracts.** The unescaped empty-operand error (`"empty operand in set"`) and the missing-operand errors (`"in/nin require operands"`, `"in/nin require at least one operand"`) keep their exact messages and trigger conditions for unescaped input. The `r.is_empty()` guard stays ahead of the split.
- **Strict escaping.** Backslash is *always* an escape introducer; only `\,` and `\\` are valid. An unknown escape (`\x`) or a dangling trailing `\` is a hard `Err`, not a silent pass-through — every operand stays round-trippable and mistakes surface immediately.
- **Tests are `rust_test` integration targets only** — extend the existing `tests/filter_coerce.rs`; no inline `#[cfg(test)]` module (the `no-inline-tests` hook enforces this), no new BUCK target.
- **No dependency / lockfile / BUCK change** — the fix is pure logic in an already-wired module. Core (`src/control-plane/core/`) is untouched.
- **Markdown lint** — `docs/ISSUES.md` and any plan/spec edits must end with exactly one trailing newline and no trailing whitespace (the `lint` CI job runs `end-of-file-fixer` / `trim trailing whitespace` on all files).

---

### Task 1: Add the escape-aware split and tests (TDD)

**Files:**
- Modify: `src/services/query-api/src/filter.rs` — add `split_set_operands`; use it in the `In | NotIn` arm; update the `coerce_predicate` grammar doc-comment (the "set ops split `rest` on `,`" sentence, ~lines 69-72) to document the `\,` / `\\` escaping rule and the strict-unknown-escape behavior.
- Modify: `src/services/query-api/tests/filter_coerce.rs` — add the contract cases below.

**Interfaces (verified against the codebase):**
- `coerce_predicate(column: &str, logical_ty: &str, raw: &str) -> Result<CallerPredicate, FilterError>` — signature unchanged.
- `coerce_filter(name: &str, logical_ty: &str, raw: &str) -> Result<SqlValue, FilterError>` — unchanged; now receives already-unescaped operand strings (`&str` borrowed from the `Vec<String>` the helper returns).
- New private helper: `fn split_set_operands(rest: &str) -> Result<Vec<String>, &'static str>` — returns operands in order, or a static error message that the existing `bad` closure wraps into `FilterError::BadValue`.
- `CallerPredicate { column, op, values: Vec<SqlValue> }`, `SqlValue::{Int,Double,Bool,Text,Date,Timestamp}`, `control_plane_core::CompareOp::{In,NotIn,Eq,...}` — all unchanged and already in scope in the test file.

- [ ] **Step 1: Write the failing tests first (TDD red).**

Append to `src/services/query-api/tests/filter_coerce.rs` a test function (or functions) covering the spec contract. The headline cases use a string-typed column (`"String"`) so commas are legitimately holdable:

```rust
#[test]
fn set_operands_escape_commas_and_backslashes() {
    // Headline: an escaped comma keeps "a,b" as one operand.
    let p = coerce_predicate("tag", "String", r"in:a\,b,c").unwrap();
    assert_eq!(p.op, CompareOp::In);
    assert_eq!(
        p.values,
        vec![SqlValue::Text("a,b".into()), SqlValue::Text("c".into())]
    );

    // Escaped backslash -> a single literal backslash operand.
    let p2 = coerce_predicate("tag", "String", r"in:a\\b").unwrap();
    assert_eq!(p2.values, vec![SqlValue::Text(r"a\b".into())]);

    // Lone escaped comma is one non-empty operand "," (NOT an empty-operand error).
    let p3 = coerce_predicate("tag", "String", r"in:\,").unwrap();
    assert_eq!(p3.values, vec![SqlValue::Text(",".into())]);

    // nin parity (same arm handles both).
    let p4 = coerce_predicate("tag", "String", r"nin:x\,y,z").unwrap();
    assert_eq!(p4.op, CompareOp::NotIn);
    assert_eq!(
        p4.values,
        vec![SqlValue::Text("x,y".into()), SqlValue::Text("z".into())]
    );
}

#[test]
fn set_operand_escape_errors_and_unescaped_empties() {
    // Unescaped empty segments still error (unchanged contract).
    assert!(coerce_predicate("tag", "String", "in:a,").is_err());
    assert!(coerce_predicate("tag", "String", "in:,a").is_err());
    assert!(coerce_predicate("tag", "String", "in:a,,b").is_err());

    // Unknown escape and dangling escape are hard errors.
    assert!(coerce_predicate("tag", "String", r"in:a\b").is_err());
    assert!(coerce_predicate("tag", "String", r"in:a\").is_err());
}

#[test]
fn scalar_op_does_not_unescape() {
    // Regression guard: escaping must NOT leak into scalar parsing.
    // `eq:a\,b` stays the literal operand `a\,b` (rest taken whole, no split/unescape).
    let p = coerce_predicate("name", "String", r"eq:a\,b").unwrap();
    assert_eq!(p.op, CompareOp::Eq);
    assert_eq!(p.values, vec![SqlValue::Text(r"a\,b".into())]);
}
```

Note on the `scalar_op_does_not_unescape` expectation: scalar ops take `rest` whole and pass it straight to `coerce_filter` with no escaping, so the backslash is preserved verbatim — `a\,b` (4 chars: `a`, `\`, `,`, `b`). This pins that escaping is localized to the set-operator arm.

Run the target and confirm the new tests FAIL (the old `split(',')` mishandles `\,` and the unknown-escape/lone-comma cases) while the existing cases stay green:

```bash
buck2 test //src/services/query-api:filter-coerce > /tmp/fc.log 2>&1; grep -E "Tests finished|FAIL|PASS" /tmp/fc.log
```

- [ ] **Step 2: Implement `split_set_operands` and wire the arm (TDD green).**

Add the helper to `src/services/query-api/src/filter.rs` (above `coerce_predicate`):

```rust
/// Split a set-operator operand list on UNESCAPED commas, unescaping each operand.
/// Recognized escapes: `\,` -> `,` and `\\` -> `\`. Any other escape (`\x`) or a
/// dangling trailing `\` is a hard error - so every string is representable
/// (double a backslash, escape a comma) and ambiguity is rejected, not mangled.
/// Empty operands (an unescaped `,,` or a leading/trailing unescaped `,`) error,
/// preserving the "empty operand in set" contract.
fn split_set_operands(rest: &str) -> Result<Vec<String>, &'static str> {
    let mut out = Vec::new();
    let mut cur = String::new();
    let mut chars = rest.chars();
    while let Some(c) = chars.next() {
        match c {
            '\\' => match chars.next() {
                Some(',') => cur.push(','),
                Some('\\') => cur.push('\\'),
                Some(_) => return Err("invalid escape in set operand (use \\, or \\\\)"),
                None => return Err("dangling escape in set operand"),
            },
            ',' => {
                if cur.is_empty() {
                    return Err("empty operand in set");
                }
                out.push(std::mem::take(&mut cur));
            }
            other => cur.push(other),
        }
    }
    if cur.is_empty() {
        return Err("empty operand in set");
    }
    out.push(cur);
    Ok(out)
}
```

Replace the `In | NotIn` arm body (`filter.rs:113-126`) with:

```rust
Some(o @ (In | NotIn)) => {
    let r = rest.ok_or_else(|| bad("in/nin require operands"))?;
    if r.is_empty() {
        return Err(bad("in/nin require at least one operand"));
    }
    let parts = split_set_operands(r).map_err(bad)?;
    let mut values = Vec::with_capacity(parts.len());
    for part in parts {
        values.push(coerce_filter(column, logical_ty, &part)?);
    }
    Ok(mk(o, values))
}
```

Update the `coerce_predicate` doc-comment so the "set ops split `rest` on `,`" clause documents the escaping: set ops split `rest` on **unescaped** `,`, with `\,` and `\\` as the only valid escapes (any other escape, or a trailing `\`, is rejected).

Re-run the target; all cases (new + existing) must pass:

```bash
buck2 test //src/services/query-api:filter-coerce > /tmp/fc.log 2>&1; grep -E "Tests finished|FAIL|PASS" /tmp/fc.log
```

- [ ] **Step 3: Lint the touched module (clippy).**

```bash
buck2 build '//src/services/query-api:query-api[clippy.txt]' > /tmp/clippy.log 2>&1; cat $(buck2 build '//src/services/query-api:query-api[clippy.txt]' --show-output 2>/dev/null | awk '{print $2}') 2>/dev/null
```

`[clippy.txt]` must be empty (clean). If unsure of the show-output path, `tools/clippy-all.sh` is the authoritative all-targets check. Fix any lint.

- [ ] **Step 4: Format.**

```bash
eval "$(./tools/env.sh)" && cargo fmt -- src/services/query-api/src/filter.rs src/services/query-api/tests/filter_coerce.rs 2>/dev/null || buck2 run //tools:rustfmt -- src/services/query-api/src/filter.rs src/services/query-api/tests/filter_coerce.rs
```

(Or rely on the prek `rustfmt` hook before push; the formatter must leave no diff.)

---

### Task 2: Close the register item

**Files:**
- Modify: `docs/ISSUES.md` — close `iss-literal-comma-in-in`.

- [ ] **Step 1: Close `iss-literal-comma-in-in`.**

Via `loom-docs-update` (or by hand): flip `- [ ]` → `- [x]`, set `status:fixed`, set `pr:#<n>` once the PR number is known (placeholder at finish), and confirm `spec:2026-06-21-filter-set-operand-comma-escaping-design` is set. Add a one-line "Fixed (PR #<n>):" note to the body summarizing the backslash-escaping fix. Ensure the file ends with exactly one trailing newline and no trailing whitespace.

---

### Task 3: Verify the broader build and finish

- [ ] **Step 1: Run the query-api test sweep (and a wider sweep as backstop).**

```bash
buck2 test //src/services/query-api/... > /tmp/qa.log 2>&1; grep -E "Tests finished|FAIL" /tmp/qa.log
```

The fix is pure logic; the `filter-coerce` target is the focused proof, but the query-api sweep confirms nothing else regressed. (Full `//src/...` runs in CI.)

- [ ] **Step 2: Commit and finish.**

```bash
git add src/services/query-api/src/filter.rs src/services/query-api/tests/filter_coerce.rs docs/ISSUES.md docs/superpowers/plans/2026-06-22-filter-set-operand-comma-escaping.md
git commit -m "fix(query-api): backslash-escape commas in in:/nin: set operands"
```

Open a PR with head branch `work/iss-literal-comma-in-in` (binds the claim to the PR) and ensure CI is green.

---

## Self-Review

**1. Spec coverage:**
- Spec "Mechanism — escape-aware split" (`split_set_operands` + rewritten `In | NotIn` arm) → Task 1 Steps 1-2 (helper + arm verbatim from spec). ✓
- Spec contract cases (`in:a\,b,c`, `in:a\\b`, `in:\,`, `nin:x\,y,z`, unescaped empties, `in:a\b`, `in:a\`, scalar `eq:a,b` unaffected) → Task 1 Step 1 tests. The spec's scalar example is `eq:a,b`; the plan additionally pins `eq:a\,b` (the stronger no-leak guard — a backslash in a scalar operand passes through verbatim). Both confirm escaping does not leak into scalar parsing. ✓
- Spec "What this does NOT change" (scalar/null ops, error messages, type coercion, no new dep/BUCK) → Global Constraints. ✓
- Spec "Files" (modify `filter.rs` incl. doc-comment, modify `filter_coerce.rs`, modify `docs/ISSUES.md`, no core/BUCK/lockfile change) → Task 1 + Task 2 files. ✓
- Spec "Testing" (extend `filter-coerce`, no new target; existing set-op cases stay green) → Task 1 + Task 3 Step 1. ✓

**2. Placeholder scan:** The only `<n>` placeholder is the PR number (unknown until the PR opens), resolved at finish via `loom-docs-update`. No TBD/TODO in code steps; the helper and test bodies are complete. ✓

**3. Type consistency:** `split_set_operands(&str) -> Result<Vec<String>, &'static str>` composes with the existing `bad: |m: &str| FilterError` closure via `.map_err(bad)` (the static `&str` coerces to `&str`); `coerce_filter(column, logical_ty, &part)` takes `&str` from each owned `String`. `coerce_predicate`/`CallerPredicate`/`SqlValue`/`CompareOp` signatures are unchanged and match `tests/filter_coerce.rs`'s existing imports. ✓
