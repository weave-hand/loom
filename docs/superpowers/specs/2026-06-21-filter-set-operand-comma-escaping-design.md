# Literal comma inside `in:`/`nin:` set operands — backslash escaping — Design

> Closes `iss-literal-comma-in-in`. The `in:`/`nin:` set operators split their
> operand list on `,` (`query-api/src/filter.rs:119`, `r.split(',')`), so a string
> operand that itself contains a comma is **unrepresentable** — `in:a,b,c` always
> parses as three operands, never as `["a,b", "c"]`. This slice adds a backslash
> escaping convention so any string operand is expressible, localized to the one
> split site, inheriting to every filter caller through the shared parser.

## Goal

A caller can express a set operand (`in:`/`nin:`) that contains a literal comma —
and a literal backslash — with a documented, unambiguous escaping rule, on both
object reads and chain reads, without changing any other operator's behavior.

## Background (where the comma is lost today)

`coerce_predicate` (`…/query-api/src/filter.rs:73`) parses a query-param filter
value. For the set operators it does:

```rust
Some(o @ (In | NotIn)) => {
    let r = rest.ok_or_else(|| bad("in/nin require operands"))?;
    if r.is_empty() { return Err(bad("in/nin require at least one operand")); }
    let mut values = Vec::new();
    for part in r.split(',') {                       // <-- splits on EVERY comma
        if part.is_empty() { return Err(bad("empty operand in set")); }
        values.push(coerce_filter(column, logical_ty, part)?);
    }
    Ok(mk(o, values))
}
```

Only the set operators split — the scalar ops (`eq`/`ne`/`lt`/`le`/`gt`/`ge`) take
`rest` **whole** as one operand (`filter.rs:128-131`), so a comma in a scalar
operand already works (`eq:a,b` → operand `"a,b"`). And only `PlainString`-repr
operands can legitimately contain a comma — numbers, bools, ISO dates and
timestamps never do (`coerce_filter`, `filter.rs:30-67`). So the defect bites
exactly one place: a string operand inside a set operator.

**All five filter call sites** (object reads + chain reads;
`handler.rs:231,613,868,998,1190`) route through `coerce_predicate`, so the fix at
the single split site covers every caller — no per-site change.

## Mechanism — escape-aware split

Replace the `r.split(',')` loop with a pure helper that splits on **unescaped**
commas and unescapes each operand:

```rust
/// Split a set-operator operand list on UNESCAPED commas, unescaping each operand.
/// Recognized escapes: `\,` → `,` and `\\` → `\`. Any other escape (`\x`) or a
/// dangling trailing `\` is a hard error — so every string is representable
/// (double backslashes, escape commas) and ambiguity is rejected, not mangled.
/// Empty operands (an unescaped `,,` or a leading/trailing unescaped `,`) error,
/// preserving today's "empty operand in set" contract.
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
                if cur.is_empty() { return Err("empty operand in set"); }
                out.push(std::mem::take(&mut cur));
            }
            other => cur.push(other),
        }
    }
    if cur.is_empty() { return Err("empty operand in set"); }
    out.push(cur);
    Ok(out)
}
```

The `In | NotIn` arm then becomes:

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

Notes on the exact contract (these are the test cases below):

- `in:a\,b,c` → operands `["a,b", "c"]` (the headline case).
- `in:a\\b` → operand `["a\b"]` (a literal backslash).
- `in:\,` → operand `[","]` — a single non-empty operand, **not** an empty one;
  the empty-operand guard fires only on an *unescaped* empty segment.
- `in:a,` / `in:,a` / `in:a,,b` → `Err` "empty operand in set" (unchanged behavior;
  the leading/trailing/empty-segment guard still applies to unescaped commas).
- `in:a\b` → `Err` "invalid escape in set operand" (unknown escape — to put a
  literal backslash-then-`b`, write `in:a\\b`).
- `in:a\` → `Err` "dangling escape in set operand".

The pre-existing `r.is_empty()` guard (no operand at all, `in:`) stays ahead of the
split. The escape pass runs **before** `coerce_filter`, so a numeric/date column
that receives an escaped comma fails coercion with its normal type error (commas
aren't valid there) — escaping does not change which *types* accept commas, only
makes a comma expressible for the string columns that can hold one.

### Why strict (error on unknown escape), not lenient pass-through

A lenient rule (`\x` → `\x` for unknown `x`) would make a literal backslash
ambiguous: `a\b` could mean "backslash-b" or a typo'd escape. The strict rule —
backslash is *always* an escape introducer, only `\,` and `\\` are valid — keeps
every operand round-trippable (`\` ⇒ write `\\`, `,` ⇒ write `\,`) and surfaces
mistakes immediately instead of silently materializing a stray backslash into a
filter value. This is the SQL/shell-quoting convention callers expect.

## What this does NOT change

- **Scalar and null operators** — they don't split; their operands (incl. commas)
  are unchanged. No escaping is applied to `eq:`/`ne:`/`lt:`/… operands (an
  `eq:a,b` operand is still the literal `"a,b"`; introducing escaping there would
  be a silent behavior change for existing callers).
- **The empty-operand and missing-operand errors** keep their exact messages and
  trigger conditions for unescaped input.
- **Type coercion** — `coerce_filter` is untouched; it receives already-unescaped
  operand strings.
- **No new dependency**, no `Cargo.lock`/`third-party/BUCK` change, no new BUCK
  target (the fix is pure logic in an already-tested module).

## Testing

All tests are `rust_test` integration targets (no inline `#[cfg(test)]`). The fix
is pure logic, fully covered by the existing **`//src/services/query-api:filter-coerce`**
target (`tests/filter_coerce.rs`) — extend it, no new target.

Add cases asserting the contract above against `coerce_predicate` (a string-typed
column, e.g. logical type `string`):

- **Escaped comma** — `in:a\,b,c` → a 2-operand `In` predicate with values
  `[Text("a,b"), Text("c")]`.
- **Escaped backslash** — `in:a\\b` → 1 operand `Text("a\b")`.
- **Lone escaped comma** — `in:\,` → 1 operand `Text(",")` (not an empty-operand
  error).
- **`nin` parity** — `nin:x\,y,z` → a `NotIn` predicate `[Text("x,y"), Text("z")]`
  (the same arm handles both; assert one `nin` case so the parity is pinned).
- **Unescaped empties still error** — `in:a,`, `in:,a`, `in:a,,b` → `BadValue`
  "empty operand in set".
- **Invalid / dangling escape** — `in:a\b` → `BadValue` "invalid escape …";
  `in:a\` → `BadValue` "dangling escape …".
- **Scalar op unaffected** — `eq:a,b` → 1-operand `Eq` `Text("a,b")` (regression
  guard that escaping did not leak into scalar parsing).

Existing `filter_coerce.rs` set-operator cases stay green unchanged — none of them
use a backslash, so their split behavior is identical.

## Out of scope

- **Choosing a different/configurable delimiter** — rejected during design (it
  only relocates the unrepresentable character). Backslash escaping makes *every*
  string expressible.
- **Repeated-query-param operand syntax** (`?c=in:a&c=in:b`) — would change
  predicate-composition semantics; out of scope.
- **Escaping for scalar operators** — unnecessary (they never split) and would be
  a silent behavior change; explicitly not done.
- A caller-facing API/grammar doc page — none exists today; the operator grammar
  lives in `filter.rs`'s module/function doc-comments, which this slice updates
  (see Files). A standalone docs page is a separate concern.

## Files

- Modify: `src/services/query-api/src/filter.rs` — add `split_set_operands`; use it
  in the `In | NotIn` arm; update the `coerce_predicate` grammar doc-comment
  (the "set ops split `rest` on `,`" sentence, ~lines 69-72) to document the
  `\,` / `\\` escaping rule and the strict-unknown-escape behavior.
- Modify: `src/services/query-api/tests/filter_coerce.rs` — add the cases above.
- Modify: `docs/ISSUES.md` — close `iss-literal-comma-in-in`
  (`[x] status:fixed pr:#<n>`), repoint its `spec:` to this design.
- Core (`src/control-plane/core/`) is **untouched**; no BUCK, dependency, or
  lockfile change.
