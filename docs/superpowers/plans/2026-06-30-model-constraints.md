# Model Constraints Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Give an object type's properties declarable per-value validation rules (numeric range, string length, regex pattern, allowed-value set) and reject writes that violate them on both write paths (the governed typed-insert action and the ingest model-binding land).

**Architecture:** A new pure `core::constraints` module owns the constraint model (`PropertyConstraints` on `PropertyDef`), the define-time declaration check, and a single write-time value validator. The two adapters (`memory`, `postgres`) persist/round-trip the constraints and call the declaration check in `define_type`; the two write paths (`query-api` action, `ingest` land) feed the same validator and surface a structured **422**. Storage is one nullable `jsonb` column on `ontology.property`.

**Tech Stack:** Rust 2024, buck2, sqlx compile-time queries (postgres), `regex` crate (vendored), utoipa OpenAPI, arrow (ingest value reads).

## Global Constraints

- **Spec:** `docs/superpowers/specs/2026-06-30-model-constraints-design.md` — every decision below traces to it.
- **Tests are `rust_test` integration targets only** — never inline `#[cfg(test)]`. Put units in `tests/<name>.rs` wired as a `rust_test`/`loom_fixture_test` target. The `no-inline-tests` prek hook fails on any `#[test]` in `src/**`.
- **Postgres fixture tests use `loom_fixture_test`** (not bare `rust_test`).
- **Clippy is strict** (pedantic + restriction enabled on prod code; test code is exempt from *panic-safety* lints only — NOT from pedantic). No `unwrap`/`expect`/`panic`/`todo`/indexing-slicing/`dbg!` in production code. Use `#[expect(lint, reason = "...")]` for local allows. Avoid `Default::default()` in a context where `default_trait_access` may fire — prefer the explicit `Type::default()`.
- **Markdown** (this plan, any `.md`): end with exactly one trailing newline, no trailing whitespace.
- **sqlx cache** at `src/control-plane/postgres/.sqlx/` is committed and must stay fresh — regenerate with `tools/sqlx-prepare.sh` after any SQL change, or the `query!` macros fail the build and `sqlx-cache-check` fails the test sweep.
- **Don't pipe `buck2 test`/`bxl` through `tail`/`head`** — redirect to a file and grep it.
- **`PropertyConstraints` carries `f64` (range bounds)** → `PropertyDef`, `ObjectType`, and the new constraint types must drop `Eq` (keep `PartialEq`). No code uses these as map/set keys, so this is safe; the first core build confirms it.

---

### Task 1: Core constraint model, validator, and declaration check

**Files:**
- Create: `src/control-plane/core/src/constraints.rs`
- Modify: `src/control-plane/core/src/ontology.rs:24-29` (add field to `PropertyDef`; drop `Eq` from `PropertyDef` + `ObjectType`)
- Modify: `src/control-plane/core/src/lib.rs` (declare `mod constraints;` + re-export)
- Modify: `src/control-plane/core/Cargo.toml` (add `regex`)
- Modify: `src/control-plane/core/BUCK` (add `//third-party:regex` to lib deps; add `constraints` test target)
- Test: `src/control-plane/core/tests/constraints.rs`

**Interfaces:**
- Produces (consumed by every later task):
  - `PropertyConstraints { range: Option<RangeConstraint>, length: Option<LengthConstraint>, pattern: Option<String>, one_of: Option<Vec<String>> }`, with `fn is_empty(&self) -> bool` and `#[derive(Default)]`.
  - `RangeConstraint { min: Option<f64>, max: Option<f64> }`, `LengthConstraint { min: Option<u32>, max: Option<u32> }`.
  - `ConstraintViolation { property: String, rule: ConstraintRule }`; `enum ConstraintRule { Range, Length, Pattern, OneOf }` with `fn as_str(&self) -> &'static str`.
  - `fn validate_constraints(properties: &[PropertyDef]) -> Result<(), ControlPlaneError>` — define-time check (type-applicability + regex validity), aggregating all problems into one `ControlPlaneError::Validation`.
  - `struct PropertyValidator` with `fn from_parts(property: &str, c: &PropertyConstraints) -> Result<PropertyValidator, ControlPlaneError>`, `fn new(prop: &PropertyDef) -> Result<PropertyValidator, ControlPlaneError>`, `fn is_noop(&self) -> bool`, `fn check_str(&self, value: &str, out: &mut Vec<ConstraintViolation>)`, `fn check_num(&self, value: f64, out: &mut Vec<ConstraintViolation>)`.
  - `PropertyDef` gains `pub constraints: PropertyConstraints` (last field, `#[serde(default, skip_serializing_if = "PropertyConstraints::is_empty")]`).

- [ ] **Step 1: Write the failing unit tests** in `src/control-plane/core/tests/constraints.rs`:

```rust
//! Unit tests for the per-value constraint validator and the define-time declaration check.

use control_plane_core::{
    ConstraintRule, LengthConstraint, ObjectType, PropertyConstraints, PropertyDef, PropertyValidator,
    RangeConstraint, TableRef, TypeName, validate_constraints,
};

fn sprop(name: &str, ty: &str, c: PropertyConstraints) -> PropertyDef {
    PropertyDef { name: name.into(), ty: ty.into(), required: false, constraints: c }
}

fn vstr(c: &PropertyConstraints, v: &str) -> Vec<ConstraintRule> {
    let validator = PropertyValidator::from_parts("p", c).expect("compiles");
    let mut out = Vec::new();
    validator.check_str(v, &mut out);
    out.into_iter().map(|x| x.rule).collect()
}

fn vnum(c: &PropertyConstraints, v: f64) -> Vec<ConstraintRule> {
    let validator = PropertyValidator::from_parts("p", c).expect("compiles");
    let mut out = Vec::new();
    validator.check_num(v, &mut out);
    out.into_iter().map(|x| x.rule).collect()
}

#[test]
fn range_below_and_above_fail_inside_passes() {
    let c = PropertyConstraints {
        range: Some(RangeConstraint { min: Some(1.0), max: Some(10.0) }),
        ..PropertyConstraints::default()
    };
    assert_eq!(vnum(&c, 0.5), vec![ConstraintRule::Range]);
    assert_eq!(vnum(&c, 10.5), vec![ConstraintRule::Range]);
    assert!(vnum(&c, 5.0).is_empty());
    assert!(vnum(&c, 1.0).is_empty(), "min is inclusive");
    assert!(vnum(&c, 10.0).is_empty(), "max is inclusive");
}

#[test]
fn length_under_and_over_fail_inside_passes() {
    let c = PropertyConstraints {
        length: Some(LengthConstraint { min: Some(2), max: Some(4) }),
        ..PropertyConstraints::default()
    };
    assert_eq!(vstr(&c, "a"), vec![ConstraintRule::Length]);
    assert_eq!(vstr(&c, "abcde"), vec![ConstraintRule::Length]);
    assert!(vstr(&c, "abc").is_empty());
    assert!(vstr(&c, "abcd").is_empty());
}

#[test]
fn pattern_match_and_no_match() {
    let c = PropertyConstraints {
        pattern: Some(r"^\d{3}$".into()),
        ..PropertyConstraints::default()
    };
    assert!(vstr(&c, "123").is_empty());
    assert_eq!(vstr(&c, "12a"), vec![ConstraintRule::Pattern]);
}

#[test]
fn one_of_in_and_out_of_set() {
    let c = PropertyConstraints {
        one_of: Some(vec!["a".into(), "b".into()]),
        ..PropertyConstraints::default()
    };
    assert!(vstr(&c, "a").is_empty());
    assert_eq!(vstr(&c, "z"), vec![ConstraintRule::OneOf]);
}

#[test]
fn multiple_violations_on_one_value_aggregate() {
    let c = PropertyConstraints {
        length: Some(LengthConstraint { min: Some(5), max: None }),
        pattern: Some(r"^\d+$".into()),
        ..PropertyConstraints::default()
    };
    // "ab" is too short AND not all-digits → both rules fire.
    let rules = vstr(&c, "ab");
    assert!(rules.contains(&ConstraintRule::Length));
    assert!(rules.contains(&ConstraintRule::Pattern));
    assert_eq!(rules.len(), 2);
}

#[test]
fn empty_constraints_is_noop() {
    let c = PropertyConstraints::default();
    assert!(c.is_empty());
    let validator = PropertyValidator::from_parts("p", &c).expect("compiles");
    assert!(validator.is_noop());
    assert!(vstr(&c, "anything").is_empty());
    assert!(vnum(&c, 999.0).is_empty());
}

#[test]
fn declaration_rejects_range_on_string() {
    let props = vec![sprop(
        "name",
        "String",
        PropertyConstraints { range: Some(RangeConstraint { min: Some(0.0), max: None }), ..PropertyConstraints::default() },
    )];
    let err = validate_constraints(&props).expect_err("range on string rejected");
    assert!(err.to_string().contains("range"), "message names the rule: {err}");
}

#[test]
fn declaration_rejects_length_on_numeric() {
    let props = vec![sprop(
        "age",
        "Long",
        PropertyConstraints { length: Some(LengthConstraint { min: Some(1), max: None }), ..PropertyConstraints::default() },
    )];
    validate_constraints(&props).expect_err("length on numeric rejected");
}

#[test]
fn declaration_rejects_invalid_regex() {
    let props = vec![sprop(
        "name",
        "String",
        PropertyConstraints { pattern: Some("(".into()), ..PropertyConstraints::default() },
    )];
    let err = validate_constraints(&props).expect_err("invalid regex rejected");
    assert!(err.to_string().contains("regex"), "message names regex: {err}");
}

#[test]
fn declaration_accepts_valid_and_empty() {
    let props = vec![
        sprop("name", "String", PropertyConstraints { pattern: Some(r"^\w+$".into()), one_of: None, length: Some(LengthConstraint { min: Some(1), max: Some(99) }), range: None }),
        sprop("age", "Long", PropertyConstraints { range: Some(RangeConstraint { min: Some(0.0), max: Some(150.0) }), ..PropertyConstraints::default() }),
        sprop("plain", "String", PropertyConstraints::default()),
    ];
    validate_constraints(&props).expect("valid declarations accepted");
}

#[test]
fn object_type_without_eq_still_partial_eq() {
    // Regression guard: ObjectType keeps PartialEq after the Eq drop.
    let t = ObjectType {
        name: TypeName("T".into()),
        properties: vec![],
        derived: vec![],
        table: TableRef { schema: "main".into(), name: "t".into() },
        identity: None,
    };
    assert_eq!(t.clone(), t);
}
```

- [ ] **Step 2: Run the tests to verify they fail to compile** (types don't exist yet)

Run: `buck2 test //src/control-plane/core:constraints > /tmp/t.log 2>&1; grep -E "error|FAIL|Tests finished" /tmp/t.log`
Expected: build failure (unresolved `constraints`, `PropertyConstraints`, etc.) — the target may not even exist yet (added in Step 6).

- [ ] **Step 3: Create `src/control-plane/core/src/constraints.rs`** with the full module:

```rust
//! Per-value validation rules ("model constraints") declared on an object type's
//! properties, plus the define-time declaration check and the write-time value
//! validator that both write paths (query-api action, ingest land) drive.
//!
//! Pure (no I/O). `range` applies to numeric properties; `length`/`pattern`/`one_of`
//! apply to string properties. Applicability + regex validity are enforced at
//! `define_type` time (`validate_constraints`); the write-time `PropertyValidator`
//! compiles the regex ONCE and reuses it across every value in a pass.

use crate::error::ControlPlaneError;
use crate::logical_type::{BaseType, resolve_logical};
use crate::ontology::PropertyDef;

/// Inclusive numeric bound. Applies to numeric properties (`Integer`/`Long`/`Double`).
#[derive(Clone, Debug, Default, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct RangeConstraint {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub min: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max: Option<f64>,
}

/// Inclusive length bound, in Unicode scalar values. Applies to string properties.
#[derive(Clone, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct LengthConstraint {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub min: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max: Option<u32>,
}

/// The declarable per-value rules on one property. All-optional; an all-`None` value
/// (the [`Default`]) is unconstrained — [`is_empty`](Self::is_empty) is `true` and every
/// check is a no-op, so existing types deserialize and write unchanged.
#[derive(Clone, Debug, Default, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct PropertyConstraints {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub range: Option<RangeConstraint>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub length: Option<LengthConstraint>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pattern: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub one_of: Option<Vec<String>>,
}

impl PropertyConstraints {
    /// `true` when no rule is declared (the default) — a guaranteed validation no-op.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.range.is_none()
            && self.length.is_none()
            && self.pattern.is_none()
            && self.one_of.is_none()
    }
}

/// Which rule a value violated. Renders to a stable wire token via [`as_str`](Self::as_str).
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum ConstraintRule {
    Range,
    Length,
    Pattern,
    OneOf,
}

impl ConstraintRule {
    /// Stable machine-readable token for HTTP bodies.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            ConstraintRule::Range => "range",
            ConstraintRule::Length => "length",
            ConstraintRule::Pattern => "pattern",
            ConstraintRule::OneOf => "one_of",
        }
    }
}

/// A single value's violation of one rule on `property`. The offending value is NOT
/// carried (confidentiality posture — the caller already holds it).
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ConstraintViolation {
    pub property: String,
    pub rule: ConstraintRule,
}

/// `true` if `ty` resolves to a numeric base type.
fn is_numeric(ty: &str) -> bool {
    matches!(
        resolve_logical(ty),
        Some(BaseType::Integer | BaseType::Long | BaseType::Double)
    )
}

/// `true` if `ty` resolves to the string base type.
fn is_string(ty: &str) -> bool {
    matches!(resolve_logical(ty), Some(BaseType::String))
}

/// Define-time check: every property's declared constraints are applicable to its logical
/// type and any `pattern` is a valid regex. Aggregates ALL problems into one
/// `ControlPlaneError::Validation`. A property with empty constraints is skipped. Both
/// adapters call this at the top of `define_type`.
///
/// # Errors
/// `ControlPlaneError::Validation` if a `range` is declared on a non-numeric property, a
/// `length`/`pattern`/`one_of` on a non-string property, or a `pattern` fails to compile.
pub fn validate_constraints(properties: &[PropertyDef]) -> Result<(), ControlPlaneError> {
    let mut errs: Vec<String> = Vec::new();
    for p in properties {
        let c = &p.constraints;
        if c.is_empty() {
            continue;
        }
        if c.range.is_some() && !is_numeric(&p.ty) {
            errs.push(format!(
                "property `{}`: a `range` constraint requires a numeric type, but its type is `{}`",
                p.name, p.ty
            ));
        }
        if (c.length.is_some() || c.pattern.is_some() || c.one_of.is_some()) && !is_string(&p.ty) {
            errs.push(format!(
                "property `{}`: `length`/`pattern`/`one_of` constraints require a string type, but its type is `{}`",
                p.name, p.ty
            ));
        }
        if let Some(pat) = &c.pattern
            && let Err(e) = regex::Regex::new(pat)
        {
            errs.push(format!("property `{}`: invalid regex pattern: {e}", p.name));
        }
    }
    if errs.is_empty() {
        Ok(())
    } else {
        Err(ControlPlaneError::Validation(errs.join("; ")))
    }
}

/// A compiled, ready-to-apply view of one property's constraints. The `pattern` regex is
/// compiled ONCE at construction and reused for every value (so a batch validates without
/// recompiling per row). Drive both write paths through `check_str` / `check_num`.
pub struct PropertyValidator {
    property: String,
    range: Option<RangeConstraint>,
    length: Option<LengthConstraint>,
    pattern: Option<regex::Regex>,
    one_of: Option<Vec<String>>,
}

impl PropertyValidator {
    /// Build from a property's name + constraints, compiling the regex once.
    ///
    /// # Errors
    /// `ControlPlaneError::Validation` if the `pattern` fails to compile (define-time
    /// validation rejects that, so on a stored constraint this is unreachable; handled
    /// defensively rather than panicking).
    pub fn from_parts(
        property: &str,
        constraints: &PropertyConstraints,
    ) -> Result<PropertyValidator, ControlPlaneError> {
        let pattern = match &constraints.pattern {
            Some(p) => Some(regex::Regex::new(p).map_err(|e| {
                ControlPlaneError::Validation(format!("property `{property}`: invalid regex: {e}"))
            })?),
            None => None,
        };
        Ok(PropertyValidator {
            property: property.to_string(),
            range: constraints.range.clone(),
            length: constraints.length.clone(),
            pattern,
            one_of: constraints.one_of.clone(),
        })
    }

    /// Convenience: build from a `PropertyDef`.
    ///
    /// # Errors
    /// As [`from_parts`](Self::from_parts).
    pub fn new(prop: &PropertyDef) -> Result<PropertyValidator, ControlPlaneError> {
        PropertyValidator::from_parts(&prop.name, &prop.constraints)
    }

    /// `true` if no rule applies — callers skip the column/value entirely.
    #[must_use]
    pub fn is_noop(&self) -> bool {
        self.range.is_none()
            && self.length.is_none()
            && self.pattern.is_none()
            && self.one_of.is_none()
    }

    /// Apply the string-applicable rules (`length`, `pattern`, `one_of`) to `value`,
    /// appending a [`ConstraintViolation`] per failed rule. Numeric `range` is ignored.
    pub fn check_str(&self, value: &str, out: &mut Vec<ConstraintViolation>) {
        if let Some(len) = &self.length {
            let n = u32::try_from(value.chars().count()).unwrap_or(u32::MAX);
            if len.min.is_some_and(|m| n < m) || len.max.is_some_and(|m| n > m) {
                out.push(self.violation(ConstraintRule::Length));
            }
        }
        if let Some(re) = &self.pattern
            && !re.is_match(value)
        {
            out.push(self.violation(ConstraintRule::Pattern));
        }
        if let Some(set) = &self.one_of
            && !set.iter().any(|v| v == value)
        {
            out.push(self.violation(ConstraintRule::OneOf));
        }
    }

    /// Apply the numeric-applicable rule (`range`) to `value`. String rules are ignored.
    pub fn check_num(&self, value: f64, out: &mut Vec<ConstraintViolation>) {
        if let Some(r) = &self.range
            && (r.min.is_some_and(|m| value < m) || r.max.is_some_and(|m| value > m))
        {
            out.push(self.violation(ConstraintRule::Range));
        }
    }

    fn violation(&self, rule: ConstraintRule) -> ConstraintViolation {
        ConstraintViolation {
            property: self.property.clone(),
            rule,
        }
    }
}
```

- [ ] **Step 4: Add the field to `PropertyDef` and drop `Eq`** in `src/control-plane/core/src/ontology.rs`. Replace the `PropertyDef` struct (lines 24-29):

```rust
/// A logical property of an object type. `ty` is the ontology's logical type
/// (loom's vocabulary), NOT the physical column type. `constraints` (default empty)
/// declares optional per-value validation rules enforced on every write path.
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct PropertyDef {
    pub name: String,
    pub ty: String,
    pub required: bool,
    #[serde(default, skip_serializing_if = "crate::constraints::PropertyConstraints::is_empty")]
    pub constraints: crate::constraints::PropertyConstraints,
}
```

Then in the same file change `ObjectType`'s derive (line 32) from `#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]` to drop `Eq`:

```rust
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct ObjectType {
```

- [ ] **Step 5: Wire the module into `src/control-plane/core/src/lib.rs`.** Add `mod constraints;` in the module list (alphabetical, near `mod compact_job;`) and add a re-export block:

```rust
pub use constraints::{
    ConstraintRule, ConstraintViolation, LengthConstraint, PropertyConstraints, PropertyValidator,
    RangeConstraint, validate_constraints,
};
```

- [ ] **Step 6: Add the `regex` dep and the test target.**

In `src/control-plane/core/Cargo.toml`, add to `[dependencies]`:
```toml
regex = "1"
```

In `src/control-plane/core/BUCK`, add `"//third-party:regex",` to the `core` `rust_library` `deps`, and append a new test target (mirror the `page` target):
```python
rust_test(
    name = "constraints",
    crate = "constraints",
    srcs = ["tests/constraints.rs"],
    crate_root = "tests/constraints.rs",
    edition = "2024",
    visibility = ["PUBLIC"],
    deps = [":core"],
)
```

- [ ] **Step 7: Regenerate third-party rules and confirm no drift.**

Run: `eval "$(./tools/env.sh)" && cargo generate-lockfile && ./tools/buckify.sh && git status --short third-party/BUCK Cargo.lock`
Expected: `regex` is vendored only as the internal `:regex-1` target today (no `//third-party:regex` alias yet), so buckify **will add** a new `alias(name = "regex", actual = ":regex-1", visibility = ["PUBLIC"])` block to `third-party/BUCK` — that is the expected, correct diff to KEEP and commit (not drift to discard). `Cargo.lock` should be unchanged (regex 1.x already resolved transitively); if it changes, diff it against the merge-base for any native/`links` crate downgrade before continuing (the reindeer-update footgun).

- [ ] **Step 8: Build core and run the validator tests.**

Run: `buck2 build //src/control-plane/core:core > /tmp/b.log 2>&1; grep -iE "error|BUILD SUCCEEDED|finished" /tmp/b.log`
Expected: core builds. If a downstream-of-`ObjectType` core type needed `Eq`, the error names it — drop `Eq` there too (verified: nothing uses these as keys).

Run: `buck2 test //src/control-plane/core:constraints > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL|error" /tmp/t.log`
Expected: all constraints tests PASS.

- [ ] **Step 9: Clippy-clean the new module.**

Run: `buck2 build '//src/control-plane/core:core[clippy.txt]' > /tmp/c.log 2>&1; cat $(buck2 build '//src/control-plane/core:core[clippy.txt]' --show-output 2>/dev/null | awk '{print $2}')` (or simply `bash tools/clippy-all.sh 2>&1 | grep -A3 constraints`)
Expected: no clippy findings on `constraints.rs`. Fix any with `#[expect(..., reason = "...")]` or a code change.

- [ ] **Step 10: Commit**

```bash
git add src/control-plane/core third-party/BUCK Cargo.lock
git commit -m "feat(core): property constraints model, declaration check, value validator"
```

---

### Task 2: Mechanically add the new field to every `PropertyDef` literal

Adding `constraints` to `PropertyDef` breaks every struct-literal construction tree-wide (~170 sites, almost all one per test file). This task restores a green `buck2 build //src/...` by inserting `constraints: control_plane_core::PropertyConstraints::default()` into each literal, then formatting.

**Files:**
- Modify: every `src/**/*.rs` that constructs a `PropertyDef { .. }` literal (NOT the struct definition, NOT `DerivedPropertyDef`).
- Create (scratch, not committed): `/tmp/claude-0/.../scratchpad/add_constraints_field.py`

**Interfaces:**
- Consumes: `control_plane_core::PropertyConstraints` (Task 1).
- Produces: a tree that builds; no behavior change (every inserted value is the empty default).

- [ ] **Step 1: Write the transform script** at the scratchpad path:

```python
import pathlib, sys

ROOT = pathlib.Path("src")
NEEDLE = "PropertyDef {"
INSERT = "constraints: control_plane_core::PropertyConstraints::default()"

def is_word(ch: str) -> bool:
    return ch.isalnum() or ch == "_"

changed_files = []
for path in ROOT.rglob("*.rs"):
    text = path.read_text()
    out = []
    i = 0
    changed = False
    while True:
        j = text.find(NEEDLE, i)
        if j == -1:
            out.append(text[i:])
            break
        # Skip "DerivedPropertyDef {" (NEEDLE is a substring) and the struct/enum def.
        prev = text[j - 1] if j > 0 else " "
        prefix = text[:j].rstrip()
        if is_word(prev) or prefix.endswith("struct") or prefix.endswith("enum"):
            out.append(text[i : j + len(NEEDLE)])
            i = j + len(NEEDLE)
            continue
        # Brace-match from the '{' that ends NEEDLE.
        brace_start = j + len(NEEDLE) - 1
        depth = 0
        k = brace_start
        while k < len(text):
            if text[k] == "{":
                depth += 1
            elif text[k] == "}":
                depth -= 1
                if depth == 0:
                    break
            k += 1
        body = text[brace_start + 1 : k]
        new_body = body.rstrip()
        if new_body and not new_body.endswith(","):
            new_body += ","
        new_body += " " + INSERT + ","
        out.append(text[i : brace_start + 1])  # up to and including '{'
        out.append(new_body)
        out.append("}")
        i = k + 1
        changed = True
    if changed:
        new_text = "".join(out)
        if new_text != text:
            path.write_text(new_text)
            changed_files.append(str(path))

for f in changed_files:
    print(f)
```

- [ ] **Step 2: Run the script** (from repo root):

Run: `python3 /tmp/claude-0/*/scratchpad/add_constraints_field.py` (use the hermetic interpreter if `python3` is absent: `eval "$(./tools/env.sh)" && python3 ...`)
Expected: prints the list of edited files (~40). Spot-check one: `grep -n -A5 "PropertyDef {" src/services/query-api/tests/action_e2e.rs | head` shows the new field present.

- [ ] **Step 3: Format the edited files** so the `lint` rustfmt hook passes:

Run: `eval "$(./tools/env.sh)" && rustfmt --edition 2024 $(python3 /tmp/claude-0/*/scratchpad/add_constraints_field.py 2>/dev/null; git diff --name-only -- 'src/**/*.rs')`
(Simpler: `git diff --name-only | grep '\.rs$' | xargs rustfmt --edition 2024`.)
Expected: files reflow to canonical form; no errors.

- [ ] **Step 4: Build the whole tree.**

Run: `buck2 build //src/... > /tmp/b.log 2>&1; grep -iE "error\[|error:|BUILD SUCCEEDED|Build ID" /tmp/b.log | head`
Expected: BUILD SUCCEEDED. If a literal was missed (e.g. an unusual macro context), the error names the file/line — add the field by hand.

- [ ] **Step 5: Run the formatter check the lint job uses.**

Run: `buck2 run //tools:prek -- run rustfmt --all-files > /tmp/f.log 2>&1; tail -5 /tmp/f.log` (if `rustfmt` hook id differs, run `--all-files` and inspect)
Expected: rustfmt hook passes (no diff). Commit any hook-applied change.

- [ ] **Step 6: Commit**

```bash
git add -A src
git commit -m "refactor: thread empty PropertyConstraints through all PropertyDef literals"
```

---

### Task 3: Postgres adapter — persist + round-trip constraints, enforce declaration check

**Files:**
- Create: `src/control-plane/postgres/migrations/0022_property_constraints.sql`
- Modify: `src/control-plane/postgres/src/ontology.rs` (`define_type` lines ~13-76, `get_type` lines ~123-174)
- Modify: `src/control-plane/postgres/.sqlx/` (regenerated, committed)
- Test: existing `src/control-plane/postgres/tests/ontology.rs` (no change yet — exercised by the Task 4 contract) + `tests/sqlx_cache.rs` (runs automatically)

**Interfaces:**
- Consumes: `control_plane_core::{PropertyConstraints, validate_constraints}`.
- Produces: postgres `Ontology::define_type` rejects bad declarations (`ControlPlaneError::Validation`) and round-trips constraints; `get_type`/`list_types` return them.

- [ ] **Step 1: Write the migration** `0022_property_constraints.sql`:

```sql
-- Per-value validation rules for an object-type property (model constraints).
-- Nullable: existing rows and unconstrained properties store NULL (the empty default).
alter table ontology.property add column constraints jsonb;
```

(Note: `//third-party:serde_json` is already on the `postgres` `rust_library` deps — no BUCK dep edit needed. `control_plane_core` is already a dep.)

- [ ] **Step 2: Enforce the declaration check + persist constraints in `define_type`.**

At the top of `define_type` (before the transaction / first write), add:
```rust
control_plane_core::validate_constraints(&ty.properties)?;
```
Change the per-property INSERT (the loop around lines 35-48) to compute and bind the jsonb:
```rust
for (i, p) in ty.properties.iter().enumerate() {
    let constraints = if p.constraints.is_empty() {
        None
    } else {
        Some(
            serde_json::to_value(&p.constraints)
                .map_err(|e| ControlPlaneError::Serialization(e.to_string()))?,
        )
    };
    sqlx::query!(
        "insert into ontology.property (type_name, ordinal, name, ty, required, constraints) \
         values ($1, $2, $3, $4, $5, $6)",
        ty.name.0,
        i32::try_from(i).unwrap_or(i32::MAX),
        p.name,
        p.ty,
        p.required,
        constraints,
    )
    .execute(&mut *tx)
    .await
    .map_err(backend)?;
}
```
(Keep the existing `i as i32` style if that is what the surrounding code uses and clippy allows it — match the file. Use whichever `execute(...)` receiver the existing loop uses: `&mut *tx`.)

- [ ] **Step 3: Read constraints back in `get_type`.** Change the property SELECT (lines ~132-139) to add the column:
```rust
let props = sqlx::query!(
    "select name, ty, required, constraints from ontology.property \
     where type_name = $1 order by ordinal",
    name.0,
)
```
And the reconstruction (lines ~163-170):
```rust
properties: props
    .into_iter()
    .map(|r| {
        let constraints = match r.constraints {
            Some(v) => serde_json::from_value(v)
                .map_err(|e| ControlPlaneError::Serialization(e.to_string()))?,
            None => control_plane_core::PropertyConstraints::default(),
        };
        Ok::<_, ControlPlaneError>(PropertyDef {
            name: r.name,
            ty: r.ty,
            required: r.required,
            constraints,
        })
    })
    .collect::<Result<Vec<_>, _>>()?,
```
(If the surrounding `get_type` builds the `ObjectType` in a way that does not allow `?` inside the `.map`, hoist the property reconstruction into a `let properties = { ... }?;` above the struct literal. Mirror the existing error-handling idiom in the file.)

- [ ] **Step 4: Regenerate the sqlx cache.**

Run: `tools/sqlx-prepare.sh > /tmp/sqlx.log 2>&1; tail -20 /tmp/sqlx.log`
Expected: completes; `git status --short src/control-plane/postgres/.sqlx` shows changed/added `query-*.json` (the modified `define_type` INSERT + `get_type` SELECT). If the script can't boot postgres / install sqlx-cli in this environment, STOP and report — this is the top environmental risk.

- [ ] **Step 5: Build + test the postgres adapter.**

Run: `buck2 test //src/control-plane/postgres:ontology //src/control-plane/postgres:sqlx-cache-check > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL|error" /tmp/t.log`
Expected: both PASS (round-trip of empty constraints still works; cache fresh).

- [ ] **Step 6: Clippy + commit.**

Run: `buck2 build '//src/control-plane/postgres:postgres[clippy.txt]' > /tmp/c.log 2>&1` and confirm clean.
```bash
git add src/control-plane/postgres
git commit -m "feat(postgres): persist + round-trip property constraints; reject bad declarations"
```

---

### Task 4: Memory adapter declaration check + testkit constraints contract

**Files:**
- Modify: `src/control-plane/memory/src/ontology.rs` (`define_type`)
- Modify: `src/control-plane/testkit/src/lib.rs` (`ontology_contract`)
- Test: `src/control-plane/memory/tests/ontology.rs` + `src/control-plane/postgres/tests/ontology.rs` (both already call `ontology_contract`; re-run, no edit needed)

**Interfaces:**
- Consumes: `control_plane_core::{validate_constraints, PropertyConstraints, RangeConstraint, LengthConstraint}`.
- Produces: both adapters reject bad declarations and round-trip constraints, asserted once in the shared contract.

- [ ] **Step 1: Enforce the declaration check in memory `define_type`.** At the top of the memory `define_type` (before inserting into the map):
```rust
control_plane_core::validate_constraints(&ty.properties)?;
```

- [ ] **Step 2: Extend `ontology_contract`** in `testkit/src/lib.rs` — after the existing define/get round-trip block, add a constraints round-trip and a rejection check:

```rust
// --- Model constraints: round-trip + define-time rejection. ---
use control_plane_core::{LengthConstraint, PropertyConstraints, RangeConstraint};

let constrained = ObjectType {
    name: tn("Account"),
    table: tref("main", "account"),
    properties: vec![
        PropertyDef {
            name: "id".into(),
            ty: "Long".into(),
            required: true,
            constraints: PropertyConstraints {
                range: Some(RangeConstraint { min: Some(1.0), max: None }),
                ..PropertyConstraints::default()
            },
        },
        PropertyDef {
            name: "code".into(),
            ty: "String".into(),
            required: true,
            constraints: PropertyConstraints {
                length: Some(LengthConstraint { min: Some(2), max: Some(8) }),
                pattern: Some(r"^[A-Z]+$".into()),
                one_of: None,
                range: None,
            },
        },
        PropertyDef {
            name: "note".into(),
            ty: "String".into(),
            required: false,
            constraints: PropertyConstraints::default(),
        },
    ],
    derived: vec![],
    identity: Some("id".into()),
};
o.define_type(constrained.clone())
    .await
    .expect("define constrained type");
assert_eq!(
    o.get_type(&tn("Account")).await.unwrap(),
    constrained,
    "constraints round-trip unchanged"
);

// A range on a string property is rejected at define time.
let bad = ObjectType {
    name: tn("BadType"),
    table: tref("main", "bad"),
    properties: vec![PropertyDef {
        name: "name".into(),
        ty: "String".into(),
        required: false,
        constraints: PropertyConstraints {
            range: Some(RangeConstraint { min: Some(0.0), max: None }),
            ..PropertyConstraints::default()
        },
    }],
    derived: vec![],
    identity: None,
};
assert!(
    matches!(
        o.define_type(bad).await,
        Err(control_plane_core::ControlPlaneError::Validation(_))
    ),
    "range on a string property is a define-time Validation error"
);

// An invalid regex is rejected at define time.
let bad_re = ObjectType {
    name: tn("BadRegex"),
    table: tref("main", "bad_regex"),
    properties: vec![PropertyDef {
        name: "code".into(),
        ty: "String".into(),
        required: false,
        constraints: PropertyConstraints { pattern: Some("(".into()), ..PropertyConstraints::default() },
    }],
    derived: vec![],
    identity: None,
};
assert!(
    matches!(
        o.define_type(bad_re).await,
        Err(control_plane_core::ControlPlaneError::Validation(_))
    ),
    "invalid regex is a define-time Validation error"
);
```
(Use the `tn`/`tref` helpers already in scope in `ontology_contract`. If the imports `use control_plane_core::{...}` are at module top, fold the new names there instead of a local `use`.)

- [ ] **Step 3: Run both adapters' ontology contract tests.**

Run: `buck2 test //src/control-plane/memory:ontology //src/control-plane/postgres:ontology > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL|error" /tmp/t.log`
Expected: both PASS — memory and postgres both round-trip constraints and reject bad declarations.

- [ ] **Step 4: Commit**

```bash
git add src/control-plane/memory src/control-plane/testkit
git commit -m "feat(memory,testkit): constraint declaration check + shared round-trip/rejection contract"
```

---

### Task 5: Query-api typed-insert enforcement (422)

**Files:**
- Modify: `src/services/query-api/src/action.rs` (`ActionError`, `run_insert`)
- Modify: `src/services/query-api/src/http.rs` (action error → response, ~line 755; `post_action` `#[utoipa::path]` responses)
- Modify: `src/services/query-api/src/openapi.rs` (new `ConstraintViolationsBody` schema)
- Test: `src/services/query-api/tests/constraints_action_e2e.rs` (new) + `src/services/query-api/BUCK` (new `loom_fixture_test`)

**Interfaces:**
- Consumes: `control_plane_core::{ConstraintViolation, PropertyValidator}`.
- Produces: `ActionError::ConstraintViolation(Vec<ConstraintViolation>)` → 422 body `{ "violations": [ { "property": .., "rule": .. }, .. ] }`.

- [ ] **Step 1: Write the failing e2e test** `tests/constraints_action_e2e.rs` as a `loom_fixture_test` (postgres + real engine). **Mirror `tests/iceberg_action_e2e.rs`** — NOT the read-only router/`StubAction` path (`StubAction::write_object` is a no-op that writes nothing, so it can neither prove a conforming insert is readable nor that a violating one wrote nothing). Use `PgFixture::start()` + `fresh_db()` + `e2e_support::spawn_engine_writer(...)` for a REAL `ActionEngine`, `InProcessServingEngine::new(IcebergCatalog::new(pool))` for read-back, `run_action(...)` directly, and a local `grant_writer`/`define_*` helper modeled on `iceberg_action_e2e.rs:25-98`. Seed a `Widget(id Long, code String)` type where `code` carries `constraints: PropertyConstraints { pattern: Some(r"^[A-Z]+$".into()), .. }` and `id` carries `range: Some(RangeConstraint { min: Some(1.0), .. })`, plus a `createWidget` Insert action. Assert:
  - **Conforming insert** (`{"id":"5","code":"AB"}`) → `run_action` returns `Ok`; a follow-up `read_object` returns the row (proves it was actually written + readable).
  - **Violating insert** (`{"id":"5","code":"ab"}` — lowercase fails `pattern`; and a second case `{"id":"0","code":"AB"}` failing `range`) → `run_action` returns `Err(ActionError::ConstraintViolation(v))` with `v[0].property == "code"` / `rule == ConstraintRule::Pattern` (resp. `"id"`/`Range`); a follow-up `read_object` shows the row was NOT written.
  - **ACL distinct:** an ungranted subject → `Err(ActionError::Forbidden)` (a 403 denial, never reaching constraint validation) — mirrors `iceberg_action_e2e.rs:174` (`ungranted_subject_is_forbidden_and_writes_nothing`).

  Then add a SECOND test (same file or `tests/constraints_action_http.rs`) **mirroring `tests/write_denial_http.rs`** to lock the HTTP **422** mapping: a violating insert through the `post_action` router (a `StubAction` engine is fine here — value validation rejects BEFORE any write) → response status `422` and body `{"violations":[{"property":"code","rule":"pattern"}]}`. This is the spec's "typed-insert violating a constraint → 422 with the violation" assertion.

- [ ] **Step 2: Run it; verify it fails** (`ConstraintViolation` variant missing / no 422).

Run: `buck2 test //src/services/query-api:constraints_action_e2e > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL|error" /tmp/t.log`
Expected: FAIL/build error.

- [ ] **Step 3: Add the `ActionError` variant** in `action.rs` (after `WriteDenied`):
```rust
/// One or more inserted values violate their property's declared constraints. Carries
/// every violation (property + rule) for the structured `422` body. Distinct from the
/// `403` ACL `WriteDenied` (a constraint violation is malformed data, not a denial).
#[error("constraint violation")]
ConstraintViolation(Vec<control_plane_core::ConstraintViolation>),
```
Add `ConstraintViolation` to the `control_plane_core::{...}` import and `PropertyValidator`.

- [ ] **Step 4: Validate values in `run_insert`.** Anchor by comment, not line number: insert immediately after the fine-grained Write-policy gate's `return Err(ActionError::WriteDenied(reason));` block and immediately before the `// 5. Expand to the target type's FULL property set` comment. `pairs` is `Vec<(String, SqlValue)>` (declared `let pairs = parse_params(...)` near the top of `run_insert`); the `SqlValue` match below covers every variant of the enum (`Text/Int/Bool/Double/Date/Timestamp/Null`):
```rust
// 4c. Per-value constraint validation: reject values violating their property's declared
//     constraints with a structured 422 (distinct from the 403 ACL denial). Validate the
//     parsed pairs; an omitted optional (NULL) carries no value to check.
let mut cviol: Vec<control_plane_core::ConstraintViolation> = Vec::new();
for (col, val) in &pairs {
    let Some(prop) = target.properties.iter().find(|p| &p.name == col) else {
        continue;
    };
    if prop.constraints.is_empty() {
        continue;
    }
    let validator = control_plane_core::PropertyValidator::new(prop)?;
    match val {
        SqlValue::Text(s) => validator.check_str(s, &mut cviol),
        SqlValue::Int(i) => validator.check_num(*i as f64, &mut cviol),
        SqlValue::Double(d) => validator.check_num(*d, &mut cviol),
        SqlValue::Bool(_) | SqlValue::Date(_) | SqlValue::Timestamp(_) | SqlValue::Null => {}
    }
}
if !cviol.is_empty() {
    tracing::info!(action = action_name, count = cviol.len(), "insert rejected: constraint violation");
    return Err(ActionError::ConstraintViolation(cviol));
}
```
(`PropertyValidator::new(prop)?` converts `ControlPlaneError` via the existing `#[from]`. `*i as f64` — if `cast_precision_loss`/`cast_possible_truncation` is enforced, wrap the line in `#[expect(clippy::cast_precision_loss, reason = "range bounds are f64; i64→f64 is acceptable for validation")]` or use `f64::from`-style where possible.)

- [ ] **Step 5: Map the variant to 422** in `http.rs`. `post_action` returns a `match run_action(...) { ... }` as its tail expression; add this as a new comma-terminated arm INSIDE that match, after the `WriteDenied` arm and before the catch-all `Err(e) => internal_error("action serving fault", e)`:
```rust
// Per-value constraint violation: a structured 422 (malformed data), distinct from the
// 403 ACL denial above. Body mirrors the ingest violations shape: { "violations": [..] }.
Err(crate::action::ActionError::ConstraintViolation(violations)) => {
    let items: Vec<serde_json::Value> = violations
        .iter()
        .map(|v| serde_json::json!({ "property": v.property, "rule": v.rule.as_str() }))
        .collect();
    (
        StatusCode::UNPROCESSABLE_ENTITY,
        Json(serde_json::json!({ "violations": items })),
    )
        .into_response()
}
```

- [ ] **Step 6: Document it.** Add a `ConstraintViolationsBody` schema in `openapi.rs`:
```rust
/// Documentation shape for a 422 constraint-violation body on a typed-insert action.
#[derive(ToSchema)]
pub struct ConstraintViolationsBody {
    pub violations: Vec<ConstraintViolationItem>,
}

/// One constraint violation: the property and the rule it failed.
#[derive(ToSchema)]
pub struct ConstraintViolationItem {
    pub property: String,
    /// `range` | `length` | `pattern` | `one_of`.
    pub rule: String,
}
```
Add both to `components(schemas(...))`, and add `(status = 422, description = "A value violates a property constraint", body = ConstraintViolationsBody)` to `post_action`'s `#[utoipa::path]` `responses(...)` (in `http.rs`).

- [ ] **Step 7: Wire the BUCK test target(s)** in `src/services/query-api/BUCK` as a **`loom_fixture_test`** (postgres-backed — mirror the `iceberg_action_e2e` target's deps: `":e2e-support"`, `"//src/control-plane/postgres"`, `"//src/control-plane/core"`, `"//third-party:tokio"`, `"//third-party:serde_json"`, `"//third-party:tempfile"`, etc.). The HTTP-422 mirror of `write_denial_http.rs` uses whatever target type that test uses. Then:

Run: `buck2 test //src/services/query-api:constraints_action_e2e > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL|error" /tmp/t.log`
Expected: PASS.

- [ ] **Step 8: Clippy + commit.**

Run: `buck2 build '//src/services/query-api:query-api[clippy.txt]' > /tmp/c.log 2>&1` — confirm clean.
```bash
git add src/services/query-api
git commit -m "feat(query-api): enforce property constraints on typed-insert actions (422)"
```

---

### Task 6: Ingest model-binding enforcement (422)

**Files:**
- Modify: `src/services/ingest/src/gate.rs` (`ColumnShape` gains `constraints`; `ViolationReason::Constraint`; new `validate_values`)
- Modify: `src/services/ingest/src/model.rs` (`model_shape_from_type` carries constraints)
- Modify: `src/services/ingest/src/http.rs` (`From<LandModel>` default constraints; `violations_json` renders constraint; `land_model` calls `validate_values`)
- Modify: `src/services/ingest/src/openapi.rs` (`Violation` gains optional `rule`)
- Test: `src/services/ingest/tests/constraints_land_e2e.rs` (new) + `src/services/ingest/BUCK` (new test target)

**Interfaces:**
- Consumes: `control_plane_core::{PropertyConstraints, PropertyValidator, ConstraintRule}`.
- Produces: a constraint-violating batch → `IngestError::DoesNotConform` → 422 with `{ "violations": [ { "column": .., "reason": "constraint", "rule": .. } ] }`.

- [ ] **Step 1: Write the failing e2e test** `tests/constraints_land_e2e.rs`. Mirror `tests/http_model.rs`: `define_type` with a constrained property (e.g. string `code` `pattern = ^[A-Z]+$`), POST an Arrow IPC batch with a violating row → assert 422 + `violations[0].reason == "constraint"`, `rule == "pattern"`, `column == "code"`, and NOTHING landed (a follow-up read / snapshot count is unchanged); POST a conforming batch → 200. Build the Arrow batch the way `http_model.rs` does.

- [ ] **Step 2: Run it; verify it fails.**

Run: `buck2 test //src/services/ingest:constraints_land_e2e > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL|error" /tmp/t.log`
Expected: FAIL/build error.

- [ ] **Step 3: Carry constraints on `ColumnShape`** in `gate.rs`:
```rust
#[derive(Clone, Debug, PartialEq)]
pub struct ColumnShape {
    pub name: String,
    pub ty: String,
    pub required: bool,
    pub constraints: control_plane_core::PropertyConstraints,
}
```
(Drop `Eq` from `ColumnShape` and `ModelShape` derives — `PropertyConstraints` is not `Eq`.)

Add the violation reason:
```rust
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ViolationReason {
    MissingRequired,
    TypeMismatch { expected: String, found: String },
    Unsupported,
    /// A present value violates the property's declared constraint. `rule` is the
    /// failed rule's stable token (`range`|`length`|`pattern`|`one_of`).
    Constraint { rule: String },
}
```

Add the value validator (uses arrow downcasts):
```rust
use arrow::array::{
    Array, Float64Array, Int32Array, Int64Array, LargeStringArray, RecordBatch, StringArray,
};
use arrow::datatypes::DataType;
use control_plane_core::PropertyValidator;

/// Validate the runtime VALUES of `batches` against each column's declared constraints
/// (after the schema-shape `validate`). Column-wise: a string column drives `check_str`,
/// a numeric column drives `check_num`; nulls and unconstrained columns are skipped.
/// Returns ALL violations (one per offending row/rule) so callers report them together.
pub fn validate_values(shape: &ModelShape, batches: &[RecordBatch]) -> Result<(), Vec<Violation>> {
    let mut violations = Vec::new();
    for col in &shape.columns {
        if col.constraints.is_empty() {
            continue;
        }
        let Ok(validator) = PropertyValidator::from_parts(&col.name, &col.constraints) else {
            continue; // define-time validated; a bad regex can't reach here
        };
        if validator.is_noop() {
            continue;
        }
        for batch in batches {
            let Ok(idx) = batch.schema().index_of(&col.name) else {
                continue; // absent optional column — already gated by `validate`
            };
            let array = batch.column(idx);
            let mut cv = Vec::new();
            match array.data_type() {
                DataType::Utf8 => {
                    if let Some(a) = array.as_any().downcast_ref::<StringArray>() {
                        for i in 0..a.len() {
                            if !a.is_null(i) {
                                validator.check_str(a.value(i), &mut cv);
                            }
                        }
                    }
                }
                DataType::LargeUtf8 => {
                    if let Some(a) = array.as_any().downcast_ref::<LargeStringArray>() {
                        for i in 0..a.len() {
                            if !a.is_null(i) {
                                validator.check_str(a.value(i), &mut cv);
                            }
                        }
                    }
                }
                DataType::Int32 => {
                    if let Some(a) = array.as_any().downcast_ref::<Int32Array>() {
                        for i in 0..a.len() {
                            if !a.is_null(i) {
                                validator.check_num(f64::from(a.value(i)), &mut cv);
                            }
                        }
                    }
                }
                DataType::Int64 => {
                    if let Some(a) = array.as_any().downcast_ref::<Int64Array>() {
                        for i in 0..a.len() {
                            if !a.is_null(i) {
                                validator.check_num(a.value(i) as f64, &mut cv);
                            }
                        }
                    }
                }
                DataType::Float64 => {
                    if let Some(a) = array.as_any().downcast_ref::<Float64Array>() {
                        for i in 0..a.len() {
                            if !a.is_null(i) {
                                validator.check_num(a.value(i), &mut cv);
                            }
                        }
                    }
                }
                _ => {}
            }
            for v in cv {
                violations.push(Violation {
                    column: v.property,
                    reason: ViolationReason::Constraint { rule: v.rule.as_str().to_string() },
                });
            }
        }
    }
    if violations.is_empty() {
        Ok(())
    } else {
        Err(violations)
    }
}
```
(`a.value(i) as i64 → f64`: if `cast_precision_loss` is enforced, add `#[expect(clippy::cast_precision_loss, reason = "i64→f64 acceptable for range validation")]` on the `Int64` arm. De-duplicate identical violations if the row count makes the body large — out of scope; one per offending cell is acceptable for the slice. If preferred, break out of a column's row loop after the first violation to bound the body; note this in a comment.)

- [ ] **Step 4: Populate constraints in `model_shape_from_type`** (`model.rs`): add `constraints: p.constraints.clone(),` to the `ColumnShape` literal.

- [ ] **Step 5: Default constraints in the `From<LandModel>` impl** (`http.rs`): add `constraints: control_plane_core::PropertyConstraints::default(),` to the `ColumnShape` literal (the raw `/datasets` request carries no ontology constraints).

- [ ] **Step 6: Render the constraint reason in `violations_json`** (`http.rs`): add an arm:
```rust
ViolationReason::Constraint { rule } => {
    serde_json::json!({ "column": v.column, "reason": "constraint", "rule": rule })
}
```

- [ ] **Step 7: Call `validate_values` in `land_model`** (`http.rs`), right after step 5 (`resolve_columns` succeeds) and before step 6 (land):
```rust
// 5b. Per-value constraint validation over the decoded batches (422 on violation).
if let Err(violations) = crate::gate::validate_values(&shape, &batches) {
    return (
        StatusCode::UNPROCESSABLE_ENTITY,
        Json(violations_json(&violations)),
    )
        .into_response();
}
```
Add `validate_values` to the `use crate::gate::{...}` import.

- [ ] **Step 8: Document the new field** in `openapi.rs` `Violation`:
```rust
/// `missing_required` | `type_mismatch` | `unsupported` | `constraint`.
pub reason: String,
...
/// The failed constraint rule — present only for `constraint`.
pub rule: Option<String>,
```
(Add the `rule` field after `found`.)

- [ ] **Step 9: Wire the BUCK test target** (mirror `http_model`'s `loom_fixture_test` or `rust_test`), then run:

Run: `buck2 test //src/services/ingest:constraints_land_e2e > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL|error" /tmp/t.log`
Expected: PASS.

- [ ] **Step 10: Clippy + commit.**

Run: `buck2 build '//src/services/ingest:ingest[clippy.txt]' > /tmp/c.log 2>&1` — confirm clean.
```bash
git add src/services/ingest
git commit -m "feat(ingest): enforce property constraints on model-binding land (422)"
```

---

### Task 7: Whole-tree verification + register update

**Files:**
- Modify: `docs/ROADMAP.md` (close `road-model-constraints`), `docs/FUTURE.md` (ensure `fut-model-constraint-uniqueness`, `fut-model-constraint-backfill` recorded) — via `loom-docs-update`.

- [ ] **Step 1: Full build + test sweep.**

Run: `buck2 build //src/... > /tmp/b.log 2>&1; grep -iE "error|SUCCEEDED" /tmp/b.log | tail` then `buck2 test //src/... > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t.log`
Expected: build SUCCEEDED; tests green (note any pre-existing unrelated failures, if any).

- [ ] **Step 2: Lint sweep** (the CI `lint` job):

Run: `buck2 run //tools:prek -- run --all-files > /tmp/l.log 2>&1; tail -20 /tmp/l.log`
Expected: all hooks pass. Commit any hook-applied fixes.

- [ ] **Step 3: Close the register item** with `loom-docs-update`: mark `road-model-constraints` `- [x]` / `status:done` and add `pr:#<N>` once the PR exists; confirm `fut-model-constraint-uniqueness` + `fut-model-constraint-backfill` exist as deferred. Run `bash tools/docs.sh validate`.

- [ ] **Step 4: Commit** any register/lint changes.

```bash
git add -A
git commit -m "docs: close road-model-constraints; record deferred constraint follow-ups"
```
