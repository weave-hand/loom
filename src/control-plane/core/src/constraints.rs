//! Per-value validation rules ("model constraints") declared on an object type's
//! properties, plus the define-time declaration check and the write-time value
//! validator that both write paths (query-api action, ingest land) drive.
//!
//! Pure (no I/O). `range` applies to numeric properties; `length`/`pattern`/`one_of`
//! apply to string properties. Applicability + regex validity are enforced at
//! `define_type` time ([`validate_constraints`]); the write-time [`PropertyValidator`]
//! compiles the regex ONCE and reuses it across every value in a pass.

use crate::error::ControlPlaneError;
use crate::logical_type::{BaseType, resolve_logical};
use crate::ontology::PropertyDef;

/// Inclusive numeric bound. Applies to numeric properties (`Integer`/`Long`/`Double`).
/// Bounds are `f64`; an `Integer`/`Long` value is widened to `f64` before comparison, so
/// magnitudes beyond 2^53 are approximate (acceptable for the validation use — a `Long`
/// near that magnitude with a same-magnitude bound is the only affected case).
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
    /// A regex the value must match. Matching is **partial** (the value need only
    /// *contain* a match, like JSON Schema's `pattern`); anchor with `^…$` for a
    /// whole-value match. Compiled and validated at `define_type` time.
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
/// [`ControlPlaneError::Validation`]. A property with empty constraints is skipped. Both
/// adapters call this at the top of `define_type`.
///
/// # Errors
/// [`ControlPlaneError::Validation`] if a `range` is declared on a non-numeric property, a
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
/// recompiling per row). Drive both write paths through [`check_str`](Self::check_str) /
/// [`check_num`](Self::check_num).
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
    /// [`ControlPlaneError::Validation`] if the `pattern` fails to compile (define-time
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

    /// Convenience: build from a [`PropertyDef`].
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
