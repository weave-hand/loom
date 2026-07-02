//! The ACL concern: loom's policy store and decision surface — subjects, roles,
//! coarse access grants (powering [`Acl::check`]), and fine-grained row/column
//! policy (powering [`Acl::policies_for`]). The `acl` schema is loom-owned: this
//! trait reads AND writes it.
//!
//! P4 stores and serves policy; it does NOT enforce. The Query API folds a
//! [`RowFilter`] into a DataFusion `Expr` and projects out `deny_columns`; the
//! control plane never interprets a filter. Targets reuse [`crate::TypeName`]
//! (ontology) and [`crate::TableRef`] (catalog).

use std::collections::HashSet;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::error::Result;
use crate::page::{Page, PageReq};
use crate::{TableRef, TypeName};

/// A user or service account.
#[derive(Clone, Debug, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub struct SubjectId(pub String);

/// The unit governance is expressed in. Grants and policies attach here.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct RoleId(pub String);

/// What a subject may do to a target.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub enum Action {
    Read,
    Write,
}

/// What a grant or policy is bound to. Matched exactly as stored — P4 never
/// resolves a `Type` to its backing `Table`.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum PolicyTarget {
    Type(TypeName),
    Table(TableRef),
}

/// The outcome of an authorization check.
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum Decision {
    Allow,
    Deny,
}

/// Whether a grant permits or forbids its `(action, target)`. Deny wins over Allow
/// in [`Acl::check`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Effect {
    Allow,
    Deny,
}

/// A comparison operator in a [`RowFilter::Compare`] leaf.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum CompareOp {
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
    /// `value` is a [`ScalarValue::List`].
    In,
    /// `value` is a [`ScalarValue::List`].
    NotIn,
    /// `value` is ignored.
    IsNull,
    /// `value` is ignored.
    IsNotNull,
    /// Caller-predicate-only: `col BETWEEN lo AND hi`. Two operands live on the
    /// query-api `CallerPredicate`, not on `ScalarValue`; rejected in ACL row filters.
    Between,
    /// Caller-predicate-only: case-insensitive `col ILIKE '%operand%'`. Rejected in ACL row filters.
    Contains,
    /// Caller-predicate-only: case-insensitive `col ILIKE 'operand%'`. Rejected in ACL row filters.
    StartsWith,
    /// Caller-predicate-only: case-insensitive `col ILIKE '%operand'`. Rejected in ACL row filters.
    EndsWith,
}

/// A literal on the right-hand side of a comparison. Float and temporal variants
/// are deliberately omitted this cycle so the type derives `Eq`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ScalarValue {
    Text(String),
    Int(i64),
    Bool(bool),
    List(Vec<ScalarValue>),
}

/// A row filter as a boolean expression tree over ontology properties. The Query
/// API folds this into a DataFusion `Expr`; the control plane never interprets it.
/// Serializes to one `jsonb` column.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum RowFilter {
    /// A leaf comparison. `property` names an ontology property (resolved to a
    /// column by-name) for `Type` targets, or a column directly for `Table` targets.
    Compare {
        property: String,
        op: CompareOp,
        value: ScalarValue,
    },
    And(Vec<RowFilter>),
    Or(Vec<RowFilter>),
    Not(Box<RowFilter>),
}

/// A fine-grained row/column restriction for one `(role, target)`.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct Policy {
    pub target: PolicyTarget,
    /// `None` = no row restriction.
    pub row_filter: Option<RowFilter>,
    /// Columns projected out (deny). Order unspecified.
    pub deny_columns: Vec<String>,
    /// Columns shown but value-masked (redacted to a marker). Distinct from
    /// `deny_columns`, which removes the column. Order unspecified.
    pub mask_columns: Vec<String>,
}

/// Validate a [`RowFilter`]'s well-formedness. Structural rules are always enforced;
/// when `properties` is `Some`, every `Compare` leaf's `property` must be a member.
/// Returns a human-readable reason on the first failure.
///
/// Structural (the CompareOp <-> ScalarValue invariant):
/// - `In` / `NotIn`         => value MUST be `ScalarValue::List`
/// - `Eq/Ne/Lt/Le/Gt/Ge`    => value must NOT be a `ScalarValue::List`
/// - `IsNull` / `IsNotNull`  => value ignored
pub fn validate_row_filter(
    f: &RowFilter,
    properties: Option<&HashSet<String>>,
) -> std::result::Result<(), String> {
    match f {
        RowFilter::Compare {
            property,
            op,
            value,
        } => {
            if let Some(props) = properties
                && !props.contains(property)
            {
                return Err(format!("unknown property: {property}"));
            }
            match op {
                CompareOp::In | CompareOp::NotIn => {
                    if !matches!(value, ScalarValue::List(_)) {
                        return Err(format!("{op:?} requires a list value"));
                    }
                }
                CompareOp::IsNull | CompareOp::IsNotNull => {}
                // Caller-predicate-only operators: not part of the ACL policy
                // grammar (Between needs two operands; text-pattern needs ILIKE).
                // Reject so op_sql's `unreachable!` arm stays unreachable.
                CompareOp::Between
                | CompareOp::Contains
                | CompareOp::StartsWith
                | CompareOp::EndsWith => {
                    return Err(format!("{op:?} is not valid in an ACL row filter"));
                }
                // Listed exhaustively (no `_`) so a future CompareOp variant is a
                // compile error here, forcing a deliberate structural-rule decision
                // rather than silently getting scalar treatment.
                CompareOp::Eq
                | CompareOp::Ne
                | CompareOp::Lt
                | CompareOp::Le
                | CompareOp::Gt
                | CompareOp::Ge => {
                    if matches!(value, ScalarValue::List(_)) {
                        return Err(format!("{op:?} requires a non-list value"));
                    }
                }
            }
            Ok(())
        }
        RowFilter::And(xs) | RowFilter::Or(xs) => {
            for x in xs {
                validate_row_filter(x, properties)?;
            }
            Ok(())
        }
        RowFilter::Not(x) => validate_row_filter(x, properties),
    }
}

#[async_trait]
pub trait Acl {
    /// Create a subject. Idempotent.
    async fn define_subject(&self, id: &SubjectId) -> Result<()>;
    /// Create a role. Idempotent.
    async fn define_role(&self, id: &RoleId) -> Result<()>;
    /// Assign a role to a subject. Both must already exist, else `NotFound`. Idempotent.
    async fn assign_role(&self, subject: &SubjectId, role: &RoleId) -> Result<()>;
    /// Remove a role assignment. Idempotent (no-op if absent).
    async fn unassign_role(&self, subject: &SubjectId, role: &RoleId) -> Result<()>;
    /// `role` gains all grants + policies of `inherits` (transitively, via the
    /// effective-role closure used by `check`/`policies_for`). Both roles must exist,
    /// else `NotFound`. Rejected with `Conflict` if the edge would form a cycle
    /// (including the self-edge `role == inherits`). Idempotent for an existing edge.
    async fn add_role_inheritance(&self, role: &RoleId, inherits: &RoleId) -> Result<()>;
    /// Remove a role-inheritance edge. Idempotent (no-op if absent).
    async fn remove_role_inheritance(&self, role: &RoleId, inherits: &RoleId) -> Result<()>;
    /// Grant or deny a coarse `(action, target)` to a role. Upserts by
    /// `(role, action, target)`: re-granting the same key replaces its effect. Role
    /// must exist, else `NotFound`. Idempotent for a fixed effect.
    async fn grant(
        &self,
        role: &RoleId,
        action: Action,
        target: PolicyTarget,
        effect: Effect,
    ) -> Result<()>;
    /// Remove a grant. Idempotent (no-op if absent).
    async fn revoke(&self, role: &RoleId, action: Action, target: &PolicyTarget) -> Result<()>;
    /// Create or replace the row/column policy for `(role, action, policy.target)`. Role
    /// must exist, else `NotFound`. Upsert. Read and write policies are independent.
    async fn set_policy(&self, role: &RoleId, action: Action, policy: Policy) -> Result<()>;
    /// Remove the policy for `(role, action, target)`. Idempotent (no-op if absent).
    async fn clear_policy(
        &self,
        role: &RoleId,
        action: Action,
        target: &PolicyTarget,
    ) -> Result<()>;

    /// `Allow` iff any role assigned to `subject` has a grant matching
    /// `(action, target)`. Unknown subject → `Deny` (not an error).
    async fn check(
        &self,
        subject: &SubjectId,
        action: Action,
        target: &PolicyTarget,
    ) -> Result<Decision>;
    /// All policies across `subject`'s roles for `(action, target)` (order unspecified).
    /// Unknown subject → empty vec. No merging. The `page` request is accepted but not yet
    /// enforced; results are a single full page.
    async fn policies_for(
        &self,
        subject: &SubjectId,
        action: Action,
        target: &PolicyTarget,
        page: PageReq,
    ) -> Result<Page<Policy>>;
}
