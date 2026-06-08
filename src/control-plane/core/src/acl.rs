//! The ACL concern: loom's policy store and decision surface — subjects, roles,
//! coarse access grants (powering [`Acl::check`]), and fine-grained row/column
//! policy (powering [`Acl::policies_for`]). The `acl` schema is loom-owned: this
//! trait reads AND writes it.
//!
//! P4 stores and serves policy; it does NOT enforce. The Query API folds a
//! [`RowFilter`] into a DataFusion `Expr` and projects out `deny_columns`; the
//! control plane never interprets a filter. Targets reuse [`crate::TypeName`]
//! (ontology) and [`crate::TableRef`] (catalog).

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::error::Result;
use crate::page::{Page, PageReq};
use crate::{TableRef, TypeName};

/// A user or service account.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct SubjectId(pub String);

/// The unit governance is expressed in. Grants and policies attach here.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct RoleId(pub String);

/// What a subject may do to a target.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Action {
    Read,
    Write,
}

/// What a grant or policy is bound to. Matched exactly as stored — P4 never
/// resolves a `Type` to its backing `Table`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PolicyTarget {
    Type(TypeName),
    Table(TableRef),
}

/// The outcome of an authorization check.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Decision {
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
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Policy {
    pub target: PolicyTarget,
    /// `None` = no row restriction.
    pub row_filter: Option<RowFilter>,
    /// Columns projected out (deny). Order unspecified.
    pub deny_columns: Vec<String>,
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
    /// Grant a coarse `(action, target)` allow to a role. Role must exist, else
    /// `NotFound`. Idempotent.
    async fn grant(&self, role: &RoleId, action: Action, target: PolicyTarget) -> Result<()>;
    /// Remove a grant. Idempotent (no-op if absent).
    async fn revoke(&self, role: &RoleId, action: Action, target: &PolicyTarget) -> Result<()>;
    /// Create or replace the row/column policy for `(role, policy.target)`. Role
    /// must exist, else `NotFound`. Upsert.
    async fn set_policy(&self, role: &RoleId, policy: Policy) -> Result<()>;
    /// Remove the policy for `(role, target)`. Idempotent (no-op if absent).
    async fn clear_policy(&self, role: &RoleId, target: &PolicyTarget) -> Result<()>;

    /// `Allow` iff any role assigned to `subject` has a grant matching
    /// `(action, target)`. Unknown subject → `Deny` (not an error).
    async fn check(
        &self,
        subject: &SubjectId,
        action: Action,
        target: &PolicyTarget,
    ) -> Result<Decision>;
    /// All policies across `subject`'s roles whose target equals `target` (order
    /// unspecified). Unknown subject → empty vec. No merging.
    /// The `page` request is accepted but not yet enforced; results are a single full page.
    async fn policies_for(
        &self,
        subject: &SubjectId,
        target: &PolicyTarget,
        page: PageReq,
    ) -> Result<Page<Policy>>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn row_filter_json_round_trips() {
        let f = RowFilter::And(vec![
            RowFilter::Compare {
                property: "tenant".into(),
                op: CompareOp::Eq,
                value: ScalarValue::Text("acme".into()),
            },
            RowFilter::Or(vec![
                RowFilter::Compare {
                    property: "is_public".into(),
                    op: CompareOp::Eq,
                    value: ScalarValue::Bool(true),
                },
                RowFilter::Compare {
                    property: "owner".into(),
                    op: CompareOp::Eq,
                    value: ScalarValue::Text("me".into()),
                },
            ]),
            RowFilter::Not(Box::new(RowFilter::Compare {
                property: "region".into(),
                op: CompareOp::In,
                value: ScalarValue::List(vec![
                    ScalarValue::Text("EU".into()),
                    ScalarValue::Text("UK".into()),
                ]),
            })),
        ]);
        let json = serde_json::to_string(&f).unwrap();
        let back: RowFilter = serde_json::from_str(&json).unwrap();
        assert_eq!(f, back);
    }
}
