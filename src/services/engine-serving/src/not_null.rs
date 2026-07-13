//! `loom_not_null` — an IDENTITY scalar UDF that re-declares its argument's field
//! as NON-nullable.
//!
//! Why this exists: [`crate::serving::build_serving_provider`]'s merge view unions
//! the file tier with the inline tier, and the inline tier MUST declare its
//! non-identity data columns nullable (a non-CDC DELETE's inline row is id-only, so
//! those columns are physically NULL — `iceberg_inline::write_inline_delta`'s
//! tombstone arm). DataFusion's union widens nullability per position (`nullable =
//! any input nullable`) and a plain column projection copies the input field
//! verbatim — so without this, the MERGED view's served schema would inherit the
//! widening.
//!
//! That widening is NOT cosmetic: the worker infers a transform/MV's output columns
//! from the served Arrow schema (`worker::transform` / `worker::stream_mv` ->
//! `datafusion_io::infer_columns`, which sets `nullable: f.is_nullable()`), and
//! `check_conformance` rejects a nullable column for a REQUIRED property while
//! `classify_schema_change` rejects any nullability change on a re-run. A benign
//! inline UPDATE shadow on a source table would start failing green transforms.
//!
//! The restore is SOUND because the final projection sits ABOVE the
//! `_loom_tomb = false` filter, which drops exactly the tombstone rows — the only
//! inline rows that carry NULL data columns (CDC `-D`/`-U`/`+U` all carry full
//! images). If a NULL ever did reach here, `ProjectionExec`'s
//! `RecordBatch::try_new` fails loudly rather than corrupting silently.

use std::sync::Arc;

use arrow::datatypes::{DataType, FieldRef};
use datafusion::common::tree_node::TreeNode;
use datafusion::common::{Result, exec_err};
use datafusion::logical_expr::{
    ColumnarValue, Expr, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature,
    Volatility,
};

/// The UDF's SQL-visible name. Namespaced so it can never collide with a user
/// function; it is only ever inserted programmatically (never parsed from SQL).
const NAME: &str = "loom_not_null";

/// `PartialEq`/`Eq`/`Hash` are REQUIRED, not incidental: DataFusion 54's
/// `ScalarUDFImpl` is bounded by `DynEq + DynHash`, which it gets from blanket
/// impls over `Eq + Any` / `Hash + Any` (datafusion-expr-common `dyn_eq.rs`).
/// The whole state is the (stateless) signature, so the derives are exact.
#[derive(Debug, PartialEq, Eq, Hash)]
struct NotNull {
    signature: Signature,
}

impl Default for NotNull {
    fn default() -> Self {
        Self {
            // Any single argument, any type; pure pass-through, no coercion.
            signature: Signature::any(1, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for NotNull {
    fn name(&self) -> &str {
        NAME
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        match arg_types {
            [t] => Ok(t.clone()),
            _ => exec_err!("{NAME} takes exactly one argument"),
        }
    }
    /// THE POINT: the input field VERBATIM — name, data type and metadata — with
    /// nullability forced to `false`. A true identity on everything but the one flag
    /// this UDF exists to set: cloning the field (rather than building a fresh one)
    /// keeps any field metadata the mirror schema carries — Iceberg field-ids being
    /// the obvious future candidate — so the served schema cannot silently diverge
    /// from the mirror on exactly the REQUIRED columns this wrapper is applied to.
    /// The name is safe to preserve: DataFusion derives a projection's output column
    /// name from the expr/alias, not from `return_field.name()`.
    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        match args.arg_fields {
            [f] => Ok(Arc::new(f.as_ref().clone().with_nullable(false))),
            _ => exec_err!("{NAME} takes exactly one argument"),
        }
    }
    /// Pure identity — the argument is returned verbatim.
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let mut it = args.args.into_iter();
        match (it.next(), it.next()) {
            (Some(v), None) => Ok(v),
            _ => exec_err!("{NAME} takes exactly one argument"),
        }
    }
}

/// Wrap `expr` so its projected field is declared NON-nullable. The caller must
/// guarantee the expression cannot yield NULL at this point in the plan (see the
/// module doc); the alias is the caller's job.
pub fn not_null(expr: Expr) -> Expr {
    ScalarUDF::from(NotNull::default()).call(vec![expr])
}

/// Does `expr` reference `loom_not_null` anywhere in its tree?
///
/// `loom_not_null` is loom's OWN marker — it exists in no database. DataFusion's
/// filter pushdown rewrites a predicate over the merged view THROUGH its final
/// projection, so a predicate on a restored column reaches the tier providers
/// wrapped in `loom_not_null(...)`. Rendering that into Postgres SQL fails the scan
/// (`function loom_not_null(bigint) does not exist`), so
/// [`crate::provider::build_scan_sql`] uses this to skip such a filter — the PG
/// provider reports `Inexact`, so DataFusion re-applies the predicate itself. The
/// physical (arrow) side needs no such guard: the UDF evaluates as the identity it
/// is.
///
/// FAILS SAFE: a tree-walk error answers `true` ("assume the marker is there"). The
/// two outcomes are not symmetric — a false negative renders `loom_not_null(...)`
/// into Postgres SQL and FAILS the scan, while a false positive merely skips
/// pushing one filter down, which is always sound because the PG provider reports
/// `Inexact` and DataFusion re-applies every filter above the scan regardless.
pub fn contains_not_null(expr: &Expr) -> bool {
    expr.exists(|e| Ok(matches!(e, Expr::ScalarFunction(f) if f.name() == NAME)))
        .unwrap_or(true)
}
