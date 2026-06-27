//! query-api's governed Arrow Flight **export** surface. A second listener (TCP) that
//! authenticates a bearer-token caller, governs a typed-object slice exactly as the HTTP
//! read path does, and streams the engine's `RecordBatch` result straight out columnar —
//! vectors carried natively as `List<Float32>`, never flattened through `SqlValue`. The
//! Flight ticket/command carries a loom `ExportCommand`, NOT SQL; loom compiles the ACL'd
//! SQL server-side per `do_get`, so a forged/replayed ticket is still a governed request.
//! See docs/superpowers/specs/2026-06-26-governed-flight-export-design.md.

use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use control_plane_core::{BaseType, resolve_logical};
use serde::{Deserialize, Serialize};

/// The governed slice to export — mirrors `GET /objects/{type}` params. JSON in the Flight
/// descriptor `cmd` (get_flight_info) and the `Ticket` bytes (do_get), mirroring
/// `FlightTicket`'s JSON convention.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExportCommand {
    /// The ontology type to export (e.g. "Chunk").
    #[serde(rename = "type")]
    pub type_name: String,
    /// Optional equality filters on allowed columns (validated server-side).
    #[serde(default)]
    pub filters: Vec<(String, String)>,
    /// Optional object-set identity values → an `In` predicate on the declared identity.
    #[serde(default)]
    pub ids: Vec<String>,
}

impl ExportCommand {
    /// JSON-encode for the descriptor `cmd` / `Ticket` bytes.
    #[expect(
        clippy::expect_used,
        reason = "ExportCommand is always serializable, mirrors FlightTicket::encode"
    )]
    pub fn encode(&self) -> Vec<u8> {
        serde_json::to_vec(self).expect("ExportCommand is always serializable")
    }
    /// Decode from descriptor `cmd` / `Ticket` bytes.
    pub fn decode(bytes: &[u8]) -> Result<Self, serde_json::Error> {
        serde_json::from_slice(bytes)
    }
}

/// Map a loom logical `BaseType` to the canonical Arrow `DataType` — an exact mirror of
/// `engine-serving`'s `base_to_arrow`, reproduced here because query-api must not depend on
/// the DataFusion serving crate. Keeping the two in lockstep ensures the schema advertised
/// by `get_flight_info` matches the schema the engine streams in `do_get`.
fn base_to_arrow(b: BaseType) -> DataType {
    match b {
        BaseType::Integer => DataType::Int32,
        BaseType::Long => DataType::Int64,
        BaseType::Double => DataType::Float64,
        BaseType::Boolean => DataType::Boolean,
        BaseType::String => DataType::Utf8,
        BaseType::Date => DataType::Date32,
        BaseType::Timestamp => DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, None),
        BaseType::Vector(_) => {
            DataType::List(Arc::new(Field::new("element", DataType::Float32, false)))
        }
    }
}

/// Build the projected Arrow schema for an export from the governed output columns, their loom
/// logical types (positionally aligned, in SELECT order), and the set of **masked** output
/// columns. Columns are nullable (the read projection does not assert non-null). A **masked**
/// column is advertised as `Utf8` — it is SELECTed as the `'***'` constant, so the engine
/// streams it as Utf8 regardless of its declared type; advertising the declared type would make
/// `get_flight_info`'s schema disagree with the `do_get` data schema. An unknown logical type is
/// an error — the ontology should never hold one.
pub fn export_arrow_schema(
    columns: &[String],
    logical_types: &[String],
    masked_columns: &[String],
) -> Result<SchemaRef, String> {
    if columns.len() != logical_types.len() {
        return Err(format!(
            "export schema: {} columns / {} types (must match)",
            columns.len(),
            logical_types.len()
        ));
    }
    let mut fields = Vec::with_capacity(columns.len());
    for (name, lt) in columns.iter().zip(logical_types) {
        let dt = if masked_columns.iter().any(|m| m == name) {
            DataType::Utf8 // masked → '***' constant streams as Utf8
        } else {
            let base = resolve_logical(lt).ok_or_else(|| format!("unknown logical type `{lt}`"))?;
            base_to_arrow(base)
        };
        fields.push(Field::new(name, dt, true));
    }
    Ok(Arc::new(Schema::new(fields)))
}
