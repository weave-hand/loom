//! Arrow `DataType` -> DuckLake type string. Used only on the un-modeled landing
//! path; when a model is supplied, the model's column types win (see materialize).

use arrow::datatypes::{DataType, Schema};
use control_plane_core::ColumnSpec;

#[derive(Debug, thiserror::Error)]
pub enum InferError {
    #[error("unsupported arrow type for ingest: {0:?}")]
    Unsupported(DataType),
}

/// The DuckLake type string for an Arrow type, or `None` if loom does not yet
/// land that type. Kept deliberately small (YAGNI) — extend as real data needs it.
pub fn duck_type(dt: &DataType) -> Option<&'static str> {
    match dt {
        DataType::Boolean => Some("boolean"),
        DataType::Int32 => Some("int32"),
        DataType::Int64 => Some("int64"),
        DataType::Float64 => Some("double"),
        DataType::Utf8 | DataType::LargeUtf8 => Some("varchar"),
        _ => None,
    }
}

/// Infer DuckLake `ColumnSpec`s from an Arrow schema, in field order. Errors on the
/// first unsupported type rather than guessing.
pub fn infer_columns(schema: &Schema) -> Result<Vec<ColumnSpec>, InferError> {
    schema
        .fields()
        .iter()
        .map(|f| {
            let ty = duck_type(f.data_type())
                .ok_or_else(|| InferError::Unsupported(f.data_type().clone()))?;
            Ok(ColumnSpec {
                name: f.name().clone(),
                ty: ty.to_string(),
                nullable: f.is_nullable(),
            })
        })
        .collect()
}
