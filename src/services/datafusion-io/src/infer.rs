//! Arrow `DataType` -> loom logical type. Used only on the un-modeled landing
//! path; when a model is supplied, the model's column types win (see materialize).

use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use control_plane_core::ColumnSpec;

#[derive(Debug, thiserror::Error)]
pub enum InferError {
    #[error("unsupported arrow type for ingest: {0:?}")]
    Unsupported(DataType),
    #[error("unsupported loom logical type for an empty input schema: {0}")]
    UnsupportedLogical(String),
}

/// The loom LOGICAL type name for an Arrow type, or `None` if loom does not yet land
/// that type. Kept deliberately small (YAGNI) — extend as real data needs it.
pub fn arrow_logical_type(dt: &DataType) -> Option<&'static str> {
    match dt {
        DataType::Boolean => Some("boolean"),
        DataType::Int32 => Some("integer"),
        DataType::Int64 => Some("long"),
        DataType::Float64 => Some("double"),
        DataType::Utf8 | DataType::LargeUtf8 => Some("string"),
        _ => None,
    }
}

/// loom logical type name -> Arrow `DataType`. The inverse of `arrow_logical_type`,
/// over exactly the five types `infer_columns` round-trips. `None` for an unmapped
/// name (kept deliberately small — YAGNI; widen via `fut-datafusion-type-coverage`).
pub fn logical_arrow_type(ty: &str) -> Option<DataType> {
    match ty {
        "boolean" => Some(DataType::Boolean),
        "integer" => Some(DataType::Int32),
        "long" => Some(DataType::Int64),
        "double" => Some(DataType::Float64),
        "string" => Some(DataType::Utf8),
        _ => None,
    }
}

/// Build an Arrow schema from loom column specs, in column order, preserving
/// nullability. Errors `InferError::UnsupportedLogical` on the first type outside the
/// supported set — a deterministic limitation (maps to `TransformError::Infer` ->
/// Abandon), symmetric with how a non-empty output of an unsupported type fails via
/// `infer_columns`. Used by the transform empty-input path to register an empty
/// relation whose schema matches what a non-empty scan would expose.
pub fn logical_arrow_schema(columns: &[ColumnSpec]) -> Result<SchemaRef, InferError> {
    let fields: Vec<Field> = columns
        .iter()
        .map(|c| {
            let dt = logical_arrow_type(&c.ty)
                .ok_or_else(|| InferError::UnsupportedLogical(c.ty.clone()))?;
            Ok(Field::new(&c.name, dt, c.nullable))
        })
        .collect::<Result<_, InferError>>()?;
    Ok(Arc::new(Schema::new(fields)))
}

/// Infer loom logical `ColumnSpec`s from an Arrow schema, in field order. Errors on the
/// first unsupported type rather than guessing.
pub fn infer_columns(schema: &Schema) -> Result<Vec<ColumnSpec>, InferError> {
    schema
        .fields()
        .iter()
        .map(|f| {
            let ty = arrow_logical_type(f.data_type())
                .ok_or_else(|| InferError::Unsupported(f.data_type().clone()))?;
            Ok(ColumnSpec {
                name: f.name().clone(),
                ty: ty.to_string(),
                nullable: f.is_nullable(),
            })
        })
        .collect()
}
