//! Physical-column resolution + model-gate validation for the landing path.
//! Backend-agnostic: `http.rs` calls [`resolve_columns`] once per request,
//! before dispatching to whichever `LandingMaterializer` is configured, to
//! validate the optional model gate and resolve the schema to land.

use arrow::datatypes::Schema;
use control_plane_core::ColumnSpec;

use crate::IngestError;
use crate::gate::{ModelShape, validate};
use datafusion_io::infer_columns;

/// Validate the optional model gate and resolve the physical schema. Backend-
/// agnostic: the HTTP handler runs this once, before dispatching to whichever
/// landing backend is configured. The model wins when supplied; otherwise infer.
pub fn resolve_columns(
    schema: &Schema,
    gate: Option<&ModelShape>,
) -> Result<Vec<ColumnSpec>, IngestError> {
    if let Some(shape) = gate {
        validate(shape, schema).map_err(IngestError::DoesNotConform)?;
    }
    Ok(match gate {
        Some(shape) => shape
            .columns
            .iter()
            .map(|c| ColumnSpec {
                name: c.name.clone(),
                ty: c.ty.clone(),
                nullable: !c.required,
            })
            .collect(),
        None => infer_columns(schema)?,
    })
}
