//! Read an explicit set of a table's data files into Arrow record batches.
//!
//! The engine is the data source for the Flight data plane: it owns the
//! object store via the iceberg `FileIO`, so it resolves the file set and
//! decodes the Parquet here. The bytes never touch the worker except as the
//! streamed Arrow batches.

use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use control_plane_core::{Result, TableRef};
use iceberg::{Catalog, TableIdent};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

use crate::backend;
use crate::iceberg_sql_catalog::SqlCatalog;

/// Read `files` (data-file paths) of `table` into Arrow batches via the
/// catalog's `FileIO`. Returns the table's current Arrow schema and the
/// batches in file order.
///
/// An empty `files` slice returns the table schema and zero batches.
/// A path not resolvable by `FileIO` is an error.
pub async fn read_files_as_batches(
    catalog: &SqlCatalog,
    table: &TableRef,
    files: &[String],
) -> Result<(SchemaRef, Vec<RecordBatch>)> {
    let ident =
        TableIdent::from_strs([table.schema.as_str(), table.name.as_str()]).map_err(backend)?;
    let tbl = catalog.load_table(&ident).await.map_err(backend)?;

    // The table's current Arrow schema — the fidelity contract for the stream.
    // `schema_to_arrow_schema` is already used in `iceberg_landing.rs` and is
    // the established path in this crate for deriving an Arrow schema from the
    // Iceberg table metadata.
    let arrow_schema: SchemaRef = Arc::new(
        iceberg::arrow::schema_to_arrow_schema(tbl.metadata().current_schema()).map_err(backend)?,
    );

    let mut batches = Vec::new();
    for path in files {
        let bytes = tbl
            .file_io()
            .new_input(path)
            .map_err(backend)?
            .read()
            .await
            .map_err(backend)?;
        let reader = ParquetRecordBatchReaderBuilder::try_new(bytes)
            .map_err(backend)?
            .build()
            .map_err(backend)?;
        for b in reader {
            batches.push(b.map_err(backend)?);
        }
    }
    Ok((arrow_schema, batches))
}
