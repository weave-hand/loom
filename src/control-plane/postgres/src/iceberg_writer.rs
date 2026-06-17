//! Real Iceberg write path: drives Arrow record batches through the `iceberg`
//! writer chain to real Parquet, then commits them as a `fast_append`. The
//! commit funnels through the vendored catalog's `update_table`, which is where
//! loom projects the `iceberg_mirror.*` rows atomically (see `iceberg_sql_catalog`).
//! This module is pure write — it holds no mirror logic.

use arrow_array::RecordBatch;
use iceberg::spec::DataFile;
use iceberg::table::Table;
use iceberg::transaction::{ApplyTransactionAction, Transaction};
use iceberg::writer::base_writer::data_file_writer::DataFileWriterBuilder;
use iceberg::writer::file_writer::ParquetWriterBuilder;
use iceberg::writer::file_writer::location_generator::{
    DefaultFileNameGenerator, DefaultLocationGenerator,
};
use iceberg::writer::file_writer::rolling_writer::RollingFileWriterBuilder;
use iceberg::writer::{IcebergWriter, IcebergWriterBuilder};
use iceberg::{Catalog, Result};
use parquet57::file::properties::WriterProperties;

/// A neutral summary of one committed Parquet data file, returned to callers
/// (tests, the seeder) that want to assert on the write without depending on
/// the `iceberg::spec::DataFile` shape.
pub struct WrittenFile {
    /// On-storage path of the written Parquet file (may be a `file://` URL).
    pub path: String,
    /// Rows written to the file.
    pub record_count: i64,
    /// On-disk size in bytes.
    pub file_size_bytes: i64,
}

/// Write `batches` to real Parquet under `table`'s location and commit them as a
/// single `fast_append`. Returns one [`WrittenFile`] per produced data file. The
/// caller must have created `table` in `catalog` already. The mirror projection
/// happens inside `catalog.update_table` during `commit` — not here.
pub async fn append_batches(
    catalog: &dyn Catalog,
    table: &Table,
    batches: Vec<RecordBatch>,
) -> Result<Vec<WrittenFile>> {
    let data_files = write_parquet(table, batches).await?;
    let summaries: Vec<WrittenFile> = data_files
        .iter()
        .map(|df| WrittenFile {
            path: df.file_path().to_string(),
            record_count: df.record_count() as i64,
            file_size_bytes: df.file_size_in_bytes() as i64,
        })
        .collect();

    let tx = Transaction::new(table);
    let action = tx.fast_append().add_data_files(data_files);
    let tx = action.apply(tx)?;
    tx.commit(catalog).await?;
    Ok(summaries)
}

async fn write_parquet(table: &Table, batches: Vec<RecordBatch>) -> Result<Vec<DataFile>> {
    let location_generator = DefaultLocationGenerator::new(table.metadata().clone())?;
    // Unique per-append prefix: DefaultFileNameGenerator restarts its counter at 0
    // each call, so a fixed prefix would emit the same `<prefix>-00000.parquet` path
    // for every append — and iceberg's fast_append rejects re-adding an already-
    // referenced path (whether from a prior sequential append or a concurrent writer).
    let file_name_generator = DefaultFileNameGenerator::new(
        format!("loom-{}", uuid::Uuid::new_v4()),
        None,
        iceberg::spec::DataFileFormat::Parquet,
    );
    let parquet_builder = ParquetWriterBuilder::new(
        WriterProperties::default(),
        table.metadata().current_schema().clone(),
    );
    let rolling = RollingFileWriterBuilder::new_with_default_file_size(
        parquet_builder,
        table.file_io().clone(),
        location_generator,
        file_name_generator,
    );
    let mut writer = DataFileWriterBuilder::new(rolling).build(None).await?;
    for batch in batches {
        writer.write(batch).await?;
    }
    writer.close().await
}
