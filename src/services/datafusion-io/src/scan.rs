//! Read path — registering a DuckLake table's Parquet files as a DataFusion table.
//! Implemented in Task 3.

#[derive(Debug, thiserror::Error)]
pub enum ScanError {
    #[error("placeholder")]
    Placeholder,
}

/// Placeholder; real signature + body land in Task 3.
pub async fn scan_table() {}
