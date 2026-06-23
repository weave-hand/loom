//! Transform write-backend selection, chosen once at boot by `LOOM_TRANSFORM_BACKEND`
//! — mirroring ingest's `LOOM_LANDING_BACKEND` and query-api's `LOOM_SERVING_BACKEND`.
//! DuckLake (the default) injects a `PgControlPlane`; Iceberg injects an
//! `IcebergControlPlane`, so transform's `run.rs` write path is unchanged — only which
//! `ControlPlane` it commits through differs.

/// Which table format a running transform worker writes its output to.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TransformBackend {
    /// DuckLake via `PgControlPlane`/`PgTx` (default; today's behaviour).
    DuckLake,
    /// Iceberg via `IcebergControlPlane`/`IcebergTx` (mirror-projected output).
    Iceberg,
}

/// Parse `LOOM_TRANSFORM_BACKEND`. Unset/empty -> DuckLake. Case-insensitive.
pub fn parse_transform_backend(v: Option<&str>) -> Result<TransformBackend, String> {
    match v.map(|s| s.trim().to_ascii_lowercase()).as_deref() {
        None | Some("") | Some("ducklake") => Ok(TransformBackend::DuckLake),
        Some("iceberg") => Ok(TransformBackend::Iceberg),
        Some(other) => Err(format!(
            "LOOM_TRANSFORM_BACKEND must be 'ducklake' or 'iceberg', got {other:?}"
        )),
    }
}
