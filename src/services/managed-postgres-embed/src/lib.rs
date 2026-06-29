//! Embeds the PostgreSQL distribution into the binary and self-extracts it at
//! runtime, so an embedded loom boot needs no PG binaries pre-staged. Gated in
//! its own crate so the 11.7 MB never reaches lean service binaries.

/// The PG distribution `.tar.gz`, baked in at compile time. `LOOM_PG_TARBALL` is
/// the buck `$(location :postgres-tarball)` (a plain absolute path).
#[expect(
    clippy::large_include_file,
    reason = "PG_TARBALL is intentionally large — it is the embedded Postgres distribution (~11 MB)"
)]
static PG_TARBALL: &[u8] = include_bytes!(env!("LOOM_PG_TARBALL"));

/// The pinned PostgreSQL version (e.g. "17.9.0"), from `PG_VERSION` in
/// `//src/control-plane/postgres:BUCK`. Used as the extraction cache key.
pub const PG_VERSION: &str = env!("LOOM_PG_VERSION");

/// Length of the embedded distribution in bytes (forces the `include_bytes!` to
/// materialize; used by the embed-present test).
#[must_use]
pub fn pg_tarball_len() -> usize {
    PG_TARBALL.len()
}

use std::path::{Path, PathBuf};

/// Where the extracted, ready-to-exec PG distribution lives.
#[derive(Debug, Clone)]
pub struct ExtractedPg {
    /// `<cache>/pg-<version>/bin` (holds initdb, postgres, pg_ctl).
    pub bin_dir: PathBuf,
    /// `<cache>/pg-<version>/lib` (the dist's bundled libs; NOT libxml2).
    pub lib_dir: PathBuf,
}

#[derive(Debug, thiserror::Error)]
pub enum EmbedError {
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
}

/// Extract the embedded PG distribution under `cache_root`, reusing an existing
/// `pg-<version>` extraction. Lock-free and concurrency-safe: a fresh extraction
/// goes to a per-pid temp dir and is published with an atomic `rename`.
pub fn extract_pg(cache_root: &Path) -> Result<ExtractedPg, EmbedError> {
    let final_dir = cache_root.join(format!("pg-{PG_VERSION}"));
    if !final_dir.exists() {
        std::fs::create_dir_all(cache_root)?;
        let tmp = cache_root.join(format!("pg-{}.tmp.{}", PG_VERSION, std::process::id()));
        // Re-run-safe: clear any stale temp from a previous crashed pid-reuse.
        if tmp.exists() {
            std::fs::remove_dir_all(&tmp)?;
        }
        std::fs::create_dir_all(&tmp)?;

        let gz = flate2::read::GzDecoder::new(PG_TARBALL);
        let mut archive = tar::Archive::new(gz);
        archive.set_preserve_permissions(true);
        for entry in archive.entries()? {
            let mut entry = entry?;
            let path = entry.path()?.into_owned();
            // Strip the single wrapping `postgresql-<ver>-<triple>/` component.
            let stripped: PathBuf = path.components().skip(1).collect();
            if stripped.as_os_str().is_empty() {
                continue;
            }
            entry.unpack(tmp.join(stripped))?;
        }

        // Publish atomically. If we lost a race, another process already
        // published a complete dir — discard ours and use theirs.
        match std::fs::rename(&tmp, &final_dir) {
            Ok(()) => {}
            Err(_) if final_dir.exists() => {
                drop(std::fs::remove_dir_all(&tmp));
            }
            Err(e) => return Err(EmbedError::Io(e)),
        }
    }
    Ok(ExtractedPg {
        bin_dir: final_dir.join("bin"),
        lib_dir: final_dir.join("lib"),
    })
}
