//! Embedded Postgres lifecycle: boot (or adopt) a persistent cluster on a unix
//! socket and hand back sqlx connect options, so loom can run with no external
//! Postgres server. Promotes the test fixture's ephemeral-cluster logic to a
//! persistent runtime. `initdb`/`postgres` refuse to run as root by design.

use std::path::PathBuf;

/// Parameters for an embedded cluster. `bin_dir`/`ld_library_path` are a
/// configurable path in slice 1 (point them at the buck `:postgres-bin` output);
/// slice 2 fills them from an extracted asset.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EmbeddedPgConfig {
    /// Postgres `bin/` directory (holds initdb, postgres, pg_ctl).
    pub bin_dir: PathBuf,
    /// LD_LIBRARY_PATH for the spawned binaries (dist libs + libxml2).
    pub ld_library_path: String,
    /// Persistent cluster data dir (e.g. `<LOOM_DATA_PATH>/pgdata`).
    pub data_dir: PathBuf,
    /// Unix-socket directory (e.g. `<LOOM_DATA_PATH>/pgrun`).
    pub socket_dir: PathBuf,
    /// Application database name. Validated as `[a-z_][a-z0-9_]*`.
    pub database: String,
}

#[derive(Debug, thiserror::Error)]
pub enum EmbeddedPgError {
    #[error("embedded Postgres cannot run as root — run loom as a normal user")]
    RunningAsRoot,
    #[error("another process already owns the data dir {0}")]
    AlreadyLocked(PathBuf),
    #[error("invalid database name {0:?}: must match [a-z_][a-z0-9_]*")]
    InvalidDbName(String),
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error("initdb failed: exit {0}")]
    Initdb(std::process::ExitStatus),
    #[error("postgres did not become ready within {0:?}")]
    NotReady(std::time::Duration),
    #[error("connect: {0}")]
    Connect(sqlx::Error),
    #[error("create database: {0}")]
    CreateDatabase(sqlx::Error),
    #[error("pg_ctl stop failed: exit {0}")]
    Stop(std::process::ExitStatus),
}

/// Guard the database name before it is spliced into `CREATE DATABASE` (which
/// cannot be parameterised). Conservative identifier subset; rejects anything
/// that could break out of the identifier position.
pub fn validate_db_name(name: &str) -> Result<(), EmbeddedPgError> {
    let mut chars = name.chars();
    let ok_first = matches!(chars.next(), Some(c) if c == '_' || c.is_ascii_lowercase());
    let ok_rest = chars.all(|c| c == '_' || c.is_ascii_lowercase() || c.is_ascii_digit());
    if ok_first && ok_rest && !name.is_empty() {
        Ok(())
    } else {
        Err(EmbeddedPgError::InvalidDbName(name.to_string()))
    }
}
