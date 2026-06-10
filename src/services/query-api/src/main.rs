//! query-api binary: builds the app state from a Postgres pool + embedded DuckDB
//! and serves the HTTP API. (Config plumbing is intentionally minimal for the slice;
//! wiring a real pool + bind address is the serving-tier spec.)

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    eprintln!("query-api: serving-tier wiring is a later spec; see the slice design doc.");
    Ok(())
}
