//! `loom`: the single self-contained binary. Self-extracts the embedded Postgres
//! (when embedded and no external bin dir is set), then runs the composite with a
//! SIGINT/SIGTERM-driven graceful shutdown.
use std::collections::HashMap;

use standalone::StandaloneAddrs;

type BoxErr = Box<dyn std::error::Error + Send + Sync>;

const DEFAULT_QUERY_API_ADDR: &str = "0.0.0.0:8080";
const DEFAULT_INGEST_ADDR: &str = "0.0.0.0:8081";

#[tokio::main]
async fn main() -> Result<(), BoxErr> {
    service_runtime::init_tracing();

    // Out-of-band first-admin bootstrap. `loom create-admin --username <u>` reads
    // the password from stdin (prompt to stderr); no network path.
    if std::env::args().nth(1).as_deref() == Some("create-admin") {
        return create_admin_cli().await;
    }

    let mut env = service_runtime::env_map();

    // Migrate-and-exit works for the loom image too (chart one-shot migrator).
    // This path targets an EXTERNAL/managed PG (it connects to `cfg.db`), so it runs
    // before any embedded `extract_pg` and does not set `LOOM_PG_MODE=embedded`.
    if service_runtime::migrate_requested(&env) {
        let cfg = service_runtime::Config::from_map(&env)?;
        service_runtime::run_migrations(&cfg.db).await?;
        return Ok(());
    }

    // Embedded + no external bin dir: self-extract the baked-in PG and inject it.
    let embedded = env.get("LOOM_PG_MODE").map(String::as_str) == Some("embedded");
    if embedded && !env.contains_key("LOOM_PG_BIN_DIR") {
        let data_path = env
            .get("LOOM_DATA_PATH")
            .ok_or_else(|| -> BoxErr { "LOOM_DATA_PATH is required in embedded mode".into() })?;
        let cache_root = std::path::Path::new(data_path).join("cache");
        std::fs::create_dir_all(&cache_root)?;
        let ex = managed_postgres_embed::extract_pg(&cache_root)?;
        env.insert("LOOM_PG_BIN_DIR".into(), ex.bin_dir.display().to_string());
        env.insert(
            "LOOM_PG_LD_LIBRARY_PATH".into(),
            ex.lib_dir.display().to_string(),
        );
    }

    let cfg = service_runtime::Config::from_map(&env)?;
    let addrs = resolve_addrs(&env)?;
    let tuning = standalone::StandaloneTuning::from_map(&env)?;

    // The seam registers SIGINT/SIGTERM synchronously, so a signal in the first
    // instants of process life is caught rather than killing the process. The
    // composite is deliberately NOT wrapped in `run_bounded`: cutting its drain
    // short would skip stopping the embedded Postgres, which is worse than waiting.
    let shutdown = service_runtime::Shutdown::install(service_runtime::shutdown_timeout(&env)?);

    standalone::run(cfg, addrs, tuning, shutdown.signalled(), ready_noop()).await
}

async fn create_admin_cli() -> Result<(), BoxErr> {
    use std::io::Write;
    let mut username = None;
    let mut args = std::env::args().skip(2);
    while let Some(a) = args.next() {
        match a.as_str() {
            "--username" => username = args.next(),
            "--password-stdin" => {} // password always read from stdin
            other => return Err(format!("unknown create-admin arg: {other}").into()),
        }
    }
    let username = username.ok_or_else(|| -> BoxErr { "--username is required".into() })?;

    // Read one line of password from stdin (works piped and interactive).
    let mut stderr = std::io::stderr();
    write!(stderr, "Password: ")?;
    stderr.flush()?;
    let mut password = String::new();
    std::io::stdin().read_line(&mut password)?;
    let password = password.trim_end_matches(['\n', '\r']).to_string();

    let env = service_runtime::env_map();
    let cfg = service_runtime::Config::from_map(&env)?;
    let pool = service_runtime::build_pool(&cfg.db).await?;
    let cp = service_runtime::control_plane(pool, cfg.lock_timeout);
    service_runtime::create_admin::run_create_admin(&cp, &username, &password).await?;
    #[expect(
        clippy::print_stdout,
        reason = "CLI subcommand result — the intended user-facing confirmation"
    )]
    {
        println!("created admin '{username}' and sealed the instance");
    }
    Ok(())
}

fn resolve_addrs(env: &HashMap<String, String>) -> Result<StandaloneAddrs, BoxErr> {
    let parse = |key: &str, default: &str| -> Result<std::net::SocketAddr, BoxErr> {
        let raw = env.get(key).map_or(default, String::as_str);
        raw.parse()
            .map_err(|e| -> BoxErr { format!("{key} `{raw}` invalid: {e}").into() })
    };
    let engine_socket = env
        .get("LOOM_ENGINE_SOCKET")
        .cloned()
        .ok_or_else(|| -> BoxErr { "LOOM_ENGINE_SOCKET is required".into() })?;
    Ok(StandaloneAddrs {
        query_api: parse("LOOM_QUERY_API_BIND_ADDR", DEFAULT_QUERY_API_ADDR)?,
        ingest: parse("LOOM_INGEST_BIND_ADDR", DEFAULT_INGEST_ADDR)?,
        engine_socket,
    })
}

/// The composite requires a `ready` sender; the binary does not consume readiness,
/// so it hands over a channel whose receiver it immediately drops.
fn ready_noop() -> tokio::sync::oneshot::Sender<()> {
    let (tx, _rx) = tokio::sync::oneshot::channel();
    tx
}
