//! Quack serving tier: a hermetic `quack_serve`'d DuckDB (QuackServer) over a
//! seeded DuckLake catalog, and the QuackServingEngine reading through it.
//! Proves parity with EmbeddedDuckDb (tests/serving_engine.rs covers the embedded
//! engine's own mechanics).

use std::io::Write;
use std::net::TcpStream;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::Duration;

use control_plane_postgres::fixture::{DuckLakeWriter, PgFixture};
use query_api::serving::{QuackServingEngine, ServingEngine, SqlValue};

const TOKEN: &str = "loom-test-quack-token";

/// A DuckDB process running `quack_serve` over a DuckLake catalog. Spawns the
/// pinned duckdb-cli (DUCKDB_BIN), holds its stdin open so the server thread keeps
/// running, and kills it on Drop.
struct QuackServer {
    child: Child,
    _stdin: std::process::ChildStdin,
    port: u16,
}

impl QuackServer {
    fn start(socket: &Path, db: &str, data_path: &Path) -> Self {
        let bin = PathBuf::from(std::env::var("DUCKDB_BIN").expect("DUCKDB_BIN"));
        let ext = std::env::var("DUCKDB_EXTENSION_DIR").expect("DUCKDB_EXTENSION_DIR");
        let port = {
            let l = std::net::TcpListener::bind("127.0.0.1:0").expect("bind ephemeral");
            l.local_addr().expect("addr").port()
        };
        let script = format!(
            "SET extension_directory='{ext}';\n\
             LOAD ducklake; LOAD postgres_scanner; LOAD quack;\n\
             ATTACH 'ducklake:postgres:dbname={db} host={sock} user=postgres' AS lake \
             (DATA_PATH '{data}/', DATA_INLINING_ROW_LIMIT 0);\n\
             USE lake;\n\
             SELECT listen_uri FROM quack_serve('quack://127.0.0.1:{port}', token := '{TOKEN}', disable_ssl := true);\n",
            sock = socket.display(),
            data = data_path.display(),
        );
        let mut child = Command::new(&bin)
            .stdin(Stdio::piped())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("spawn duckdb quack server");
        let mut stdin = child.stdin.take().expect("child stdin");
        stdin
            .write_all(script.as_bytes())
            .expect("write serve script");
        stdin.flush().expect("flush serve script");
        let server = Self {
            child,
            _stdin: stdin,
            port,
        };
        server.wait_ready();
        server
    }

    fn wait_ready(&self) {
        for _ in 0..200 {
            if TcpStream::connect(("127.0.0.1", self.port)).is_ok() {
                return;
            }
            std::thread::sleep(Duration::from_millis(50));
        }
        panic!("quack server did not accept connections within ~10s");
    }

    /// Client-form URI for quack_query (single-colon scheme).
    fn uri(&self) -> String {
        format!("quack:127.0.0.1:{}", self.port)
    }
}

impl Drop for QuackServer {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

/// Boot pg + bootstrap a DuckLake catalog + start a QuackServer over it. Returns
/// the pieces a test needs; keep `_pg`/`writer` alive (they own the cluster/dirs).
async fn harness() -> (PgFixture, DuckLakeWriter, QuackServer, String) {
    let fx = PgFixture::start();
    let (_cp, db) = fx.fresh_db().await;
    let writer = DuckLakeWriter::new(fx.socket_path(), &db);
    writer.bootstrap().await;
    let server = QuackServer::start(fx.socket_path(), &db, writer.data_path());
    let uri = server.uri();
    (fx, writer, server, uri)
}

#[tokio::test(flavor = "multi_thread")]
async fn quack_engine_executes_a_trivial_query_over_the_wire() {
    // `_server` keeps the serving process alive for the test's scope (Drop kills it);
    // `uri` is the client-form URI returned by harness(), `TOKEN` the shared token.
    let (_fx, _writer, _server, uri) = harness().await;
    let eng = QuackServingEngine::new(uri, TOKEN).expect("engine");
    let rows = eng
        .fetch_rows("SELECT 42 AS n, 'hi' AS s", &[])
        .await
        .unwrap();
    assert_eq!(rows.columns, vec!["n".to_string(), "s".to_string()]);
    assert_eq!(rows.rows.len(), 1);
    assert_eq!(rows.rows[0][0], SqlValue::Int(42));
    assert_eq!(rows.rows[0][1], SqlValue::Text("hi".into()));
}
