//! Acceptance e2e (road-external-sql-wire, slice 2): a REAL TCP `FlightServiceClient`
//! dials `FlightSqlWireService` bound on an ephemeral port, authenticates with a bearer
//! token, and runs ARBITRARY SQL it authored — over the full stack (landing -> engine
//! over UDS -> the wire service). Asserts the caller's own SQL is governed row-for-row
//! and column-for-column: a row filter survives a JOIN and a GROUP BY, a denied column
//! is both absent from `SELECT *` and unnameable directly, an ungranted type / an
//! unbound (untyped) dataset / a genuinely nonexistent table are all rejected with the
//! SAME error class (no existence leak), both session and service bearer tokens work,
//! missing/garbage bearers are `Unauthenticated`, a forged loom-native governed ticket
//! (which would let an external caller choose its own catalog) is rejected outright, and
//! the row cap fails the stream rather than truncating silently.

use loom_test_flight::{EngineGuard, spawn_flight_uds};
use loom_test_seed::local_sql_catalog;
use std::sync::Arc;

use arrow_array::{Array, Int64Array, RecordBatch, StringArray};
use arrow_flight::Ticket;
use arrow_flight::decode::FlightRecordBatchStream;
use arrow_flight::error::FlightError;
use arrow_flight::flight_service_client::FlightServiceClient;
use arrow_flight::flight_service_server::FlightServiceServer;
use arrow_flight::sql::{CommandStatementQuery, ProstMessageExt};
use arrow_flight::{FlightDescriptor, FlightEndpoint, FlightInfo};
use arrow_schema::{DataType, Field, Schema};
use control_plane_core::{
    Acl, Action, Auth, ColumnSpec, CompareOp, ControlPlane, DatasetId, EventType, GovernedCatalog,
    GovernedTable, LineageEvent, NewServiceAccount, ObjectType, Ontology, Policy, PolicyTarget,
    PropertyDef, RowFilter, RunId, ScalarValue, SubjectId, TableRef, TypeName,
};
use control_plane_postgres::PgControlPlane;
use control_plane_postgres::fixture::PgFixture;
use control_plane_postgres::iceberg_landing::{InlineLimits, land};
use engine_wire::flight::{FlightSqlClient, GovernedStatementQuery};
use futures::TryStreamExt;
use prost::Message as _;
use query_api::flight_sql::FlightSqlWireService;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::Channel;
use tonic::transport::Server;

/// Everything a test needs, kept alive for its duration: the TCP address of the wire
/// service, a session-token bearer for `reader`, a service-token bearer for the same
/// role, the control-plane handle (for out-of-band ACL tweaks), and the guards that
/// must not drop (the warehouse tempdir + the UDS engine).
struct Harness {
    addr: std::net::SocketAddr,
    session_token: String,
    service_token: String,
    #[expect(dead_code, reason = "kept for potential out-of-band ACL assertions")]
    cp: Arc<PgControlPlane>,
    _wh: tempfile::TempDir,
    _eng: EngineGuard,
}

fn t_orders() -> TableRef {
    TableRef {
        schema: "wh".into(),
        name: "orders".into(),
    }
}
fn t_customers() -> TableRef {
    TableRef {
        schema: "wh".into(),
        name: "customers".into(),
    }
}
fn t_raw_dump() -> TableRef {
    TableRef {
        schema: "wh".into(),
        name: "raw_dump".into(),
    }
}
fn t_secrets() -> TableRef {
    TableRef {
        schema: "wh".into(),
        name: "secrets".into(),
    }
}

fn cols_orders() -> Vec<ColumnSpec> {
    vec![
        ColumnSpec {
            name: "id".into(),
            ty: "long".into(),
            nullable: false,
        },
        ColumnSpec {
            name: "customer_id".into(),
            ty: "long".into(),
            nullable: false,
        },
        ColumnSpec {
            name: "email".into(),
            ty: "string".into(),
            nullable: false,
        },
    ]
}
fn cols_customers() -> Vec<ColumnSpec> {
    vec![
        ColumnSpec {
            name: "id".into(),
            ty: "long".into(),
            nullable: false,
        },
        ColumnSpec {
            name: "name".into(),
            ty: "string".into(),
            nullable: false,
        },
        ColumnSpec {
            name: "ssn".into(),
            ty: "string".into(),
            nullable: false,
        },
    ]
}
fn cols_raw_dump() -> Vec<ColumnSpec> {
    vec![
        ColumnSpec {
            name: "id".into(),
            ty: "long".into(),
            nullable: false,
        },
        ColumnSpec {
            name: "payload".into(),
            ty: "string".into(),
            nullable: false,
        },
    ]
}
fn cols_secrets() -> Vec<ColumnSpec> {
    vec![
        ColumnSpec {
            name: "id".into(),
            ty: "long".into(),
            nullable: false,
        },
        ColumnSpec {
            name: "value".into(),
            ty: "string".into(),
            nullable: false,
        },
    ]
}

/// `orders(id, customer_id, email)`: 4 rows, 2 orders per customer, each a distinct
/// email — distinct enough that a GROUP BY on the unmasked column would yield 4
/// groups, so a single masked group is a meaningful (non-vacuous) assertion.
fn orders_batch() -> (Arc<Schema>, Vec<RecordBatch>) {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("customer_id", DataType::Int64, false),
        Field::new("email", DataType::Utf8, false),
    ]));
    let id = Int64Array::from(vec![1i64, 2, 3, 4]);
    let cust = Int64Array::from(vec![1i64, 1, 2, 2]);
    let email = StringArray::from(vec!["a1@x.com", "a2@x.com", "a3@x.com", "a4@x.com"]);
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(id), Arc::new(cust), Arc::new(email)],
    )
    .expect("orders batch");
    (schema, vec![batch])
}

/// `customers(id, name, ssn)`: 2 rows.
fn customers_batch() -> (Arc<Schema>, Vec<RecordBatch>) {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("ssn", DataType::Utf8, false),
    ]));
    let id = Int64Array::from(vec![1i64, 2]);
    let name = StringArray::from(vec!["Alice", "Bob"]);
    let ssn = StringArray::from(vec!["111-11-1111", "222-22-2222"]);
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(id), Arc::new(name), Arc::new(ssn)],
    )
    .expect("customers batch");
    (schema, vec![batch])
}

/// `raw_dump(id, payload)`: a landed dataset bound to NO ontology type at all.
fn raw_dump_batch() -> (Arc<Schema>, Vec<RecordBatch>) {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("payload", DataType::Utf8, false),
    ]));
    let id = Int64Array::from(vec![1i64]);
    let payload = StringArray::from(vec!["raw"]);
    let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(id), Arc::new(payload)])
        .expect("raw_dump batch");
    (schema, vec![batch])
}

/// `secrets(id, value)`: a typed table the `reader` subject holds NO grant on.
fn secrets_batch() -> (Arc<Schema>, Vec<RecordBatch>) {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("value", DataType::Utf8, false),
    ]));
    let id = Int64Array::from(vec![1i64]);
    let value = StringArray::from(vec!["top-secret"]);
    let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(id), Arc::new(value)])
        .expect("secrets batch");
    (schema, vec![batch])
}

fn lineage(run: RunId, schema: &str, name: &str) -> LineageEvent {
    let out = TableRef {
        schema: schema.into(),
        name: name.into(),
    };
    LineageEvent {
        run_id: run,
        event_type: EventType::Complete,
        event_time: time::OffsetDateTime::now_utc(),
        inputs: vec![],
        outputs: vec![DatasetId::from(&out).dataset_ref()],
        payload: serde_json::json!({ "source": "test" }),
    }
}

fn prop(name: &str, ty: &str) -> PropertyDef {
    PropertyDef::new(name, ty).required()
}

/// Wrap a Flight message in a `tonic::Request` carrying a `Bearer` authorization header.
fn authed<T>(msg: T, token: &str) -> tonic::Request<T> {
    let mut req = tonic::Request::new(msg);
    req.metadata_mut().insert(
        "authorization",
        format!("Bearer {token}").parse().expect("metadata value"),
    );
    req
}

async fn connect_client(addr: std::net::SocketAddr) -> FlightServiceClient<Channel> {
    let url = format!("http://{addr}");
    let channel = tonic::transport::Endpoint::try_from(url)
        .expect("endpoint")
        .connect()
        .await
        .expect("connect");
    FlightServiceClient::new(channel)
}

/// A `FlightError` carrying a `tonic::Status` unwraps to it directly (preserving the
/// gRPC code); anything else (a genuine transport/decode fault) becomes `Unknown`.
fn flight_err_to_status(e: FlightError) -> tonic::Status {
    match e {
        FlightError::Tonic(s) => *s,
        other => tonic::Status::unknown(other.to_string()),
    }
}

/// The standard Flight SQL client dance a real external caller performs: encode `sql`
/// as a `CommandStatementQuery`, `get_flight_info` for the ticket, `do_get` + decode.
/// Any failure at any step (auth, planning, or mid-stream) surfaces as a `tonic::Status`
/// with its original gRPC code — never panics — so callers can assert on the code and
/// message even on the expected-failure paths.
async fn run_sql(
    client: &mut FlightServiceClient<Channel>,
    sql: &str,
    token: &str,
) -> Result<Vec<RecordBatch>, tonic::Status> {
    let cmd = CommandStatementQuery {
        query: sql.to_string(),
        transaction_id: None,
    };
    let desc = FlightDescriptor::new_cmd(cmd.as_any().encode_to_vec());
    let info: FlightInfo = client
        .get_flight_info(authed(desc, token))
        .await?
        .into_inner();
    let ticket = info
        .endpoint
        .into_iter()
        .next()
        .and_then(|e: FlightEndpoint| e.ticket)
        .expect("ticket");
    let stream = client.do_get(authed(ticket, token)).await?.into_inner();
    let data = stream.map_err(FlightError::from);
    FlightRecordBatchStream::new_from_flight_data(data)
        .try_collect()
        .await
        .map_err(flight_err_to_status)
}

/// Full harness: land `wh.orders`/`wh.customers`/`wh.raw_dump`/`wh.secrets` through the
/// REAL landing path, bind `Order`/`Customer`/`Secret` ontology types (leaving
/// `raw_dump` unbound), grant `reader` a row-filtered + column-masked/denied Read on
/// `Order`/`Customer` only (nothing on `Secret`, nothing on `raw_dump` — it isn't even a
/// type), mint a session token AND a service token for `reader`'s role, boot the engine
/// over a UDS, and stand `FlightSqlWireService` on an ephemeral TCP port.
async fn setup(fx: &PgFixture) -> Harness {
    setup_with_cap(fx, 100_000).await
}

/// Like [`setup`] but with a configurable `max_rows` wire cap, for the row-cap case.
async fn setup_with_cap(fx: &PgFixture, max_rows: u32) -> Harness {
    let (cp, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;
    let dsn = fx.pg_dsn(&db);
    let wh = tempfile::tempdir().expect("warehouse");
    let warehouse = wh.path().display().to_string();
    let catalog = local_sql_catalog(dsn.clone(), &warehouse).await;

    // Land all four tables through the real landing path (Parquet-forced).
    let limits = InlineLimits {
        inline_byte_limit: 0,
        flush_byte_threshold: i64::MAX,
    };
    let (schema, batches) = orders_batch();
    land(
        &pool,
        &catalog,
        &t_orders(),
        &cols_orders(),
        schema,
        batches,
        limits,
        lineage(RunId(uuid::Uuid::new_v4()), "wh", "orders"),
        None,
    )
    .await
    .expect("land orders");
    let (schema, batches) = customers_batch();
    land(
        &pool,
        &catalog,
        &t_customers(),
        &cols_customers(),
        schema,
        batches,
        limits,
        lineage(RunId(uuid::Uuid::new_v4()), "wh", "customers"),
        None,
    )
    .await
    .expect("land customers");
    let (schema, batches) = raw_dump_batch();
    land(
        &pool,
        &catalog,
        &t_raw_dump(),
        &cols_raw_dump(),
        schema,
        batches,
        limits,
        lineage(RunId(uuid::Uuid::new_v4()), "wh", "raw_dump"),
        None,
    )
    .await
    .expect("land raw_dump");
    let (schema, batches) = secrets_batch();
    land(
        &pool,
        &catalog,
        &t_secrets(),
        &cols_secrets(),
        schema,
        batches,
        limits,
        lineage(RunId(uuid::Uuid::new_v4()), "wh", "secrets"),
        None,
    )
    .await
    .expect("land secrets");

    // Ontology: Order, Customer, Secret. `raw_dump` is deliberately left unbound.
    cp.define_type(
        ObjectType::build("Order", (t_orders().schema, t_orders().name))
            .add_prop(prop("id", "long"))
            .add_prop(prop("customer_id", "long"))
            .add_prop(prop("email", "string"))
            .identity("id")
            .done(),
    )
    .await
    .expect("define Order type");
    cp.define_type(
        ObjectType::build("Customer", (t_customers().schema, t_customers().name))
            .add_prop(prop("id", "long"))
            .add_prop(prop("name", "string"))
            .add_prop(prop("ssn", "string"))
            .identity("id")
            .done(),
    )
    .await
    .expect("define Customer type");
    cp.define_type(
        ObjectType::build("Secret", (t_secrets().schema, t_secrets().name))
            .add_prop(prop("id", "long"))
            .add_prop(prop("value", "string"))
            .identity("id")
            .done(),
    )
    .await
    .expect("define Secret type");

    // ACL: `reader` gets a row-filtered AND column-masked Read on Order (a SINGLE
    // `set_policy` call — it upserts one row per (role, action, target), so a second
    // call would silently overwrite the first rather than compose with it), a
    // column-denied Read on Customer, and NOTHING on Secret or raw_dump.
    let (_subj, role) = e2e_support::subject_with_role(&cp, "reader").await;
    e2e_support::grant_read(&cp, &role, "Order").await;
    cp.set_policy(
        &role,
        Action::Read,
        Policy {
            target: PolicyTarget::Type(TypeName("Order".into())),
            row_filter: Some(RowFilter::Compare {
                property: "id".into(),
                op: CompareOp::Ge,
                value: ScalarValue::Int(2),
            }),
            deny_columns: vec![],
            mask_columns: vec!["email".into()],
        },
    )
    .await
    .expect("set Order row-filter + mask policy");
    e2e_support::grant_read_columns(&cp, &role, "Customer", vec!["ssn".into()], vec![]).await;
    let session_token = e2e_support::session_token(&cp, "reader").await;

    // A service token mapped to the SAME reader role.
    let svc_subject = SubjectId("ci-bot-svc".into());
    cp.create_service_account(&NewServiceAccount {
        subject_id: svc_subject.clone(),
        name: "ci-bot".to_string(),
    })
    .await
    .expect("create_service_account");
    cp.assign_role(&svc_subject, &role)
        .await
        .expect("assign service account to reader role");
    let svc_tok = service_runtime::generate_session_token();
    let expires = time::OffsetDateTime::now_utc() + time::Duration::hours(1);
    cp.create_service_token(
        &svc_subject,
        &service_runtime::token_sha256(&svc_tok),
        "e2e",
        expires,
    )
    .await
    .expect("create_service_token");

    // Boot the engine over a UDS, then stand the wire service on an ephemeral TCP port.
    let eng = spawn_flight_uds(fx, &db, &warehouse).await;
    let cp = Arc::new(cp);
    let auth: Arc<dyn Auth + Send + Sync> = cp.clone();
    let cp_dyn: Arc<dyn ControlPlane> = cp.clone();
    let flight_engine = FlightSqlClient::connect(eng.sock.clone())
        .await
        .expect("engine connect");
    let wire = FlightSqlWireService::new(auth, cp_dyn, flight_engine, max_rows);

    let tcp = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind tcp");
    let addr = tcp.local_addr().expect("addr");
    let incoming = TcpListenerStream::new(tcp);
    tokio::spawn(async move {
        let _serve = Server::builder()
            .add_service(FlightServiceServer::new(wire))
            .serve_with_incoming(incoming)
            .await;
    });
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    Harness {
        addr,
        session_token,
        service_token: svc_tok,
        cp,
        _wh: wh,
        _eng: eng,
    }
}

/// Case 1: an arbitrary client-authored JOIN is governed row-for-row (the `id >= 2` row
/// filter on `Order` survives the join) and column-for-column (the masked `email` reads
/// as the literal `"***"` while the unmasked `name` from the joined `Customer` side
/// survives value-exact) — plus a GROUP BY flavor: masking collapses 3 distinct emails
/// (each a different row) into a single `'***'` group, which would be 3 groups unmasked.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn arbitrary_join_is_governed() {
    let fx = PgFixture::shared();
    let h = setup(fx).await;
    let mut client = connect_client(h.addr).await;

    let sql = r#"SELECT o."id", c."name", o."email" FROM "wh"."orders" o JOIN "wh"."customers" c ON o."customer_id" = c."id" ORDER BY o."id""#;
    let batches = run_sql(&mut client, sql, &h.session_token)
        .await
        .expect("governed join must succeed");
    let total: usize = batches.iter().map(RecordBatch::num_rows).sum();
    assert_eq!(
        total, 3,
        "row filter id >= 2 must keep exactly orders 2,3,4"
    );

    let mut ids = Vec::new();
    let mut names = Vec::new();
    let mut emails = Vec::new();
    for b in &batches {
        let id_col = b
            .column_by_name("id")
            .expect("id column")
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("id i64");
        let name_col = b
            .column_by_name("name")
            .expect("name column")
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("name utf8");
        let email_col = b
            .column_by_name("email")
            .expect("email column")
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("email utf8 (masked)");
        for i in 0..b.num_rows() {
            ids.push(id_col.value(i));
            names.push(name_col.value(i).to_string());
            emails.push(email_col.value(i).to_string());
        }
    }
    assert_eq!(ids, vec![2, 3, 4], "exactly the filtered ids, in order");
    assert_eq!(
        names,
        vec!["Alice", "Bob", "Bob"],
        "the unmasked joined `name` must survive value-exact"
    );
    for e in &emails {
        assert_eq!(
            e, "***",
            "every email must be masked, even carried through a JOIN"
        );
    }

    // GROUP BY flavor: 3 distinct real emails (a2/a3/a4) collapse to a single masked group.
    let gsql = r#"SELECT "email", count(*) AS cnt FROM "wh"."orders" GROUP BY "email""#;
    let gbatches = run_sql(&mut client, gsql, &h.session_token)
        .await
        .expect("governed group by must succeed");
    let total_groups: usize = gbatches.iter().map(RecordBatch::num_rows).sum();
    assert_eq!(
        total_groups, 1,
        "masking collapses every row into a single '***' group (would be 3 groups unmasked)"
    );
    let email_col = gbatches[0]
        .column_by_name("email")
        .expect("email column")
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("email utf8");
    assert_eq!(email_col.value(0), "***");
    let cnt_col = gbatches[0]
        .column_by_name("cnt")
        .expect("cnt column")
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("cnt i64");
    assert_eq!(
        cnt_col.value(0),
        3,
        "the row filter's 3 surviving rows all fold into the one group"
    );
}

/// Case 2: `SELECT *` on a column-denied table never advertises the denied column at
/// all, and naming it explicitly is `invalid_argument` (unnameable, not merely hidden).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn denied_column_absent_and_unnameable() {
    let fx = PgFixture::shared();
    let h = setup(fx).await;
    let mut client = connect_client(h.addr).await;

    let batches = run_sql(
        &mut client,
        r#"SELECT * FROM "wh"."customers""#,
        &h.session_token,
    )
    .await
    .expect("select * on customers must succeed");
    assert!(
        !batches.is_empty(),
        "customers select produced at least one batch"
    );
    let schema = batches[0].schema();
    assert!(schema.field_with_name("id").is_ok(), "id must be present");
    assert!(
        schema.field_with_name("name").is_ok(),
        "name must be present"
    );
    assert!(
        schema.field_with_name("ssn").is_err(),
        "the denied ssn column must not appear in the result schema at all"
    );

    let err = run_sql(
        &mut client,
        r#"SELECT "ssn" FROM "wh"."customers""#,
        &h.session_token,
    )
    .await
    .expect_err("naming the denied column directly must fail");
    assert_eq!(
        err.code(),
        tonic::Code::InvalidArgument,
        "an unnameable denied column is a client-SQL-vocabulary planning error"
    );
}

/// Case 3: a granted subject's query against a type it holds NO grant on (`Secret`) and
/// against a landed-but-unbound dataset (`raw_dump`) are both `invalid_argument` with a
/// table-resolution error — and, critically, the SAME error class as querying a table
/// that never existed at all (`does_not_exist`). No existence leak: none of the three
/// error messages may be distinguishable from one another beyond the table name the
/// caller itself supplied, and none may carry internal (path/backend) detail.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ungranted_and_unbound_tables_unresolvable() {
    let fx = PgFixture::shared();
    let h = setup(fx).await;
    let mut client = connect_client(h.addr).await;

    let err_secret = run_sql(
        &mut client,
        r#"SELECT * FROM "wh"."secrets""#,
        &h.session_token,
    )
    .await
    .expect_err("an ungranted type's table must be unresolvable");
    let err_raw = run_sql(
        &mut client,
        r#"SELECT * FROM "wh"."raw_dump""#,
        &h.session_token,
    )
    .await
    .expect_err("an unbound (untyped) landed dataset must be unresolvable");
    let err_missing = run_sql(
        &mut client,
        r#"SELECT * FROM "wh"."does_not_exist""#,
        &h.session_token,
    )
    .await
    .expect_err("a genuinely nonexistent table must be unresolvable");

    for e in [&err_secret, &err_raw, &err_missing] {
        assert_eq!(
            e.code(),
            tonic::Code::InvalidArgument,
            "table-resolution failures are client planning errors: {e}"
        );
    }

    // No-existence-leak: normalize each message by masking out the caller's own table
    // name, then require the resulting template to be identical across all three. An
    // ungranted type, an unbound dataset, and a table that never existed must be
    // indistinguishable to the caller beyond the name it supplied itself.
    let norm = |msg: &str, needle: &str| msg.replace(needle, "<table>");
    let tmpl_secret = norm(err_secret.message(), "secrets");
    let tmpl_raw = norm(err_raw.message(), "raw_dump");
    let tmpl_missing = norm(err_missing.message(), "does_not_exist");
    assert_eq!(
        tmpl_secret, tmpl_raw,
        "ungranted-type error must be worded identically to the unbound-dataset error"
    );
    assert_eq!(
        tmpl_raw, tmpl_missing,
        "unbound-dataset error must be worded identically to the nonexistent-table error \
         (no signal that distinguishes 'exists but ungoverned' from 'never existed')"
    );

    // Scrubbing: the message is DataFusion's own SQL-planning vocabulary — no internal
    // detail (filesystem/socket paths, backend identifiers) reaches the external client.
    for e in [&err_secret, &err_raw, &err_missing] {
        let m = e.message();
        assert!(!m.contains(".sock"), "must not leak a socket path: {m}");
        assert!(
            !m.contains("postgres"),
            "must not leak a backend identifier: {m}"
        );
        assert!(!m.contains('/'), "must not leak a filesystem path: {m}");
    }
}

/// Case 4: no bearer at all is `Unauthenticated`; a syntactically-valid-but-unknown
/// bearer is also `Unauthenticated` (not conflated with "missing"); and a **service**
/// token mapped to the same reader role runs case 1's governed query successfully.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn auth_matrix() {
    let fx = PgFixture::shared();
    let h = setup(fx).await;
    let mut client = connect_client(h.addr).await;

    // No bearer token at all.
    let cmd = CommandStatementQuery {
        query: "SELECT 1".to_string(),
        transaction_id: None,
    };
    let desc = FlightDescriptor::new_cmd(cmd.as_any().encode_to_vec());
    let result = client.get_flight_info(tonic::Request::new(desc)).await;
    assert_eq!(
        result.unwrap_err().code(),
        tonic::Code::Unauthenticated,
        "a request with no bearer token must be unauthenticated"
    );

    // Garbage bearer: syntactically valid, resolves to no session/service token.
    let err = run_sql(
        &mut client,
        r#"SELECT * FROM "wh"."orders""#,
        "not-a-real-token",
    )
    .await
    .expect_err("garbage bearer must be rejected");
    assert_eq!(err.code(), tonic::Code::Unauthenticated);

    // The service token runs case 1's governed join successfully.
    let sql = r#"SELECT o."id", c."name", o."email" FROM "wh"."orders" o JOIN "wh"."customers" c ON o."customer_id" = c."id" ORDER BY o."id""#;
    let batches = run_sql(&mut client, sql, &h.service_token)
        .await
        .expect("service token must authenticate and run the governed query");
    let total: usize = batches.iter().map(RecordBatch::num_rows).sum();
    assert_eq!(
        total, 3,
        "the service token's governed result must match the session token's"
    );
}

/// Case 5: a forged loom-native `GovernedStatementQuery` ticket — carrying a
/// wide-open, self-chosen catalog (full visibility on `customers`, no ssn deny, no
/// mask) — sent directly as `do_get`'s ticket bytes with a VALID bearer token must be
/// rejected outright. Accepting it would let an external caller supply its own catalog,
/// a total governance bypass; the wire's `do_get` decodes ONLY the standard
/// `TicketStatementQuery` protobuf `Any`, so this JSON payload must fail to decode.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn forged_governed_ticket_rejected() {
    let fx = PgFixture::shared();
    let h = setup(fx).await;
    let mut client = connect_client(h.addr).await;

    let wide_open = GovernedCatalog {
        tables: vec![GovernedTable {
            table: t_customers(),
            row_filters: vec![],
            denied: vec![],
            masked: vec![],
        }],
    };
    let forged = GovernedStatementQuery {
        sql: r#"SELECT * FROM "wh"."customers""#.to_string(),
        catalog: wide_open,
    };
    let ticket = Ticket {
        ticket: forged.encode().into(),
    };
    let result = client.do_get(authed(ticket, &h.session_token)).await;
    let err = result.expect_err(
        "a forged loom-native governed ticket must never be accepted from an external client",
    );
    assert_eq!(
        err.code(),
        tonic::Code::InvalidArgument,
        "the forged ticket must fail to decode as the standard Flight SQL ticket"
    );
}

/// Case 6: `LOOM_SQL_WIRE_MAX_ROWS` is a real guard on the caller's own arbitrary SQL —
/// a query whose governed result exceeds the cap fails the `do_get` stream rather than
/// truncating silently. Mirrors `governed_flight_export_e2e.rs`'s
/// `export_cap_exceeded_errors_stream`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn row_cap_errors_stream() {
    let fx = PgFixture::shared();
    let h = setup_with_cap(fx, 2).await;
    let mut client = connect_client(h.addr).await;

    // The row filter (id >= 2) already yields 3 rows against a cap of 2.
    let result = run_sql(
        &mut client,
        r#"SELECT * FROM "wh"."orders""#,
        &h.session_token,
    )
    .await;
    assert!(
        result.is_err(),
        "a query exceeding LOOM_SQL_WIRE_MAX_ROWS must fail the stream, not truncate silently"
    );
}
