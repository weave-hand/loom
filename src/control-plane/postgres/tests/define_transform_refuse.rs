//! Define-time UX guard: `define_transform` refuses a Physical def whose output
//! names a declared stream table, and a Typed def whose output type binds to one.
//! A batch output, or an output table that does not exist yet, defines cleanly.

use control_plane_core::{
    ControlPlane, ControlPlaneError, ObjectType, OutputMode, StreamTables, TableRef, TransformBody,
    TransformDef, TransformName,
};
use control_plane_postgres::fixture::PgFixture;
use control_plane_postgres::iceberg_mirror::{ensure_table, next_snapshot};

fn tref(schema: &str, name: &str) -> TableRef {
    TableRef {
        schema: schema.into(),
        name: name.into(),
    }
}

fn physical_def(name: &str, input: TableRef, output: TableRef) -> TransformDef {
    TransformDef {
        name: TransformName(name.into()),
        body: TransformBody::Physical {
            inputs: vec![input],
            output,
            sql: "select * from src".into(),
            output_mode: OutputMode::Append,
        },
        schedule: None,
        on_input_commit: false,
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn define_transform_refuses_stream_output() {
    let fx = PgFixture::shared();
    let (cp, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;

    // (a) output does not exist yet → defines cleanly (nothing to refuse).
    cp.transforms()
        .define_transform(physical_def(
            "t_new",
            tref("s", "src"),
            tref("s", "not_yet"),
        ))
        .await
        .expect("undeclared output defines");

    // (b) output is a declared stream table → refused.
    let out = tref("s", "declared_stream_out");
    let mut tx = pool.begin().await.expect("begin");
    let at = next_snapshot(&mut tx, None).await.expect("snap");
    let tid = ensure_table(&mut tx, &out.schema, &out.name, at)
        .await
        .expect("ensure");
    tx.commit().await.expect("commit");
    cp.declare_stream(tid, 4).await.expect("declare_stream");

    let err = cp
        .transforms()
        .define_transform(physical_def("t_stream", tref("s", "src"), out.clone()))
        .await
        .expect_err("stream output must be refused at define time");
    assert!(
        matches!(err, ControlPlaneError::Validation(_)),
        "got {err:?}"
    );
    assert!(
        err.to_string().contains("stream-table target refused:"),
        "msg: {err}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn define_transform_refuses_typed_stream_output() {
    let fx = PgFixture::shared();
    let (cp, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;

    // Bind a type to a backing table, then declare that backing table a stream.
    cp.ontology()
        .define_type(
            ObjectType::build("LineStream", ("s", "line_stream"))
                .prop_req("id", "Long")
                .prop("note", "String")
                .identity("id")
                .done(),
        )
        .await
        .expect("define type");
    let mut tx = pool.begin().await.expect("begin");
    let at = next_snapshot(&mut tx, None).await.expect("snap");
    let tid = ensure_table(&mut tx, "s", "line_stream", at)
        .await
        .expect("ensure");
    tx.commit().await.expect("commit");
    cp.declare_stream(tid, 4).await.expect("declare_stream");

    // A Typed def whose output type binds to the stream table → refused at define time.
    let def = TransformDef {
        name: TransformName("typed_stream".into()),
        body: TransformBody::Typed {
            inputs: vec!["LineStream".into()],
            output: "LineStream".into(),
            sql: "select * from LineStream".into(),
            output_mode: OutputMode::Append,
        },
        schedule: None,
        on_input_commit: false,
    };
    let err = cp
        .transforms()
        .define_transform(def)
        .await
        .expect_err("typed stream output must be refused");
    assert!(
        matches!(err, ControlPlaneError::Validation(_)),
        "got {err:?}"
    );
    assert!(
        err.to_string().contains("stream-table target refused:"),
        "msg: {err}"
    );
}
