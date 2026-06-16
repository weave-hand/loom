//! THE governed-read oracle: an ontology type resolves to a DuckLake table; an ACL
//! policy (row filter + denied column) shapes the result; a request equality filter
//! narrows it. Seeds real rows via the snapshot-commit primitive + a DuckDB-written
//! Parquet (mirrors postgres/tests/ducklake_interop.rs::duckdb_scans_loom_appended_file).

use control_plane_core::{
    Acl, Action, ColumnSpec, ColumnStat, CompareOp, ControlPlane, DataFile, Effect, FileFormat,
    ObjectType, Ontology, Policy, PolicyTarget, PropertyDef, RoleId, RowFilter, ScalarValue,
    StatValue, SubjectId, TableRef, TypeName,
};
use control_plane_postgres::fixture::{DuckLakeWriter, PgFixture};
use query_api::handler::{ObjectQuery, QueryDeps, QueryError, Subject, read_object};
use query_api::serving::{EmbeddedDuckDb, SqlValue};

#[tokio::test(flavor = "multi_thread")]
async fn governed_object_read() {
    let fx = PgFixture::start();
    let (cp, db) = fx.fresh_db().await;
    let writer = DuckLakeWriter::new(fx.socket_path(), &db);
    writer.bootstrap().await;

    let table = TableRef {
        schema: "main".into(),
        name: "orders".into(),
    };

    // 1. loom natively creates the table (id, status, secret).
    let mut tx = cp.begin().await.unwrap();
    tx.create_table(
        &table,
        &[
            ColumnSpec {
                name: "id".into(),
                ty: "int64".into(),
                nullable: false,
            },
            ColumnSpec {
                name: "status".into(),
                ty: "varchar".into(),
                nullable: true,
            },
            ColumnSpec {
                name: "secret".into(),
                ty: "varchar".into(),
                nullable: true,
            },
        ],
    )
    .await
    .unwrap();
    tx.commit().await.unwrap().expect("create snapshot");

    // 2. DuckDB writes the Parquet at the resolved relative path, loom registers it.
    let dir = writer.data_path().join("main").join("orders");
    std::fs::create_dir_all(&dir).unwrap();
    let abs = dir.join("o.parquet");
    writer
        .exec(&format!(
            "COPY (SELECT * FROM (VALUES \
           (1, 'open', 's1'), (2, 'closed', 's2'), (3, 'open', 's3')) \
           AS t(id, status, secret)) TO '{}' (FORMAT parquet);",
            abs.display()
        ))
        .await;
    let bytes = std::fs::read(&abs).unwrap();
    let footer = {
        let l = &bytes[bytes.len() - 8..bytes.len() - 4];
        u32::from_le_bytes(l.try_into().unwrap()) as i64
    };
    let mut tx = cp.begin().await.unwrap();
    tx.append_files(
        &table,
        &[DataFile {
            path: "o.parquet".into(),
            path_is_relative: true,
            file_format: FileFormat::Parquet,
            record_count: 3,
            file_size_bytes: bytes.len() as i64,
            column_stats: vec![
                ColumnStat {
                    column_name: "id".into(),
                    null_count: 0,
                    column_size_bytes: 24,
                    min: Some(StatValue::I64(1)),
                    max: Some(StatValue::I64(3)),
                },
                ColumnStat {
                    column_name: "status".into(),
                    null_count: 0,
                    column_size_bytes: 24,
                    min: Some(StatValue::Str("closed".into())),
                    max: Some(StatValue::Str("open".into())),
                },
                ColumnStat {
                    column_name: "secret".into(),
                    null_count: 0,
                    column_size_bytes: 24,
                    min: Some(StatValue::Str("s1".into())),
                    max: Some(StatValue::Str("s3".into())),
                },
            ],
            parquet_footer_size: Some(footer),
        }],
    )
    .await
    .unwrap();
    tx.commit().await.unwrap().expect("append snapshot");

    // 3. Ontology: type Order -> main.orders, with properties id/status/secret.
    cp.define_type(ObjectType {
        name: TypeName("Order".into()),
        properties: vec![
            PropertyDef {
                name: "id".into(),
                ty: "Long".into(),
                required: true,
            },
            PropertyDef {
                name: "status".into(),
                ty: "String".into(),
                required: false,
            },
            PropertyDef {
                name: "secret".into(),
                ty: "String".into(),
                required: false,
            },
        ],
        derived: vec![],
        table: table.clone(),
    })
    .await
    .unwrap();

    // 4. ACL: subject `analyst` in role `analysts`; policy on type Order denies `secret`
    //    and restricts rows to status = 'open'.
    let subj = SubjectId("analyst".into());
    let role = RoleId("analysts".into());
    cp.define_subject(&subj).await.unwrap();
    cp.define_role(&role).await.unwrap();
    cp.assign_role(&subj, &role).await.unwrap();
    cp.grant(
        &role,
        Action::Read,
        PolicyTarget::Type(TypeName("Order".into())),
        Effect::Allow,
    )
    .await
    .unwrap();
    cp.set_policy(
        &role,
        Policy {
            target: PolicyTarget::Type(TypeName("Order".into())),
            row_filter: Some(RowFilter::Compare {
                property: "status".into(),
                op: CompareOp::Eq,
                value: ScalarValue::Text("open".into()),
            }),
            deny_columns: vec!["secret".into()],
            mask_columns: vec![],
        },
    )
    .await
    .unwrap();

    // 5. Read it.
    let eng = EmbeddedDuckDb::attach(
        &format!(
            "dbname={} host={} user=postgres",
            db,
            fx.socket_path().display()
        ),
        writer.data_path(),
    )
    .await
    .unwrap();
    let deps = QueryDeps {
        ontology: &cp,
        acl: &cp,
        serving: &eng,
    };
    let rows = read_object(
        &ObjectQuery {
            type_name: "Order".into(),
            eq_filters: vec![],
        },
        &Subject(subj.clone()),
        &deps,
    )
    .await
    .unwrap();

    // secret projected out; only status='open' rows (ids 1 and 3) returned.
    assert_eq!(rows.columns, vec!["id".to_string(), "status".to_string()]);
    // logical types travel with the projection, aligned to columns.
    assert_eq!(
        rows.logical_types,
        vec!["Long".to_string(), "String".to_string()]
    );
    let ids: Vec<&SqlValue> = rows.rows.iter().map(|r| &r[0]).collect();
    assert_eq!(rows.rows.len(), 2, "ACL row filter kept only status=open");
    assert!(ids.contains(&&SqlValue::Int(1)) && ids.contains(&&SqlValue::Int(3)));
    assert!(!ids.contains(&&SqlValue::Int(2)), "closed row filtered out");

    // 6. A request equality filter narrows further.
    let rows2 = read_object(
        &ObjectQuery {
            type_name: "Order".into(),
            eq_filters: vec![("id".into(), "1".into())],
        },
        &Subject(subj.clone()),
        &deps,
    )
    .await
    .unwrap();
    assert_eq!(rows2.rows.len(), 1);
    assert_eq!(rows2.rows[0][0], SqlValue::Int(1));

    // 7. Deny-by-default: a subject with no Read grant is Forbidden (no open default).
    let stranger = SubjectId("stranger".into());
    let err = read_object(
        &ObjectQuery {
            type_name: "Order".into(),
            eq_filters: vec![],
        },
        &Subject(stranger),
        &deps,
    )
    .await
    .unwrap_err();
    assert!(
        matches!(err, QueryError::Forbidden),
        "ungranted subject must be denied, got {err:?}"
    );

    // Deny-override: analyst keeps the Allow grant via `analysts`, but a second role
    // with a Deny grant on Order must override it -> Forbidden (deny wins).
    let blocked = RoleId("blocked".into());
    cp.define_role(&blocked).await.unwrap();
    cp.assign_role(&subj, &blocked).await.unwrap();
    cp.grant(
        &blocked,
        Action::Read,
        PolicyTarget::Type(TypeName("Order".into())),
        Effect::Deny,
    )
    .await
    .unwrap();
    let denied = read_object(
        &ObjectQuery {
            type_name: "Order".into(),
            eq_filters: vec![],
        },
        &Subject(subj.clone()),
        &deps,
    )
    .await
    .unwrap_err();
    assert!(
        matches!(denied, QueryError::Forbidden),
        "deny grant overrides allow at the read gate, got {denied:?}"
    );

    // Masking: a policy that MASKS `secret` (vs denying it) -> the column is present
    // but every value is the marker, and it cannot be filtered on.
    let masker = SubjectId("masker".into());
    let mrole = RoleId("mask_role".into());
    cp.define_subject(&masker).await.unwrap();
    cp.define_role(&mrole).await.unwrap();
    cp.assign_role(&masker, &mrole).await.unwrap();
    cp.grant(
        &mrole,
        Action::Read,
        PolicyTarget::Type(TypeName("Order".into())),
        Effect::Allow,
    )
    .await
    .unwrap();
    cp.set_policy(
        &mrole,
        Policy {
            target: PolicyTarget::Type(TypeName("Order".into())),
            row_filter: None,
            deny_columns: vec![],
            mask_columns: vec!["secret".into()],
        },
    )
    .await
    .unwrap();
    let masked_rows = read_object(
        &ObjectQuery {
            type_name: "Order".into(),
            eq_filters: vec![],
        },
        &Subject(masker.clone()),
        &deps,
    )
    .await
    .unwrap();
    let secret_idx = masked_rows
        .columns
        .iter()
        .position(|c| c == "secret")
        .expect("secret column present (masked, not dropped)");
    assert!(
        masked_rows
            .rows
            .iter()
            .all(|r| r[secret_idx] == SqlValue::Text("***".into())),
        "every secret value is the redaction marker",
    );
    assert!(
        masked_rows
            .rows
            .iter()
            .all(|r| r[secret_idx] != SqlValue::Text("s1".into())),
        "no real secret value leaks",
    );
    let bad = read_object(
        &ObjectQuery {
            type_name: "Order".into(),
            eq_filters: vec![("secret".into(), "s1".into())],
        },
        &Subject(masker),
        &deps,
    )
    .await
    .unwrap_err();
    assert!(
        matches!(bad, QueryError::BadFilter(ref c) if c == "secret"),
        "a filter on a masked column is rejected, got {bad:?}",
    );
}
