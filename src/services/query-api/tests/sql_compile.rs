use control_plane_core::{Aggregation, CompareOp, LinkBacking, RowFilter, ScalarValue, TableRef};
use query_api::filter::CallerPredicate;
use query_api::serving::SqlValue;
use query_api::sql::{ChainType, DerivedAggregate, DerivedSelect, compile_chain, compile_select};

fn t() -> TableRef {
    TableRef {
        schema: "main".into(),
        name: "orders".into(),
    }
}

fn eqp(col: &str, val: SqlValue) -> CallerPredicate {
    CallerPredicate {
        column: col.into(),
        op: CompareOp::Eq,
        values: vec![val],
    }
}

#[test]
fn projects_allowed_columns_and_quotes_identifiers() {
    let (sql, params) = compile_select(
        &t(),
        &["id".into(), "status".into()],
        &[],
        &[],
        &[],
        &[],
        100,
    )
    .unwrap();
    assert_eq!(
        sql,
        r#"SELECT "id", "status" FROM "main"."orders" LIMIT 100"#
    );
    assert!(params.is_empty());
}

#[test]
fn compiles_acl_compare_leaf_as_bound_param() {
    let f = RowFilter::Compare {
        property: "status".into(),
        op: CompareOp::Eq,
        value: ScalarValue::Text("open".into()),
    };
    let (sql, params) = compile_select(
        &t(),
        &["id".into()],
        &[],
        std::slice::from_ref(&f),
        &[],
        &[],
        100,
    )
    .unwrap();
    assert_eq!(
        sql,
        r#"SELECT "id" FROM "main"."orders" WHERE ("status" = ?) LIMIT 100"#
    );
    assert_eq!(params, vec![SqlValue::Text("open".into())]);
}

#[test]
fn compiles_and_or_not_tree() {
    let f = RowFilter::And(vec![
        RowFilter::Compare {
            property: "a".into(),
            op: CompareOp::Eq,
            value: ScalarValue::Int(1),
        },
        RowFilter::Or(vec![
            RowFilter::Compare {
                property: "b".into(),
                op: CompareOp::Gt,
                value: ScalarValue::Int(2),
            },
            RowFilter::Not(Box::new(RowFilter::Compare {
                property: "c".into(),
                op: CompareOp::IsNull,
                value: ScalarValue::Bool(true),
            })),
        ]),
    ]);
    let (sql, params) = compile_select(
        &t(),
        &["a".into()],
        &[],
        std::slice::from_ref(&f),
        &[],
        &[],
        10,
    )
    .unwrap();
    assert_eq!(
        sql,
        r#"SELECT "a" FROM "main"."orders" WHERE (("a" = ?) AND (("b" > ?) OR (NOT ("c" IS NULL)))) LIMIT 10"#
    );
    assert_eq!(params, vec![SqlValue::Int(1), SqlValue::Int(2)]);
}

#[test]
fn expands_in_list_into_placeholders() {
    let f = RowFilter::Compare {
        property: "region".into(),
        op: CompareOp::In,
        value: ScalarValue::List(vec![
            ScalarValue::Text("EU".into()),
            ScalarValue::Text("UK".into()),
        ]),
    };
    let (sql, params) = compile_select(
        &t(),
        &["id".into()],
        &[],
        std::slice::from_ref(&f),
        &[],
        &[],
        10,
    )
    .unwrap();
    assert_eq!(
        sql,
        r#"SELECT "id" FROM "main"."orders" WHERE ("region" IN (?, ?)) LIMIT 10"#
    );
    assert_eq!(
        params,
        vec![SqlValue::Text("EU".into()), SqlValue::Text("UK".into())]
    );
}

#[test]
fn ands_acl_filter_with_request_equality_filter() {
    let acl = RowFilter::Compare {
        property: "tenant".into(),
        op: CompareOp::Eq,
        value: ScalarValue::Text("acme".into()),
    };
    let preds = vec![eqp("status", SqlValue::Text("open".into()))];
    let (sql, params) = compile_select(
        &t(),
        &["id".into()],
        &[],
        std::slice::from_ref(&acl),
        &preds,
        &[],
        10,
    )
    .unwrap();
    assert_eq!(
        sql,
        r#"SELECT "id" FROM "main"."orders" WHERE ("tenant" = ?) AND ("status" = ?) LIMIT 10"#
    );
    assert_eq!(
        params,
        vec![SqlValue::Text("acme".into()), SqlValue::Text("open".into())]
    );
}

#[test]
fn expands_not_in_list_into_placeholders() {
    let f = RowFilter::Compare {
        property: "region".into(),
        op: CompareOp::NotIn,
        value: ScalarValue::List(vec![
            ScalarValue::Text("EU".into()),
            ScalarValue::Text("UK".into()),
        ]),
    };
    let (sql, params) = compile_select(
        &t(),
        &["id".into()],
        &[],
        std::slice::from_ref(&f),
        &[],
        &[],
        10,
    )
    .unwrap();
    assert_eq!(
        sql,
        r#"SELECT "id" FROM "main"."orders" WHERE ("region" NOT IN (?, ?)) LIMIT 10"#
    );
    assert_eq!(
        params,
        vec![SqlValue::Text("EU".into()), SqlValue::Text("UK".into())]
    );
}

#[test]
fn compiles_is_not_null_without_a_param() {
    // value is ignored for IS [NOT] NULL ops.
    let f = RowFilter::Compare {
        property: "closed_at".into(),
        op: CompareOp::IsNotNull,
        value: ScalarValue::Bool(true),
    };
    let (sql, params) = compile_select(
        &t(),
        &["id".into()],
        &[],
        std::slice::from_ref(&f),
        &[],
        &[],
        10,
    )
    .unwrap();
    assert_eq!(
        sql,
        r#"SELECT "id" FROM "main"."orders" WHERE ("closed_at" IS NOT NULL) LIMIT 10"#
    );
    assert!(params.is_empty());
}

#[test]
fn eq_filters_only_form_the_where_clause() {
    // No ACL row filter, only a request equality filter: the WHERE prefix and
    // conjunct-joining must still be correct (no leading/trailing AND).
    let preds = vec![eqp("status", SqlValue::Text("open".into()))];
    let (sql, params) = compile_select(&t(), &["id".into()], &[], &[], &preds, &[], 10).unwrap();
    assert_eq!(
        sql,
        r#"SELECT "id" FROM "main"."orders" WHERE ("status" = ?) LIMIT 10"#
    );
    assert_eq!(params, vec![SqlValue::Text("open".into())]);
}

#[test]
fn masks_a_column_with_marker() {
    let (sql, params) = compile_select(
        &t(),
        &["id".into(), "secret".into()],
        &["secret".into()],
        &[],
        &[],
        &[],
        100,
    )
    .unwrap();
    assert_eq!(
        sql,
        r#"SELECT "id", '***' AS "secret" FROM "main"."orders" LIMIT 100"#
    );
    assert!(
        params.is_empty(),
        "the marker is a constant, not a bound param"
    );
}

#[test]
fn masking_preserves_projection_order_and_other_columns() {
    let (sql, _params) = compile_select(
        &t(),
        &["a".into(), "b".into(), "c".into()],
        &["b".into()],
        &[],
        &[],
        &[],
        10,
    )
    .unwrap();
    assert_eq!(
        sql,
        r#"SELECT "a", '***' AS "b", "c" FROM "main"."orders" LIMIT 10"#
    );
}

#[test]
fn malformed_filter_is_an_error_not_a_panic() {
    // `In` with a non-list value is malformed; compile_select must return Err, not panic.
    let bad = RowFilter::Compare {
        property: "region".into(),
        op: CompareOp::In,
        value: ScalarValue::Text("EU".into()),
    };
    let res = compile_select(
        &t(),
        &["id".into()],
        &[],
        std::slice::from_ref(&bad),
        &[],
        &[],
        10,
    );
    assert!(
        matches!(res, Err(query_api::sql::CompileError::MalformedFilter(_))),
        "malformed filter -> Err(MalformedFilter), no panic; got {res:?}"
    );
}

#[test]
fn derived_fk_count_compiles_to_a_correlated_subquery() {
    let derived = vec![DerivedSelect::Aggregate(Box::new(DerivedAggregate {
        name: "orderCount".into(),
        agg: Aggregation::Count,
        backing: LinkBacking::ForeignKey {
            from_column: "id".into(),
            to_column: "customer_id".into(),
        },
        target_table: TableRef {
            schema: "main".into(),
            name: "orders".into(),
        },
        target_filters: vec![],
    }))];
    let (sql, params) = compile_select(
        &TableRef {
            schema: "main".into(),
            name: "customer".into(),
        },
        &["id".to_string()],
        &[],
        &[],
        &[],
        &derived,
        100,
    )
    .unwrap();
    assert_eq!(
        sql,
        "SELECT \"id\", (SELECT COUNT(*) FROM \"main\".\"orders\" sub \
         WHERE sub.\"customer_id\" = o.\"id\") AS \"orderCount\" \
         FROM \"main\".\"customer\" o LIMIT 100"
    );
    assert!(params.is_empty());
}

#[test]
fn derived_jointable_sum_with_target_filter_orders_params_first() {
    let derived = vec![DerivedSelect::Aggregate(Box::new(DerivedAggregate {
        name: "totalSpend".into(),
        agg: Aggregation::Sum("amount".into()),
        backing: LinkBacking::JoinTable {
            table: TableRef {
                schema: "main".into(),
                name: "customer_order".into(),
            },
            from_key: "id".into(),
            from_column: "customer_id".into(),
            to_column: "order_id".into(),
            to_key: "id".into(),
        },
        target_table: TableRef {
            schema: "main".into(),
            name: "orders".into(),
        },
        target_filters: vec![RowFilter::Compare {
            property: "status".into(),
            op: CompareOp::Eq,
            value: ScalarValue::Text("shipped".into()),
        }],
    }))];
    let (sql, params) = compile_select(
        &TableRef {
            schema: "main".into(),
            name: "customer".into(),
        },
        &["id".to_string()],
        &[],
        &[],
        &[eqp("region", SqlValue::Text("CA".into()))],
        &derived,
        100,
    )
    .unwrap();
    assert_eq!(
        sql,
        "SELECT \"id\", (SELECT COALESCE(SUM(sub.\"amount\"), 0) FROM \"main\".\"orders\" sub \
         JOIN \"main\".\"customer_order\" j ON j.\"order_id\" = sub.\"id\" \
         WHERE j.\"customer_id\" = o.\"id\" AND (sub.\"status\" = ?)) AS \"totalSpend\" \
         FROM \"main\".\"customer\" o WHERE (\"region\" = ?) LIMIT 100"
    );
    assert_eq!(
        params,
        vec![
            SqlValue::Text("shipped".into()),
            SqlValue::Text("CA".into())
        ]
    );
}

#[test]
fn masked_derived_emits_marker_no_subquery_no_alias() {
    let derived = vec![DerivedSelect::Masked("orderCount".into())];
    let (sql, params) = compile_select(
        &TableRef {
            schema: "main".into(),
            name: "customer".into(),
        },
        &["id".to_string()],
        &[],
        &[],
        &[],
        &derived,
        100,
    )
    .unwrap();
    assert_eq!(
        sql,
        "SELECT \"id\", '***' AS \"orderCount\" FROM \"main\".\"customer\" LIMIT 100"
    );
    assert!(params.is_empty());
}

fn tr(schema: &str, name: &str) -> TableRef {
    TableRef {
        schema: schema.into(),
        name: name.into(),
    }
}

#[test]
fn chain_two_hop_fk_compiles_to_nested_joins() {
    let types = vec![
        ChainType {
            table: tr("main", "customer"),
            row_filters: vec![],
            predicates: vec![eqp("region", SqlValue::Text("CA".into()))],
        },
        ChainType {
            table: tr("main", "orders"),
            row_filters: vec![],
            predicates: vec![],
        },
        ChainType {
            table: tr("main", "line_items"),
            row_filters: vec![],
            predicates: vec![],
        },
    ];
    let hops = vec![
        LinkBacking::ForeignKey {
            from_column: "id".into(),
            to_column: "customer_id".into(),
        },
        LinkBacking::ForeignKey {
            from_column: "id".into(),
            to_column: "order_id".into(),
        },
    ];
    let (sql, params) = compile_chain(
        &types,
        &hops,
        &["id".to_string(), "sku".to_string()],
        &[],
        100,
    )
    .unwrap();
    assert_eq!(
        sql,
        "SELECT DISTINCT t_2.\"id\", t_2.\"sku\" FROM \"main\".\"line_items\" t_2 \
         JOIN \"main\".\"orders\" t_1 ON t_1.\"id\" = t_2.\"order_id\" \
         JOIN \"main\".\"customer\" t_0 ON t_0.\"id\" = t_1.\"customer_id\" \
         WHERE (t_0.\"region\" = ?) LIMIT 100"
    );
    assert_eq!(params, vec![SqlValue::Text("CA".into())]);
}

#[test]
fn chain_fk_then_jointable_adds_mapping_join_for_that_hop_only() {
    let types = vec![
        ChainType {
            table: tr("main", "customer"),
            row_filters: vec![],
            predicates: vec![],
        },
        ChainType {
            table: tr("main", "orders"),
            row_filters: vec![],
            predicates: vec![],
        },
        ChainType {
            table: tr("main", "tags"),
            row_filters: vec![],
            predicates: vec![],
        },
    ];
    let hops = vec![
        LinkBacking::ForeignKey {
            from_column: "id".into(),
            to_column: "customer_id".into(),
        },
        LinkBacking::JoinTable {
            table: tr("main", "order_tag"),
            from_key: "id".into(),
            from_column: "order_id".into(),
            to_column: "tag_id".into(),
            to_key: "id".into(),
        },
    ];
    let (sql, params) = compile_chain(&types, &hops, &["name".to_string()], &[], 100).unwrap();
    assert_eq!(
        sql,
        "SELECT DISTINCT t_2.\"name\" FROM \"main\".\"tags\" t_2 \
         JOIN \"main\".\"order_tag\" j2 ON j2.\"tag_id\" = t_2.\"id\" \
         JOIN \"main\".\"orders\" t_1 ON t_1.\"id\" = j2.\"order_id\" \
         JOIN \"main\".\"customer\" t_0 ON t_0.\"id\" = t_1.\"customer_id\" \
         LIMIT 100"
    );
    assert!(params.is_empty());
}

#[test]
fn chain_params_source_eq_precedes_hop_row_filters_in_chain_order() {
    let types = vec![
        ChainType {
            table: tr("main", "customer"),
            row_filters: vec![],
            predicates: vec![eqp("region", SqlValue::Text("CA".into()))],
        },
        ChainType {
            table: tr("main", "orders"),
            row_filters: vec![RowFilter::Compare {
                property: "status".into(),
                op: CompareOp::Eq,
                value: ScalarValue::Text("shipped".into()),
            }],
            predicates: vec![],
        },
        ChainType {
            table: tr("main", "line_items"),
            row_filters: vec![],
            predicates: vec![],
        },
    ];
    let hops = vec![
        LinkBacking::ForeignKey {
            from_column: "id".into(),
            to_column: "customer_id".into(),
        },
        LinkBacking::ForeignKey {
            from_column: "id".into(),
            to_column: "order_id".into(),
        },
    ];
    let (sql, params) = compile_chain(&types, &hops, &["id".to_string()], &[], 100).unwrap();
    assert_eq!(
        sql,
        "SELECT DISTINCT t_2.\"id\" FROM \"main\".\"line_items\" t_2 \
         JOIN \"main\".\"orders\" t_1 ON t_1.\"id\" = t_2.\"order_id\" \
         JOIN \"main\".\"customer\" t_0 ON t_0.\"id\" = t_1.\"customer_id\" \
         WHERE (t_0.\"region\" = ?) AND (t_1.\"status\" = ?) LIMIT 100"
    );
    assert_eq!(
        params,
        vec![
            SqlValue::Text("CA".into()),
            SqlValue::Text("shipped".into())
        ]
    );
}

#[test]
fn chain_single_hop_jointable_renders_j1_mapping() {
    // Customer --tags(join-table customer_tag)--> Tag, single hop (N=1).
    let types = vec![
        ChainType {
            table: tr("main", "customer"),
            row_filters: vec![],
            predicates: vec![],
        },
        ChainType {
            table: tr("main", "tags"),
            row_filters: vec![],
            predicates: vec![],
        },
    ];
    let hops = vec![LinkBacking::JoinTable {
        table: tr("main", "customer_tag"),
        from_key: "id".into(),
        from_column: "customer_id".into(),
        to_column: "tag_id".into(),
        to_key: "id".into(),
    }];
    let (sql, params) = compile_chain(&types, &hops, &["name".to_string()], &[], 100).unwrap();
    assert_eq!(
        sql,
        "SELECT DISTINCT t_1.\"name\" FROM \"main\".\"tags\" t_1 \
         JOIN \"main\".\"customer_tag\" j1 ON j1.\"tag_id\" = t_1.\"id\" \
         JOIN \"main\".\"customer\" t_0 ON t_0.\"id\" = j1.\"customer_id\" \
         LIMIT 100"
    );
    assert!(params.is_empty());
}

#[test]
fn chain_single_hop_reproduces_traversal_semantics() {
    let types = vec![
        ChainType {
            table: tr("main", "customer"),
            row_filters: vec![RowFilter::Compare {
                property: "region".into(),
                op: CompareOp::Eq,
                value: ScalarValue::Text("CA".into()),
            }],
            predicates: vec![],
        },
        ChainType {
            table: tr("main", "orders"),
            row_filters: vec![],
            predicates: vec![],
        },
    ];
    let hops = vec![LinkBacking::ForeignKey {
        from_column: "id".into(),
        to_column: "customer_id".into(),
    }];
    let (sql, params) = compile_chain(
        &types,
        &hops,
        &["id".to_string(), "secret".to_string()],
        &["secret".to_string()],
        100,
    )
    .unwrap();
    assert_eq!(
        sql,
        "SELECT DISTINCT t_1.\"id\", '***' AS \"secret\" FROM \"main\".\"orders\" t_1 \
         JOIN \"main\".\"customer\" t_0 ON t_0.\"id\" = t_1.\"customer_id\" \
         WHERE (t_0.\"region\" = ?) LIMIT 100"
    );
    assert_eq!(params, vec![SqlValue::Text("CA".into())]);
}

#[test]
fn chain_eq_filter_on_final_target_binds_at_t_k() {
    let types = vec![
        ChainType {
            table: tr("main", "customer"),
            row_filters: vec![],
            predicates: vec![],
        },
        ChainType {
            table: tr("main", "orders"),
            row_filters: vec![],
            predicates: vec![],
        },
        ChainType {
            table: tr("main", "line_items"),
            row_filters: vec![],
            predicates: vec![eqp("sku", SqlValue::Text("A".into()))],
        },
    ];
    let hops = vec![
        LinkBacking::ForeignKey {
            from_column: "id".into(),
            to_column: "customer_id".into(),
        },
        LinkBacking::ForeignKey {
            from_column: "id".into(),
            to_column: "order_id".into(),
        },
    ];
    let (sql, params) = compile_chain(&types, &hops, &["id".to_string()], &[], 100).unwrap();
    assert_eq!(
        sql,
        "SELECT DISTINCT t_2.\"id\" FROM \"main\".\"line_items\" t_2 \
         JOIN \"main\".\"orders\" t_1 ON t_1.\"id\" = t_2.\"order_id\" \
         JOIN \"main\".\"customer\" t_0 ON t_0.\"id\" = t_1.\"customer_id\" \
         WHERE (t_2.\"sku\" = ?) LIMIT 100"
    );
    assert_eq!(params, vec![SqlValue::Text("A".into())]);
}

#[test]
fn chain_eq_filters_bind_per_position_in_chain_order() {
    let types = vec![
        ChainType {
            table: tr("main", "customer"),
            row_filters: vec![],
            predicates: vec![eqp("region", SqlValue::Text("CA".into()))],
        },
        ChainType {
            table: tr("main", "orders"),
            row_filters: vec![],
            predicates: vec![eqp("id", SqlValue::Int(10))],
        },
        ChainType {
            table: tr("main", "line_items"),
            row_filters: vec![],
            predicates: vec![],
        },
    ];
    let hops = vec![
        LinkBacking::ForeignKey {
            from_column: "id".into(),
            to_column: "customer_id".into(),
        },
        LinkBacking::ForeignKey {
            from_column: "id".into(),
            to_column: "order_id".into(),
        },
    ];
    let (sql, params) = compile_chain(&types, &hops, &["id".to_string()], &[], 100).unwrap();
    assert_eq!(
        sql,
        "SELECT DISTINCT t_2.\"id\" FROM \"main\".\"line_items\" t_2 \
         JOIN \"main\".\"orders\" t_1 ON t_1.\"id\" = t_2.\"order_id\" \
         JOIN \"main\".\"customer\" t_0 ON t_0.\"id\" = t_1.\"customer_id\" \
         WHERE (t_0.\"region\" = ?) AND (t_1.\"id\" = ?) LIMIT 100"
    );
    assert_eq!(params, vec![SqlValue::Text("CA".into()), SqlValue::Int(10)]);
}

#[test]
fn caller_predicate_gt_renders_with_param() {
    let preds = vec![CallerPredicate {
        column: "amount".into(),
        op: CompareOp::Gt,
        values: vec![SqlValue::Int(100)],
    }];
    let (sql, params) = compile_select(&t(), &["id".into()], &[], &[], &preds, &[], 10).unwrap();
    assert_eq!(
        sql,
        r#"SELECT "id" FROM "main"."orders" WHERE ("amount" > ?) LIMIT 10"#
    );
    assert_eq!(params, vec![SqlValue::Int(100)]);
}

#[test]
fn caller_predicate_in_expands_placeholders() {
    let preds = vec![CallerPredicate {
        column: "status".into(),
        op: CompareOp::In,
        values: vec![SqlValue::Text("open".into()), SqlValue::Text("paid".into())],
    }];
    let (sql, params) = compile_select(&t(), &["id".into()], &[], &[], &preds, &[], 10).unwrap();
    assert_eq!(
        sql,
        r#"SELECT "id" FROM "main"."orders" WHERE ("status" IN (?, ?)) LIMIT 10"#
    );
    assert_eq!(
        params,
        vec![SqlValue::Text("open".into()), SqlValue::Text("paid".into())]
    );
}

#[test]
fn caller_predicate_isnotnull_no_param() {
    let preds = vec![CallerPredicate {
        column: "closed_at".into(),
        op: CompareOp::IsNotNull,
        values: vec![],
    }];
    let (sql, params) = compile_select(&t(), &["id".into()], &[], &[], &preds, &[], 10).unwrap();
    assert_eq!(
        sql,
        r#"SELECT "id" FROM "main"."orders" WHERE ("closed_at" IS NOT NULL) LIMIT 10"#
    );
    assert!(params.is_empty());
}

#[test]
fn caller_predicate_range_two_same_column_ands() {
    let preds = vec![
        CallerPredicate {
            column: "amount".into(),
            op: CompareOp::Ge,
            values: vec![SqlValue::Int(100)],
        },
        CallerPredicate {
            column: "amount".into(),
            op: CompareOp::Le,
            values: vec![SqlValue::Int(200)],
        },
    ];
    let (sql, params) = compile_select(&t(), &["id".into()], &[], &[], &preds, &[], 10).unwrap();
    assert_eq!(
        sql,
        r#"SELECT "id" FROM "main"."orders" WHERE ("amount" >= ?) AND ("amount" <= ?) LIMIT 10"#
    );
    assert_eq!(params, vec![SqlValue::Int(100), SqlValue::Int(200)]);
}

#[test]
fn compiles_between_as_two_bound_params() {
    let p = CallerPredicate {
        column: "amount".into(),
        op: CompareOp::Between,
        values: vec![SqlValue::Int(11), SqlValue::Int(25)],
    };
    let (sql, params) = compile_select(
        &t(),
        &["id".into()],
        &[],
        &[],
        std::slice::from_ref(&p),
        &[],
        100,
    )
    .unwrap();
    assert_eq!(
        sql,
        r#"SELECT "id" FROM "main"."orders" WHERE ("amount" BETWEEN ? AND ?) LIMIT 100"#
    );
    assert_eq!(params, vec![SqlValue::Int(11), SqlValue::Int(25)]);
}

#[test]
fn compiles_contains_as_ilike_with_escape_and_bound_param() {
    let p = CallerPredicate {
        column: "name".into(),
        op: CompareOp::Contains,
        values: vec![SqlValue::Text("%AC%".into())],
    };
    let (sql, params) = compile_select(
        &t(),
        &["id".into()],
        &[],
        &[],
        std::slice::from_ref(&p),
        &[],
        100,
    )
    .unwrap();
    assert_eq!(
        sql,
        r#"SELECT "id" FROM "main"."orders" WHERE ("name" ILIKE ? ESCAPE '\') LIMIT 100"#
    );
    assert_eq!(params, vec![SqlValue::Text("%AC%".into())]);
}

#[test]
fn caller_predicate_binds_at_chain_alias() {
    let types = vec![
        ChainType {
            table: tr("main", "customer"),
            row_filters: vec![],
            predicates: vec![],
        },
        ChainType {
            table: tr("main", "orders"),
            row_filters: vec![],
            predicates: vec![CallerPredicate {
                column: "amount".into(),
                op: CompareOp::Gt,
                values: vec![SqlValue::Int(50)],
            }],
        },
    ];
    let hops = vec![LinkBacking::ForeignKey {
        from_column: "id".into(),
        to_column: "customer_id".into(),
    }];
    let (sql, params) = compile_chain(&types, &hops, &["id".to_string()], &[], 100).unwrap();
    assert_eq!(
        sql,
        "SELECT DISTINCT t_1.\"id\" FROM \"main\".\"orders\" t_1 \
         JOIN \"main\".\"customer\" t_0 ON t_0.\"id\" = t_1.\"customer_id\" \
         WHERE (t_1.\"amount\" > ?) LIMIT 100"
    );
    assert_eq!(params, vec![SqlValue::Int(50)]);
}
