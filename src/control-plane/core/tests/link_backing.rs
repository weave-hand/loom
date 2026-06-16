//! `LinkBacking::reversed()` — the column-role swap that lets the symmetric chain
//! compiler traverse a link from its `to` side back to its `from` side.

use control_plane_core::{LinkBacking, TableRef};

#[test]
fn foreign_key_swaps_from_and_to_columns() {
    let fk = LinkBacking::ForeignKey {
        from_column: "id".into(),
        to_column: "customer_id".into(),
    };
    assert_eq!(
        fk.reversed(),
        LinkBacking::ForeignKey {
            from_column: "customer_id".into(),
            to_column: "id".into(),
        }
    );
}

#[test]
fn join_table_swaps_key_and_column_pairs_keeping_table() {
    let jt = LinkBacking::JoinTable {
        table: TableRef {
            schema: "main".into(),
            name: "customer_order".into(),
        },
        from_key: "id".into(),
        from_column: "customer_id".into(),
        to_column: "order_id".into(),
        to_key: "oid".into(),
    };
    assert_eq!(
        jt.reversed(),
        LinkBacking::JoinTable {
            table: TableRef {
                schema: "main".into(),
                name: "customer_order".into(),
            },
            from_key: "oid".into(),
            from_column: "order_id".into(),
            to_column: "customer_id".into(),
            to_key: "id".into(),
        }
    );
}

#[test]
fn reversed_is_an_involution() {
    let fk = LinkBacking::ForeignKey {
        from_column: "a".into(),
        to_column: "b".into(),
    };
    assert_eq!(fk.reversed().reversed(), fk);
    let jt = LinkBacking::JoinTable {
        table: TableRef {
            schema: "s".into(),
            name: "t".into(),
        },
        from_key: "fk".into(),
        from_column: "fc".into(),
        to_column: "tc".into(),
        to_key: "tk".into(),
    };
    assert_eq!(jt.reversed().reversed(), jt);
}
