//! Round-trip for the GovernedStatementQuery Flight ticket (JSON, deny_unknown_fields).

use control_plane_core::{
    CompareOp, GovernedCatalog, GovernedTable, RowFilter, ScalarValue, TableRef,
};
use engine_wire::flight::GovernedStatementQuery;

#[test]
fn governed_ticket_json_roundtrips() {
    let q = GovernedStatementQuery {
        sql: "SELECT * FROM \"s\".\"t\"".into(),
        catalog: GovernedCatalog {
            tables: vec![GovernedTable {
                table: TableRef {
                    schema: "s".into(),
                    name: "t".into(),
                },
                row_filters: vec![RowFilter::Compare {
                    property: "id".into(),
                    op: CompareOp::Ge,
                    value: ScalarValue::Int(2),
                }],
                denied: vec!["secret".into()],
                masked: vec!["email".into()],
            }],
        },
    };
    let bytes = q.encode();
    let back = GovernedStatementQuery::decode(&bytes).expect("decode");
    assert_eq!(back, q);
}

#[test]
fn governed_ticket_rejects_flight_ticket_json() {
    // A FlightTicket's JSON (schema/name/files) must NOT decode as a governed ticket.
    let ft = engine_wire::flight::FlightTicket {
        schema: "s".into(),
        name: "t".into(),
        files: vec![],
    };
    assert!(GovernedStatementQuery::decode(&ft.encode()).is_err());
}
