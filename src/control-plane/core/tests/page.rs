use control_plane_core::{Cursor, Page, PageReq};

#[test]
fn from_full_is_a_single_final_page() {
    let p = Page::from_full(vec![1, 2, 3]);
    assert_eq!(p.items, vec![1, 2, 3]);
    assert_eq!(p.next, None);
    assert_eq!(p.len(), 3);
    assert!(!p.is_empty());
    assert!(Page::<i32>::from_full(vec![]).is_empty());
}

#[test]
fn page_into_iter_yields_items() {
    let collected: Vec<i32> = Page::from_full(vec![10, 20]).into_iter().collect();
    assert_eq!(collected, vec![10, 20]);
}

#[test]
fn page_req_constructors() {
    assert_eq!(PageReq::unbounded(), PageReq::default());
    assert_eq!(
        PageReq::unbounded(),
        PageReq {
            after: None,
            limit: None,
        }
    );
    assert_eq!(
        PageReq::limit(10),
        PageReq {
            after: None,
            limit: Some(10),
        }
    );
    assert_eq!(
        PageReq::after(Cursor("c".into())),
        PageReq {
            after: Some(Cursor("c".into())),
            limit: None,
        }
    );
}

#[test]
fn cursor_json_round_trips() {
    let c = Cursor("opaque-keyset-token".into());
    let json = serde_json::to_string(&c).unwrap();
    assert_eq!(serde_json::from_str::<Cursor>(&json).unwrap(), c);
}
