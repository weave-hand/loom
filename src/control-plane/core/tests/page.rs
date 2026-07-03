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

#[test]
fn from_keyset_under_limit_is_final_page() {
    let p = Page::from_keyset(vec![1, 2, 3], Some(5), |n| Cursor(n.to_string()));
    assert_eq!(p.items, vec![1, 2, 3]);
    assert_eq!(p.next, None);
}

#[test]
fn from_keyset_over_limit_truncates_and_sets_cursor() {
    // 4 items fetched (limit + 1) signals a next page; truncate to 3, cursor = last kept.
    let p = Page::from_keyset(vec![10, 20, 30, 40], Some(3), |n| Cursor(n.to_string()));
    assert_eq!(p.items, vec![10, 20, 30]);
    assert_eq!(p.next, Some(Cursor("30".into())));
}

#[test]
fn from_keyset_unbounded_is_final_page() {
    let p = Page::from_keyset(vec![1, 2, 3], None, |n| Cursor(n.to_string()));
    assert_eq!(p.items, vec![1, 2, 3]);
    assert_eq!(p.next, None);
}

#[test]
fn fetch_limit_helpers_carry_the_plus_one_sentinel() {
    use control_plane_core::PageReq;
    assert_eq!(PageReq::limit(3).fetch_limit_i64(), 4);
    assert_eq!(PageReq::unbounded().fetch_limit_i64(), i64::MAX);
    assert_eq!(
        PageReq::limit(u32::MAX).fetch_limit_i64(),
        i64::from(u32::MAX) + 1
    );
    assert_eq!(PageReq::limit(3).fetch_take(), 4);
    assert_eq!(PageReq::unbounded().fetch_take(), usize::MAX);
}
