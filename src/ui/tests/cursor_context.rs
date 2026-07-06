use loom_ui_core::cursor_context;

#[test]
fn word_at_end_no_qualifier() {
    let text = "SELECT * FROM cust";
    assert_eq!(cursor_context(text, text.len()), ("cust".to_owned(), None));
}

#[test]
fn empty_prefix_with_qualifier_after_dot() {
    let text = "SELECT c FROM customers c WHERE c.";
    assert_eq!(
        cursor_context(text, text.len()),
        (String::new(), Some("c".to_owned()))
    );
}

#[test]
fn prefix_with_qualifier() {
    let text = "WHERE c.na";
    assert_eq!(
        cursor_context(text, text.len()),
        ("na".to_owned(), Some("c".to_owned()))
    );
}

#[test]
fn offset_zero_is_empty() {
    assert_eq!(cursor_context("SELECT", 0), (String::new(), None));
}

#[test]
fn offset_mid_word_takes_only_left_of_cursor() {
    // cursor after "cu" in "customers"
    let text = "FROM customers";
    assert_eq!(cursor_context(text, 7), ("cu".to_owned(), None));
}

#[test]
fn dot_without_leading_identifier_has_no_qualifier() {
    let text = "SELECT .col";
    assert_eq!(cursor_context(text, text.len()), ("col".to_owned(), None));
}
