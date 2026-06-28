use managed_postgres::validate_db_name;

#[test]
fn accepts_simple_lowercase_names() {
    assert!(validate_db_name("loom").is_ok());
    assert!(validate_db_name("loom_local_2").is_ok());
}

#[test]
fn rejects_empty_uppercase_and_punctuation() {
    assert!(validate_db_name("").is_err());
    assert!(validate_db_name("Loom").is_err());
    assert!(validate_db_name("loom; drop").is_err());
    assert!(validate_db_name("2loom").is_err());
}
