//! Proves the compile-time embed is non-empty and complete: `sqlx::migrate!`
//! baked every `migrations/*.sql` into the binary, with contiguous versions
//! starting at 1. Pure-logic (no DB) — RE-eligible.

#[test]
fn embedded_migrator_versions_are_contiguous_from_one() {
    let migrator = control_plane_postgres::embedded_migrator();
    let mut versions: Vec<i64> = migrator.iter().map(|m| m.version).collect();
    versions.sort_unstable();

    assert!(!versions.is_empty(), "embedded migrations must not be empty");
    let expected: Vec<i64> = (1..=versions.len() as i64).collect();
    assert_eq!(
        versions, expected,
        "embedded migration versions must be contiguous starting at 1"
    );
}
