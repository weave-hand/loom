//! Pure selection logic for compaction: which live files fall below the size threshold.

use control_plane_core::{FileRef, small_files};

fn f(path: &str, size: i64) -> FileRef {
    FileRef {
        path: path.into(),
        record_count: 1,
        file_size_bytes: size,
    }
}

#[test]
fn small_files_selects_sub_threshold() {
    let files = vec![f("a", 10), f("b", 200), f("c", 50)];
    let small = small_files(&files, 100);
    let paths: Vec<&str> = small.iter().map(|f| f.path.as_str()).collect();
    assert_eq!(paths, vec!["a", "c"], "only sub-threshold files selected");
}

#[test]
fn small_files_empty_when_all_large() {
    let files = vec![f("a", 200), f("b", 300)];
    assert!(small_files(&files, 100).is_empty());
}
