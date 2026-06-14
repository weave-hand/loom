use ingest::write::{IngestWriteConfig, estimate_partitions};

fn cfg(target: u64, max_files: usize) -> IngestWriteConfig {
    IngestWriteConfig {
        target_file_size_bytes: target,
        max_files,
        compression_factor: 0.3,
    }
}

#[test]
fn empty_input_is_one_partition() {
    assert_eq!(estimate_partitions(0, &cfg(1, 8)), 1);
}

#[test]
fn small_input_fits_one_file() {
    // 100 in-memory bytes * 0.3 = 30 est compressed; target 128 MiB -> 1 file.
    assert_eq!(estimate_partitions(100, &cfg(128 * 1024 * 1024, 8)), 1);
}

#[test]
fn large_input_splits_up_to_target() {
    // 1000 bytes * 0.3 = 300 est; target 100 -> ceil(300/100) = 3 files.
    assert_eq!(estimate_partitions(1000, &cfg(100, 8)), 3);
}

#[test]
fn partition_count_is_clamped_to_max_files() {
    // 1_000_000 * 0.3 = 300_000 est; target 1 -> 300_000, clamped to max_files = 4.
    assert_eq!(estimate_partitions(1_000_000, &cfg(1, 4)), 4);
}
