//! Heap usage of the smol-tar reader.
//!
//! The reader streams: its peak heap does not depend on the file size or on
//! the entry count, and its allocation count grows linearly with the entry
//! count.

use smol_tar_bench::{
    adapters, files, gnu_archive, long_paths, pax_archive, ustar_archive, Runtime, BUF_SIZE,
};

#[global_allocator]
static ALLOC: dhat::Alloc = dhat::Alloc;

// With tokio, the `tokio::sync::Mutex` in the reader uses the cooperative
// budget. When the budget runs out, the scheduler allocates a list of deferred
// wakers in each `block_on` call. `unconstrained` disables the budget, so the
// statistics show only the reader.
#[cfg(feature = "tokio")]
use tokio::task::unconstrained;
#[cfg(feature = "smol")]
fn unconstrained<F>(fut: F) -> F {
    fut
}

/// Builds an archive with the given number of entries.
type ArchiveOf = fn(usize) -> Vec<u8>;

#[derive(Clone, Copy, Debug)]
enum Mode {
    ReadAll,
    ReadSkip,
}

/// Heap statistics of one read of `archive`.
fn measure(rt: &Runtime, archive: &[u8], mode: Mode) -> dhat::HeapStats {
    let mut buf = vec![0; BUF_SIZE];
    let _profiler = dhat::Profiler::builder().testing().build();
    rt.block_on(unconstrained(async {
        match mode {
            Mode::ReadAll => adapters::smol_tar::read_all(archive, &mut buf)
                .await
                .map(drop),
            Mode::ReadSkip => adapters::smol_tar::read_skip(archive).await.map(drop),
        }
    }))
    .unwrap();
    dhat::HeapStats::get()
}

// dhat allows one profiler at a time, so a single test runs all cases.
#[test]
fn reader_heap_usage() {
    let rt = Runtime::new();
    // Initialize the thread-local state of the runtime before profiling.
    rt.block_on(async {});

    let formats: [(&str, ArchiveOf); 3] = [
        ("ustar", |n| ustar_archive(&files(n, 1024))),
        ("pax", |n| pax_archive(&long_paths(n, true))),
        ("gnu", |n| gnu_archive(&long_paths(n, false))),
    ];
    let small = ustar_archive(&files(1, 1 << 20));
    let large = ustar_archive(&files(1, 64 << 20));

    for mode in [Mode::ReadAll, Mode::ReadSkip] {
        let a = measure(&rt, &small, mode);
        let b = measure(&rt, &large, mode);
        assert_eq!(
            a.max_bytes, b.max_bytes,
            "{mode:?}: peak heap depends on file size"
        );

        for (format, archive) in formats {
            let stats: Vec<dhat::HeapStats> = [1000, 2000, 3000]
                .map(|n| measure(&rt, &archive(n), mode))
                .into();
            assert_eq!(
                stats[0].max_bytes, stats[2].max_bytes,
                "{mode:?} {format}: peak heap depends on entry count"
            );
            let first = stats[1].total_blocks - stats[0].total_blocks;
            let second = stats[2].total_blocks - stats[1].total_blocks;
            assert_eq!(
                first, second,
                "{mode:?} {format}: allocations are not linear"
            );
            eprintln!(
                "{mode:?} {format}: {} allocations and {} bytes per entry, peak heap {} bytes",
                first / 1000,
                (stats[1].total_bytes - stats[0].total_bytes) / 1000,
                stats[2].max_bytes,
            );
        }
    }
}
