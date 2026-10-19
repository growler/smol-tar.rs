//! Large heap allocations of the reader when it skips file bodies.
//!
//! A `#[global_allocator]` applies to the full test binary, so these tests
//! are in their own file.

use std::{
    alloc::{GlobalAlloc, Layout, System},
    cell::Cell,
    future::Future,
    io::Result,
};

#[cfg(all(feature = "smol", not(feature = "tokio")))]
use smol::io::AsyncReadExt;
#[cfg(all(feature = "smol", not(feature = "tokio")))]
use smol::stream::StreamExt;
use smol_tar::{TarEntry, TarReader, TarRegularFile, TarWriter};
#[cfg(feature = "tokio")]
use tokio::io::AsyncReadExt;
#[cfg(feature = "tokio")]
use tokio_stream::StreamExt;

#[cfg(all(feature = "smol", not(feature = "tokio")))]
fn block_on_test<F: Future>(fut: F) -> F::Output {
    smol::block_on(fut)
}

#[cfg(feature = "tokio")]
fn block_on_test<F: Future>(fut: F) -> F::Output {
    tokio::runtime::Builder::new_current_thread()
        .build()
        .expect("build tokio runtime")
        .block_on(fut)
}

/// The size of the reader skip buffer. The allocator counts allocations of
/// this size or more.
const LARGE: usize = 64 * 1024;

thread_local! {
    static COUNTING: Cell<bool> = const { Cell::new(false) };
    static LARGE_ALLOCS: Cell<usize> = const { Cell::new(0) };
}

fn record(size: usize) {
    if size >= LARGE && COUNTING.try_with(Cell::get).unwrap_or(false) {
        let _ = LARGE_ALLOCS.try_with(|n| n.set(n.get() + 1));
    }
}

/// Counts the large allocations of the thread that enabled counting.
struct CountingAlloc;

unsafe impl GlobalAlloc for CountingAlloc {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        record(layout.size());
        System.alloc(layout)
    }
    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        record(layout.size());
        System.alloc_zeroed(layout)
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        System.dealloc(ptr, layout)
    }
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        record(new_size);
        System.realloc(ptr, layout, new_size)
    }
}

#[global_allocator]
static ALLOC: CountingAlloc = CountingAlloc;

/// Counts large allocations on this thread from `start` until drop.
struct Counter;

impl Counter {
    fn start() -> Self {
        LARGE_ALLOCS.with(|n| n.set(0));
        COUNTING.with(|c| c.set(true));
        Counter
    }
    fn count(&self) -> usize {
        LARGE_ALLOCS.with(Cell::get)
    }
}

impl Drop for Counter {
    fn drop(&mut self) {
        COUNTING.with(|c| c.set(false));
    }
}

fn write_archive<'b>(entries: Vec<TarEntry<'b, &'b [u8]>>) -> Result<Vec<u8>> {
    block_on_test(async {
        let mut archive = Vec::new();
        let mut writer = TarWriter::new(&mut archive);
        for entry in entries {
            writer.write(entry).await?;
        }
        writer.finish().await?;
        Ok(archive)
    })
}

#[test]
fn skipped_entries_allocate_one_skip_buffer() -> Result<()> {
    const FILES: usize = 128;
    const FILE_SIZE: usize = 1024;
    let body = vec![0x5a; FILE_SIZE];
    let archive = write_archive(
        (0..FILES)
            .map(|i| {
                TarRegularFile::new(format!("file-{i}.bin"), FILE_SIZE as u64, &body[..]).into()
            })
            .collect(),
    )?;

    let (entries, large) = block_on_test(async {
        let counter = Counter::start();
        let mut stream = Box::pin(TarReader::new(&archive[..]));
        let mut entries = 0;
        while let Some(entry) = stream.next().await {
            match entry? {
                TarEntry::File(file) => drop(file),
                other => panic!("expected file entry, got {:?}", other),
            }
            entries += 1;
        }
        Ok::<_, std::io::Error>((entries, counter.count()))
    })?;

    assert_eq!(entries, FILES);
    assert_eq!(
        large, 1,
        "expected exactly 1 allocation of {LARGE} bytes or more, got {large}"
    );
    Ok(())
}

#[test]
fn skip_after_large_extension_reuses_skip_buffer() -> Result<()> {
    let small = vec![0x11; 1024];
    let big = vec![0x5a; 200_000];
    let tail = b"tail data";
    // The writer stores this path in a PAX record that is larger than one
    // block, so the reader puts the record in the extension buffer.
    let long_path = format!("{}/big.bin", "d".repeat(592));
    assert_eq!(long_path.len(), 600);
    let archive = write_archive(vec![
        TarRegularFile::new("a.bin", small.len() as u64, &small[..]).into(),
        TarRegularFile::new(long_path.as_str(), big.len() as u64, &big[..]).into(),
        TarRegularFile::new("c.txt", tail.len() as u64, &tail[..]).into(),
    ])?;

    let (data, large) = block_on_test(async {
        let counter = Counter::start();
        let mut stream = Box::pin(TarReader::new(&archive[..]));
        match stream.next().await.unwrap()? {
            TarEntry::File(file) => assert_eq!(file.path(), "a.bin"),
            other => panic!("expected file entry, got {:?}", other),
        }
        match stream.next().await.unwrap()? {
            TarEntry::File(file) => {
                assert_eq!(file.path(), long_path);
                assert_eq!(file.size(), 200_000);
            }
            other => panic!("expected file entry, got {:?}", other),
        }
        let data = match stream.next().await.unwrap()? {
            TarEntry::File(mut file) => {
                assert_eq!(file.path(), "c.txt");
                let mut data = Vec::new();
                file.read_to_end(&mut data).await?;
                data
            }
            other => panic!("expected file entry, got {:?}", other),
        };
        assert!(stream.next().await.is_none());
        Ok::<_, std::io::Error>((data, counter.count()))
    })?;

    assert_eq!(data, tail);
    assert_eq!(
        large, 1,
        "expected exactly 1 allocation of {LARGE} bytes or more, got {large}"
    );
    Ok(())
}

#[test]
fn counter_counts_large_allocations() {
    let counter = Counter::start();
    let buf = std::hint::black_box(vec![0u8; LARGE]);
    let large = counter.count();
    drop(counter);
    drop(buf);
    assert_eq!(large, 1);
}
