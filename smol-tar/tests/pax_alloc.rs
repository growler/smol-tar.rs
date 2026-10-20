//! Heap allocations of the reader for global PAX records.
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
use smol::stream::StreamExt;
use smol_tar::{TarEntry, TarReader};
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

thread_local! {
    static COUNTING: Cell<bool> = const { Cell::new(false) };
    static ALLOCS: Cell<usize> = const { Cell::new(0) };
}

fn record() {
    if COUNTING.try_with(Cell::get).unwrap_or(false) {
        let _ = ALLOCS.try_with(|n| n.set(n.get() + 1));
    }
}

/// Counts the allocations of the thread that enabled counting.
struct CountingAlloc;

unsafe impl GlobalAlloc for CountingAlloc {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        record();
        System.alloc(layout)
    }
    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        record();
        System.alloc_zeroed(layout)
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        System.dealloc(ptr, layout)
    }
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        record();
        System.realloc(ptr, layout, new_size)
    }
}

#[global_allocator]
static ALLOC: CountingAlloc = CountingAlloc;

/// Counts allocations on this thread from `start` until drop.
struct Counter;

impl Counter {
    fn start() -> Self {
        ALLOCS.with(|n| n.set(0));
        COUNTING.with(|c| c.set(true));
        Counter
    }
    fn count(&self) -> usize {
        ALLOCS.with(Cell::get)
    }
}

impl Drop for Counter {
    fn drop(&mut self) {
        COUNTING.with(|c| c.set(false));
    }
}

/// Builds a ustar header.
fn header(name: &str, typeflag: u8, size: usize) -> [u8; 512] {
    let mut block = [0u8; 512];
    block[..name.len()].copy_from_slice(name.as_bytes());
    block[100..108].copy_from_slice(b"0000755\0");
    block[108..116].copy_from_slice(b"0000000\0");
    block[116..124].copy_from_slice(b"0000000\0");
    block[124..136].copy_from_slice(format!("{size:011o}\0").as_bytes());
    block[136..148].copy_from_slice(b"00000000000\0");
    block[148..156].fill(b' ');
    block[156] = typeflag;
    block[257..263].copy_from_slice(b"ustar\0");
    block[263..265].copy_from_slice(b"00");
    let sum: u32 = block.iter().map(|&b| u32::from(b)).sum();
    block[148..156].copy_from_slice(format!("{sum:06o}\0 ").as_bytes());
    block
}

fn pax_record(key: &str, value: &[u8]) -> Vec<u8> {
    let payload = key.len() + value.len() + 3;
    let mut len = payload + 1;
    while len != payload + len.to_string().len() {
        len = payload + len.to_string().len();
    }
    let mut record = format!("{len} {key}=").into_bytes();
    record.extend_from_slice(value);
    record.push(b'\n');
    record
}

fn append(archive: &mut Vec<u8>, header: [u8; 512], data: &[u8]) {
    archive.extend_from_slice(&header);
    archive.extend_from_slice(data);
    archive.resize(archive.len().next_multiple_of(512), 0);
}

/// Reads all entries. Gives the number of entries and the number of
/// allocations.
fn read_counting(archive: &[u8]) -> Result<(usize, usize)> {
    block_on_test(async {
        let counter = Counter::start();
        let mut stream = Box::pin(TarReader::new(archive));
        let mut entries = 0;
        while let Some(entry) = stream.next().await {
            match entry? {
                TarEntry::Directory(dir) => drop(dir),
                other => panic!("expected directory entry, got {:?}", other),
            }
            entries += 1;
        }
        Ok((entries, counter.count()))
    })
}

#[test]
fn global_attrs_are_not_copied_into_each_entry() -> Result<()> {
    const ATTRS: usize = 1000;
    const DIRS: usize = 100;
    let mut global = Vec::new();
    for i in 0..ATTRS {
        global.extend_from_slice(&pax_record(&format!("SCHILY.xattr.user.a{i}"), b"value"));
    }
    let mut archive = Vec::new();
    append(
        &mut archive,
        header("pax_global_header", b'g', global.len()),
        &global,
    );
    for i in 0..DIRS {
        append(&mut archive, header(&format!("d{i}/"), b'5', 0), &[]);
    }
    archive.extend_from_slice(&[0u8; 1024]);

    let (entries, allocs) = read_counting(&archive)?;
    assert_eq!(entries, DIRS);
    // The reader parses the global header once. Each entry then allocates a
    // fixed number of times.
    assert!(
        allocs < 4 * ATTRS + 16 * DIRS,
        "{allocs} allocations for {ATTRS} global attributes and {DIRS} entries"
    );
    Ok(())
}
