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

/// Builds a ustar hard link header with `size` bytes of data.
fn link_header(name: &str, link: &str, size: usize) -> [u8; 512] {
    let mut block = [0u8; 512];
    block[..name.len()].copy_from_slice(name.as_bytes());
    block[100..108].copy_from_slice(b"0000644\0");
    block[108..116].copy_from_slice(b"0000000\0");
    block[116..124].copy_from_slice(b"0000000\0");
    block[124..136].copy_from_slice(format!("{size:011o}\0").as_bytes());
    block[136..148].copy_from_slice(b"00000000000\0");
    block[148..156].fill(b' ');
    block[156] = b'1';
    block[157..157 + link.len()].copy_from_slice(link.as_bytes());
    block[257..263].copy_from_slice(b"ustar\0");
    block[263..265].copy_from_slice(b"00");
    let sum: u32 = block.iter().map(|&b| u32::from(b)).sum();
    block[148..156].copy_from_slice(format!("{sum:06o}\0 ").as_bytes());
    block
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
fn skipped_link_data_allocates_one_skip_buffer() -> Result<()> {
    const LINKS: usize = 128;
    const LINK_SIZE: usize = 1000;
    let tail = b"tail data";
    let tail_file = TarRegularFile::new("tail.txt", tail.len() as u64, &tail[..]);
    let tail_archive = write_archive(vec![tail_file.into()])?;
    let mut archive = Vec::new();
    for i in 0..LINKS {
        archive.extend_from_slice(&link_header(&format!("link-{i}"), "target", LINK_SIZE));
        archive.extend_from_slice(&[0x5a; LINK_SIZE]);
        archive.resize(archive.len().next_multiple_of(512), 0);
    }
    archive.extend_from_slice(&tail_archive);

    let (links, data, large) = block_on_test(async {
        let counter = Counter::start();
        let mut stream = Box::pin(TarReader::new(&archive[..]));
        let mut links = 0;
        let data = loop {
            match stream.next().await.unwrap()? {
                TarEntry::Link(link) => {
                    assert_eq!(link.path(), format!("link-{links}"));
                    assert_eq!(link.link(), "target");
                    links += 1;
                }
                TarEntry::File(mut file) => {
                    assert_eq!(file.path(), "tail.txt");
                    let mut data = Vec::new();
                    file.read_to_end(&mut data).await?;
                    break data;
                }
                other => panic!("expected link or file entry, got {:?}", other),
            }
        };
        assert!(stream.next().await.is_none());
        Ok::<_, std::io::Error>((links, data, counter.count()))
    })?;

    assert_eq!(links, LINKS);
    assert_eq!(data, tail);
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
