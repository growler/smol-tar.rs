use std::{
    future::Future,
    io::{self, ErrorKind, Result},
    path::PathBuf,
    pin::Pin,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex,
    },
    task::{Context, Poll, RawWaker, RawWakerVTable, Wake, Waker},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use futures::{future, SinkExt, TryStreamExt};
use futures::{FutureExt, Sink};
#[cfg(all(feature = "smol", not(feature = "tokio")))]
use smol::io::{AsyncBufRead, AsyncBufReadExt, AsyncRead, AsyncReadExt, AsyncWrite};
#[cfg(all(feature = "smol", not(feature = "tokio")))]
use smol::{io::Cursor, stream::StreamExt};
use smol_tar::{
    AttrList, TarDevice, TarDirectory, TarEntry, TarFifo, TarLink, TarReader, TarRegularFile,
    TarSymlink, TarWriter,
};
#[cfg(feature = "tokio")]
use std::io::Cursor;
#[cfg(feature = "tokio")]
use tokio::io::{AsyncBufRead, AsyncBufReadExt, AsyncRead, AsyncReadExt, AsyncWrite, ReadBuf};
#[cfg(feature = "tokio")]
use tokio_stream::StreamExt;

fn fixture(name: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests/data")
        .join(name)
}

fn ts(secs: i64, nanos: u32) -> SystemTime {
    if secs >= 0 {
        UNIX_EPOCH + Duration::new(secs as u64, nanos)
    } else {
        let duration = Duration::new(secs.unsigned_abs(), nanos);
        UNIX_EPOCH - duration
    }
}

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

#[cfg(all(feature = "smol", not(feature = "tokio")))]
type FixtureFile = smol::fs::File;
#[cfg(feature = "tokio")]
type FixtureFile = tokio::fs::File;
#[cfg(all(feature = "smol", not(feature = "tokio")))]
type TestCursor<T> = futures::io::Cursor<T>;
#[cfg(feature = "tokio")]
type TestCursor<T> = std::io::Cursor<T>;

#[cfg(all(feature = "smol", not(feature = "tokio")))]
async fn open_fixture_file(name: &str) -> Result<FixtureFile> {
    smol::fs::File::open(fixture(name)).await
}

#[cfg(feature = "tokio")]
async fn open_fixture_file(name: &str) -> Result<FixtureFile> {
    tokio::fs::File::open(fixture(name)).await
}

fn assert_attrs_eq(attrs: &AttrList, expected: &[(&str, &[u8])]) {
    assert_eq!(attrs.len(), expected.len());
    for (name, value) in expected {
        assert!(
            attrs.iter().any(|(n, v)| n == *name && v == *value),
            "missing attr {}",
            name
        );
    }
}

const BLOCK_SIZE: usize = 512;
const PATH_MAX: usize = 4096;
const PAX_HEADER_MAX_SIZE: usize = 1024 * 1024;

const NAME_START: usize = 0;
const MODE_START: usize = 100;
const UID_START: usize = 108;
const GID_START: usize = 116;
const SIZE_START: usize = 124;
const MTIME_START: usize = 136;
const CKSUM_START: usize = 148;
const TYPEFLAG_START: usize = 156;
const LINKNAME_START: usize = 157;
const MAGIC_START: usize = 257;
const VERSION_START: usize = 263;
const UNAME_START: usize = 265;
const GNAME_START: usize = 297;
const GNU_ATIME_START: usize = 345;
const GNU_CTIME_START: usize = 357;

#[derive(Clone, Copy)]
enum HeaderFlavor {
    Old,
    Ustar,
    Gnu,
}

#[derive(Debug, PartialEq, Eq)]
struct ObservedEntry {
    kind: &'static str,
    path: String,
    link: Option<String>,
    data: Vec<u8>,
    attrs: Vec<(String, Vec<u8>)>,
}

struct ChunkedReader<R> {
    inner: R,
    max_chunk: usize,
}

impl<R> ChunkedReader<R> {
    fn new(inner: R, max_chunk: usize) -> Self {
        Self { inner, max_chunk }
    }
}

#[cfg(all(feature = "smol", not(feature = "tokio")))]
impl<R: AsyncRead + Unpin> AsyncRead for ChunkedReader<R> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<std::io::Result<usize>> {
        let limit = buf.len().min(self.max_chunk.max(1));
        Pin::new(&mut self.inner).poll_read(cx, &mut buf[..limit])
    }
}

#[cfg(feature = "tokio")]
impl<R: AsyncRead + Unpin> AsyncRead for ChunkedReader<R> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        let limit = buf.remaining().min(self.max_chunk.max(1));
        if limit == 0 {
            return Poll::Ready(Ok(()));
        }

        let mut scratch = vec![0; limit];
        let mut read_buf = ReadBuf::new(&mut scratch);
        match Pin::new(&mut self.inner).poll_read(cx, &mut read_buf) {
            Poll::Ready(Ok(())) => {
                buf.put_slice(read_buf.filled());
                Poll::Ready(Ok(()))
            }
            Poll::Ready(Err(err)) => Poll::Ready(Err(err)),
            Poll::Pending => Poll::Pending,
        }
    }
}

/// Reader that fails one time on read number `fail_at` (from 1) and passes
/// all other reads to `inner`. `failed_len` holds the buffer length of the
/// failed read.
struct FailingReader<R> {
    inner: R,
    fail_at: usize,
    reads: usize,
    failed_len: Option<usize>,
}

impl<R> FailingReader<R> {
    fn new(inner: R, fail_at: usize) -> Self {
        Self {
            inner,
            fail_at,
            reads: 0,
            failed_len: None,
        }
    }

    /// Count the read and give `true` if this read must fail.
    fn fail_now(&mut self, len: usize) -> bool {
        self.reads += 1;
        if self.reads == self.fail_at {
            self.failed_len = Some(len);
            true
        } else {
            false
        }
    }
}

#[cfg(all(feature = "smol", not(feature = "tokio")))]
impl<R: AsyncRead + Unpin> AsyncRead for FailingReader<R> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<std::io::Result<usize>> {
        if self.fail_now(buf.len()) {
            return Poll::Ready(Err(io::Error::other("source failure")));
        }
        Pin::new(&mut self.inner).poll_read(cx, buf)
    }
}

#[cfg(feature = "tokio")]
impl<R: AsyncRead + Unpin> AsyncRead for FailingReader<R> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        if self.fail_now(buf.remaining()) {
            return Poll::Ready(Err(io::Error::other("source failure")));
        }
        Pin::new(&mut self.inner).poll_read(cx, buf)
    }
}

/// Writer that fails one time on write number `fail_at` (from 1) and passes
/// all other writes to `inner`.
struct FailingWriter<W> {
    inner: W,
    fail_at: usize,
    writes: usize,
}

impl<W> FailingWriter<W> {
    fn new(inner: W, fail_at: usize) -> Self {
        Self {
            inner,
            fail_at,
            writes: 0,
        }
    }
}

impl<W: AsyncWrite + Unpin> AsyncWrite for FailingWriter<W> {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<Result<usize>> {
        self.writes += 1;
        if self.writes == self.fail_at {
            return Poll::Ready(Err(io::Error::other("destination failure")));
        }
        Pin::new(&mut self.inner).poll_write(cx, buf)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<()>> {
        Pin::new(&mut self.inner).poll_flush(cx)
    }

    #[cfg(all(feature = "smol", not(feature = "tokio")))]
    fn poll_close(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<()>> {
        Pin::new(&mut self.inner).poll_close(cx)
    }

    #[cfg(feature = "tokio")]
    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<()>> {
        Pin::new(&mut self.inner).poll_shutdown(cx)
    }
}

struct ChunkedAsyncWriter {
    inner: Arc<Mutex<Vec<u8>>>,
    max_chunk: usize,
}

impl ChunkedAsyncWriter {
    fn new(max_chunk: usize) -> (Self, Arc<Mutex<Vec<u8>>>) {
        let inner = Arc::new(Mutex::new(Vec::new()));
        (
            Self {
                inner: inner.clone(),
                max_chunk,
            },
            inner,
        )
    }
}

struct ZeroWriter;

#[cfg(all(feature = "smol", not(feature = "tokio")))]
impl AsyncWrite for ZeroWriter {
    fn poll_write(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        _buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Poll::Ready(Ok(0))
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_close(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

#[cfg(feature = "tokio")]
impl AsyncWrite for ZeroWriter {
    fn poll_write(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        _buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Poll::Ready(Ok(0))
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

#[cfg(all(feature = "smol", not(feature = "tokio")))]
impl AsyncWrite for ChunkedAsyncWriter {
    fn poll_write(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        let n = buf.len().min(self.max_chunk.max(1));
        self.inner.lock().unwrap().extend_from_slice(&buf[..n]);
        Poll::Ready(Ok(n))
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_close(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

#[cfg(feature = "tokio")]
impl AsyncWrite for ChunkedAsyncWriter {
    fn poll_write(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        let n = buf.len().min(self.max_chunk.max(1));
        self.inner.lock().unwrap().extend_from_slice(&buf[..n]);
        Poll::Ready(Ok(n))
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

fn attrs_snapshot(attrs: &AttrList) -> Vec<(String, Vec<u8>)> {
    attrs
        .iter()
        .map(|(name, value)| (name.to_string(), value.to_vec()))
        .collect()
}

async fn collect_entries_async<R: AsyncRead + Unpin>(reader: R) -> Result<Vec<ObservedEntry>> {
    let mut stream = Box::pin(TarReader::new(reader));
    let mut entries = Vec::new();
    while let Some(entry) = stream.next().await {
        match entry? {
            TarEntry::Directory(dir) => entries.push(ObservedEntry {
                kind: "dir",
                path: dir.path().to_string(),
                link: None,
                data: Vec::new(),
                attrs: attrs_snapshot(dir.attrs()),
            }),
            TarEntry::File(mut file) => {
                let mut data = Vec::new();
                file.read_to_end(&mut data).await?;
                entries.push(ObservedEntry {
                    kind: "file",
                    path: file.path().to_string(),
                    link: None,
                    data,
                    attrs: attrs_snapshot(file.attrs()),
                });
            }
            TarEntry::Symlink(link) => entries.push(ObservedEntry {
                kind: "symlink",
                path: link.path().to_string(),
                link: Some(link.link().to_string()),
                data: Vec::new(),
                attrs: attrs_snapshot(link.attrs()),
            }),
            TarEntry::Link(link) => entries.push(ObservedEntry {
                kind: "hardlink",
                path: link.path().to_string(),
                link: Some(link.link().to_string()),
                data: Vec::new(),
                attrs: Vec::new(),
            }),
            TarEntry::Fifo(fifo) => entries.push(ObservedEntry {
                kind: "fifo",
                path: fifo.path().to_string(),
                link: None,
                data: Vec::new(),
                attrs: attrs_snapshot(fifo.attrs()),
            }),
            TarEntry::Device(device) => entries.push(ObservedEntry {
                kind: if device.is_char() {
                    "char-device"
                } else {
                    "block-device"
                },
                path: device.path().to_string(),
                link: None,
                data: Vec::new(),
                attrs: attrs_snapshot(device.attrs()),
            }),
        }
    }
    Ok(entries)
}

fn collect_entries<R: AsyncRead + Unpin>(reader: R) -> Result<Vec<ObservedEntry>> {
    block_on_test(collect_entries_async(reader))
}

async fn archive_error_async<R: AsyncRead + Unpin>(reader: R) -> std::io::Error {
    let mut stream = Box::pin(TarReader::new(reader));
    loop {
        match stream.next().await {
            Some(Ok(TarEntry::File(mut file))) => {
                let mut data = Vec::new();
                if let Err(err) = file.read_to_end(&mut data).await {
                    return err;
                }
            }
            Some(Ok(_)) => {}
            Some(Err(err)) => return err,
            None => panic!("expected tar reader error"),
        }
    }
}

fn archive_error<R: AsyncRead + Unpin>(reader: R) -> std::io::Error {
    block_on_test(archive_error_async(reader))
}

fn assert_archive_error<R: AsyncRead + Unpin>(reader: R, kind: ErrorKind, message_substr: &str) {
    let err = archive_error(reader);
    assert_eq!(err.kind(), kind);
    assert!(
        err.to_string().contains(message_substr),
        "error {:?} did not contain {:?}",
        err,
        message_substr
    );
}

fn write_octal(value: u64, field: &mut [u8]) {
    field.fill(0);
    let mut value = value;
    let mut index = field.len() - 1;
    while index > 0 {
        index -= 1;
        field[index] = b'0' + (value & 0o7) as u8;
        value >>= 3;
    }
    assert_eq!(value, 0, "value does not fit into tar field");
}

fn set_bytes(block: &mut [u8; BLOCK_SIZE], start: usize, len: usize, bytes: &[u8]) {
    assert!(bytes.len() <= len);
    block[start..start + len].fill(0);
    block[start..start + bytes.len()].copy_from_slice(bytes);
}

fn tar_checksum(block: &[u8; BLOCK_SIZE]) -> u32 {
    block
        .iter()
        .enumerate()
        .map(|(index, byte)| {
            if (CKSUM_START..CKSUM_START + 8).contains(&index) {
                u32::from(b' ')
            } else {
                u32::from(*byte)
            }
        })
        .sum()
}

fn finalize_checksum(block: &mut [u8; BLOCK_SIZE]) {
    block[CKSUM_START..CKSUM_START + 8].fill(b' ');
    write_octal(
        tar_checksum(block) as u64,
        &mut block[CKSUM_START..CKSUM_START + 8],
    );
}

fn build_header(name: &[u8], typeflag: u8, size: u64, flavor: HeaderFlavor) -> [u8; BLOCK_SIZE] {
    let mut block = [0u8; BLOCK_SIZE];
    set_bytes(&mut block, NAME_START, 100, name);
    write_octal(0o644, &mut block[MODE_START..MODE_START + 8]);
    write_octal(0, &mut block[UID_START..UID_START + 8]);
    write_octal(0, &mut block[GID_START..GID_START + 8]);
    write_octal(size, &mut block[SIZE_START..SIZE_START + 12]);
    write_octal(0, &mut block[MTIME_START..MTIME_START + 12]);
    block[TYPEFLAG_START] = typeflag;
    match flavor {
        HeaderFlavor::Old => {}
        HeaderFlavor::Ustar => {
            set_bytes(&mut block, MAGIC_START, 6, b"ustar\0");
            set_bytes(&mut block, VERSION_START, 2, b"00");
        }
        HeaderFlavor::Gnu => {
            set_bytes(&mut block, MAGIC_START, 6, b"ustar ");
            set_bytes(&mut block, VERSION_START, 2, b" \0");
        }
    }
    finalize_checksum(&mut block);
    block
}

fn append_entry(archive: &mut Vec<u8>, header: [u8; BLOCK_SIZE], data: &[u8]) {
    archive.extend_from_slice(&header);
    archive.extend_from_slice(data);
    let padded_len = archive.len().next_multiple_of(BLOCK_SIZE);
    archive.resize(padded_len, 0);
}

fn append_eof(archive: &mut Vec<u8>) {
    archive.extend_from_slice(&[0u8; BLOCK_SIZE * 2]);
}

fn pax_record_bytes(key: &str, value: &[u8]) -> Vec<u8> {
    let payload_len = key.len() + 1 + value.len() + 1;
    let mut len = payload_len + 2;
    loop {
        let digits = len.to_string().len();
        let next = payload_len + 1 + digits;
        if next == len {
            break;
        }
        len = next;
    }
    let mut record = Vec::with_capacity(len);
    record.extend_from_slice(len.to_string().as_bytes());
    record.push(b' ');
    record.extend_from_slice(key.as_bytes());
    record.push(b'=');
    record.extend_from_slice(value);
    record.push(b'\n');
    assert_eq!(record.len(), len);
    record
}

fn append_pax_entry(archive: &mut Vec<u8>, typeflag: u8, payload: &[u8]) {
    append_entry(
        archive,
        build_header(
            b"././@PaxHeader",
            typeflag,
            payload.len() as u64,
            HeaderFlavor::Ustar,
        ),
        payload,
    );
}

fn cve_2025_62518_archive() -> ([u8; BLOCK_SIZE], Vec<u8>) {
    let mut archive = Vec::new();
    append_entry(
        &mut archive,
        build_header(b"normal.txt", b'0', 6, HeaderFlavor::Ustar),
        b"normal",
    );

    let pax = pax_record_bytes("size", b"1024");
    append_pax_entry(&mut archive, b'x', &pax);

    let fake_header = build_header(b"smuggled.txt", b'0', 4, HeaderFlavor::Ustar);
    let mut payload = vec![0; 1024];
    payload[..BLOCK_SIZE].copy_from_slice(&fake_header);
    payload[BLOCK_SIZE..BLOCK_SIZE + 4].copy_from_slice(b"EVIL");

    append_entry(
        &mut archive,
        build_header(b"blob.bin", b'0', 0, HeaderFlavor::Ustar),
        &payload,
    );
    append_entry(
        &mut archive,
        build_header(b"marker.txt", b'0', 6, HeaderFlavor::Ustar),
        b"marker",
    );
    append_eof(&mut archive);
    (fake_header, archive)
}

fn smuggled_entry() -> Vec<u8> {
    let mut entry = Vec::new();
    append_entry(
        &mut entry,
        build_header(b"smuggled.txt", b'0', 6, HeaderFlavor::Ustar),
        b"pwned\n",
    );
    entry
}

fn link_archive(
    name: &str,
    typeflag: u8,
    flavor: HeaderFlavor,
    header_size: u64,
    pax_size: Option<u64>,
    data: &[u8],
) -> Vec<u8> {
    let mut archive = Vec::new();
    append_entry(
        &mut archive,
        build_header(b"visible0.txt", b'0', 6, HeaderFlavor::Ustar),
        b"first\n",
    );
    if let Some(size) = pax_size {
        let pax = pax_record_bytes("size", size.to_string().as_bytes());
        append_pax_entry(&mut archive, b'x', &pax);
    }
    let mut header = build_header(name.as_bytes(), typeflag, header_size, flavor);
    set_bytes(&mut header, LINKNAME_START, 100, b"visible0.txt");
    finalize_checksum(&mut header);
    append_entry(&mut archive, header, data);
    append_entry(
        &mut archive,
        build_header(b"visible1.txt", b'0', 7, HeaderFlavor::Ustar),
        b"second\n",
    );
    append_eof(&mut archive);
    archive
}

fn link_entries(kind: &'static str, name: &str) -> Vec<ObservedEntry> {
    vec![
        ObservedEntry {
            kind: "file",
            path: "visible0.txt".to_string(),
            link: None,
            data: b"first\n".to_vec(),
            attrs: Vec::new(),
        },
        ObservedEntry {
            kind,
            path: name.to_string(),
            link: Some("visible0.txt".to_string()),
            data: Vec::new(),
            attrs: Vec::new(),
        },
        ObservedEntry {
            kind: "file",
            path: "visible1.txt".to_string(),
            link: None,
            data: b"second\n".to_vec(),
            attrs: Vec::new(),
        },
    ]
}

#[test]
fn read_simple_archive() -> Result<()> {
    block_on_test(async {
        let archive = open_fixture_file("simple.tar").await?;
        let reader = TarReader::new(archive);
        let mut seen = Vec::new();
        let mut stream = Box::pin(reader);
        while let Some(entry) = stream.next().await {
            let entry = entry?;
            match entry {
                TarEntry::Directory(dir) => {
                    seen.push(("dir".to_string(), dir.path().to_string()));
                    assert_eq!(dir.mode(), 0o755);
                }
                TarEntry::File(mut file) => {
                    let mut buf = Vec::new();
                    file.read_to_end(&mut buf).await?;
                    seen.push(("file".to_string(), file.path().to_string()));
                    assert_eq!(buf, b"hello world\n");
                }
                TarEntry::Symlink(link) => {
                    seen.push(("symlink".to_string(), link.path().to_string()));
                    assert_eq!(link.link(), "hello.txt");
                }
                TarEntry::Link(link) => {
                    seen.push(("hardlink".to_string(), link.path().to_string()));
                    assert_eq!(link.link(), "root/hello.txt");
                }
                other => panic!("unexpected entry: {:?}", other),
            }
        }
        assert_eq!(
            seen,
            vec![
                ("dir".into(), "root/".into()),
                ("file".into(), "root/hello.txt".into()),
                ("symlink".into(), "root/hello-link".into()),
                ("hardlink".into(), "root/hello-hard".into())
            ]
        );
        Ok(())
    })
}

#[test]
fn read_gnu_long_name_archive() -> Result<()> {
    block_on_test(async {
        let archive = open_fixture_file("gnu_long_name.tar").await?;
        let reader = TarReader::new(archive);
        let long_name = format!("nested/{}{}", "a".repeat(120), "/file.txt");
        let long_link = format!("nested/{}{}", "b".repeat(130), "/target.txt");
        let mut paths = Vec::new();
        let mut stream = Box::pin(reader);
        while let Some(entry) = stream.next().await {
            let entry = entry?;
            match entry {
                TarEntry::File(mut file) => {
                    let mut buf = Vec::new();
                    file.read_to_end(&mut buf).await?;
                    assert_eq!(buf, b"long data");
                    paths.push(file.path().to_string());
                }
                TarEntry::Symlink(link) => {
                    assert_eq!(link.link(), long_name);
                    paths.push(link.path().to_string());
                }
                other => panic!("unexpected entry {:?}", other),
            }
        }
        assert_eq!(paths, vec![long_name, long_link]);
        Ok(())
    })
}

#[test]
fn read_pax_archive() -> Result<()> {
    block_on_test(async {
        let archive = open_fixture_file("pax.tar").await?;
        let reader = TarReader::new(archive);
        let pax_path = format!(
            "pax/{}/{}.txt",
            std::iter::repeat_n("deep", 10)
                .collect::<Vec<_>>()
                .join("/"),
            "c".repeat(200)
        );
        let pax_link = format!("pax/link-{}", "d".repeat(190));
        let mut paths = Vec::new();
        let mut stream = Box::pin(reader);
        while let Some(entry) = stream.next().await {
            let entry = entry?;
            match entry {
                TarEntry::File(file) => {
                    assert_eq!(file.path(), pax_path);
                    paths.push(file.path().to_string());
                }
                TarEntry::Symlink(link) => {
                    assert_eq!(link.link(), pax_path);
                    assert_eq!(link.path(), pax_link);
                    paths.push(link.path().to_string());
                }
                other => panic!("unexpected entry {:?}", other),
            }
        }
        assert_eq!(paths, vec![pax_path, pax_link]);
        Ok(())
    })
}

struct VecAsyncWriter {
    inner: Arc<Mutex<Vec<u8>>>,
}

impl VecAsyncWriter {
    fn new() -> (Self, Arc<Mutex<Vec<u8>>>) {
        let inner = Arc::new(Mutex::new(Vec::new()));
        (
            Self {
                inner: inner.clone(),
            },
            inner,
        )
    }
}

#[cfg(all(feature = "smol", not(feature = "tokio")))]
impl AsyncWrite for VecAsyncWriter {
    fn poll_write(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        self.inner.lock().unwrap().extend_from_slice(buf);
        Poll::Ready(Ok(buf.len()))
    }

    fn poll_flush(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut Context<'_>,
    ) -> Poll<std::io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_close(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut Context<'_>,
    ) -> Poll<std::io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

#[cfg(feature = "tokio")]
impl AsyncWrite for VecAsyncWriter {
    fn poll_write(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        self.inner.lock().unwrap().extend_from_slice(buf);
        Poll::Ready(Ok(buf.len()))
    }

    fn poll_flush(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut Context<'_>,
    ) -> Poll<std::io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut Context<'_>,
    ) -> Poll<std::io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

#[test]
fn tar_writer_round_trip() -> Result<()> {
    block_on_test(async {
        let (writer_sink, shared) = VecAsyncWriter::new();
        let mut writer = TarWriter::<'static, 'static, _, Cursor<&'static [u8]>>::new(writer_sink);
        writer
            .send(TarEntry::Directory(
                TarDirectory::new("dir/").with_mtime(ts(1, 0)),
            ))
            .await?;
        const DATA: &[u8] = b"writer round trip";
        writer
            .send(TarEntry::File(
                TarRegularFile::new("dir/file.txt", DATA.len() as u64, Cursor::new(DATA))
                    .with_uid(1000)
                    .with_gid(1001)
                    .with_mode(0o640)
                    .with_mtime(ts(2, 0)),
            ))
            .await?;
        writer
            .send(TarEntry::Device(
                TarDevice::new_char("dir/dev", 1, 3).with_mtime(ts(3, 0)),
            ))
            .await?;
        writer
            .send(TarEntry::Symlink(
                TarSymlink::new("dir/link", "file.txt").with_mtime(ts(4, 0)),
            ))
            .await?;
        writer.close().await?;

        let buffer = shared.lock().unwrap().clone();
        let cursor = Cursor::new(buffer);
        let reader = TarReader::new(cursor);
        let mut stream = Box::pin(reader);
        let mut order = Vec::new();
        while let Some(entry) = stream.next().await {
            let entry = entry?;
            match entry {
                TarEntry::Directory(dir) => {
                    order.push(dir.path().to_string());
                    assert_eq!(dir.mode(), 0o755);
                }
                TarEntry::File(mut file) => {
                    let mut buf = Vec::new();
                    file.read_to_end(&mut buf).await?;
                    assert_eq!(buf, DATA);
                    order.push(file.path().to_string());
                }
                TarEntry::Device(device) => {
                    assert!(device.is_char());
                    assert_eq!(device.major(), 1);
                    assert_eq!(device.minor(), 3);
                    order.push(device.path().to_string());
                }
                TarEntry::Symlink(link) => {
                    assert_eq!(link.link(), "file.txt");
                    order.push(link.path().to_string());
                }
                other => panic!("unexpected entry {:?}", other),
            }
        }
        assert_eq!(
            order,
            vec![
                String::from("dir/"),
                String::from("dir/file.txt"),
                String::from("dir/dev"),
                String::from("dir/link")
            ]
        );
        Ok(())
    })
}

#[test]
fn tar_writer_inherent_write_and_finish_round_trip() -> Result<()> {
    block_on_test(async {
        let (writer_sink, shared) = VecAsyncWriter::new();
        let mut writer = TarWriter::<'static, 'static, _, Cursor<&'static [u8]>>::new(writer_sink);

        writer
            .write(TarEntry::Directory(
                TarDirectory::new("dir/").with_mtime(ts(1, 0)),
            ))
            .await?;

        const DATA: &[u8] = b"inherent writer";
        writer
            .write(TarEntry::File(TarRegularFile::new(
                "dir/file.txt",
                DATA.len() as u64,
                Cursor::new(DATA),
            )))
            .await?;

        writer.finish().await?;

        let buffer = shared.lock().unwrap().clone();
        let entries = collect_entries_async(Cursor::new(buffer)).await?;
        assert_eq!(
            entries,
            vec![
                ObservedEntry {
                    kind: "dir",
                    path: "dir/".to_string(),
                    link: None,
                    data: Vec::new(),
                    attrs: Vec::new(),
                },
                ObservedEntry {
                    kind: "file",
                    path: "dir/file.txt".to_string(),
                    link: None,
                    data: DATA.to_vec(),
                    attrs: Vec::new(),
                },
            ]
        );

        Ok(())
    })
}

#[test]
fn tar_writer_round_trip_long_names() -> Result<()> {
    block_on_test(async {
        let (writer_sink, shared) = VecAsyncWriter::new();
        let mut writer = TarWriter::<'static, 'static, _, Cursor<&'static [u8]>>::new(writer_sink);
        let long_file_path = format!("root/{}/{}", "nested".repeat(5), "a".repeat(180));
        let long_symlink_path = format!("root/link-{}", "b".repeat(170));
        let long_symlink_target = format!("target/{}", "c".repeat(175));
        const DATA: &[u8] = b"long names payload";

        writer
            .send(TarEntry::File(
                TarRegularFile::new(long_file_path.clone(), DATA.len() as u64, Cursor::new(DATA))
                    .with_mtime(ts(1, 0)),
            ))
            .await?;
        writer
            .send(TarEntry::Symlink(
                TarSymlink::new(long_symlink_path.clone(), long_symlink_target.clone())
                    .with_mtime(ts(2, 0)),
            ))
            .await?;
        writer.close().await?;

        let buffer = shared.lock().unwrap().clone();
        let cursor = Cursor::new(buffer);
        let reader = TarReader::new(cursor);
        let mut stream = Box::pin(reader);

        // First entry: long path file.
        match stream.next().await.unwrap()? {
            TarEntry::File(mut file) => {
                assert_eq!(file.path(), long_file_path);
                let mut buf = Vec::new();
                file.read_to_end(&mut buf).await?;
                assert_eq!(buf, DATA);
            }
            other => panic!("expected file entry, got {:?}", other),
        }
        // Second entry: symlink with long metadata.
        match stream.next().await.unwrap()? {
            TarEntry::Symlink(link) => {
                assert_eq!(link.path(), long_symlink_path);
                assert_eq!(link.link(), long_symlink_target);
            }
            other => panic!("expected symlink entry, got {:?}", other),
        }
        assert!(stream.next().await.is_none());
        Ok(())
    })
}

/// Archive with the one entry `entry`.
fn one_entry_archive(entry: TarEntry<'static, &'static [u8]>) -> Result<Vec<u8>> {
    block_on_test(async {
        let (sink, bytes) = VecAsyncWriter::new();
        let mut writer = TarWriter::new(sink);
        writer.write(entry).await?;
        writer.finish().await?;
        let archive = bytes.lock().unwrap().clone();
        Ok(archive)
    })
}

#[test]
fn writer_keeps_leading_slash_of_101_byte_path() -> Result<()> {
    let path = format!("/{}", "a".repeat(100));
    let entries: [(TarEntry<'static, &'static [u8]>, Option<&str>); 3] = [
        (TarRegularFile::new(path.clone(), 0, &[][..]).into(), None),
        (TarDirectory::new(path.clone()).into(), None),
        (TarLink::new(path.clone(), "target").into(), Some("target")),
    ];
    for (entry, link) in entries {
        let archive = one_entry_archive(entry)?;
        let observed = collect_entries(Cursor::new(&archive))?;
        assert_eq!(observed.len(), 1);
        assert_eq!(observed[0].path, path);
        assert_eq!(observed[0].link.as_deref(), link);
        assert_eq!(archive[TYPEFLAG_START], b'x');
    }
    Ok(())
}

#[test]
fn writer_writes_no_pax_header_for_absolute_paths_that_fit_ustar() -> Result<()> {
    for path in [
        format!("/{}", "a".repeat(99)),
        format!("//{}", "a".repeat(99)),
    ] {
        let archive = one_entry_archive(TarRegularFile::new(path.clone(), 0, &[][..]).into())?;
        assert_eq!(archive.len(), 3 * BLOCK_SIZE);
        assert_eq!(archive[TYPEFLAG_START], b'0');
        let observed = collect_entries(Cursor::new(&archive))?;
        assert_eq!(observed.len(), 1);
        assert_eq!(observed[0].path, path);
    }
    Ok(())
}

#[test]
fn writer_entry_builders_set_defaults_and_overrides() {
    let dir = TarDirectory::new("dir/")
        .with_uid(10)
        .with_gid(11)
        .with_uname("alice")
        .with_gname("staff")
        .with_mode(0o700)
        .with_mtime(ts(12, 500_000_000))
        .with_atime(ts(1, 250_000_000))
        .with_ctime(ts(-2, 0));
    assert_eq!(dir.path(), "dir/");
    assert_eq!(dir.size(), 0);
    assert_eq!(dir.uid(), 10);
    assert_eq!(dir.gid(), 11);
    assert_eq!(dir.uname(), "alice");
    assert_eq!(dir.gname(), "staff");
    assert_eq!(dir.mode(), 0o700);
    assert_eq!(dir.mtime(), ts(12, 500_000_000));
    assert_eq!(dir.atime(), Some(ts(1, 250_000_000)));
    assert_eq!(dir.ctime(), Some(ts(-2, 0)));

    let file = TarRegularFile::new("file.txt", 3, Cursor::new(b"abc".as_slice()))
        .with_uid(20)
        .with_gid(21)
        .with_uname("bob")
        .with_gname("users")
        .with_mode(0o600)
        .with_mtime(ts(22, 0));
    assert_eq!(file.path(), "file.txt");
    assert_eq!(file.size(), 3);
    assert_eq!(file.uid(), 20);
    assert_eq!(file.gid(), 21);
    assert_eq!(file.uname(), "bob");
    assert_eq!(file.gname(), "users");
    assert_eq!(file.mode(), 0o600);
    assert_eq!(file.mtime(), ts(22, 0));

    let symlink = TarSymlink::new("link", "target")
        .with_uid(30)
        .with_gid(31)
        .with_uname("carol")
        .with_gname("wheel")
        .with_mode(0o700)
        .with_mtime(ts(32, 0));
    assert_eq!(symlink.path(), "link");
    assert_eq!(symlink.link(), "target");
    assert_eq!(symlink.uid(), 30);
    assert_eq!(symlink.gid(), 31);
    assert_eq!(symlink.uname(), "carol");
    assert_eq!(symlink.gname(), "wheel");
    assert_eq!(symlink.mode(), 0o700);
    assert_eq!(symlink.mtime(), ts(32, 0));

    let link = TarLink::new("hard", "target");
    assert_eq!(link.path(), "hard");
    assert_eq!(link.link(), "target");

    let fifo = TarFifo::new("fifo")
        .with_uid(40)
        .with_gid(41)
        .with_uname("dave")
        .with_gname("audio")
        .with_mode(0o620)
        .with_mtime(ts(42, 0));
    assert_eq!(fifo.path(), "fifo");
    assert_eq!(fifo.uid(), 40);
    assert_eq!(fifo.gid(), 41);
    assert_eq!(fifo.uname(), "dave");
    assert_eq!(fifo.gname(), "audio");
    assert_eq!(fifo.mode(), 0o620);
    assert_eq!(fifo.mtime(), ts(42, 0));

    let char_device = TarDevice::new_char("char", 1, 3)
        .with_uid(50)
        .with_gid(51)
        .with_uname("erin")
        .with_gname("disk")
        .with_mode(0o640)
        .with_mtime(ts(52, 0));
    assert!(char_device.is_char());
    assert_eq!(char_device.major(), 1);
    assert_eq!(char_device.minor(), 3);
    assert_eq!(char_device.uid(), 50);
    assert_eq!(char_device.gid(), 51);
    assert_eq!(char_device.uname(), "erin");
    assert_eq!(char_device.gname(), "disk");
    assert_eq!(char_device.mode(), 0o640);
    assert_eq!(char_device.mtime(), ts(52, 0));

    let block_device = TarDevice::new_block("block", 8, 0);
    assert!(block_device.is_block());
    assert_eq!(block_device.mode(), 0o600);
    assert_eq!(block_device.uid(), 0);
    assert_eq!(block_device.gid(), 0);
    assert_eq!(block_device.uname(), "");
    assert_eq!(block_device.gname(), "");
    assert_eq!(block_device.mtime(), ts(0, 0));
}

#[test]
fn writer_entry_constructors_apply_default_metadata() {
    let dir = TarDirectory::new("dir/");
    assert_eq!(dir.mode(), 0o755);
    assert_eq!(dir.uid(), 0);
    assert_eq!(dir.gid(), 0);
    assert_eq!(dir.uname(), "");
    assert_eq!(dir.gname(), "");
    assert_eq!(dir.mtime(), ts(0, 0));
    assert_eq!(dir.atime(), None);
    assert_eq!(dir.ctime(), None);

    let file = TarRegularFile::new("file.txt", 0, Cursor::new(&[][..]));
    assert_eq!(file.mode(), 0o644);
    assert_eq!(file.uid(), 0);
    assert_eq!(file.gid(), 0);
    assert_eq!(file.uname(), "");
    assert_eq!(file.gname(), "");
    assert_eq!(file.mtime(), ts(0, 0));
    assert_eq!(file.atime(), None);
    assert_eq!(file.ctime(), None);

    let symlink = TarSymlink::new("link", "target");
    assert_eq!(symlink.mode(), 0o777);
    assert_eq!(symlink.uid(), 0);
    assert_eq!(symlink.gid(), 0);
    assert_eq!(symlink.uname(), "");
    assert_eq!(symlink.gname(), "");
    assert_eq!(symlink.mtime(), ts(0, 0));
    assert_eq!(symlink.atime(), None);
    assert_eq!(symlink.ctime(), None);

    let link = TarLink::new("hard", "target");
    assert_eq!(link.path(), "hard");
    assert_eq!(link.link(), "target");

    let fifo = TarFifo::new("fifo");
    assert_eq!(fifo.mode(), 0o644);
    assert_eq!(fifo.uid(), 0);
    assert_eq!(fifo.gid(), 0);
    assert_eq!(fifo.uname(), "");
    assert_eq!(fifo.gname(), "");
    assert_eq!(fifo.mtime(), ts(0, 0));
    assert_eq!(fifo.atime(), None);
    assert_eq!(fifo.ctime(), None);

    let device = TarDevice::new_char("dev", 1, 3);
    assert_eq!(device.mode(), 0o600);
    assert_eq!(device.uid(), 0);
    assert_eq!(device.gid(), 0);
    assert_eq!(device.uname(), "");
    assert_eq!(device.gname(), "");
    assert_eq!(device.mtime(), ts(0, 0));
    assert_eq!(device.atime(), None);
    assert_eq!(device.ctime(), None);
}

#[test]
fn entry_builders_support_mutable_attrs_and_debug_output() {
    let mut dir = TarDirectory::new("dir/")
        .with_atime(ts(1, 0))
        .with_ctime(ts(2, 0));
    dir.attrs_mut().push("user.dir", b"one".as_slice());
    assert_eq!(dir.atime(), Some(ts(1, 0)));
    assert_eq!(dir.ctime(), Some(ts(2, 0)));
    assert_attrs_eq(dir.attrs(), &[("user.dir", b"one")]);

    let mut file = TarRegularFile::new("file.txt", 3, Cursor::new(b"abc".as_slice()))
        .with_atime(ts(3, 0))
        .with_ctime(ts(4, 0));
    file.attrs_mut().push("user.file", b"two".as_slice());
    assert_eq!(file.atime(), Some(ts(3, 0)));
    assert_eq!(file.ctime(), Some(ts(4, 0)));
    assert_attrs_eq(file.attrs(), &[("user.file", b"two")]);

    let mut symlink = TarSymlink::new("link", "target")
        .with_atime(ts(5, 0))
        .with_ctime(ts(6, 0));
    symlink
        .attrs_mut()
        .push("user.symlink", b"three".as_slice());
    assert_eq!(symlink.atime(), Some(ts(5, 0)));
    assert_eq!(symlink.ctime(), Some(ts(6, 0)));
    assert_attrs_eq(symlink.attrs(), &[("user.symlink", b"three")]);

    let mut fifo = TarFifo::new("fifo")
        .with_atime(ts(7, 0))
        .with_ctime(ts(8, 0));
    fifo.attrs_mut().push("user.fifo", b"four".as_slice());
    assert_eq!(fifo.atime(), Some(ts(7, 0)));
    assert_eq!(fifo.ctime(), Some(ts(8, 0)));
    assert_attrs_eq(fifo.attrs(), &[("user.fifo", b"four")]);

    let mut device = TarDevice::new_block("block", 8, 0)
        .with_atime(ts(9, 0))
        .with_ctime(ts(10, 0));
    device.attrs_mut().push("user.dev", b"five".as_slice());
    assert_eq!(device.atime(), Some(ts(9, 0)));
    assert_eq!(device.ctime(), Some(ts(10, 0)));
    assert_attrs_eq(device.attrs(), &[("user.dev", b"five")]);

    let file_debug = format!("{:?}", TarEntry::<Cursor<&'static [u8]>>::from(file));
    assert!(file_debug.contains("TarEntry::File"));
    assert!(file_debug.contains("path_name"));
    assert!(file_debug.contains("attrs"));

    let dir_debug = format!("{:?}", TarEntry::<Cursor<&'static [u8]>>::from(dir));
    assert!(dir_debug.contains("TarEntry::Directory"));
    assert!(dir_debug.contains("dir/"));

    let symlink_debug = format!("{:?}", TarEntry::<Cursor<&'static [u8]>>::from(symlink));
    assert!(symlink_debug.contains("TarEntry::Symlink"));
    assert!(symlink_debug.contains("target"));

    let link_debug = format!(
        "{:?}",
        TarEntry::<Cursor<&'static [u8]>>::from(TarLink::new("hard", "dest"))
    );
    assert!(link_debug.contains("TarEntry::Link"));
    assert!(link_debug.contains("dest"));

    let fifo_debug = format!("{:?}", TarEntry::<Cursor<&'static [u8]>>::from(fifo));
    assert!(fifo_debug.contains("TarEntry::Fifo"));
    assert!(fifo_debug.contains("fifo"));

    let device_debug = format!("{:?}", TarEntry::<Cursor<&'static [u8]>>::from(device));
    assert!(device_debug.contains("TarEntry::Device"));
    assert!(device_debug.contains("block"));
}

#[test]
fn writer_reports_write_zero_and_rejects_concurrent_start_send() {
    block_on_test(async {
        let mut writer =
            TarWriter::<'static, 'static, _, TestCursor<&'static [u8]>>::new(ZeroWriter);
        let err = writer
            .send(TarEntry::from(TarDirectory::new("dir/")))
            .await
            .unwrap_err();
        assert_eq!(err.kind(), ErrorKind::WriteZero);
    });

    let mut writer = TarWriter::<'static, 'static, _, TestCursor<&'static [u8]>>::new(ZeroWriter);
    Pin::new(&mut writer)
        .start_send(TarEntry::from(TarRegularFile::new(
            "file.txt",
            3,
            TestCursor::new(b"abc".as_slice()),
        )))
        .unwrap();
    let err = Pin::new(&mut writer)
        .start_send(TarEntry::from(TarDirectory::new("later/")))
        .unwrap_err();
    assert!(err.to_string().contains("previous entry still in progress"));
}

#[test]
fn writer_inherent_write_reports_write_zero() {
    block_on_test(async {
        let mut writer =
            TarWriter::<'static, 'static, _, TestCursor<&'static [u8]>>::new(ZeroWriter);
        let err = writer
            .write(TarEntry::from(TarDirectory::new("dir/")))
            .await
            .unwrap_err();
        assert_eq!(err.kind(), ErrorKind::WriteZero);
    });
}

#[test]
fn writer_rejects_entry_after_finish() -> Result<()> {
    block_on_test(async {
        let (sink, bytes) = VecAsyncWriter::new();
        let mut writer = TarWriter::<'static, 'static, _, Cursor<&'static [u8]>>::new(sink);
        writer
            .write(TarEntry::from(TarDirectory::new("dir/")))
            .await?;
        writer.finish().await?;
        let archive = bytes.lock().unwrap().clone();
        assert_eq!(archive.len(), 3 * BLOCK_SIZE);
        assert!(archive[BLOCK_SIZE..].iter().all(|&b| b == 0));

        let err = writer
            .write(TarEntry::from(TarDirectory::new("late/")))
            .await
            .unwrap_err();
        assert_eq!(err.kind(), ErrorKind::Other);
        assert!(err.to_string().contains("already finished"));
        assert_eq!(*bytes.lock().unwrap(), archive);

        let entries = collect_entries_async(Cursor::new(archive)).await?;
        let paths: Vec<_> = entries.iter().map(|e| e.path.as_str()).collect();
        assert_eq!(paths, ["dir/"]);
        Ok(())
    })
}

#[test]
fn writer_sink_rejects_entry_after_close() -> Result<()> {
    block_on_test(async {
        let (sink, _bytes) = VecAsyncWriter::new();
        let mut writer = TarWriter::<'static, 'static, _, Cursor<&'static [u8]>>::new(sink);
        writer
            .send(TarEntry::from(TarDirectory::new("dir/")))
            .await?;
        writer.close().await?;

        let err = writer
            .send(TarEntry::from(TarDirectory::new("late/")))
            .await
            .unwrap_err();
        assert_eq!(err.kind(), ErrorKind::Other);
        assert!(err.to_string().contains("already finished"));

        let err = Pin::new(&mut writer)
            .start_send(TarEntry::from(TarDirectory::new("late/")))
            .unwrap_err();
        assert_eq!(err.kind(), ErrorKind::Other);
        assert!(err.to_string().contains("already finished"));
        Ok(())
    })
}

#[test]
fn attr_list_with_and_from_preserve_pairs() {
    let attrs = AttrList::new()
        .with("user.first", b"one".as_slice())
        .with("user.second", b"two".as_slice());
    assert!(!attrs.is_empty());
    assert_attrs_eq(&attrs, &[("user.first", b"one"), ("user.second", b"two")]);

    let attrs = AttrList::from(vec![(
        "user.third".to_string().into_boxed_str(),
        b"three".to_vec().into_boxed_slice(),
    )]);
    assert_eq!(attrs.len(), 1);
    assert_attrs_eq(&attrs, &[("user.third", b"three")]);
}

#[test]
fn attr_list_retain_keeps_order() {
    let mut attrs = AttrList::new()
        .with("user.a", b"1".as_slice())
        .with("trusted.b", b"2".as_slice())
        .with("user.c", b"3".as_slice())
        .with("user.d", b"drop".as_slice());
    attrs.retain(|name, value| name.starts_with("user.") && value != b"drop");
    let kept: Vec<(&str, &[u8])> = attrs.iter().collect();
    assert_eq!(
        kept,
        [("user.a", b"1".as_slice()), ("user.c", b"3".as_slice())]
    );
}

#[test]
fn tar_writer_writes_xattrs() -> Result<()> {
    block_on_test(async {
        let (writer_sink, shared) = VecAsyncWriter::new();
        let mut writer = TarWriter::<'static, 'static, _, Cursor<&'static [u8]>>::new(writer_sink);
        let attrs = AttrList::new().with("user.comment", b"hello".as_slice());
        writer
            .send(TarEntry::File(
                TarRegularFile::new("file.txt", 0, Cursor::new(&[][..]))
                    .with_mtime(ts(1, 0))
                    .with_attrs(attrs),
            ))
            .await?;
        writer.close().await?;

        let buffer = shared.lock().unwrap();
        let needle = b"SCHILY.xattr.user.comment=hello";
        assert!(buffer.windows(needle.len()).any(|window| window == needle));
        Ok(())
    })
}

#[test]
fn tar_reader_reads_xattrs() -> Result<()> {
    block_on_test(async {
        let (writer_sink, shared) = VecAsyncWriter::new();
        let mut writer = TarWriter::<'static, 'static, _, Cursor<&'static [u8]>>::new(writer_sink);
        let attrs = AttrList::new().with("user.comment", b"hello".as_slice());
        writer
            .send(TarEntry::File(
                TarRegularFile::new("file.txt", 0, Cursor::new(&[][..]))
                    .with_mtime(ts(1, 0))
                    .with_attrs(attrs),
            ))
            .await?;
        writer.close().await?;

        let buffer = shared.lock().unwrap().clone();
        let cursor = Cursor::new(buffer);
        let reader = TarReader::new(cursor);
        let mut stream = Box::pin(reader);
        match stream.next().await.unwrap()? {
            TarEntry::File(file) => {
                let attrs = file.attrs();
                assert_eq!(attrs.len(), 1);
                let mut iter = attrs.iter();
                let (name, value) = iter.next().unwrap();
                assert_eq!(name, "user.comment");
                assert_eq!(value, b"hello");
            }
            other => panic!("expected file entry, got {:?}", other),
        }
        Ok(())
    })
}

#[test]
fn reader_accepts_binary_xattr_value() -> Result<()> {
    block_on_test(async {
        let (writer_sink, shared) = VecAsyncWriter::new();
        let mut writer = TarWriter::<'static, 'static, _, Cursor<&'static [u8]>>::new(writer_sink);
        let attrs = AttrList::new().with("user.binary", b"\x00\xff\x80\n".as_slice());
        writer
            .send(TarEntry::File(
                TarRegularFile::new("file.txt", 0, Cursor::new(&[][..]))
                    .with_mtime(ts(1, 0))
                    .with_attrs(attrs),
            ))
            .await?;
        writer.close().await?;

        let buffer = shared.lock().unwrap().clone();
        let cursor = Cursor::new(buffer);
        let reader = TarReader::new(cursor);
        let mut stream = Box::pin(reader);
        match stream.next().await.unwrap()? {
            TarEntry::File(file) => {
                assert_attrs_eq(file.attrs(), &[("user.binary", b"\x00\xff\x80\n")]);
            }
            other => panic!("expected file entry, got {:?}", other),
        }
        Ok(())
    })
}

#[test]
fn tar_writer_writes_pax_header_larger_than_initial_buffer() -> Result<()> {
    block_on_test(async {
        let (writer_sink, shared) = VecAsyncWriter::new();
        let mut writer = TarWriter::<'static, 'static, _, Cursor<&'static [u8]>>::new(writer_sink);
        let value = vec![b'x'; 20 * 1024];
        writer
            .send(TarEntry::File(
                TarRegularFile::new("big.txt", 3, Cursor::new(&b"abc"[..]))
                    .with_attrs(AttrList::new().with("user.big", value.clone())),
            ))
            .await?;
        writer
            .send(TarEntry::File(TarRegularFile::new(
                "small.txt",
                3,
                Cursor::new(&b"def"[..]),
            )))
            .await?;
        writer.close().await?;

        let buffer = shared.lock().unwrap().clone();
        let mut stream = Box::pin(TarReader::new(Cursor::new(buffer)));
        match stream.next().await.unwrap()? {
            TarEntry::File(mut file) => {
                assert_eq!(file.path(), "big.txt");
                assert_attrs_eq(file.attrs(), &[("user.big", value.as_slice())]);
                let mut body = Vec::new();
                file.read_to_end(&mut body).await?;
                assert_eq!(body, b"abc");
            }
            other => panic!("expected file entry, got {:?}", other),
        }
        match stream.next().await.unwrap()? {
            TarEntry::File(mut file) => {
                assert_eq!(file.path(), "small.txt");
                let mut body = Vec::new();
                file.read_to_end(&mut body).await?;
                assert_eq!(body, b"def");
            }
            other => panic!("expected file entry, got {:?}", other),
        }
        assert!(stream.next().await.is_none());
        Ok(())
    })
}

#[test]
fn tar_writer_limits_header_size() -> Result<()> {
    block_on_test(async {
        let (writer_sink, shared) = VecAsyncWriter::new();
        let mut writer = TarWriter::<'static, 'static, _, Cursor<&'static [u8]>>::new(writer_sink);
        // The record "130048 SCHILY.xattr.user.big=<value>\n" is 130048 bytes,
        // which is 254 blocks. With the two ustar headers, the entry headers
        // are 131072 bytes, which is the limit.
        let at_limit = vec![b'x'; 130_018];
        writer
            .send(TarEntry::File(
                TarRegularFile::new("at-limit.txt", 0, Cursor::new(&[][..]))
                    .with_attrs(AttrList::new().with("user.big", at_limit.clone())),
            ))
            .await?;
        let err = writer
            .send(TarEntry::File(
                TarRegularFile::new("over-limit.txt", 0, Cursor::new(&[][..]))
                    .with_attrs(AttrList::new().with("user.big", vec![b'x'; 130_019])),
            ))
            .await
            .unwrap_err();
        assert_eq!(err.kind(), ErrorKind::Other);
        assert!(err.to_string().contains("header too large"));
        writer
            .send(TarEntry::File(TarRegularFile::new(
                "after.txt",
                3,
                Cursor::new(&b"abc"[..]),
            )))
            .await?;
        writer.close().await?;

        let buffer = shared.lock().unwrap().clone();
        let mut stream = Box::pin(TarReader::new(Cursor::new(buffer)));
        match stream.next().await.unwrap()? {
            TarEntry::File(file) => {
                assert_eq!(file.path(), "at-limit.txt");
                assert_attrs_eq(file.attrs(), &[("user.big", at_limit.as_slice())]);
            }
            other => panic!("expected file entry, got {:?}", other),
        }
        match stream.next().await.unwrap()? {
            TarEntry::File(mut file) => {
                assert_eq!(file.path(), "after.txt");
                assert!(file.attrs().is_empty());
                let mut body = Vec::new();
                file.read_to_end(&mut body).await?;
                assert_eq!(body, b"abc");
            }
            other => panic!("expected file entry, got {:?}", other),
        }
        assert!(stream.next().await.is_none());
        Ok(())
    })
}

#[test]
fn tar_writer_round_trip_extended_pax_metadata() -> Result<()> {
    block_on_test(async {
        let (writer_sink, shared) = VecAsyncWriter::new();
        let mut writer = TarWriter::<'static, 'static, _, Cursor<&'static [u8]>>::new(writer_sink);

        writer
            .send(TarEntry::Directory(
                TarDirectory::new("dir/")
                    .with_size(10_000_000_000)
                    .with_mtime(ts(9, 125_000_000)),
            ))
            .await?;
        writer
            .send(TarEntry::File(
                TarRegularFile::new("dir/file.txt", 5, Cursor::new(b"hello".as_slice()))
                    .with_uid(3_000_000)
                    .with_gid(4_000_000)
                    .with_mtime(ts(7, 500_000_000))
                    .with_atime(ts(1, 250_000_000))
                    .with_ctime(ts(-2, 0)),
            ))
            .await?;
        writer
            .send(TarEntry::Link(TarLink::new("dir/hard", "dir/file.txt")))
            .await?;
        writer.close().await?;

        let buffer = shared.lock().unwrap().clone();
        for needle in [
            b"size=10000000000".as_slice(),
            b"mtime=9.125".as_slice(),
            b"uid=3000000".as_slice(),
            b"gid=4000000".as_slice(),
            b"mtime=7.5".as_slice(),
            b"atime=1.25".as_slice(),
            b"ctime=-2".as_slice(),
        ] {
            assert!(
                buffer.windows(needle.len()).any(|window| window == needle),
                "missing {:?}",
                String::from_utf8_lossy(needle)
            );
        }

        let mut stream = Box::pin(TarReader::new(Cursor::new(buffer)));

        match stream.next().await.unwrap()? {
            TarEntry::Directory(dir) => {
                assert_eq!(dir.path(), "dir/");
                assert_eq!(dir.size(), 10_000_000_000);
                assert_eq!(dir.mtime(), ts(9, 125_000_000));
            }
            other => panic!("expected directory entry, got {:?}", other),
        }

        match stream.next().await.unwrap()? {
            TarEntry::File(mut file) => {
                let mut data = Vec::new();
                file.read_to_end(&mut data).await?;
                assert_eq!(file.uid(), 3_000_000);
                assert_eq!(file.gid(), 4_000_000);
                assert_eq!(file.mtime(), ts(7, 500_000_000));
                assert_eq!(file.atime(), Some(ts(1, 250_000_000)));
                assert_eq!(file.ctime(), Some(ts(-2, 0)));
                assert_eq!(data, b"hello");
            }
            other => panic!("expected file entry, got {:?}", other),
        }

        match stream.next().await.unwrap()? {
            TarEntry::Link(link) => {
                assert_eq!(link.path(), "dir/hard");
                assert_eq!(link.link(), "dir/file.txt");
            }
            other => panic!("expected hard-link entry, got {:?}", other),
        }

        assert!(stream.next().await.is_none());
        Ok(())
    })
}

#[test]
fn tar_writer_uses_header_mtime_when_it_fits() -> Result<()> {
    block_on_test(async {
        let (writer_sink, shared) = VecAsyncWriter::new();
        let mut writer = TarWriter::<'static, 'static, _, Cursor<&'static [u8]>>::new(writer_sink);
        writer
            .send(TarEntry::File(
                TarRegularFile::new("file.txt", 0, Cursor::new(&[][..])).with_mtime(ts(7, 0)),
            ))
            .await?;
        writer.close().await?;

        let buffer = shared.lock().unwrap().clone();
        assert!(!buffer.windows(6).any(|window| window == b"mtime="));

        let mut stream = Box::pin(TarReader::new(Cursor::new(buffer)));
        match stream.next().await.unwrap()? {
            TarEntry::File(file) => assert_eq!(file.mtime(), ts(7, 0)),
            other => panic!("expected file entry, got {:?}", other),
        }
        assert!(stream.next().await.is_none());
        Ok(())
    })
}

#[test]
fn tar_writer_uses_pax_mtime_when_header_cannot_represent_it() -> Result<()> {
    block_on_test(async {
        let (writer_sink, shared) = VecAsyncWriter::new();
        let mut writer = TarWriter::<'static, 'static, _, Cursor<&'static [u8]>>::new(writer_sink);
        writer
            .send(TarEntry::File(
                TarRegularFile::new("file.txt", 0, Cursor::new(&[][..]))
                    .with_mtime(ts(10_000_000_000, 0)),
            ))
            .await?;
        writer.close().await?;

        let buffer = shared.lock().unwrap().clone();
        assert!(buffer
            .windows(17)
            .any(|window| window == b"mtime=10000000000"));

        let mut stream = Box::pin(TarReader::new(Cursor::new(buffer)));
        match stream.next().await.unwrap()? {
            TarEntry::File(file) => assert_eq!(file.mtime(), ts(10_000_000_000, 0)),
            other => panic!("expected file entry, got {:?}", other),
        }
        assert!(stream.next().await.is_none());
        Ok(())
    })
}

#[test]
fn tar_writer_uses_ustar_symbolic_names_when_they_fit() -> Result<()> {
    block_on_test(async {
        let (writer_sink, shared) = VecAsyncWriter::new();
        let mut writer = TarWriter::<'static, 'static, _, Cursor<&'static [u8]>>::new(writer_sink);
        writer
            .send(TarEntry::File(
                TarRegularFile::new("file.txt", 0, Cursor::new(&[][..]))
                    .with_uname("builder")
                    .with_gname("staff"),
            ))
            .await?;
        writer.close().await?;

        let buffer = shared.lock().unwrap().clone();
        assert!(!buffer.windows(6).any(|window| window == b"uname="));
        assert!(!buffer.windows(6).any(|window| window == b"gname="));
        assert_eq!(
            &buffer[UNAME_START..UNAME_START + "builder".len()],
            b"builder"
        );
        assert_eq!(&buffer[GNAME_START..GNAME_START + "staff".len()], b"staff");

        let mut stream = Box::pin(TarReader::new(Cursor::new(buffer)));
        match stream.next().await.unwrap()? {
            TarEntry::File(file) => {
                assert_eq!(file.uname(), "builder");
                assert_eq!(file.gname(), "staff");
            }
            other => panic!("expected file entry, got {:?}", other),
        }
        assert!(stream.next().await.is_none());
        Ok(())
    })
}

#[test]
fn tar_writer_uses_ustar_symbolic_names_at_max_boundary() -> Result<()> {
    block_on_test(async {
        let uname = "u".repeat(31);
        let gname = "g".repeat(31);
        let (writer_sink, shared) = VecAsyncWriter::new();
        let mut writer = TarWriter::<'static, 'static, _, Cursor<&'static [u8]>>::new(writer_sink);
        writer
            .send(TarEntry::File(
                TarRegularFile::new("file.txt", 0, Cursor::new(&[][..]))
                    .with_uname(uname.clone())
                    .with_gname(gname.clone()),
            ))
            .await?;
        writer.close().await?;

        let buffer = shared.lock().unwrap().clone();
        assert!(
            !buffer.windows(6).any(|window| window == b"uname="),
            "31-char uname should fit in ustar header without PAX"
        );
        assert!(
            !buffer.windows(6).any(|window| window == b"gname="),
            "31-char gname should fit in ustar header without PAX"
        );
        assert_eq!(&buffer[UNAME_START..UNAME_START + 31], uname.as_bytes());
        assert_eq!(&buffer[GNAME_START..GNAME_START + 31], gname.as_bytes());

        let mut stream = Box::pin(TarReader::new(Cursor::new(buffer)));
        match stream.next().await.unwrap()? {
            TarEntry::File(file) => {
                assert_eq!(file.uname(), uname);
                assert_eq!(file.gname(), gname);
            }
            other => panic!("expected file entry, got {:?}", other),
        }
        assert!(stream.next().await.is_none());
        Ok(())
    })
}

#[test]
fn tar_writer_uses_pax_symbolic_names_when_they_do_not_fit() -> Result<()> {
    block_on_test(async {
        let uname = "u".repeat(32);
        let gname = "g".repeat(32);
        let (writer_sink, shared) = VecAsyncWriter::new();
        let mut writer = TarWriter::<'static, 'static, _, Cursor<&'static [u8]>>::new(writer_sink);
        writer
            .send(TarEntry::File(
                TarRegularFile::new("file.txt", 0, Cursor::new(&[][..]))
                    .with_uname(uname.clone())
                    .with_gname(gname.clone()),
            ))
            .await?;
        writer.close().await?;

        let buffer = shared.lock().unwrap().clone();
        assert!(buffer.windows(6).any(|window| window == b"uname="));
        assert!(buffer.windows(6).any(|window| window == b"gname="));
        let entry_header_offset = BLOCK_SIZE * 2;
        assert!(
            buffer[entry_header_offset + UNAME_START..entry_header_offset + UNAME_START + 32]
                .iter()
                .all(|byte| *byte == 0)
        );
        assert!(
            buffer[entry_header_offset + GNAME_START..entry_header_offset + GNAME_START + 32]
                .iter()
                .all(|byte| *byte == 0)
        );

        let mut stream = Box::pin(TarReader::new(Cursor::new(buffer)));
        match stream.next().await.unwrap()? {
            TarEntry::File(file) => {
                assert_eq!(file.uname(), uname);
                assert_eq!(file.gname(), gname);
            }
            other => panic!("expected file entry, got {:?}", other),
        }
        assert!(stream.next().await.is_none());
        Ok(())
    })
}

#[test]
fn tar_writer_does_not_emit_metadata_pax_records_for_hardlinks() -> Result<()> {
    block_on_test(async {
        let (writer_sink, shared) = VecAsyncWriter::new();
        let mut writer = TarWriter::<'static, 'static, _, Cursor<&'static [u8]>>::new(writer_sink);

        writer
            .send(TarEntry::Link(TarLink::new("dir/hard", "dir/file.txt")))
            .await?;
        writer.close().await?;

        let buffer = shared.lock().unwrap().clone();
        for needle in [
            b"uid=".as_slice(),
            b"gid=".as_slice(),
            b"uname=".as_slice(),
            b"gname=".as_slice(),
            b"mtime=".as_slice(),
            b"atime=".as_slice(),
            b"ctime=".as_slice(),
            b"size=".as_slice(),
            b"SCHILY.xattr.".as_slice(),
        ] {
            assert!(
                !buffer.windows(needle.len()).any(|window| window == needle),
                "unexpected hard-link pax metadata {:?}",
                String::from_utf8_lossy(needle)
            );
        }

        let mut stream = Box::pin(TarReader::new(Cursor::new(buffer)));
        match stream.next().await.unwrap()? {
            TarEntry::Link(link) => {
                assert_eq!(link.path(), "dir/hard");
                assert_eq!(link.link(), "dir/file.txt");
            }
            other => panic!("expected hard-link entry, got {:?}", other),
        }
        assert!(stream.next().await.is_none());
        Ok(())
    })
}

#[test]
fn tar_round_trip_xattrs_all_kinds() -> Result<()> {
    block_on_test(async {
        let (writer_sink, shared) = VecAsyncWriter::new();
        let mut writer = TarWriter::<'static, 'static, _, Cursor<&'static [u8]>>::new(writer_sink);
        const DATA: &[u8] = b"round trip xattrs";

        writer
            .send(TarEntry::Directory(
                TarDirectory::new("dir/").with_mtime(ts(1, 0)).with_attrs(
                    AttrList::new()
                        .with("user.dir", b"dir".as_slice())
                        .with("user.dir2", b"dir2".as_slice()),
                ),
            ))
            .await?;
        writer
            .send(TarEntry::File(
                TarRegularFile::new("dir/file.txt", DATA.len() as u64, Cursor::new(DATA))
                    .with_mtime(ts(2, 0))
                    .with_attrs(
                        AttrList::new()
                            .with("user.file", b"file".as_slice())
                            .with("user.file2", b"file2".as_slice()),
                    ),
            ))
            .await?;
        writer
            .send(TarEntry::Symlink(
                TarSymlink::new("dir/link", "file.txt")
                    .with_mtime(ts(3, 0))
                    .with_attrs(AttrList::new().with("user.symlink", b"link".as_slice())),
            ))
            .await?;
        writer
            .send(TarEntry::Link(TarLink::new("dir/hard", "dir/file.txt")))
            .await?;
        writer
            .send(TarEntry::Fifo(
                TarFifo::new("dir/fifo")
                    .with_mtime(ts(4, 0))
                    .with_attrs(AttrList::new().with("user.fifo", b"fifo".as_slice())),
            ))
            .await?;
        writer
            .send(TarEntry::Device(
                TarDevice::new_char("dir/dev", 1, 3)
                    .with_mtime(ts(5, 0))
                    .with_attrs(AttrList::new().with("user.dev", b"dev".as_slice())),
            ))
            .await?;
        writer.close().await?;

        let buffer = shared.lock().unwrap().clone();
        let cursor = Cursor::new(buffer);
        let reader = TarReader::new(cursor);
        let mut stream = Box::pin(reader);
        let mut seen = Vec::new();
        while let Some(entry) = stream.next().await {
            match entry? {
                TarEntry::Directory(dir) => {
                    assert_attrs_eq(dir.attrs(), &[("user.dir", b"dir"), ("user.dir2", b"dir2")]);
                    seen.push(dir.path().to_string());
                }
                TarEntry::File(mut file) => {
                    let mut buf = Vec::new();
                    file.read_to_end(&mut buf).await?;
                    assert_eq!(buf, DATA);
                    assert_attrs_eq(
                        file.attrs(),
                        &[("user.file", b"file"), ("user.file2", b"file2")],
                    );
                    seen.push(file.path().to_string());
                }
                TarEntry::Symlink(link) => {
                    assert_attrs_eq(link.attrs(), &[("user.symlink", b"link")]);
                    seen.push(link.path().to_string());
                }
                TarEntry::Link(link) => {
                    seen.push(link.path().to_string());
                }
                TarEntry::Fifo(fifo) => {
                    assert_attrs_eq(fifo.attrs(), &[("user.fifo", b"fifo")]);
                    seen.push(fifo.path().to_string());
                }
                TarEntry::Device(device) => {
                    assert_attrs_eq(device.attrs(), &[("user.dev", b"dev")]);
                    seen.push(device.path().to_string());
                }
            }
        }
        assert_eq!(
            seen,
            vec![
                "dir/".to_string(),
                "dir/file.txt".to_string(),
                "dir/link".to_string(),
                "dir/hard".to_string(),
                "dir/fifo".to_string(),
                "dir/dev".to_string()
            ]
        );
        Ok(())
    })
}

#[test]
fn dropping_regular_file_reader_skips_entry() -> Result<()> {
    block_on_test(async {
        let archive = open_fixture_file("simple.tar").await?;
        let reader = TarReader::new(archive);
        let mut stream = Box::pin(reader);

        // First entry should be the directory.
        assert!(matches!(
            stream.next().await.unwrap()?,
            TarEntry::Directory(_)
        ));

        // Next entry is the file; read a single byte then drop without finishing.
        if let TarEntry::File(mut file) = stream.next().await.unwrap()? {
            let mut buf = [0u8; 1];
            file.read_exact(&mut buf).await?;
        } else {
            panic!("expected file entry");
        }

        // Stream should continue with the symlink even though we didn't drain the file.
        match stream.next().await.unwrap()? {
            TarEntry::Symlink(link) => assert_eq!(link.path(), "root/hello-link"),
            other => panic!("expected symlink, got {:?}", other),
        }
        Ok(())
    })
}

#[test]
fn dropping_zero_length_regular_file_reader_skips_entry() -> Result<()> {
    block_on_test(async {
        let (sink, out) = ChunkedAsyncWriter::new(1024);
        let mut writer = TarWriter::new(sink);

        writer
            .send(TarEntry::from(TarDirectory::new("dir/")))
            .await?;
        writer
            .send(TarEntry::from(TarRegularFile::new(
                "dir/empty",
                0,
                Cursor::new(Vec::new()),
            )))
            .await?;
        writer
            .send(TarEntry::from(TarRegularFile::new(
                "dir/after.txt",
                5,
                Cursor::new(b"after".to_vec()),
            )))
            .await?;

        writer.close().await?;
        let archive = out.lock().unwrap().clone();
        let reader = TarReader::new(Cursor::new(archive));
        let mut stream = Box::pin(reader);

        assert!(matches!(
            stream.next().await.unwrap()?,
            TarEntry::Directory(_)
        ));

        if let TarEntry::File(file) = stream.next().await.unwrap()? {
            assert_eq!(file.path(), "dir/empty");
            assert_eq!(file.size(), 0);
        } else {
            panic!("expected zero-length file entry");
        }

        match stream.next().await.unwrap()? {
            TarEntry::File(mut file) => {
                assert_eq!(file.path(), "dir/after.txt");
                let mut data = Vec::new();
                file.read_to_end(&mut data).await?;
                assert_eq!(data, b"after");
            }
            other => panic!("expected following file, got {:?}", other),
        }

        Ok(())
    })
}

fn assert_dropped_large_file_is_skipped(archive: Vec<u8>, path: &str) -> Result<()> {
    block_on_test(async {
        let mut stream = Box::pin(TarReader::new(Cursor::new(archive)));
        match stream.next().await.unwrap()? {
            TarEntry::File(file) => {
                assert_eq!(file.path(), path);
                assert_eq!(file.size(), 200_000);
            }
            other => panic!("expected file entry, got {:?}", other),
        }
        match stream.next().await.unwrap()? {
            TarEntry::File(mut file) => {
                let mut data = Vec::new();
                file.read_to_end(&mut data).await?;
                assert_eq!(file.path(), "next.txt");
                assert_eq!(data, b"next");
            }
            other => panic!("expected file entry, got {:?}", other),
        }
        assert!(stream.next().await.is_none());
        Ok(())
    })
}

#[test]
fn dropping_large_file_after_large_pax_header_skips_entry() -> Result<()> {
    let path = format!("{}/big.bin", "d".repeat(595));
    let local = pax_record_bytes("path", path.as_bytes());
    assert_eq!(local.len(), 613);
    let mut archive = Vec::new();
    append_pax_entry(&mut archive, b'x', &local);
    append_entry(
        &mut archive,
        build_header(b"big.bin", b'0', 200_000, HeaderFlavor::Ustar),
        &vec![0x5a; 200_000],
    );
    append_entry(
        &mut archive,
        build_header(b"next.txt", b'0', 4, HeaderFlavor::Ustar),
        b"next",
    );
    append_eof(&mut archive);
    assert_dropped_large_file_is_skipped(archive, &path)
}

#[test]
fn dropping_large_file_after_large_gnu_long_name_skips_entry() -> Result<()> {
    let path = format!("{}/big.bin", "d".repeat(592));
    assert_eq!(path.len(), 600);
    let mut long_name = path.as_bytes().to_vec();
    long_name.push(0);
    let mut archive = Vec::new();
    append_entry(
        &mut archive,
        build_header(
            b"././@LongLink",
            b'L',
            long_name.len() as u64,
            HeaderFlavor::Gnu,
        ),
        &long_name,
    );
    append_entry(
        &mut archive,
        build_header(b"big.bin", b'0', 200_000, HeaderFlavor::Gnu),
        &vec![0x5a; 200_000],
    );
    append_entry(
        &mut archive,
        build_header(b"next.txt", b'0', 4, HeaderFlavor::Gnu),
        b"next",
    );
    append_eof(&mut archive);
    assert_dropped_large_file_is_skipped(archive, &path)
}

#[test]
fn reader_rejects_invalid_header_checksum() {
    for flavor in [HeaderFlavor::Old, HeaderFlavor::Ustar, HeaderFlavor::Gnu] {
        let mut archive = Vec::new();
        let mut header = build_header(b"bad.txt", b'0', 0, flavor);
        header[NAME_START] ^= 0x01;
        archive.extend_from_slice(&header);
        append_eof(&mut archive);
        assert_archive_error(Cursor::new(archive), ErrorKind::InvalidData, "checksum");
    }
}

#[test]
fn reader_rejects_unsupported_typeflag() {
    let mut archive = Vec::new();
    let header = build_header(b"weird", b'Z', 0, HeaderFlavor::Ustar);
    archive.extend_from_slice(&header);
    append_eof(&mut archive);
    assert_archive_error(
        Cursor::new(archive),
        ErrorKind::InvalidData,
        "not supported",
    );
}

#[test]
fn reader_rejects_invalid_octal_fields() {
    let mut archive = Vec::new();
    let mut header = build_header(b"bad-size", b'0', 0, HeaderFlavor::Ustar);
    set_bytes(&mut header, SIZE_START, 12, b"0000000000z");
    finalize_checksum(&mut header);
    archive.extend_from_slice(&header);
    append_eof(&mut archive);
    assert_archive_error(
        Cursor::new(archive),
        ErrorKind::InvalidData,
        "invalid numeric field",
    );
}

#[test]
fn reader_rejects_invalid_utf8_paths() {
    let mut archive = Vec::new();
    let mut header = build_header(&[0xff, 0], b'0', 0, HeaderFlavor::Ustar);
    finalize_checksum(&mut header);
    archive.extend_from_slice(&header);
    append_eof(&mut archive);
    assert_archive_error(
        Cursor::new(archive),
        ErrorKind::InvalidData,
        "invalid utf8 in file path",
    );
}

#[test]
fn reader_reports_truncated_header() {
    assert_archive_error(
        Cursor::new(vec![0u8; BLOCK_SIZE - 10]),
        ErrorKind::UnexpectedEof,
        "tar header",
    );
}

#[test]
fn reader_reports_truncated_file_payload() {
    let mut archive = Vec::new();
    archive.extend_from_slice(&build_header(b"short.txt", b'0', 4, HeaderFlavor::Ustar));
    archive.extend_from_slice(b"abc");
    assert_archive_error(
        Cursor::new(archive),
        ErrorKind::UnexpectedEof,
        "reading archive file",
    );
}

#[test]
fn reader_reports_truncated_padding() {
    let mut archive = Vec::new();
    archive.extend_from_slice(&build_header(b"pad.txt", b'0', 1, HeaderFlavor::Ustar));
    archive.push(b'x');
    assert_archive_error(
        Cursor::new(archive),
        ErrorKind::UnexpectedEof,
        "unexpected end of tar file",
    );
}

#[test]
fn reader_reports_truncated_hardlink_data() {
    let mut archive = Vec::new();
    let mut header = build_header(b"hl", b'1', 4096, HeaderFlavor::Ustar);
    set_bytes(&mut header, LINKNAME_START, 100, b"target");
    finalize_checksum(&mut header);
    archive.extend_from_slice(&header);
    archive.extend_from_slice(&[b'x'; BLOCK_SIZE]);
    assert_archive_error(
        Cursor::new(archive),
        ErrorKind::UnexpectedEof,
        "unexpected end of tar file",
    );
}

#[test]
fn reader_requires_two_zero_blocks() {
    assert_archive_error(
        Cursor::new(vec![0u8; BLOCK_SIZE]),
        ErrorKind::UnexpectedEof,
        "unexpected end of tar file",
    );
}

#[test]
fn reader_rejects_data_after_first_zero_block() {
    let mut archive = vec![0u8; BLOCK_SIZE];
    archive.extend_from_slice(&[1u8; BLOCK_SIZE]);
    assert_archive_error(
        Cursor::new(archive),
        ErrorKind::InvalidData,
        "unexpected data after first zero block",
    );
}

#[test]
fn reader_drains_trailing_zero_padding_to_eof() -> Result<()> {
    block_on_test(async {
        let mut archive = Vec::new();
        append_entry(
            &mut archive,
            build_header(b"file.txt", b'0', 5, HeaderFlavor::Ustar),
            b"hello",
        );
        append_eof(&mut archive);
        // Trailing zero padding past the two-block marker (e.g. record padding).
        archive.extend_from_slice(&[0u8; BLOCK_SIZE * 4]);
        let total = archive.len();

        let mut cursor = Cursor::new(archive);
        {
            let mut stream = Box::pin(TarReader::new(&mut cursor));
            match stream.next().await.unwrap()? {
                TarEntry::File(mut file) => {
                    assert_eq!(file.path(), "file.txt");
                    let mut data = Vec::new();
                    file.read_to_end(&mut data).await?;
                    assert_eq!(data, b"hello");
                }
                other => panic!("expected file entry, got {:?}", other),
            }
            assert!(stream.next().await.is_none());
        }
        // The reader must consume the whole stream, not stop at the marker.
        assert_eq!(cursor.position() as usize, total);
        Ok(())
    })
}

#[test]
fn reader_drains_trailing_garbage_to_eof() -> Result<()> {
    block_on_test(async {
        let mut archive = Vec::new();
        append_entry(
            &mut archive,
            build_header(b"file.txt", b'0', 5, HeaderFlavor::Ustar),
            b"hello",
        );
        append_eof(&mut archive);
        // Non-zero trailing bytes past the marker are discarded, not rejected.
        archive.extend_from_slice(&[0xABu8; 100]);
        let total = archive.len();

        let mut cursor = Cursor::new(archive);
        {
            let mut stream = Box::pin(TarReader::new(&mut cursor));
            match stream.next().await.unwrap()? {
                TarEntry::File(mut file) => {
                    assert_eq!(file.path(), "file.txt");
                    let mut data = Vec::new();
                    file.read_to_end(&mut data).await?;
                    assert_eq!(data, b"hello");
                }
                other => panic!("expected file entry, got {:?}", other),
            }
            assert!(stream.next().await.is_none());
        }
        assert_eq!(cursor.position() as usize, total);
        Ok(())
    })
}

#[test]
fn reader_rejects_oversized_gnu_long_name() {
    let mut archive = Vec::new();
    archive.extend_from_slice(&build_header(
        b"././@LongLink",
        b'L',
        (PATH_MAX + 1) as u64,
        HeaderFlavor::Gnu,
    ));
    assert_archive_error(
        Cursor::new(archive),
        ErrorKind::InvalidData,
        "long filename exceeds",
    );
}

#[test]
fn reader_rejects_empty_gnu_long_name_and_link() {
    for (typeflag, expected) in [(b'L', "empty GNU long name"), (b'K', "empty GNU long link")] {
        let mut archive = Vec::new();
        append_entry(
            &mut archive,
            build_header(b"././@LongLink", typeflag, 0, HeaderFlavor::Gnu),
            &[],
        );
        append_entry(
            &mut archive,
            build_header(b"file.txt", b'0', 0, HeaderFlavor::Gnu),
            &[],
        );
        append_eof(&mut archive);
        assert_archive_error(Cursor::new(archive), ErrorKind::InvalidData, expected);
    }
}

#[test]
fn reader_rejects_oversized_pax_extension() {
    let mut archive = Vec::new();
    archive.extend_from_slice(&build_header(
        b"././@PaxHeader",
        b'x',
        (PAX_HEADER_MAX_SIZE + 1) as u64,
        HeaderFlavor::Ustar,
    ));
    assert_archive_error(
        Cursor::new(archive),
        ErrorKind::InvalidData,
        "PAX extension exceeds",
    );
}

#[test]
fn reader_rejects_oversized_base256_extension_sizes() {
    // 2^32 + 16 must not pass the bound check after truncation to 32 bits
    let size = [0x80, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0x10];
    for (name, typeflag, flavor, message) in [
        (
            &b"././@LongLink"[..],
            b'L',
            HeaderFlavor::Gnu,
            "long filename exceeds",
        ),
        (
            &b"././@PaxHeader"[..],
            b'x',
            HeaderFlavor::Ustar,
            "PAX extension exceeds",
        ),
    ] {
        let mut archive = Vec::new();
        let mut header = build_header(name, typeflag, 0, flavor);
        set_bytes(&mut header, SIZE_START, 12, &size);
        finalize_checksum(&mut header);
        archive.extend_from_slice(&header);
        archive.extend_from_slice(&[0u8; BLOCK_SIZE]);
        assert_archive_error(Cursor::new(archive), ErrorKind::InvalidData, message);
    }
}

#[test]
fn reader_rejects_too_many_global_pax_extensions() {
    let mut archive = Vec::new();
    let record = pax_record_bytes("SCHILY.xattr.user.x", b"val");
    for _ in 0..257 {
        append_pax_entry(&mut archive, b'g', &record);
    }
    append_entry(
        &mut archive,
        build_header(b"file.txt", b'0', 0, HeaderFlavor::Ustar),
        &[],
    );
    append_eof(&mut archive);

    assert_archive_error(
        Cursor::new(archive),
        ErrorKind::InvalidData,
        "too many global PAX extensions",
    );
}

#[test]
fn reader_rejects_malformed_pax_records() {
    for (payload, expected) in [
        (b"9path=a\n".as_slice(), "missing length separator"),
        (b"x path=a\n".as_slice(), "invalid length"),
        (b"10 path=aX".as_slice(), "missing trailing newline"),
        (b"11 pathabc\n".as_slice(), "missing '='"),
        (b"10 p\xffth=a\n".as_slice(), "invalid key"),
        (b"16 path=broken".as_slice(), "malformed PAX record"),
    ] {
        let mut archive = Vec::new();
        append_pax_entry(&mut archive, b'x', payload);
        append_eof(&mut archive);
        assert_archive_error(Cursor::new(archive), ErrorKind::InvalidData, expected);
    }
}

#[test]
fn reader_rejects_pax_record_length_inside_utf8_codepoint() {
    let payload = b"8 path=\xDE\x8F\n";
    let mut archive = Vec::new();
    append_entry(
        &mut archive,
        build_header(
            b"././@PaxHeader",
            b'x',
            payload.len() as u64,
            HeaderFlavor::Ustar,
        ),
        payload,
    );
    append_eof(&mut archive);
    assert_archive_error(
        Cursor::new(archive),
        ErrorKind::InvalidData,
        "malformed PAX record: missing trailing newline",
    );
}

#[test]
fn reader_rejects_non_utf8_pax_path() {
    let mut archive = Vec::new();
    append_pax_entry(&mut archive, b'x', &pax_record_bytes("path", b"\xff"));
    append_entry(
        &mut archive,
        build_header(b"file.txt", b'0', 0, HeaderFlavor::Ustar),
        &[],
    );
    append_eof(&mut archive);
    assert_archive_error(
        Cursor::new(archive),
        ErrorKind::InvalidData,
        "invalid UTF-8",
    );
}

#[test]
fn reader_ignores_binary_value_of_unknown_pax_key() -> Result<()> {
    let mut archive = Vec::new();
    append_pax_entry(&mut archive, b'x', &pax_record_bytes("comment", b"\xff"));
    append_entry(
        &mut archive,
        build_header(b"file.txt", b'0', 5, HeaderFlavor::Ustar),
        b"hello",
    );
    append_eof(&mut archive);
    let entries = collect_entries(Cursor::new(archive))?;
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].kind, "file");
    assert_eq!(entries[0].path, "file.txt");
    assert_eq!(entries[0].data, b"hello");
    Ok(())
}

#[test]
fn reader_reads_binary_xattr_value_from_large_pax_extensions() -> Result<()> {
    let mut value = vec![0xff; 700];
    value.extend_from_slice(b"\0\n=");
    let mut archive = Vec::new();
    append_pax_entry(
        &mut archive,
        b'g',
        &pax_record_bytes("SCHILY.xattr.user.global", &value),
    );
    append_pax_entry(
        &mut archive,
        b'x',
        &pax_record_bytes("SCHILY.xattr.user.local", &value),
    );
    append_entry(
        &mut archive,
        build_header(b"file.txt", b'0', 0, HeaderFlavor::Ustar),
        &[],
    );
    append_eof(&mut archive);
    let entries = collect_entries(ChunkedReader::new(Cursor::new(archive), 7))?;
    assert_eq!(entries.len(), 1);
    assert_eq!(
        entries[0].attrs,
        vec![
            ("user.global".to_string(), value.clone()),
            ("user.local".to_string(), value),
        ]
    );
    Ok(())
}

#[test]
fn reader_accepts_empty_pax_extension() -> Result<()> {
    for typeflag in *b"xg" {
        let mut archive = Vec::new();
        append_pax_entry(&mut archive, typeflag, &[]);
        append_entry(
            &mut archive,
            build_header(b"file.txt", b'0', 5, HeaderFlavor::Ustar),
            b"hello",
        );
        append_eof(&mut archive);
        let entries = collect_entries(Cursor::new(archive))?;
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].kind, "file");
        assert_eq!(entries[0].path, "file.txt");
        assert_eq!(entries[0].data, b"hello");
    }
    Ok(())
}

#[test]
fn reader_applies_global_and_local_pax_precedence() -> Result<()> {
    let mut archive = Vec::new();

    let global = pax_record_bytes("SCHILY.xattr.user.scope", b"global");
    append_entry(
        &mut archive,
        build_header(
            b"././@PaxHeader",
            b'g',
            global.len() as u64,
            HeaderFlavor::Ustar,
        ),
        &global,
    );

    let mut local = Vec::new();
    local.extend_from_slice(&pax_record_bytes("path", b"local-name.txt"));
    local.extend_from_slice(&pax_record_bytes("SCHILY.xattr.user.scope", b"local"));
    local.extend_from_slice(&pax_record_bytes("SCHILY.xattr.user.dupe", b"first"));
    local.extend_from_slice(&pax_record_bytes("SCHILY.xattr.user.dupe", b"second"));
    append_entry(
        &mut archive,
        build_header(
            b"././@PaxHeader",
            b'x',
            local.len() as u64,
            HeaderFlavor::Ustar,
        ),
        &local,
    );

    append_entry(
        &mut archive,
        build_header(b"header-name.txt", b'0', 0, HeaderFlavor::Ustar),
        &[],
    );
    append_entry(
        &mut archive,
        build_header(b"second.txt", b'0', 0, HeaderFlavor::Ustar),
        &[],
    );
    append_eof(&mut archive);

    let entries = collect_entries(Cursor::new(archive))?;
    assert_eq!(entries.len(), 2);
    assert_eq!(entries[0].path, "local-name.txt");
    assert_eq!(
        entries[0].attrs,
        vec![
            ("user.dupe".to_string(), b"second".to_vec()),
            ("user.scope".to_string(), b"local".to_vec()),
        ]
    );
    assert_eq!(entries[1].path, "second.txt");
    assert_eq!(
        entries[1].attrs,
        vec![("user.scope".to_string(), b"global".to_vec())]
    );
    Ok(())
}

fn attr(name: &str, value: &[u8]) -> (String, Vec<u8>) {
    (name.to_string(), value.to_vec())
}

#[test]
fn reader_gives_attrs_sorted_by_name() -> Result<()> {
    let mut archive = Vec::new();
    append_pax_entry(
        &mut archive,
        b'g',
        &pax_record_bytes("SCHILY.xattr.user.bb", b"g"),
    );
    let mut local = Vec::new();
    for name in ["user.c", "user.a", "user.b"] {
        local.extend_from_slice(&pax_record_bytes(&format!("SCHILY.xattr.{name}"), b"l"));
    }
    append_pax_entry(&mut archive, b'x', &local);
    append_entry(
        &mut archive,
        build_header(b"file.txt", b'0', 0, HeaderFlavor::Ustar),
        &[],
    );
    append_eof(&mut archive);

    let entries = collect_entries(Cursor::new(archive))?;
    assert_eq!(
        entries[0].attrs,
        vec![
            attr("user.a", b"l"),
            attr("user.b", b"l"),
            attr("user.bb", b"g"),
            attr("user.c", b"l"),
        ]
    );
    Ok(())
}

#[test]
fn reader_applies_later_global_pax_values() -> Result<()> {
    let mut archive = Vec::new();
    let mut first = Vec::new();
    first.extend_from_slice(&pax_record_bytes("SCHILY.xattr.user.a", b"1"));
    first.extend_from_slice(&pax_record_bytes("SCHILY.xattr.user.b", b"1"));
    append_pax_entry(&mut archive, b'g', &first);
    append_entry(
        &mut archive,
        build_header(b"first.txt", b'0', 0, HeaderFlavor::Ustar),
        &[],
    );
    append_pax_entry(
        &mut archive,
        b'g',
        &pax_record_bytes("SCHILY.xattr.user.a", b"2"),
    );
    append_entry(
        &mut archive,
        build_header(b"second.txt", b'0', 0, HeaderFlavor::Ustar),
        &[],
    );
    append_eof(&mut archive);

    let entries = collect_entries(Cursor::new(archive))?;
    assert_eq!(
        entries[0].attrs,
        vec![attr("user.a", b"1"), attr("user.b", b"1")]
    );
    assert_eq!(
        entries[1].attrs,
        vec![attr("user.a", b"2"), attr("user.b", b"1")]
    );
    Ok(())
}

#[test]
fn reader_deletes_global_pax_value_with_empty_value() -> Result<()> {
    block_on_test(async {
        let mut archive = Vec::new();
        append_pax_entry(&mut archive, b'g', &pax_record_bytes("mtime", b"7"));
        append_pax_entry(&mut archive, b'g', &pax_record_bytes("mtime", b""));
        let mut header = build_header(b"file.txt", b'0', 0, HeaderFlavor::Ustar);
        write_octal(3, &mut header[MTIME_START..MTIME_START + 12]);
        finalize_checksum(&mut header);
        append_entry(&mut archive, header, &[]);
        append_eof(&mut archive);

        let mut stream = Box::pin(TarReader::new(Cursor::new(archive)));
        match stream.next().await.unwrap()? {
            TarEntry::File(file) => assert_eq!(file.mtime(), ts(3, 0)),
            other => panic!("expected file entry, got {:?}", other),
        }
        Ok(())
    })
}

#[test]
fn reader_keeps_empty_xattr_values() -> Result<()> {
    let mut archive = Vec::new();
    let mut global = Vec::new();
    global.extend_from_slice(&pax_record_bytes("SCHILY.xattr.user.g", b""));
    global.extend_from_slice(&pax_record_bytes("SCHILY.xattr.user.over", b"global"));
    append_pax_entry(&mut archive, b'g', &global);
    let mut local = Vec::new();
    local.extend_from_slice(&pax_record_bytes("SCHILY.xattr.user.l", b""));
    local.extend_from_slice(&pax_record_bytes("SCHILY.xattr.user.over", b""));
    append_pax_entry(&mut archive, b'x', &local);
    append_entry(
        &mut archive,
        build_header(b"file.txt", b'0', 0, HeaderFlavor::Ustar),
        &[],
    );
    append_eof(&mut archive);

    let entries = collect_entries(Cursor::new(archive))?;
    assert_eq!(
        entries[0].attrs,
        vec![
            attr("user.g", b""),
            attr("user.l", b""),
            attr("user.over", b""),
        ]
    );
    Ok(())
}

#[test]
fn reader_ignores_global_pax_path_linkpath_and_size() -> Result<()> {
    let mut archive = Vec::new();
    let mut global = Vec::new();
    global.extend_from_slice(&pax_record_bytes("path", b"global-name"));
    global.extend_from_slice(&pax_record_bytes("linkpath", b"global-target"));
    global.extend_from_slice(&pax_record_bytes("size", b"5"));
    append_pax_entry(&mut archive, b'g', &global);
    let mut link = build_header(b"link", b'2', 0, HeaderFlavor::Ustar);
    set_bytes(&mut link, LINKNAME_START, 100, b"target");
    finalize_checksum(&mut link);
    append_entry(&mut archive, link, &[]);
    append_entry(
        &mut archive,
        build_header(b"file.txt", b'0', 2, HeaderFlavor::Ustar),
        b"hi",
    );
    append_eof(&mut archive);

    let entries = collect_entries(Cursor::new(archive))?;
    assert_eq!(entries.len(), 2);
    assert_eq!(entries[0].kind, "symlink");
    assert_eq!(entries[0].path, "link");
    assert_eq!(entries[0].link.as_deref(), Some("target"));
    assert_eq!(entries[1].path, "file.txt");
    assert_eq!(entries[1].data, b"hi");
    Ok(())
}

#[test]
fn reader_merges_local_pax_headers() -> Result<()> {
    let mut archive = Vec::new();
    let mut first = Vec::new();
    first.extend_from_slice(&pax_record_bytes("path", b"first.txt"));
    first.extend_from_slice(&pax_record_bytes("SCHILY.xattr.user.a", b"1"));
    first.extend_from_slice(&pax_record_bytes("SCHILY.xattr.user.b", b"1"));
    append_pax_entry(&mut archive, b'x', &first);
    append_pax_entry(
        &mut archive,
        b'x',
        &pax_record_bytes("SCHILY.xattr.user.a", b"2"),
    );
    append_entry(
        &mut archive,
        build_header(b"file.txt", b'0', 0, HeaderFlavor::Ustar),
        &[],
    );
    append_eof(&mut archive);

    let entries = collect_entries(Cursor::new(archive))?;
    assert_eq!(entries[0].path, "first.txt");
    assert_eq!(
        entries[0].attrs,
        vec![attr("user.a", b"2"), attr("user.b", b"1")]
    );
    Ok(())
}

#[test]
fn reader_rejects_large_global_pax_records() {
    let mut archive = Vec::new();
    let value = vec![b'v'; PAX_HEADER_MAX_SIZE / 2];
    for name in ["user.a", "user.b", "user.c"] {
        append_pax_entry(
            &mut archive,
            b'g',
            &pax_record_bytes(&format!("SCHILY.xattr.{name}"), &value),
        );
    }
    append_entry(
        &mut archive,
        build_header(b"file.txt", b'0', 0, HeaderFlavor::Ustar),
        &[],
    );
    append_eof(&mut archive);

    assert_archive_error(
        Cursor::new(archive),
        ErrorKind::InvalidData,
        "global PAX records exceed",
    );
}

#[test]
fn reader_ends_after_large_malformed_pax_extension() {
    for typeflag in [b'x', b'g'] {
        let mut archive = Vec::new();
        append_pax_entry(&mut archive, typeflag, &[b'a'; 600]);
        append_entry(
            &mut archive,
            build_header(b"file.txt", b'0', 0, HeaderFlavor::Ustar),
            &[],
        );
        append_eof(&mut archive);
        block_on_test(async {
            let mut stream = Box::pin(TarReader::new(Cursor::new(archive)));
            let err = match stream.next().await {
                Some(Err(err)) => err,
                other => panic!("expected PAX error, got {:?}", other),
            };
            assert_eq!(err.kind(), ErrorKind::InvalidData);
            assert!(
                err.to_string().contains("missing length separator"),
                "{err}"
            );
            assert!(stream.next().await.is_none());
        });
    }
}

#[test]
fn reader_ends_after_large_global_pax_records() {
    let mut archive = Vec::new();
    let value = vec![b'v'; PAX_HEADER_MAX_SIZE / 2];
    for name in ["user.a", "user.b", "user.c"] {
        append_pax_entry(
            &mut archive,
            b'g',
            &pax_record_bytes(&format!("SCHILY.xattr.{name}"), &value),
        );
    }
    append_entry(
        &mut archive,
        build_header(b"file.txt", b'0', 0, HeaderFlavor::Ustar),
        &[],
    );
    append_eof(&mut archive);
    block_on_test(async {
        let mut stream = Box::pin(TarReader::new(Cursor::new(archive)));
        let err = match stream.next().await {
            Some(Err(err)) => err,
            other => panic!("expected global PAX error, got {:?}", other),
        };
        assert_eq!(err.kind(), ErrorKind::InvalidData);
        assert!(
            err.to_string().contains("global PAX records exceed"),
            "{err}"
        );
        assert!(stream.next().await.is_none());
    });
}

#[test]
fn reader_applies_extended_pax_metadata() -> Result<()> {
    block_on_test(async {
        let mut archive = Vec::new();
        let mut local = Vec::new();
        local.extend_from_slice(&pax_record_bytes("mtime", b"7.5"));
        local.extend_from_slice(&pax_record_bytes("atime", b"1.25"));
        local.extend_from_slice(&pax_record_bytes("ctime", b"-2.75"));
        local.extend_from_slice(&pax_record_bytes("size", b"5"));
        local.extend_from_slice(&pax_record_bytes("uid", b"123"));
        local.extend_from_slice(&pax_record_bytes("gid", b"456"));
        append_entry(
            &mut archive,
            build_header(
                b"././@PaxHeader",
                b'x',
                local.len() as u64,
                HeaderFlavor::Ustar,
            ),
            &local,
        );
        append_entry(
            &mut archive,
            build_header(b"file.txt", b'0', 0, HeaderFlavor::Ustar),
            b"hello",
        );
        append_eof(&mut archive);

        let mut stream = Box::pin(TarReader::new(Cursor::new(archive)));
        match stream.next().await.unwrap()? {
            TarEntry::File(mut file) => {
                let mut data = Vec::new();
                file.read_to_end(&mut data).await?;
                assert_eq!(file.path(), "file.txt");
                assert_eq!(file.size(), 5);
                assert_eq!(file.uid(), 123);
                assert_eq!(file.gid(), 456);
                assert_eq!(file.mtime(), ts(7, 500_000_000));
                assert_eq!(file.atime(), Some(ts(1, 250_000_000)));
                assert_eq!(file.ctime(), Some(ts(-2, 750_000_000)));
                assert_eq!(data, b"hello");
            }
            other => panic!("expected file entry, got {:?}", other),
        }
        assert!(stream.next().await.is_none());
        Ok(())
    })
}

#[test]
fn reader_does_not_surface_smuggled_entries_from_pax_size_override() -> Result<()> {
    block_on_test(async {
        let (fake_header, archive) = cve_2025_62518_archive();
        let mut stream = Box::pin(TarReader::new(Cursor::new(archive)));

        match stream.next().await.unwrap()? {
            TarEntry::File(mut file) => {
                let mut data = Vec::new();
                file.read_to_end(&mut data).await?;
                assert_eq!(file.path(), "normal.txt");
                assert_eq!(data, b"normal");
            }
            other => panic!("expected leading file, got {:?}", other),
        }

        match stream.next().await.unwrap()? {
            TarEntry::File(mut file) => {
                let mut data = Vec::new();
                file.read_to_end(&mut data).await?;
                assert_eq!(file.path(), "blob.bin");
                assert_eq!(file.size(), 1024);
                assert_eq!(data.len(), 1024);
                assert_eq!(&data[..BLOCK_SIZE], &fake_header);
                assert_eq!(&data[BLOCK_SIZE..BLOCK_SIZE + 4], b"EVIL");
            }
            other => panic!("expected CVE regression file, got {:?}", other),
        }

        match stream.next().await.unwrap()? {
            TarEntry::File(mut file) => {
                let mut data = Vec::new();
                file.read_to_end(&mut data).await?;
                assert_eq!(file.path(), "marker.txt");
                assert_eq!(data, b"marker");
            }
            other => panic!("expected trailing marker file, got {:?}", other),
        }

        assert!(stream.next().await.is_none());
        Ok(())
    })
}

#[test]
fn dropping_pax_sized_file_skips_over_embedded_tar_headers() -> Result<()> {
    block_on_test(async {
        let (_, archive) = cve_2025_62518_archive();
        let reader = ChunkedReader::new(Cursor::new(archive), 17);
        let mut stream = Box::pin(TarReader::new(reader));

        match stream.next().await.unwrap()? {
            TarEntry::File(mut file) => {
                let mut data = Vec::new();
                file.read_to_end(&mut data).await?;
                assert_eq!(file.path(), "normal.txt");
                assert_eq!(data, b"normal");
            }
            other => panic!("expected leading file, got {:?}", other),
        }

        match stream.next().await.unwrap()? {
            TarEntry::File(mut file) => {
                assert_eq!(file.path(), "blob.bin");
                assert_eq!(file.size(), 1024);
                let mut byte = [0u8; 1];
                file.read_exact(&mut byte).await?;
            }
            other => panic!("expected CVE regression file, got {:?}", other),
        }

        match stream.next().await.unwrap()? {
            TarEntry::File(mut file) => {
                let mut data = Vec::new();
                file.read_to_end(&mut data).await?;
                assert_eq!(file.path(), "marker.txt");
                assert_eq!(data, b"marker");
            }
            other => panic!("expected trailing marker file, got {:?}", other),
        }

        assert!(stream.next().await.is_none());
        Ok(())
    })
}

#[test]
fn reader_skips_hardlink_data() -> Result<()> {
    let hidden = smuggled_entry();
    let archive = link_archive("hl", b'1', HeaderFlavor::Ustar, 1024, None, &hidden);
    assert_eq!(
        collect_entries(Cursor::new(archive.clone()))?,
        link_entries("hardlink", "hl")
    );
    assert_eq!(
        collect_entries(ChunkedReader::new(Cursor::new(archive), 17))?,
        link_entries("hardlink", "hl")
    );
    Ok(())
}

#[test]
fn reader_skips_hardlink_data_with_pax_size() -> Result<()> {
    let hidden = smuggled_entry();
    for header_size in [0, 2048] {
        let archive = link_archive(
            "hl",
            b'1',
            HeaderFlavor::Ustar,
            header_size,
            Some(1024),
            &hidden,
        );
        assert_eq!(
            collect_entries(Cursor::new(archive))?,
            link_entries("hardlink", "hl")
        );
    }
    Ok(())
}

#[test]
fn reader_skips_hardlink_data_in_gnu_and_old_headers() -> Result<()> {
    let hidden = smuggled_entry();
    for flavor in [HeaderFlavor::Gnu, HeaderFlavor::Old] {
        let archive = link_archive("hl", b'1', flavor, 1024, None, &hidden);
        assert_eq!(
            collect_entries(Cursor::new(archive))?,
            link_entries("hardlink", "hl")
        );
    }
    Ok(())
}

#[test]
fn reader_skips_unaligned_hardlink_data_and_padding() -> Result<()> {
    let hidden = smuggled_entry();
    let archive = link_archive("hl", b'1', HeaderFlavor::Ustar, 513, None, &hidden[..513]);
    assert_eq!(
        collect_entries(Cursor::new(archive))?,
        link_entries("hardlink", "hl")
    );
    Ok(())
}

#[test]
fn reader_rejects_symlink_with_size() {
    let hidden = smuggled_entry();
    for flavor in [HeaderFlavor::Ustar, HeaderFlavor::Gnu, HeaderFlavor::Old] {
        let archive = link_archive("sl", b'2', flavor, 1024, None, &hidden);
        assert_archive_error(
            Cursor::new(archive.as_slice()),
            ErrorKind::InvalidData,
            "symlink entry has a non-zero size",
        );
        assert_archive_error(
            ChunkedReader::new(Cursor::new(archive), 17),
            ErrorKind::InvalidData,
            "symlink entry has a non-zero size",
        );
    }
}

#[test]
fn reader_rejects_symlink_with_pax_size() {
    let hidden = smuggled_entry();
    let archive = link_archive("sl", b'2', HeaderFlavor::Ustar, 0, Some(1024), &hidden);
    assert_archive_error(
        Cursor::new(archive),
        ErrorKind::InvalidData,
        "symlink entry has a non-zero size",
    );
}

#[test]
fn reader_reads_symlink_with_zero_size() -> Result<()> {
    for pax_size in [None, Some(0)] {
        let archive = link_archive("sl", b'2', HeaderFlavor::Ustar, 0, pax_size, &[]);
        assert_eq!(
            collect_entries(Cursor::new(archive))?,
            link_entries("symlink", "sl")
        );
    }
    Ok(())
}

#[test]
fn reader_prefers_pax_size_for_symlink() -> Result<()> {
    let archive = link_archive("sl", b'2', HeaderFlavor::Ustar, 1024, Some(0), &[]);
    assert_eq!(
        &archive[4 * BLOCK_SIZE + SIZE_START..][..12],
        b"00000002000\0"
    );
    assert_eq!(
        collect_entries(Cursor::new(archive))?,
        link_entries("symlink", "sl")
    );
    Ok(())
}

#[test]
fn reader_rejects_symlink_with_space_padded_size() {
    let hidden = smuggled_entry();
    let mut archive = link_archive("sl", b'2', HeaderFlavor::Ustar, 1024, None, &hidden);
    let header: &mut [u8; BLOCK_SIZE] = (&mut archive[2 * BLOCK_SIZE..3 * BLOCK_SIZE])
        .try_into()
        .unwrap();
    assert_eq!(&header[SIZE_START..][..12], b"00000002000\0");
    set_bytes(header, SIZE_START, 12, b"    0002000\0");
    finalize_checksum(header);
    assert_archive_error(
        Cursor::new(archive.as_slice()),
        ErrorKind::InvalidData,
        "symlink entry has a non-zero size",
    );
}

#[test]
fn reader_returns_no_entry_after_symlink_size_error() {
    let hidden = smuggled_entry();
    let archive = link_archive("sl", b'2', HeaderFlavor::Ustar, 1024, None, &hidden);
    block_on_test(async {
        let mut stream = Box::pin(TarReader::new(Cursor::new(archive.as_slice())));
        match stream.next().await {
            Some(Ok(entry)) => assert_eq!(entry.path(), "visible0.txt"),
            other => panic!("expected visible0.txt, got {:?}", other),
        }
        let err = match stream.next().await {
            Some(Err(err)) => err,
            other => panic!("expected symlink error, got {:?}", other),
        };
        assert_eq!(err.kind(), ErrorKind::InvalidData);
        assert!(
            err.to_string()
                .contains("symlink entry has a non-zero size"),
            "{err}"
        );
        for _ in 0..5 {
            match stream.next().await {
                None => break,
                Some(Ok(entry)) => panic!("unexpected entry {} after error", entry.path()),
                Some(Err(_)) => {}
            }
        }
    });
}

#[test]
fn reader_returns_no_entry_after_directory_mode_error() {
    let mut archive = Vec::new();
    let mut header = build_header(b"dir/", b'5', 0, HeaderFlavor::Ustar);
    set_bytes(&mut header, MODE_START, 8, b"zzzzzzz");
    finalize_checksum(&mut header);
    append_entry(&mut archive, header, &[]);
    append_entry(
        &mut archive,
        build_header(b"file.txt", b'0', 0, HeaderFlavor::Ustar),
        &[],
    );
    append_eof(&mut archive);
    block_on_test(async {
        let mut stream = Box::pin(TarReader::new(Cursor::new(archive)));
        let mut error = None;
        for _ in 0..5 {
            match stream.next().await {
                None => break,
                Some(Ok(entry)) if error.is_some() => {
                    panic!("unexpected entry {} after error", entry.path())
                }
                Some(Ok(entry)) => panic!("unexpected entry {} before error", entry.path()),
                Some(Err(err)) => {
                    error.get_or_insert(err);
                }
            }
        }
        let err = error.expect("expected mode error");
        assert_eq!(err.kind(), ErrorKind::InvalidData);
        assert!(stream.next().await.is_none());
    });
}

#[test]
fn reader_reads_extension_again_after_source_error() {
    let path = vec![b'p'; 600];
    let mut archive = Vec::new();
    append_pax_entry(&mut archive, b'x', &pax_record_bytes("path", &path));
    append_entry(
        &mut archive,
        build_header(b"file.txt", b'0', 0, HeaderFlavor::Ustar),
        &[],
    );
    append_eof(&mut archive);
    // The first read gets the PAX header block. The second read gets the PAX
    // data blocks.
    let mut reader = FailingReader::new(Cursor::new(archive), 2);
    block_on_test(async {
        let mut stream = Box::pin(TarReader::new(&mut reader));
        let err = match stream.next().await {
            Some(Err(err)) => err,
            other => panic!("expected source error, got {:?}", other),
        };
        assert_eq!(err.kind(), ErrorKind::Other);
        assert!(err.to_string().contains("source failure"), "{err}");
        match stream.next().await {
            Some(Ok(entry)) => assert_eq!(entry.path().as_bytes(), path.as_slice()),
            other => panic!("expected file entry, got {:?}", other),
        }
        assert!(stream.next().await.is_none());
    });
    // Only the read of the PAX data blocks uses a buffer larger than one
    // block.
    assert!(reader.failed_len.unwrap() > BLOCK_SIZE);
}

#[test]
fn reader_continues_after_source_error_at_any_read() {
    // The archive has a file body with padding, a body that the reader skips
    // in more than one read, a PAX extension larger than one block, the end
    // marker and data after the end marker.
    let long_path = "p".repeat(600);
    let mut archive = Vec::new();
    append_entry(
        &mut archive,
        build_header(b"padded.txt", b'0', 100, HeaderFlavor::Ustar),
        &[b'a'; 100],
    );
    append_entry(
        &mut archive,
        build_header(b"skipped.bin", b'0', 70000, HeaderFlavor::Ustar),
        &[b'b'; 70000],
    );
    append_pax_entry(
        &mut archive,
        b'x',
        &pax_record_bytes("path", long_path.as_bytes()),
    );
    append_entry(
        &mut archive,
        build_header(b"file.txt", b'0', 0, HeaderFlavor::Ustar),
        &[],
    );
    append_eof(&mut archive);
    archive.extend_from_slice(&[b'z'; 700]);

    // Read all entries and the body of "padded.txt". Poll again after each
    // source error. Give the entry paths and the number of source errors.
    async fn read_all(reader: &mut FailingReader<Cursor<Vec<u8>>>) -> (Vec<String>, usize) {
        let mut stream = Box::pin(TarReader::new(reader));
        let (mut paths, mut errors) = (Vec::new(), 0);
        while let Some(item) = stream.next().await {
            let entry = match item {
                Ok(entry) => entry,
                Err(err) if err.kind() == ErrorKind::Other => {
                    errors += 1;
                    continue;
                }
                Err(err) => panic!("unexpected error {err}"),
            };
            paths.push(entry.path().to_string());
            if let TarEntry::File(mut file) = entry {
                if file.path() == "padded.txt" {
                    let (mut data, mut buf) = (Vec::new(), [0u8; 64]);
                    loop {
                        match file.read(&mut buf).await {
                            Ok(0) => break,
                            Ok(n) => data.extend_from_slice(&buf[..n]),
                            Err(err) if err.kind() == ErrorKind::Other => errors += 1,
                            Err(err) => panic!("unexpected body error {err}"),
                        }
                    }
                    assert_eq!(data, [b'a'; 100]);
                }
            }
        }
        assert!(stream.next().await.is_none());
        (paths, errors)
    }

    let mut reader = FailingReader::new(Cursor::new(archive.clone()), 0);
    let (expected, errors) = block_on_test(read_all(&mut reader));
    assert_eq!(errors, 0);
    assert_eq!(expected, ["padded.txt", "skipped.bin", long_path.as_str()]);
    for fail_at in 1..=reader.reads {
        let mut reader = FailingReader::new(Cursor::new(archive.clone()), fail_at);
        let (paths, errors) = block_on_test(read_all(&mut reader));
        assert_eq!(paths, expected, "fail at read {fail_at}");
        assert_eq!(errors, 1, "fail at read {fail_at}");
    }
}

#[test]
fn reader_reads_file_with_space_padded_size() -> Result<()> {
    let hidden = smuggled_entry();
    let mut archive = Vec::new();
    let mut header = build_header(b"data.bin", b'0', 0, HeaderFlavor::Ustar);
    set_bytes(&mut header, SIZE_START, 12, b"    0002000\0");
    finalize_checksum(&mut header);
    append_entry(&mut archive, header, &hidden);
    append_entry(
        &mut archive,
        build_header(b"visible1.txt", b'0', 7, HeaderFlavor::Ustar),
        b"second\n",
    );
    append_eof(&mut archive);
    assert_eq!(
        collect_entries(Cursor::new(archive.as_slice()))?,
        vec![
            ObservedEntry {
                kind: "file",
                path: "data.bin".to_string(),
                link: None,
                data: hidden,
                attrs: Vec::new(),
            },
            ObservedEntry {
                kind: "file",
                path: "visible1.txt".to_string(),
                link: None,
                data: b"second\n".to_vec(),
                attrs: Vec::new(),
            },
        ]
    );
    Ok(())
}

#[test]
fn reader_rejects_invalid_pax_time_metadata() {
    for (value, expected) in [
        ("1.".to_string(), "invalid PAX timestamp value"),
        ("1.x".to_string(), "invalid PAX timestamp value"),
        (
            "1.0000000001".to_string(),
            "PAX timestamp exceeds nanosecond precision",
        ),
        ("abc".to_string(), "invalid PAX timestamp value"),
        (u64::MAX.to_string(), "invalid PAX timestamp value"),
    ] {
        let mut archive = Vec::new();
        let payload = pax_record_bytes("mtime", value.as_bytes());
        append_pax_entry(&mut archive, b'x', &payload);
        append_entry(
            &mut archive,
            build_header(b"file.txt", b'0', 0, HeaderFlavor::Ustar),
            &[],
        );
        append_eof(&mut archive);

        assert_archive_error(Cursor::new(archive), ErrorKind::InvalidData, expected);
    }
}

#[test]
fn reader_reads_ustar_header_symbolic_names() -> Result<()> {
    block_on_test(async {
        let mut archive = Vec::new();

        let mut file = build_header(b"file.txt", b'0', 0, HeaderFlavor::Ustar);
        set_bytes(&mut file, UNAME_START, 32, b"builder");
        set_bytes(&mut file, GNAME_START, 32, b"staff");
        finalize_checksum(&mut file);
        append_entry(&mut archive, file, b"");

        let mut dir = build_header(b"dir/", b'5', 0, HeaderFlavor::Ustar);
        set_bytes(&mut dir, UNAME_START, 32, b"daemon");
        set_bytes(&mut dir, GNAME_START, 32, b"wheel");
        finalize_checksum(&mut dir);
        append_entry(&mut archive, dir, b"");

        append_eof(&mut archive);

        let mut stream = Box::pin(TarReader::new(Cursor::new(archive)));
        match stream.next().await.unwrap()? {
            TarEntry::File(file) => {
                assert_eq!(file.uname(), "builder");
                assert_eq!(file.gname(), "staff");
            }
            other => panic!("expected file entry, got {:?}", other),
        }
        match stream.next().await.unwrap()? {
            TarEntry::Directory(dir) => {
                assert_eq!(dir.uname(), "daemon");
                assert_eq!(dir.gname(), "wheel");
            }
            other => panic!("expected directory entry, got {:?}", other),
        }
        assert!(stream.next().await.is_none());
        Ok(())
    })
}

#[test]
fn reader_reads_gnu_header_symbolic_names() -> Result<()> {
    block_on_test(async {
        let mut archive = Vec::new();
        let mut header = build_header(b"file.txt", b'0', 0, HeaderFlavor::Gnu);
        set_bytes(&mut header, UNAME_START, 32, b"gnu-user");
        set_bytes(&mut header, GNAME_START, 32, b"gnu-group");
        finalize_checksum(&mut header);
        append_entry(&mut archive, header, b"");
        append_eof(&mut archive);

        let mut stream = Box::pin(TarReader::new(Cursor::new(archive)));
        match stream.next().await.unwrap()? {
            TarEntry::File(file) => {
                assert_eq!(file.uname(), "gnu-user");
                assert_eq!(file.gname(), "gnu-group");
            }
            other => panic!("expected file entry, got {:?}", other),
        }
        assert!(stream.next().await.is_none());
        Ok(())
    })
}

#[test]
fn reader_applies_pax_symbolic_names() -> Result<()> {
    block_on_test(async {
        let mut archive = Vec::new();
        let mut local = Vec::new();
        local.extend_from_slice(&pax_record_bytes("uname", b"pax-user"));
        local.extend_from_slice(&pax_record_bytes("gname", b"pax-group"));
        append_entry(
            &mut archive,
            build_header(
                b"././@PaxHeader",
                b'x',
                local.len() as u64,
                HeaderFlavor::Ustar,
            ),
            &local,
        );

        let mut header = build_header(b"file.txt", b'0', 0, HeaderFlavor::Ustar);
        set_bytes(&mut header, UNAME_START, 32, b"header-user");
        set_bytes(&mut header, GNAME_START, 32, b"header-group");
        finalize_checksum(&mut header);
        append_entry(&mut archive, header, b"");
        append_eof(&mut archive);

        let mut stream = Box::pin(TarReader::new(Cursor::new(archive)));
        match stream.next().await.unwrap()? {
            TarEntry::File(file) => {
                assert_eq!(file.uname(), "pax-user");
                assert_eq!(file.gname(), "pax-group");
            }
            other => panic!("expected file entry, got {:?}", other),
        }
        assert!(stream.next().await.is_none());
        Ok(())
    })
}

#[test]
fn reader_applies_global_and_local_pax_symbolic_name_precedence() -> Result<()> {
    block_on_test(async {
        let mut archive = Vec::new();
        let mut global = Vec::new();
        global.extend_from_slice(&pax_record_bytes("uname", b"global-user"));
        global.extend_from_slice(&pax_record_bytes("gname", b"global-group"));
        append_entry(
            &mut archive,
            build_header(
                b"././@PaxHeader",
                b'g',
                global.len() as u64,
                HeaderFlavor::Ustar,
            ),
            &global,
        );

        let mut local = Vec::new();
        local.extend_from_slice(&pax_record_bytes("uname", b"local-user"));
        local.extend_from_slice(&pax_record_bytes("gname", b"local-group"));
        append_entry(
            &mut archive,
            build_header(
                b"././@PaxHeader",
                b'x',
                local.len() as u64,
                HeaderFlavor::Ustar,
            ),
            &local,
        );
        append_entry(
            &mut archive,
            build_header(b"local.txt", b'0', 0, HeaderFlavor::Ustar),
            b"",
        );
        append_entry(
            &mut archive,
            build_header(b"global.txt", b'0', 0, HeaderFlavor::Ustar),
            b"",
        );
        append_eof(&mut archive);

        let mut stream = Box::pin(TarReader::new(Cursor::new(archive)));
        match stream.next().await.unwrap()? {
            TarEntry::File(file) => {
                assert_eq!(file.path(), "local.txt");
                assert_eq!(file.uname(), "local-user");
                assert_eq!(file.gname(), "local-group");
            }
            other => panic!("expected file entry, got {:?}", other),
        }
        match stream.next().await.unwrap()? {
            TarEntry::File(file) => {
                assert_eq!(file.path(), "global.txt");
                assert_eq!(file.uname(), "global-user");
                assert_eq!(file.gname(), "global-group");
            }
            other => panic!("expected file entry, got {:?}", other),
        }
        assert!(stream.next().await.is_none());
        Ok(())
    })
}

#[test]
fn reader_reads_gnu_header_times() -> Result<()> {
    block_on_test(async {
        let mut archive = Vec::new();
        let mut header = build_header(b"file.txt", b'0', 0, HeaderFlavor::Gnu);
        write_octal(7, &mut header[MTIME_START..MTIME_START + 12]);
        write_octal(3, &mut header[GNU_ATIME_START..GNU_ATIME_START + 12]);
        write_octal(5, &mut header[GNU_CTIME_START..GNU_CTIME_START + 12]);
        finalize_checksum(&mut header);
        append_entry(&mut archive, header, b"");
        append_eof(&mut archive);

        let mut stream = Box::pin(TarReader::new(Cursor::new(archive)));
        match stream.next().await.unwrap()? {
            TarEntry::File(file) => {
                assert_eq!(file.mtime(), ts(7, 0));
                assert_eq!(file.atime(), Some(ts(3, 0)));
                assert_eq!(file.ctime(), Some(ts(5, 0)));
            }
            other => panic!("expected file entry, got {:?}", other),
        }
        assert!(stream.next().await.is_none());
        Ok(())
    })
}

#[test]
fn reader_ignores_zero_gnu_header_times() -> Result<()> {
    block_on_test(async {
        let mut archive = Vec::new();
        append_entry(
            &mut archive,
            build_header(b"file.txt", b'0', 0, HeaderFlavor::Gnu),
            b"",
        );
        append_eof(&mut archive);

        let mut stream = Box::pin(TarReader::new(Cursor::new(archive)));
        match stream.next().await.unwrap()? {
            TarEntry::File(file) => {
                assert_eq!(file.mtime(), ts(0, 0));
                assert_eq!(file.atime(), None);
                assert_eq!(file.ctime(), None);
            }
            other => panic!("expected file entry, got {:?}", other),
        }
        assert!(stream.next().await.is_none());
        Ok(())
    })
}

#[test]
fn reader_pax_times_override_gnu_header_times() -> Result<()> {
    block_on_test(async {
        let mut archive = Vec::new();
        let mut local = Vec::new();
        local.extend_from_slice(&pax_record_bytes("mtime", b"7.5"));
        local.extend_from_slice(&pax_record_bytes("atime", b"1.25"));
        local.extend_from_slice(&pax_record_bytes("ctime", b"-2"));
        append_entry(
            &mut archive,
            build_header(
                b"././@PaxHeader",
                b'x',
                local.len() as u64,
                HeaderFlavor::Ustar,
            ),
            &local,
        );

        let mut header = build_header(b"file.txt", b'0', 0, HeaderFlavor::Gnu);
        write_octal(3, &mut header[MTIME_START..MTIME_START + 12]);
        write_octal(4, &mut header[GNU_ATIME_START..GNU_ATIME_START + 12]);
        write_octal(5, &mut header[GNU_CTIME_START..GNU_CTIME_START + 12]);
        finalize_checksum(&mut header);
        append_entry(&mut archive, header, b"");
        append_eof(&mut archive);

        let mut stream = Box::pin(TarReader::new(Cursor::new(archive)));
        match stream.next().await.unwrap()? {
            TarEntry::File(file) => {
                assert_eq!(file.mtime(), ts(7, 500_000_000));
                assert_eq!(file.atime(), Some(ts(1, 250_000_000)));
                assert_eq!(file.ctime(), Some(ts(-2, 0)));
            }
            other => panic!("expected file entry, got {:?}", other),
        }
        assert!(stream.next().await.is_none());
        Ok(())
    })
}

#[test]
fn reader_pax_times_override_unparsable_gnu_header_times() -> Result<()> {
    block_on_test(async {
        let mut archive = Vec::new();
        let mut local = Vec::new();
        local.extend_from_slice(&pax_record_bytes("atime", b"1.25"));
        local.extend_from_slice(&pax_record_bytes("ctime", b"-2"));
        append_pax_entry(&mut archive, b'x', &local);

        let mut header = build_header(b"file.txt", b'0', 0, HeaderFlavor::Gnu);
        write_octal(3, &mut header[MTIME_START..MTIME_START + 12]);
        set_bytes(&mut header, GNU_ATIME_START, 12, b"zzzzzzzzzzz\0");
        set_bytes(&mut header, GNU_CTIME_START, 12, b"zzzzzzzzzzz\0");
        finalize_checksum(&mut header);
        append_entry(&mut archive, header, b"");
        append_eof(&mut archive);

        let mut stream = Box::pin(TarReader::new(Cursor::new(archive)));
        match stream.next().await.unwrap()? {
            TarEntry::File(file) => {
                assert_eq!(file.mtime(), ts(3, 0));
                assert_eq!(file.atime(), Some(ts(1, 250_000_000)));
                assert_eq!(file.ctime(), Some(ts(-2, 0)));
            }
            other => panic!("expected file entry, got {:?}", other),
        }
        assert!(stream.next().await.is_none());
        Ok(())
    })
}

#[test]
fn reader_ignores_hardlink_xattrs() -> Result<()> {
    block_on_test(async {
        let mut archive = Vec::new();
        let mut local = Vec::new();
        local.extend_from_slice(&pax_record_bytes("SCHILY.xattr.user.hard", b"ignored"));
        append_entry(
            &mut archive,
            build_header(
                b"././@PaxHeader",
                b'x',
                local.len() as u64,
                HeaderFlavor::Ustar,
            ),
            &local,
        );

        let mut header = build_header(b"hard", b'1', 0, HeaderFlavor::Ustar);
        set_bytes(&mut header, 157, 100, b"target");
        finalize_checksum(&mut header);
        append_entry(&mut archive, header, &[]);
        append_eof(&mut archive);

        let mut stream = Box::pin(TarReader::new(Cursor::new(archive)));
        match stream.next().await.unwrap()? {
            TarEntry::Link(link) => {
                assert_eq!(link.path(), "hard");
                assert_eq!(link.link(), "target");
            }
            other => panic!("expected hard-link entry, got {:?}", other),
        }
        assert!(stream.next().await.is_none());
        Ok(())
    })
}

#[test]
fn reader_ignores_hardlink_symbolic_names() -> Result<()> {
    block_on_test(async {
        let mut archive = Vec::new();
        let mut local = Vec::new();
        local.extend_from_slice(&pax_record_bytes("uname", b"ignored-user"));
        local.extend_from_slice(&pax_record_bytes("gname", b"ignored-group"));
        append_entry(
            &mut archive,
            build_header(
                b"././@PaxHeader",
                b'x',
                local.len() as u64,
                HeaderFlavor::Ustar,
            ),
            &local,
        );

        let mut header = build_header(b"hard", b'1', 0, HeaderFlavor::Ustar);
        set_bytes(&mut header, 157, 100, b"target");
        set_bytes(&mut header, UNAME_START, 32, &[0xff]);
        set_bytes(&mut header, GNAME_START, 32, &[0xfe]);
        finalize_checksum(&mut header);
        append_entry(&mut archive, header, &[]);
        append_eof(&mut archive);

        let mut stream = Box::pin(TarReader::new(Cursor::new(archive)));
        match stream.next().await.unwrap()? {
            TarEntry::Link(link) => {
                assert_eq!(link.path(), "hard");
                assert_eq!(link.link(), "target");
            }
            other => panic!("expected hard-link entry, got {:?}", other),
        }
        assert!(stream.next().await.is_none());
        Ok(())
    })
}

#[test]
fn reader_rejects_invalid_pax_numeric_metadata() {
    let mut archive = Vec::new();
    let payload = pax_record_bytes("uid", b"4294967296");
    append_entry(
        &mut archive,
        build_header(
            b"././@PaxHeader",
            b'x',
            payload.len() as u64,
            HeaderFlavor::Ustar,
        ),
        &payload,
    );
    append_entry(
        &mut archive,
        build_header(b"file.txt", b'0', 0, HeaderFlavor::Ustar),
        &[],
    );
    append_eof(&mut archive);

    assert_archive_error(
        Cursor::new(archive),
        ErrorKind::InvalidData,
        "PAX uid value out of range",
    );
}

#[test]
fn reader_rejects_pax_size_causing_position_overflow() {
    let mut archive = Vec::new();
    let payload = pax_record_bytes("size", u64::MAX.to_string().as_bytes());
    append_pax_entry(&mut archive, b'x', &payload);
    append_entry(
        &mut archive,
        build_header(b"overflow.bin", b'0', 0, HeaderFlavor::Ustar),
        &[],
    );
    append_eof(&mut archive);

    assert_archive_error(
        Cursor::new(archive),
        ErrorKind::InvalidData,
        "file size overflows archive position",
    );
}

#[test]
fn reader_rejects_pax_hardlink_size_causing_position_overflow() {
    let archive = link_archive("hl", b'1', HeaderFlavor::Ustar, 0, Some(u64::MAX), &[]);
    assert_archive_error(
        Cursor::new(archive),
        ErrorKind::InvalidData,
        "link size overflows archive position",
    );
}

#[test]
fn reader_rejects_invalid_header_numeric_fields() {
    for (field_start, field_len, typeflag, flavor, message) in [
        (
            UID_START,
            8,
            b'0',
            HeaderFlavor::Ustar,
            "invalid numeric field",
        ),
        (
            GID_START,
            8,
            b'0',
            HeaderFlavor::Ustar,
            "invalid numeric field",
        ),
        (
            MTIME_START,
            12,
            b'0',
            HeaderFlavor::Ustar,
            "invalid numeric field",
        ),
        (
            SIZE_START,
            12,
            b'0',
            HeaderFlavor::Ustar,
            "invalid numeric field",
        ),
        (
            GNU_ATIME_START,
            12,
            b'0',
            HeaderFlavor::Gnu,
            "invalid numeric field",
        ),
        (
            GNU_CTIME_START,
            12,
            b'0',
            HeaderFlavor::Gnu,
            "invalid numeric field",
        ),
        (329, 8, b'3', HeaderFlavor::Ustar, "invalid numeric field"),
        (337, 8, b'4', HeaderFlavor::Ustar, "invalid numeric field"),
        (
            SIZE_START,
            12,
            b'1',
            HeaderFlavor::Ustar,
            "invalid numeric field",
        ),
        (
            SIZE_START,
            12,
            b'2',
            HeaderFlavor::Ustar,
            "invalid numeric field",
        ),
    ] {
        let mut archive = Vec::new();
        let mut header = build_header(b"bad", typeflag, 0, flavor);
        set_bytes(&mut header, field_start, field_len, b"8");
        finalize_checksum(&mut header);
        append_entry(&mut archive, header, &[]);
        append_eof(&mut archive);
        assert_archive_error(Cursor::new(archive), ErrorKind::InvalidData, message);
    }

    let mut archive = Vec::new();
    let mut header = build_header(b"bad", b'0', 0, HeaderFlavor::Gnu);
    set_bytes(&mut header, CKSUM_START, 8, b"8");
    append_entry(&mut archive, header, &[]);
    append_eof(&mut archive);
    assert_archive_error(
        Cursor::new(archive),
        ErrorKind::InvalidData,
        "invalid tar header checksum field",
    );
}

#[test]
fn reader_reads_gnu_base256_numeric_fields() -> Result<()> {
    block_on_test(async {
        let mut archive = Vec::new();
        let mut header = build_header(b"big.bin", b'0', 0, HeaderFlavor::Gnu);
        set_bytes(
            &mut header,
            SIZE_START,
            12,
            &[0x80, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 5],
        );
        set_bytes(&mut header, UID_START, 8, &[0x80, 0, 0, 0, 0, 0x40, 0, 0]);
        set_bytes(
            &mut header,
            MTIME_START,
            12,
            &[0x80, 0, 0, 0, 0, 0, 0, 0x02, 0, 0, 0, 0],
        );
        finalize_checksum(&mut header);
        append_entry(&mut archive, header, b"hello");
        append_entry(
            &mut archive,
            build_header(b"next.txt", b'0', 4, HeaderFlavor::Gnu),
            b"next",
        );
        append_eof(&mut archive);

        let mut stream = Box::pin(TarReader::new(Cursor::new(archive)));
        match stream.next().await.unwrap()? {
            TarEntry::File(mut file) => {
                let mut data = Vec::new();
                file.read_to_end(&mut data).await?;
                assert_eq!(file.path(), "big.bin");
                assert_eq!(file.size(), 5);
                assert_eq!(file.uid(), 0x0040_0000);
                assert_eq!(file.mtime(), ts(1 << 33, 0));
                assert_eq!(data, b"hello");
            }
            other => panic!("expected file entry, got {:?}", other),
        }
        match stream.next().await.unwrap()? {
            TarEntry::File(mut file) => {
                let mut data = Vec::new();
                file.read_to_end(&mut data).await?;
                assert_eq!(file.path(), "next.txt");
                assert_eq!(data, b"next");
            }
            other => panic!("expected file entry, got {:?}", other),
        }
        assert!(stream.next().await.is_none());
        Ok(())
    })
}

#[test]
fn reader_pax_overrides_unparsable_header_numeric_fields() -> Result<()> {
    block_on_test(async {
        let mut archive = Vec::new();
        let mut local = Vec::new();
        local.extend_from_slice(&pax_record_bytes("size", b"5"));
        local.extend_from_slice(&pax_record_bytes("uid", b"123"));
        local.extend_from_slice(&pax_record_bytes("gid", b"456"));
        local.extend_from_slice(&pax_record_bytes("mtime", b"7"));
        append_pax_entry(&mut archive, b'x', &local);
        let mut header = build_header(b"file.txt", b'0', 0, HeaderFlavor::Ustar);
        set_bytes(&mut header, SIZE_START, 12, b"8");
        set_bytes(&mut header, UID_START, 8, b"8");
        set_bytes(&mut header, GID_START, 8, b"8");
        set_bytes(&mut header, MTIME_START, 12, b"8");
        finalize_checksum(&mut header);
        append_entry(&mut archive, header, b"hello");
        append_entry(
            &mut archive,
            build_header(b"next.txt", b'0', 4, HeaderFlavor::Ustar),
            b"next",
        );
        append_eof(&mut archive);

        let mut stream = Box::pin(TarReader::new(Cursor::new(archive)));
        match stream.next().await.unwrap()? {
            TarEntry::File(mut file) => {
                let mut data = Vec::new();
                file.read_to_end(&mut data).await?;
                assert_eq!(file.path(), "file.txt");
                assert_eq!(file.size(), 5);
                assert_eq!(file.uid(), 123);
                assert_eq!(file.gid(), 456);
                assert_eq!(file.mtime(), ts(7, 0));
                assert_eq!(data, b"hello");
            }
            other => panic!("expected file entry, got {:?}", other),
        }
        match stream.next().await.unwrap()? {
            TarEntry::File(mut file) => {
                let mut data = Vec::new();
                file.read_to_end(&mut data).await?;
                assert_eq!(file.path(), "next.txt");
                assert_eq!(data, b"next");
            }
            other => panic!("expected file entry, got {:?}", other),
        }
        assert!(stream.next().await.is_none());
        Ok(())
    })
}

#[test]
fn reader_rejects_negative_gnu_base256_numeric_fields() {
    for (start, len) in [(SIZE_START, 12), (UID_START, 8), (GID_START, 8)] {
        let mut archive = Vec::new();
        let mut header = build_header(b"bad", b'0', 0, HeaderFlavor::Gnu);
        set_bytes(&mut header, start, len, &[0xff; 12][..len]);
        finalize_checksum(&mut header);
        append_entry(&mut archive, header, b"");
        append_eof(&mut archive);
        assert_archive_error(
            Cursor::new(archive),
            ErrorKind::InvalidData,
            "invalid numeric field",
        );
    }
}

#[test]
fn reader_rejects_out_of_range_gnu_base256_ids() {
    for start in [UID_START, GID_START] {
        let mut archive = Vec::new();
        let mut header = build_header(b"bad", b'0', 0, HeaderFlavor::Gnu);
        set_bytes(&mut header, start, 8, &[0x80, 0, 0, 1, 0, 0, 0, 0]);
        finalize_checksum(&mut header);
        append_entry(&mut archive, header, b"");
        append_eof(&mut archive);
        assert_archive_error(Cursor::new(archive), ErrorKind::InvalidData, "out of range");
    }
}

#[test]
fn reader_reads_gnu_base256_times() -> Result<()> {
    block_on_test(async {
        let mut archive = Vec::new();
        let mut header = build_header(b"file.txt", b'0', 0, HeaderFlavor::Gnu);
        set_bytes(
            &mut header,
            GNU_ATIME_START,
            12,
            &[0x80, 0, 0, 0, 0, 0, 0, 0x02, 0, 0, 0, 0],
        );
        set_bytes(
            &mut header,
            GNU_CTIME_START,
            12,
            &[0x80, 0, 0, 0, 0, 0, 0, 0x03, 0, 0, 0, 0x01],
        );
        finalize_checksum(&mut header);
        append_entry(&mut archive, header, b"");
        append_eof(&mut archive);

        let mut stream = Box::pin(TarReader::new(Cursor::new(archive)));
        match stream.next().await.unwrap()? {
            TarEntry::File(file) => {
                assert_eq!(file.atime(), Some(ts(1 << 33, 0)));
                assert_eq!(file.ctime(), Some(ts((3 << 32) + 1, 0)));
            }
            other => panic!("expected file entry, got {:?}", other),
        }
        assert!(stream.next().await.is_none());
        Ok(())
    })
}

#[test]
fn reader_reads_gnu_base256_device_numbers() -> Result<()> {
    block_on_test(async {
        let mut archive = Vec::new();
        for (name, typeflag) in [(&b"char"[..], b'3'), (&b"block"[..], b'4')] {
            let mut header = build_header(name, typeflag, 0, HeaderFlavor::Gnu);
            set_bytes(&mut header, 329, 8, &[0x80, 0, 0, 0, 0x01, 0, 0, 0]);
            set_bytes(
                &mut header,
                337,
                8,
                &[0x80, 0, 0, 0, 0xff, 0xff, 0xff, 0xff],
            );
            finalize_checksum(&mut header);
            append_entry(&mut archive, header, b"");
        }
        append_eof(&mut archive);

        let mut stream = Box::pin(TarReader::new(Cursor::new(archive)));
        match stream.next().await.unwrap()? {
            TarEntry::Device(device) => {
                assert_eq!(device.path(), "char");
                assert!(device.is_char());
                assert_eq!(device.major(), 0x0100_0000);
                assert_eq!(device.minor(), u32::MAX);
            }
            other => panic!("expected device entry, got {:?}", other),
        }
        match stream.next().await.unwrap()? {
            TarEntry::Device(device) => {
                assert_eq!(device.path(), "block");
                assert!(device.is_block());
                assert_eq!(device.major(), 0x0100_0000);
                assert_eq!(device.minor(), u32::MAX);
            }
            other => panic!("expected device entry, got {:?}", other),
        }
        assert!(stream.next().await.is_none());
        Ok(())
    })
}

#[test]
fn reader_rejects_invalid_header_symbolic_name_utf8() {
    let mut archive = Vec::new();
    let mut header = build_header(b"file.txt", b'0', 0, HeaderFlavor::Ustar);
    set_bytes(&mut header, UNAME_START, 32, &[0xff]);
    finalize_checksum(&mut header);
    append_entry(&mut archive, header, &[]);
    append_eof(&mut archive);

    assert_archive_error(
        Cursor::new(archive),
        ErrorKind::InvalidData,
        "invalid UTF-8",
    );
}

#[test]
fn reader_treats_old_style_file_with_trailing_slash_as_directory() -> Result<()> {
    block_on_test(async {
        let mut archive = Vec::new();
        append_entry(
            &mut archive,
            build_header(b"legacy-dir/", b'0', 0, HeaderFlavor::Old),
            &[],
        );
        append_eof(&mut archive);

        let mut stream = Box::pin(TarReader::new(Cursor::new(archive)));
        match stream.next().await.unwrap()? {
            TarEntry::Directory(dir) => {
                assert_eq!(dir.path(), "legacy-dir/");
                assert_eq!(dir.size(), 0);
            }
            other => panic!("expected directory entry, got {:?}", other),
        }
        assert!(stream.next().await.is_none());
        Ok(())
    })
}

#[test]
fn reader_gnu_long_name_applies_only_to_next_entry() -> Result<()> {
    let long_name = format!("nested/{}/file.txt", "a".repeat(140));
    let mut archive = Vec::new();

    append_entry(
        &mut archive,
        build_header(
            b"././@LongLink",
            b'L',
            long_name.len() as u64 + 1,
            HeaderFlavor::Gnu,
        ),
        format!("{long_name}\0").as_bytes(),
    );
    append_entry(
        &mut archive,
        build_header(b"placeholder", b'0', 0, HeaderFlavor::Gnu),
        &[],
    );
    append_entry(
        &mut archive,
        build_header(b"plain.txt", b'0', 0, HeaderFlavor::Ustar),
        &[],
    );
    append_eof(&mut archive);

    let entries = collect_entries(Cursor::new(archive))?;
    assert_eq!(
        entries
            .iter()
            .map(|entry| entry.path.as_str())
            .collect::<Vec<_>>(),
        vec![long_name.as_str(), "plain.txt"]
    );
    Ok(())
}

#[test]
fn reader_handles_fragmented_reads() -> Result<()> {
    let archive = std::fs::read(fixture("simple.tar"))?;
    let entries = collect_entries(ChunkedReader::new(Cursor::new(archive), 7))?;
    assert_eq!(
        entries
            .iter()
            .map(|entry| (entry.kind, entry.path.as_str()))
            .collect::<Vec<_>>(),
        vec![
            ("dir", "root/"),
            ("file", "root/hello.txt"),
            ("symlink", "root/hello-link"),
            ("hardlink", "root/hello-hard"),
        ]
    );
    Ok(())
}

#[test]
fn writer_round_trip_with_partial_writes() -> Result<()> {
    block_on_test(async {
        let (writer_sink, shared) = ChunkedAsyncWriter::new(13);
        let mut writer = TarWriter::<'static, 'static, _, Cursor<&'static [u8]>>::new(writer_sink);

        writer
            .send(TarEntry::Directory(
                TarDirectory::new("dir/").with_mtime(ts(1, 0)),
            ))
            .await?;
        writer
            .send(TarEntry::File(
                TarRegularFile::new("dir/file.txt", 5, Cursor::new(b"hello".as_slice()))
                    .with_mtime(ts(2, 0)),
            ))
            .await?;
        writer.close().await?;

        let buffer = shared.lock().unwrap().clone();
        let entries = collect_entries_async(ChunkedReader::new(Cursor::new(buffer), 11)).await?;
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].path, "dir/");
        assert_eq!(entries[1].path, "dir/file.txt");
        assert_eq!(entries[1].data, b"hello");
        Ok(())
    })
}

#[test]
fn filtered_stream_can_feed_writer_sink_with_partial_writes() -> Result<()> {
    block_on_test(async {
        let (source_sink, source_bytes) = VecAsyncWriter::new();
        let mut source = TarWriter::<'static, 'static, _, Cursor<&'static [u8]>>::new(source_sink);

        source
            .send(TarEntry::Directory(
                TarDirectory::new("bin/").with_mtime(ts(1, 0)),
            ))
            .await?;
        source
            .send(TarEntry::File(
                TarRegularFile::new("bin/keep.txt", 5, Cursor::new(b"keep\n".as_slice()))
                    .with_mtime(ts(2, 0)),
            ))
            .await?;
        source
            .send(TarEntry::File(
                TarRegularFile::new("share/skip.txt", 5, Cursor::new(b"skip\n".as_slice()))
                    .with_mtime(ts(3, 0)),
            ))
            .await?;
        source
            .send(TarEntry::File(
                TarRegularFile::new("bin/run.sh", 8, Cursor::new(b"echo hi\n".as_slice()))
                    .with_mode(0o755)
                    .with_mtime(ts(4, 0)),
            ))
            .await?;
        source.close().await?;

        let source_archive = source_bytes.lock().unwrap().clone();
        let (sink, output_bytes) = ChunkedAsyncWriter::new(17);
        let mut filtered = TarWriter::new(sink);

        let mut entries = TarReader::new(Cursor::new(source_archive))
            .try_filter(|entry| future::ready(entry.path().starts_with("bin/")));
        filtered.send_all(&mut entries).await?;
        filtered.close().await?;

        let output_archive = output_bytes.lock().unwrap().clone();
        let entries =
            collect_entries_async(ChunkedReader::new(Cursor::new(output_archive), 11)).await?;
        assert_eq!(entries.len(), 3);
        assert_eq!(entries[0].path, "bin/");
        assert_eq!(entries[1].path, "bin/keep.txt");
        assert_eq!(entries[1].data, b"keep\n");
        assert_eq!(entries[2].path, "bin/run.sh");
        assert_eq!(entries[2].data, b"echo hi\n");
        Ok(())
    })
}

#[test]
fn writer_errors_when_file_source_is_short() {
    block_on_test(async {
        let (writer_sink, _) = VecAsyncWriter::new();
        let mut writer = TarWriter::<'static, 'static, _, Cursor<&'static [u8]>>::new(writer_sink);
        let err = writer
            .send(TarEntry::File(
                TarRegularFile::new("short.txt", 4, Cursor::new(b"abc".as_slice()))
                    .with_mtime(ts(1, 0)),
            ))
            .await
            .unwrap_err();
        assert_eq!(err.kind(), ErrorKind::UnexpectedEof);
        assert!(err.to_string().contains("reading file"));
    });
}

#[test]
fn writer_reads_only_size_bytes_when_file_source_is_long() -> Result<()> {
    block_on_test(async {
        let (writer_sink, shared) = VecAsyncWriter::new();
        let mut writer = TarWriter::<'static, 'static, _, Cursor<&'static [u8]>>::new(writer_sink);
        writer
            .write(TarEntry::File(TarRegularFile::new(
                "long.txt",
                3,
                Cursor::new(b"abcdef".as_slice()),
            )))
            .await?;
        writer.finish().await?;

        let buffer = shared.lock().unwrap().clone();
        let entries = collect_entries_async(Cursor::new(buffer)).await?;
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].path, "long.txt");
        assert_eq!(entries[0].data, b"abc");
        Ok(())
    })
}

#[test]
fn writer_rejects_nonportable_xattr_names() {
    block_on_test(async {
        let (writer_sink, _) = VecAsyncWriter::new();
        let mut writer = TarWriter::<'static, 'static, _, Cursor<&'static [u8]>>::new(writer_sink);
        let attrs = AttrList::new().with("user invalid", b"value".as_slice());
        let err = writer
            .send(TarEntry::File(
                TarRegularFile::new("file.txt", 0, Cursor::new(&[][..]))
                    .with_mtime(ts(1, 0))
                    .with_attrs(attrs),
            ))
            .await
            .unwrap_err();
        assert_eq!(err.kind(), ErrorKind::Other);
        assert!(err
            .to_string()
            .contains("xattr name contains non-portable characters"));
    });
}

/// Writer that keeps the size of each write.
struct WriteSizes {
    sizes: Arc<Mutex<Vec<usize>>>,
}

impl WriteSizes {
    fn new() -> (Self, Arc<Mutex<Vec<usize>>>) {
        let sizes = Arc::new(Mutex::new(Vec::new()));
        (
            Self {
                sizes: sizes.clone(),
            },
            sizes,
        )
    }
}

#[cfg(all(feature = "smol", not(feature = "tokio")))]
impl AsyncWrite for WriteSizes {
    fn poll_write(self: Pin<&mut Self>, _cx: &mut Context<'_>, buf: &[u8]) -> Poll<Result<usize>> {
        self.sizes.lock().unwrap().push(buf.len());
        Poll::Ready(Ok(buf.len()))
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_close(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<()>> {
        Poll::Ready(Ok(()))
    }
}

#[cfg(feature = "tokio")]
impl AsyncWrite for WriteSizes {
    fn poll_write(self: Pin<&mut Self>, _cx: &mut Context<'_>, buf: &[u8]) -> Poll<Result<usize>> {
        self.sizes.lock().unwrap().push(buf.len());
        Poll::Ready(Ok(buf.len()))
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<()>> {
        Poll::Ready(Ok(()))
    }
}

/// Body of `len` bytes.
fn body_data(len: usize) -> Vec<u8> {
    (0..len).map(|i| (i % 251) as u8).collect()
}

/// Archive with the file `a` and the file `b`.
fn two_file_archive(a: &[u8], b: &[u8]) -> Result<Vec<u8>> {
    block_on_test(async {
        let (sink, bytes) = VecAsyncWriter::new();
        let mut writer = TarWriter::new(sink);
        for (path, body) in [("a", a), ("b", b)] {
            writer
                .write(TarRegularFile::new(path, body.len() as u64, body).into())
                .await?;
        }
        writer.finish().await?;
        let archive = bytes.lock().unwrap().clone();
        Ok(archive)
    })
}

/// Read the next entry of `tar` as a file with the path `path`.
async fn next_file<'a, R: AsyncRead + 'a>(
    tar: &mut TarReader<'a, R>,
    path: &str,
) -> Result<TarRegularFile<'a, smol_tar::TarRegularFileReader<'a, R>>> {
    match tar.next().await.transpose()? {
        Some(TarEntry::File(file)) if file.path() == path => Ok(file),
        other => panic!("expected file {path}, got {other:?}"),
    }
}

#[test]
fn file_body_buf_read_stops_at_end_of_body() -> Result<()> {
    let a = body_data(3000);
    let b = b"next entry\n";
    let archive = two_file_archive(&a, b)?;
    block_on_test(async {
        let mut tar = TarReader::new(Cursor::new(archive));
        let mut file = next_file(&mut tar, "a").await?;
        let mut data = Vec::new();
        loop {
            let buf = file.fill_buf().await?;
            if buf.is_empty() {
                break;
            }
            // The source buffer has the full archive. The reader gives only
            // the bytes of the body.
            assert!(data.len() + buf.len() <= a.len());
            let n = buf.len().min(700);
            data.extend_from_slice(&buf[..n]);
            file.consume(n);
        }
        assert_eq!(data, a);
        assert!(file.fill_buf().await?.is_empty());
        let mut file = next_file(&mut tar, "b").await?;
        let mut data = Vec::new();
        file.read_to_end(&mut data).await?;
        assert_eq!(data, b);
        drop(file);
        assert!(tar.next().await.is_none());
        Ok(())
    })
}

#[test]
fn next_entry_reads_after_partial_consume() -> Result<()> {
    let a = body_data(3000);
    let b = b"next entry\n";
    let archive = two_file_archive(&a, b)?;
    // `None` drops the file after `fill_buf` without `consume`.
    for consumed in [None, Some(0), Some(10), Some(a.len())] {
        block_on_test(async {
            let mut tar = TarReader::new(Cursor::new(archive.clone()));
            let mut file = next_file(&mut tar, "a").await?;
            assert_eq!(file.fill_buf().await?, &a[..]);
            if let Some(n) = consumed {
                file.consume(n);
            }
            drop(file);
            let mut file = next_file(&mut tar, "b").await?;
            let mut data = Vec::new();
            file.read_to_end(&mut data).await?;
            assert_eq!(data, b, "consumed {consumed:?}");
            drop(file);
            assert!(tar.next().await.is_none());
            Ok::<_, io::Error>(())
        })?;
    }
    Ok(())
}

#[test]
fn file_body_read_continues_after_partial_consume() -> Result<()> {
    let a = body_data(3000);
    let archive = two_file_archive(&a, b"b")?;
    block_on_test(async {
        let mut tar = TarReader::new(Cursor::new(archive));
        let mut file = next_file(&mut tar, "a").await?;
        // A read after `fill_buf` without `consume` gives the same data.
        let mut head = [0; 5];
        assert_eq!(file.fill_buf().await?.len(), a.len());
        file.read_exact(&mut head).await?;
        assert_eq!(head, a[..5]);
        file.fill_buf().await?;
        file.consume(10);
        let mut data = Vec::new();
        file.read_to_end(&mut data).await?;
        assert_eq!(data, a[15..]);
        drop(file);
        let mut file = next_file(&mut tar, "b").await?;
        let mut data = Vec::new();
        file.read_to_end(&mut data).await?;
        assert_eq!(data, b"b");
        Ok(())
    })
}

/// Waker that counts its wakeups.
#[derive(Default)]
struct WakeCount(AtomicUsize);

impl Wake for WakeCount {
    fn wake(self: Arc<Self>) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

impl WakeCount {
    fn get(&self) -> usize {
        self.0.load(Ordering::SeqCst)
    }
}

/// Poll `fut` one time with a waker that counts its wakeups in `wakes`.
fn poll_once<F: Future>(fut: F, wakes: &Arc<WakeCount>) -> Poll<F::Output> {
    let waker = Waker::from(Arc::clone(wakes));
    std::pin::pin!(fut).poll(&mut Context::from_waker(&waker))
}

/// Number of clones of the counting waker.
static CLONES: AtomicUsize = AtomicUsize::new(0);
/// Number of wakeups of the counting waker.
static WAKES: AtomicUsize = AtomicUsize::new(0);

// A static gives the vtable one address, so `Waker::will_wake` can match it.
static COUNTING_VTABLE: RawWakerVTable =
    RawWakerVTable::new(counting_clone, counting_wake, counting_wake, counting_drop);

fn counting_clone(_: *const ()) -> RawWaker {
    CLONES.fetch_add(1, Ordering::SeqCst);
    RawWaker::new(std::ptr::null(), &COUNTING_VTABLE)
}

fn counting_wake(_: *const ()) {
    WAKES.fetch_add(1, Ordering::SeqCst);
}

fn counting_drop(_: *const ()) {}

/// Make a waker that counts its clones in `CLONES` and its wakeups in
/// `WAKES`. Only one test can use it, because the counters are static.
fn counting_waker() -> Waker {
    // SAFETY: The vtable functions ignore the data pointer and change only
    // static atomics, so the waker is valid on all threads for all time.
    unsafe { Waker::from_raw(RawWaker::new(std::ptr::null(), &COUNTING_VTABLE)) }
}

/// Poll `fut` one time with `waker`.
fn poll_with<F: Future>(fut: F, waker: &Waker) -> Poll<F::Output> {
    std::pin::pin!(fut).poll(&mut Context::from_waker(waker))
}

#[test]
fn pending_next_clones_same_waker_once() -> Result<()> {
    let archive = two_file_archive(b"abcdef", b"b")?;
    block_on_test(async {
        let mut tar = TarReader::new(Cursor::new(archive));
        let file = next_file(&mut tar, "a").await?;
        let waker = counting_waker();
        for _ in 0..10 {
            assert!(poll_with(tar.next(), &waker).is_pending());
        }
        assert_eq!(CLONES.load(Ordering::SeqCst), 1);
        assert_eq!(WAKES.load(Ordering::SeqCst), 0);
        drop(file);
        assert_eq!(WAKES.load(Ordering::SeqCst), 1);
        match poll_with(tar.next(), &waker) {
            Poll::Ready(Some(Ok(entry))) => assert_eq!(entry.path(), "b"),
            other => panic!("expected file b, got {other:?}"),
        }
        // The archive reader takes the returned source without a clone.
        assert_eq!(CLONES.load(Ordering::SeqCst), 1);
        assert!(tar.next().await.is_none());
        Ok(())
    })
}

#[test]
fn next_entry_wakes_when_consume_finishes_the_file() -> Result<()> {
    let a = body_data(3000);
    let archive = two_file_archive(&a, b"b")?;
    block_on_test(async {
        let mut tar = TarReader::new(Cursor::new(archive));
        let mut file = next_file(&mut tar, "a").await?;
        // The archive waits until the current body ends.
        let len = file.fill_buf().now_or_never().unwrap()?.len();
        assert_eq!(len, a.len());
        let wakes = Arc::new(WakeCount::default());
        assert!(poll_once(tar.next(), &wakes).is_pending());
        file.consume(len);
        assert!(wakes.get() > 0, "consume does not wake the archive reader");
        match poll_once(tar.next(), &wakes) {
            Poll::Ready(Some(Ok(entry))) => assert_eq!(entry.path(), "b"),
            other => panic!("expected file b, got {other:?}"),
        }
        Ok(())
    })
}

#[test]
fn exhausted_file_reader_remains_ready_while_next_file_is_active() -> Result<()> {
    let archive = two_file_archive(b"a", b"body")?;
    block_on_test(async {
        let mut tar = TarReader::new(Cursor::new(archive));
        let mut a = next_file(&mut tar, "a").await?;
        a.read_to_end(&mut Vec::new()).await?;
        let mut b = next_file(&mut tar, "b").await?;
        assert_eq!(b.fill_buf().await?, b"body");
        let wakes = Arc::new(WakeCount::default());
        assert!(matches!(
            poll_once(a.read(&mut [0]), &wakes),
            Poll::Ready(Ok(0))
        ));
        assert!(matches!(
            poll_once(a.fill_buf(), &wakes),
            Poll::Ready(Ok(b""))
        ));
        b.consume(4);
        assert!(tar.next().await.is_none());
        Ok(())
    })
}

#[test]
fn empty_read_finishes_zero_size_file() -> Result<()> {
    let archive = two_file_archive(b"", b"b")?;
    block_on_test(async {
        let mut tar = TarReader::new(Cursor::new(archive));
        let mut a = next_file(&mut tar, "a").await?;
        assert_eq!(a.read(&mut []).await?, 0);
        let wakes = Arc::new(WakeCount::default());
        match poll_once(tar.next(), &wakes) {
            Poll::Ready(Some(Ok(entry))) => assert_eq!(entry.path(), "b"),
            other => panic!("expected file b while file a is alive, got {other:?}"),
        }
        drop(a);
        Ok(())
    })
}

/// Source that gives `Poll::Pending` on every other call. It wakes the task
/// before it gives `Poll::Pending`.
struct InterleavePending<R> {
    inner: R,
    pending: bool,
}

impl<R> InterleavePending<R> {
    fn new(inner: R) -> Self {
        Self {
            inner,
            pending: false,
        }
    }

    /// Give `true` on every other call. Before it gives `true`, wake the task.
    fn next_is_pending(&mut self, cx: &mut Context<'_>) -> bool {
        self.pending = !self.pending;
        if self.pending {
            cx.waker().wake_by_ref();
        }
        self.pending
    }
}

#[cfg(all(feature = "smol", not(feature = "tokio")))]
impl<R: AsyncRead + Unpin> AsyncRead for InterleavePending<R> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<Result<usize>> {
        if self.next_is_pending(cx) {
            return Poll::Pending;
        }
        Pin::new(&mut self.inner).poll_read(cx, buf)
    }
}

#[cfg(feature = "tokio")]
impl<R: AsyncRead + Unpin> AsyncRead for InterleavePending<R> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<Result<()>> {
        if self.next_is_pending(cx) {
            return Poll::Pending;
        }
        Pin::new(&mut self.inner).poll_read(cx, buf)
    }
}

impl<R: AsyncBufRead + Unpin> AsyncBufRead for InterleavePending<R> {
    fn poll_fill_buf(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<&[u8]>> {
        let this = self.get_mut();
        if this.next_is_pending(cx) {
            return Poll::Pending;
        }
        Pin::new(&mut this.inner).poll_fill_buf(cx)
    }

    fn consume(mut self: Pin<&mut Self>, amt: usize) {
        Pin::new(&mut self.inner).consume(amt)
    }
}

#[test]
fn exhausted_file_reader_remains_ready_while_the_source_is_pending() -> Result<()> {
    let a = body_data(3000);
    let b = body_data(100);
    let archive = two_file_archive(&a, &b)?;
    block_on_test(async {
        let mut tar = TarReader::new(InterleavePending::new(Cursor::new(archive)));
        let mut file_a = next_file(&mut tar, "a").await?;
        let mut data = Vec::new();
        file_a.read_to_end(&mut data).await?;
        let mut file_b = next_file(&mut tar, "b").await?;
        let wakes = Arc::new(WakeCount::default());
        let waker = Waker::from(Arc::clone(&wakes));
        let mut buf = [0; 16];
        for _ in 0..100 {
            // The `fill_buf` future of futures-lite calls `poll_fill_buf` two
            // times and panics if the second call gives `Poll::Pending`. Thus
            // call `poll_fill_buf` directly.
            match Pin::new(&mut file_b).poll_fill_buf(&mut Context::from_waker(&waker)) {
                Poll::Ready(data) => {
                    assert_eq!(data?, b);
                    return Ok(());
                }
                // The previous file remains at EOF while the source is pending.
                Poll::Pending => assert!(matches!(
                    poll_once(file_a.read(&mut buf), &wakes),
                    Poll::Ready(Ok(0))
                )),
            }
        }
        panic!("fill_buf gives no data after 100 polls");
    })
}

#[test]
fn writer_writes_body_from_reader_buffer() -> Result<()> {
    let a = body_data(100_000);
    let padding = 512 - a.len() % 512;
    // A `&[u8]` body gives all its data in one buffer.
    let (sink, sizes) = WriteSizes::new();
    block_on_test(async {
        let mut writer = TarWriter::new(sink);
        writer
            .write(TarRegularFile::new("a", a.len() as u64, &a[..]).into())
            .await
    })?;
    assert_eq!(*sizes.lock().unwrap(), [512, a.len(), padding]);
    // The body of a `TarReader` entry comes from the buffer of the source.
    let archive = two_file_archive(&a, b"b")?;
    let (sink, sizes) = WriteSizes::new();
    block_on_test(async {
        let mut writer = TarWriter::new(sink);
        let mut entries = TarReader::new(Cursor::new(archive))
            .try_filter(|entry| future::ready(entry.path() == "a"));
        writer.send_all(&mut entries).await?;
        writer.close().await
    })?;
    assert_eq!(*sizes.lock().unwrap(), [512, a.len(), padding, 1024]);
    Ok(())
}

#[test]
fn cancelled_next_does_not_block_current_file() -> Result<()> {
    let archive = two_file_archive(b"abcdef", b"b")?;
    block_on_test(async {
        let mut tar = TarReader::new(Cursor::new(archive));
        let mut file = next_file(&mut tar, "a").await?;
        assert_eq!(file.fill_buf().await?, b"abcdef");
        let next_wakes = Arc::new(WakeCount::default());
        assert!(poll_once(tar.next(), &next_wakes).is_pending());
        // The temporary next() future has now been dropped.
        file.consume(1);
        let file_wakes = Arc::new(WakeCount::default());
        assert!(
            matches!(
                poll_once(file.fill_buf(), &file_wakes),
                Poll::Ready(Ok(b"bcdef"))
            ),
            "cancelled next() prevents current file from continuing"
        );
        Ok(())
    })
}

#[test]
fn body_completion_wakes_archive_waiter() -> Result<()> {
    let archive = two_file_archive(b"abcdef", b"b")?;
    block_on_test(async {
        let mut tar = TarReader::new(Cursor::new(archive));
        let mut file = next_file(&mut tar, "a").await?;
        assert_eq!(file.fill_buf().await?, b"abcdef");
        let next_wakes = Arc::new(WakeCount::default());
        assert!(poll_once(tar.next(), &next_wakes).is_pending());
        file.consume(1);
        assert!(poll_once(tar.next(), &next_wakes).is_pending());
        let before = next_wakes.get();
        let file_wakes = Arc::new(WakeCount::default());
        assert!(matches!(
            poll_once(file.fill_buf(), &file_wakes),
            Poll::Ready(Ok(b"bcdef"))
        ));
        file.consume(5);
        assert!(
            next_wakes.get() > before,
            "completion wakes file task but not waiting archive task"
        );
        Ok(())
    })
}

#[test]
fn repeated_consume_advances_each_time() -> Result<()> {
    let archive = two_file_archive(b"abcdef", b"b")?;
    block_on_test(async {
        let mut tar = TarReader::new(Cursor::new(archive));
        let mut file = next_file(&mut tar, "a").await?;
        assert_eq!(file.fill_buf().await?, b"abcdef");
        file.consume(1);
        file.consume(1);
        assert_eq!(file.fill_buf().await?, b"cdef");
        Ok(())
    })
}

#[test]
fn dropping_file_wakes_latest_archive_waiter() -> Result<()> {
    let archive = two_file_archive(b"abcdef", b"b")?;
    block_on_test(async {
        let mut tar = TarReader::new(InterleavePending::new(Cursor::new(archive)));
        let mut file = next_file(&mut tar, "a").await?;
        let file_wakes = Arc::new(WakeCount::default());
        let waker = Waker::from(file_wakes);
        assert!(Pin::new(&mut file)
            .poll_fill_buf(&mut Context::from_waker(&waker))
            .is_pending());
        let first = Arc::new(WakeCount::default());
        let latest = Arc::new(WakeCount::default());
        assert!(poll_once(tar.next(), &first).is_pending());
        assert!(poll_once(tar.next(), &latest).is_pending());
        drop(file);
        assert_eq!(first.get(), 0);
        assert_eq!(latest.get(), 1);
        let mut b = next_file(&mut tar, "b").await?;
        let mut data = Vec::new();
        b.read_to_end(&mut data).await?;
        assert_eq!(data, b"b");
        assert!(tar.next().await.is_none());
        Ok(())
    })
}

#[test]
fn empty_file_fill_buf_wakes_archive_waiter() -> Result<()> {
    let archive = two_file_archive(b"", b"b")?;
    block_on_test(async {
        let mut tar = TarReader::new(Cursor::new(archive));
        let mut file = next_file(&mut tar, "a").await?;
        let wakes = Arc::new(WakeCount::default());
        assert!(poll_once(tar.next(), &wakes).is_pending());
        assert!(file.fill_buf().await?.is_empty());
        assert_eq!(wakes.get(), 1);
        let b = next_file(&mut tar, "b").await?;
        assert!(file.fill_buf().await?.is_empty());
        drop(file);
        drop(b);
        assert!(tar.next().await.is_none());
        Ok(())
    })
}

#[test]
fn empty_read_preserves_body_and_full_read_wakes_archive() -> Result<()> {
    let archive = two_file_archive(b"abcdef", b"b")?;
    block_on_test(async {
        let mut tar = TarReader::new(Cursor::new(archive));
        let mut file = next_file(&mut tar, "a").await?;
        let wakes = Arc::new(WakeCount::default());
        assert!(poll_once(tar.next(), &wakes).is_pending());
        assert_eq!(file.read(&mut []).await?, 0);
        assert_eq!(wakes.get(), 0);
        let mut data = [0; 6];
        file.read_exact(&mut data).await?;
        assert_eq!(&data, b"abcdef");
        assert_eq!(wakes.get(), 1);
        let b = next_file(&mut tar, "b").await?;
        assert_eq!(file.read(&mut data).await?, 0);
        drop(b);
        assert!(tar.next().await.is_none());
        Ok(())
    })
}

#[test]
fn file_fill_buf_reports_truncated_body() -> Result<()> {
    let mut archive = two_file_archive(b"abcdef", b"b")?;
    archive.truncate(BLOCK_SIZE + 3);
    block_on_test(async {
        let mut tar = TarReader::new(Cursor::new(archive));
        let mut file = next_file(&mut tar, "a").await?;
        assert_eq!(file.fill_buf().await?, b"abc");
        file.consume(3);
        assert_eq!(
            file.fill_buf().await.unwrap_err().kind(),
            ErrorKind::UnexpectedEof
        );
        let wakes = Arc::new(WakeCount::default());
        assert!(poll_once(tar.next(), &wakes).is_pending());
        drop(file);
        assert_eq!(wakes.get(), 1);
        assert_eq!(
            tar.next().await.unwrap().unwrap_err().kind(),
            ErrorKind::UnexpectedEof
        );
        Ok(())
    })
}

pin_project_lite::pin_project! {
    /// Source that checks its address on each operation and stays on one thread.
    struct PinnedSource<R> {
        #[pin]
        inner: R,
        address: std::rc::Rc<std::cell::Cell<Option<usize>>>,
        #[pin]
        _pin: std::marker::PhantomPinned,
    }
}

impl<R> PinnedSource<R> {
    fn new(inner: R) -> Self {
        Self {
            inner,
            address: std::rc::Rc::new(std::cell::Cell::new(None)),
            _pin: std::marker::PhantomPinned,
        }
    }

    fn check_address(&self) {
        let address = std::ptr::from_ref(self) as usize;
        if let Some(previous) = self.address.replace(Some(address)) {
            assert_eq!(address, previous, "the source moved after its first poll");
        }
    }
}

#[cfg(all(feature = "smol", not(feature = "tokio")))]
impl<R: AsyncRead> AsyncRead for PinnedSource<R> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<Result<usize>> {
        self.as_ref().get_ref().check_address();
        self.project().inner.poll_read(cx, buf)
    }
}

#[cfg(feature = "tokio")]
impl<R: AsyncRead> AsyncRead for PinnedSource<R> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<Result<()>> {
        self.as_ref().get_ref().check_address();
        self.project().inner.poll_read(cx, buf)
    }
}

impl<R: AsyncBufRead> AsyncBufRead for PinnedSource<R> {
    fn poll_fill_buf(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<&[u8]>> {
        self.as_ref().get_ref().check_address();
        self.project().inner.poll_fill_buf(cx)
    }

    fn consume(self: Pin<&mut Self>, amt: usize) {
        self.as_ref().get_ref().check_address();
        self.project().inner.consume(amt);
    }
}

#[test]
fn pinned_local_source_survives_transfers_and_archive_drop() -> Result<()> {
    static_assertions::assert_not_impl_any!(PinnedSource<Cursor<Vec<u8>>>: Unpin, Send, Sync);
    let archive = two_file_archive(b"abcdef", b"body")?;
    block_on_test(async {
        let source = PinnedSource::new(Cursor::new(archive.as_slice()));
        let mut tar = TarReader::new(source);
        let mut a = next_file(&mut tar, "a").await?;
        assert_eq!(a.fill_buf().await?, b"abcdef");
        a.consume(1);
        let mut data = Vec::new();
        a.read_to_end(&mut data).await?;
        assert_eq!(data, b"bcdef");
        let mut b = next_file(&mut tar, "b").await?;
        // Dropping either handle must leave the active file usable.
        drop(a);
        drop(tar);
        assert_eq!(b.fill_buf().await?, b"body");
        b.consume(2);
        b.consume(2);
        assert!(b.fill_buf().await?.is_empty());
        Ok(())
    })
}

impl<W: AsyncWrite + Unpin> AsyncWrite for InterleavePending<W> {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<Result<usize>> {
        if self.next_is_pending(cx) {
            return Poll::Pending;
        }
        Pin::new(&mut self.inner).poll_write(cx, buf)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<()>> {
        Pin::new(&mut self.inner).poll_flush(cx)
    }

    #[cfg(all(feature = "smol", not(feature = "tokio")))]
    fn poll_close(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<()>> {
        Pin::new(&mut self.inner).poll_close(cx)
    }

    #[cfg(feature = "tokio")]
    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<()>> {
        Pin::new(&mut self.inner).poll_shutdown(cx)
    }
}

#[test]
fn buffered_entries_feed_writer_with_pending_reads_and_short_writes() -> Result<()> {
    let a = body_data(1003);
    let b = body_data(51);
    let archive = two_file_archive(&a, &b)?;
    block_on_test(async {
        let source = InterleavePending::new(Cursor::new(archive));
        #[cfg(all(feature = "smol", not(feature = "tokio")))]
        let source = smol::io::BufReader::with_capacity(17, source);
        #[cfg(feature = "tokio")]
        let source = tokio::io::BufReader::with_capacity(17, source);
        let mut entries = TarReader::new(source);
        let (sink, bytes) = ChunkedAsyncWriter::new(3);
        let mut writer = TarWriter::new(InterleavePending::new(sink));
        writer.send_all(&mut entries).await?;
        writer.close().await?;
        let archive = bytes.lock().unwrap().clone();
        let mut tar = TarReader::new(Cursor::new(archive));
        for (path, body) in [("a", a), ("b", b)] {
            let mut file = next_file(&mut tar, path).await?;
            let mut data = Vec::new();
            file.read_to_end(&mut data).await?;
            assert_eq!(data, body);
        }
        assert!(tar.next().await.is_none());
        Ok(())
    })
}

#[test]
fn writer_rejects_entry_after_dropped_finish() -> Result<()> {
    block_on_test(async {
        let (sink, bytes) = VecAsyncWriter::new();
        let mut writer = TarWriter::<'static, 'static, _, Cursor<&'static [u8]>>::new(
            InterleavePending::new(sink),
        );
        writer
            .write(TarEntry::from(TarDirectory::new("dir/")))
            .await?;
        let wakes = Arc::new(WakeCount::default());
        assert!(poll_once(writer.finish(), &wakes).is_pending());

        let err = writer
            .write(TarEntry::from(TarDirectory::new("late/")))
            .await
            .unwrap_err();
        assert_eq!(err.kind(), ErrorKind::Other);
        assert!(err.to_string().contains("already finished"));
        let err = Pin::new(&mut writer)
            .start_send(TarEntry::from(TarDirectory::new("late/")))
            .unwrap_err();
        assert!(err.to_string().contains("already finished"));
        assert_eq!(bytes.lock().unwrap().len(), BLOCK_SIZE);

        writer.finish().await?;
        let archive = bytes.lock().unwrap().clone();
        assert_eq!(archive.len(), 3 * BLOCK_SIZE);
        assert!(archive[BLOCK_SIZE..].iter().all(|&b| b == 0));
        let entries = collect_entries_async(Cursor::new(archive)).await?;
        let paths: Vec<_> = entries.iter().map(|e| e.path.as_str()).collect();
        assert_eq!(paths, ["dir/"]);
        Ok(())
    })
}

#[test]
fn writer_feed_does_not_accept_entry_when_dropped() -> Result<()> {
    block_on_test(async {
        let (sink, bytes) = VecAsyncWriter::new();
        let mut writer = TarWriter::new(InterleavePending::new(sink));
        writer
            .feed(TarEntry::from(TarRegularFile::new(
                "a",
                3,
                Cursor::new(b"abc".as_slice()),
            )))
            .await?;
        let wakes = Arc::new(WakeCount::default());
        let dir = || TarEntry::from(TarDirectory::new("b/"));
        // poll_ready writes the previous entry and returns Pending.
        assert!(poll_once(writer.feed(dir()), &wakes).is_pending());

        // The dropped feed did not accept the entry, so send it again.
        writer.feed(dir()).await?;
        writer.finish().await?;
        let archive = bytes.lock().unwrap().clone();
        let entries = collect_entries_async(Cursor::new(archive)).await?;
        let paths: Vec<_> = entries.iter().map(|e| e.path.as_str()).collect();
        assert_eq!(paths, ["a", "b/"]);
        Ok(())
    })
}

#[test]
fn writer_feed_does_not_accept_entry_after_error() -> Result<()> {
    const BODY: &[u8] = b"body";
    let entry = |i: usize| -> TarEntry<'static, Cursor<&'static [u8]>> {
        match i {
            0 => TarDirectory::new("a/").into(),
            1 => TarRegularFile::new("a/f", BODY.len() as u64, Cursor::new(BODY)).into(),
            _ => TarDirectory::new("b/").into(),
        }
    };
    // Writes: three headers, the body, the padding and the end marker.
    for fail_at in 1..=6 {
        block_on_test(async {
            let (sink, bytes) = VecAsyncWriter::new();
            let mut writer = TarWriter::new(FailingWriter::new(sink, fail_at));
            let mut errors = 0;
            for i in 0..3 {
                // A feed that fails did not accept the entry, so send it
                // again.
                if let Err(err) = writer.feed(entry(i)).await {
                    assert_eq!(err.to_string(), "destination failure");
                    errors += 1;
                    writer.feed(entry(i)).await?;
                }
            }
            if let Err(err) = writer.finish().await {
                assert_eq!(err.to_string(), "destination failure");
                errors += 1;
                writer.finish().await?;
            }
            assert_eq!(errors, 1, "fail_at {fail_at}");

            let archive = bytes.lock().unwrap().clone();
            let entries = collect_entries_async(Cursor::new(archive)).await?;
            let paths: Vec<_> = entries.iter().map(|e| e.path.as_str()).collect();
            assert_eq!(paths, ["a/", "a/f", "b/"], "fail_at {fail_at}");
            assert_eq!(entries[1].data, BODY);
            Result::Ok(())
        })?;
    }
    Ok(())
}

#[test]
fn writer_flush_writes_accepted_entry() -> Result<()> {
    block_on_test(async {
        let (sink, bytes) = VecAsyncWriter::new();
        let mut writer = TarWriter::new(sink);
        writer
            .feed(TarEntry::from(TarRegularFile::new(
                "a",
                3,
                Cursor::new(b"abc".as_slice()),
            )))
            .await?;
        assert!(bytes.lock().unwrap().is_empty());

        writer.flush().await?;
        let archive = bytes.lock().unwrap().clone();
        assert_eq!(archive.len(), 2 * BLOCK_SIZE);
        assert_eq!(&archive[..2], b"a\0");
        assert_eq!(&archive[BLOCK_SIZE..BLOCK_SIZE + 3], b"abc");
        assert!(archive[BLOCK_SIZE + 3..].iter().all(|&b| b == 0));

        writer.finish().await?;
        let archive = bytes.lock().unwrap().clone();
        let entries = collect_entries_async(Cursor::new(archive)).await?;
        let paths: Vec<_> = entries.iter().map(|e| e.path.as_str()).collect();
        assert_eq!(paths, ["a"]);
        assert_eq!(entries[0].data, b"abc");
        Ok(())
    })
}

#[test]
fn invalid_file_mode_does_not_leave_an_active_file() -> Result<()> {
    let mut header = build_header(b"bad-mode", b'0', 0, HeaderFlavor::Ustar);
    set_bytes(&mut header, MODE_START, 8, b"invalid");
    finalize_checksum(&mut header);
    block_on_test(async {
        let mut tar = TarReader::new(Cursor::new(header.to_vec()));
        assert_eq!(
            tar.next().await.unwrap().unwrap_err().kind(),
            ErrorKind::InvalidData
        );
        let wakes = Arc::new(WakeCount::default());
        assert!(matches!(poll_once(tar.next(), &wakes), Poll::Ready(None)));
        Ok(())
    })
}
