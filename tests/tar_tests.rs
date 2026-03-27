use std::{
    future::Future,
    io::{self, ErrorKind, Result},
    path::PathBuf,
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use futures::Sink;
use futures::{future, SinkExt, TryStreamExt};
#[cfg(feature = "smol")]
use futures_lite::io::{AsyncRead, AsyncReadExt, AsyncWrite};
#[cfg(feature = "smol")]
use smol::{io::Cursor, stream::StreamExt};
use smol_tar::{
    AttrList, TarDevice, TarDirectory, TarEntry, TarFifo, TarLink, TarReader, TarRegularFile,
    TarSymlink, TarWriter,
};
#[cfg(feature = "tokio")]
use std::io::Cursor;
#[cfg(feature = "tokio")]
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, ReadBuf};
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

#[cfg(feature = "smol")]
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

#[cfg(feature = "smol")]
type FixtureFile = smol::fs::File;
#[cfg(feature = "tokio")]
type FixtureFile = tokio::fs::File;
#[cfg(feature = "smol")]
type TestCursor<T> = futures::io::Cursor<T>;
#[cfg(feature = "tokio")]
type TestCursor<T> = std::io::Cursor<T>;

#[cfg(feature = "smol")]
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

#[cfg(feature = "smol")]
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

#[cfg(feature = "smol")]
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

#[cfg(feature = "smol")]
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

#[cfg(feature = "smol")]
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

        let entries = collect_entries_async(Cursor::new(shared.lock().unwrap().clone())).await?;
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
fn tar_writer_uses_pax_symbolic_names_when_they_do_not_fit() -> Result<()> {
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
        "invalid octal digit",
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
fn reader_rejects_malformed_pax_records() {
    for (payload, expected) in [
        (b"9path=a\n".as_slice(), "missing length separator"),
        (b"x path=a\n".as_slice(), "invalid length"),
        (b"10 path=aX".as_slice(), "missing trailing newline"),
        (b"11 pathabc\n".as_slice(), "missing '='"),
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
        "malformed PAX record: invalid length",
    );
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
            ("user.scope".to_string(), b"local".to_vec()),
            ("user.dupe".to_string(), b"second".to_vec()),
        ]
    );
    assert_eq!(entries[1].path, "second.txt");
    assert_eq!(
        entries[1].attrs,
        vec![("user.scope".to_string(), b"global".to_vec())]
    );
    Ok(())
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
fn reader_rejects_invalid_header_numeric_fields() {
    for (field_start, field_len, typeflag, flavor, message) in [
        (
            UID_START,
            8,
            b'0',
            HeaderFlavor::Ustar,
            "invalid octal digit",
        ),
        (
            GID_START,
            8,
            b'0',
            HeaderFlavor::Ustar,
            "invalid octal digit",
        ),
        (
            MTIME_START,
            12,
            b'0',
            HeaderFlavor::Ustar,
            "invalid octal digit",
        ),
        (
            SIZE_START,
            12,
            b'0',
            HeaderFlavor::Ustar,
            "invalid octal digit",
        ),
        (
            GNU_ATIME_START,
            12,
            b'0',
            HeaderFlavor::Gnu,
            "invalid octal digit",
        ),
        (
            GNU_CTIME_START,
            12,
            b'0',
            HeaderFlavor::Gnu,
            "invalid octal digit",
        ),
        (329, 8, b'3', HeaderFlavor::Ustar, "invalid octal digit"),
        (337, 8, b'4', HeaderFlavor::Ustar, "invalid octal digit"),
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
