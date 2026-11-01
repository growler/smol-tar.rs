//! Runtime shim. It runs blocking work on the blocking thread pool of the
//! selected runtime and gives the async file type of that runtime.

use std::{
    future::Future,
    io,
    pin::Pin,
    task::{ready, Context, Poll},
};

/// Chunk size for file body I/O.
pub(crate) const FILE_BUFFER_SIZE: usize = 256 * 1024;

/// Maximum number of files of up to `FILE_BUFFER_SIZE` that one job on the
/// blocking thread pool reads or writes.
pub(crate) const BATCH_FILES: usize = 64;

/// Maximum number of body bytes that one job on the blocking thread pool
/// reads or writes for files of up to `FILE_BUFFER_SIZE`.
pub(crate) const BATCH_BYTES: usize = 1024 * 1024;

/// Async file type for file bodies.
#[cfg(all(feature = "smol", not(feature = "tokio")))]
pub(crate) type AsyncFile = blocking::Unblock<std::fs::File>;
/// Async file type for file bodies.
#[cfg(feature = "tokio")]
pub(crate) type AsyncFile = tokio::fs::File;

/// Run a blocking closure on the blocking thread pool.
///
/// The call gives the job to the thread pool at once. The job does not wait
/// for the first poll of the returned future. A drop of the future does not
/// stop a job that runs. If the job did not start, the drop cancels it.
///
/// A panic in the closure continues in the caller.
#[cfg(all(feature = "smol", not(feature = "tokio")))]
pub(crate) fn unblock<T, F>(f: F) -> impl Future<Output = io::Result<T>> + Send + Sync
where
    F: FnOnce() -> io::Result<T> + Send + 'static,
    T: Send + 'static,
{
    blocking::unblock(f)
}

/// Run a blocking closure on the blocking thread pool.
///
/// The call gives the job to the thread pool at once. The job does not wait
/// for the first poll of the returned future. A drop of the future does not
/// stop the job.
///
/// The call must run inside a Tokio runtime context, because
/// `spawn_blocking` panics outside of it.
///
/// A panic in the closure continues in the caller. A cancelled task gives an
/// error.
#[cfg(feature = "tokio")]
pub(crate) fn unblock<T, F>(f: F) -> impl Future<Output = io::Result<T>> + Send + Sync
where
    F: FnOnce() -> io::Result<T> + Send + 'static,
    T: Send + 'static,
{
    let handle = tokio::task::spawn_blocking(f);
    async move {
        match handle.await {
            Ok(result) => result,
            Err(err) => match err.try_into_panic() {
                Ok(payload) => std::panic::resume_unwind(payload),
                Err(err) => Err(io::Error::other(err)),
            },
        }
    }
}

/// Wrap a blocking file in the async file type.
#[cfg(all(feature = "smol", not(feature = "tokio")))]
pub(crate) fn async_file(file: std::fs::File) -> AsyncFile {
    blocking::Unblock::with_capacity(FILE_BUFFER_SIZE, file)
}

/// Wrap a blocking file in the async file type.
#[cfg(feature = "tokio")]
pub(crate) fn async_file(file: std::fs::File) -> AsyncFile {
    tokio::fs::File::from_std(file)
}

/// Buffered reader for file bodies.
///
/// The reader reads the file in chunks of up to `FILE_BUFFER_SIZE` bytes on
/// the blocking thread pool. While the caller takes the data of one chunk,
/// the read of the next chunk runs. The reader gives no more than the size
/// that the source read, so it does no read at the end of the file. If the
/// file has less data than that size, the read gives an `InvalidData` error
/// "file changed".
///
/// The reader implements `AsyncBufRead`. The buffer is the data of the last
/// chunk, thus a caller can take the data without a copy.
///
/// After a read error, each later read gives an error.
pub(crate) struct FileReader {
    /// Buffer of the last chunk. Only `data[..len]` has data.
    data: Vec<u8>,
    /// Number of bytes of the last chunk.
    len: usize,
    /// Position of the next byte in `data`.
    pos: usize,
    /// Read of the next chunk. It gives back the file, the buffer, and the
    /// number of bytes that it read.
    next: Option<ChunkRead>,
    /// The file and an unused buffer while no read runs.
    idle: Option<(std::fs::File, Vec<u8>)>,
    /// Number of bytes that no read has asked for.
    remain: u64,
    /// A chunk read gave an error. Each later read gives an error.
    failed: bool,
}

/// Read of one chunk on the blocking thread pool.
type ChunkRead =
    Pin<Box<dyn Future<Output = io::Result<(std::fs::File, Vec<u8>, usize)>> + Send + Sync>>;

/// Read `len` bytes of `file` into `buf`, in place of the data in `buf`.
/// Give back the file, the buffer, and `len`. If the file has fewer than
/// `len` bytes, give an `InvalidData` error "file changed".
fn read_chunk(
    mut file: std::fs::File,
    mut buf: Vec<u8>,
    len: usize,
) -> io::Result<(std::fs::File, Vec<u8>, usize)> {
    if buf.len() < len {
        buf.resize(len, 0);
    }
    let mut filled = 0;
    while filled < len {
        match std::io::Read::read(&mut file, &mut buf[filled..len]) {
            Ok(0) => return Err(io::Error::new(io::ErrorKind::InvalidData, "file changed")),
            Ok(n) => filled += n,
            Err(err) if err.kind() == io::ErrorKind::Interrupted => {}
            Err(err) => return Err(err),
        }
    }
    Ok((file, buf, len))
}

impl FileReader {
    /// Give the file, an unused buffer and the size of the next chunk if no
    /// read runs and the file has more data.
    fn next_chunk(&mut self) -> Option<(std::fs::File, Vec<u8>, usize)> {
        if self.next.is_some() || self.remain == 0 {
            return None;
        }
        let (file, buf) = self.idle.take()?;
        let len = self.remain.min(FILE_BUFFER_SIZE as u64) as usize;
        self.remain -= len as u64;
        Some((file, buf, len))
    }

    /// Start the read of the next chunk if no read runs and the file has
    /// more data.
    fn start(&mut self) {
        if let Some((file, buf, len)) = self.next_chunk() {
            self.next = Some(Box::pin(unblock(move || read_chunk(file, buf, len))));
        }
    }

    /// Make the result of a chunk read the last chunk.
    fn set_chunk(&mut self, (file, chunk, len): (std::fs::File, Vec<u8>, usize)) {
        let spare = std::mem::replace(&mut self.data, chunk);
        (self.len, self.pos) = (len, 0);
        self.idle = Some((file, spare));
    }

    /// Give the data of the last chunk that the caller did not take. Wait
    /// for the next chunk if the caller took all data. An empty slice is the
    /// end of the file. After a read error, each call gives an error.
    fn poll_chunk(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<&[u8]>> {
        loop {
            if self.pos < self.len {
                self.start();
                return Poll::Ready(Ok(&self.data[self.pos..self.len]));
            }
            if self.failed {
                return Poll::Ready(Err(io::Error::other("read failed")));
            }
            self.start();
            let Some(next) = self.next.as_mut() else {
                return Poll::Ready(Ok(&[]));
            };
            let result = ready!(next.as_mut().poll(cx));
            self.next = None;
            match result {
                Ok(chunk) => self.set_chunk(chunk),
                Err(err) => {
                    // The job dropped the file. Each later read gives an error.
                    // Release the buffer of the last chunk.
                    self.failed = true;
                    self.data = Vec::new();
                    (self.len, self.pos) = (0, 0);
                    return Poll::Ready(Err(err));
                }
            }
        }
    }

    /// Mark `amt` bytes of the last chunk as taken.
    fn consume_chunk(&mut self, amt: usize) {
        self.pos = self.len.min(self.pos + amt);
    }
}

#[cfg(all(feature = "smol", not(feature = "tokio")))]
impl futures_io::AsyncRead for FileReader {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        let data = ready!(this.poll_chunk(cx))?;
        let n = buf.len().min(data.len());
        buf[..n].copy_from_slice(&data[..n]);
        this.consume_chunk(n);
        Poll::Ready(Ok(n))
    }
}

#[cfg(all(feature = "smol", not(feature = "tokio")))]
impl futures_io::AsyncBufRead for FileReader {
    fn poll_fill_buf(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<&[u8]>> {
        self.get_mut().poll_chunk(cx)
    }

    fn consume(self: Pin<&mut Self>, amt: usize) {
        self.get_mut().consume_chunk(amt)
    }
}

#[cfg(feature = "tokio")]
impl tokio::io::AsyncRead for FileReader {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        let data = ready!(this.poll_chunk(cx))?;
        let n = buf.remaining().min(data.len());
        buf.put_slice(&data[..n]);
        this.consume_chunk(n);
        Poll::Ready(Ok(()))
    }
}

#[cfg(feature = "tokio")]
impl tokio::io::AsyncBufRead for FileReader {
    fn poll_fill_buf(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<&[u8]>> {
        self.get_mut().poll_chunk(cx)
    }

    fn consume(self: Pin<&mut Self>, amt: usize) {
        self.get_mut().consume_chunk(amt)
    }
}

/// Wrap a blocking file in the buffered reader for file bodies. The reader
/// gives no more than `size` bytes.
pub(crate) fn file_reader(file: std::fs::File, size: u64) -> FileReader {
    FileReader {
        data: Vec::new(),
        len: 0,
        pos: 0,
        next: None,
        idle: Some((file, Vec::new())),
        remain: size,
        failed: false,
    }
}

/// Read the first chunk of `file` on the current thread, and give a buffered
/// reader that has the data of this chunk. The reader gives no more than
/// `size` bytes. Its first poll starts the read of the next chunk.
pub(crate) fn file_reader_with_first_chunk(
    file: std::fs::File,
    size: u64,
) -> io::Result<FileReader> {
    let mut reader = file_reader(file, size);
    if let Some((file, buf, len)) = reader.next_chunk() {
        reader.set_chunk(read_chunk(file, buf, len)?);
    }
    Ok(reader)
}

/// Wait for all pending operations, then give back the blocking file.
///
/// The caller must flush the file first, because `into_std` does not report
/// a pending write error. After a partial read, the position of the returned
/// file is not specified, because `into_std` drops the read-ahead data. The
/// caller must not close the async file before `into_std`. With `smol`, this
/// causes a panic.
#[cfg(all(feature = "smol", not(feature = "tokio")))]
pub(crate) async fn into_std(file: AsyncFile) -> std::fs::File {
    file.into_inner().await
}

/// Wait for all pending operations, then give back the blocking file.
///
/// The caller must flush the file first, because `into_std` does not report
/// a pending write error. After a partial read, the position of the returned
/// file is not specified, because `into_std` drops the read-ahead data. The
/// caller must not close the async file before `into_std`. With `smol`, this
/// causes a panic.
#[cfg(feature = "tokio")]
pub(crate) async fn into_std(file: AsyncFile) -> std::fs::File {
    file.into_std().await
}

/// Run a future to completion on the selected runtime.
#[cfg(all(test, feature = "smol", not(feature = "tokio")))]
pub(crate) fn block_on<F: std::future::Future>(fut: F) -> F::Output {
    smol::block_on(fut)
}

/// Run a future to completion on the selected runtime.
#[cfg(all(test, feature = "tokio"))]
pub(crate) fn block_on<F: std::future::Future>(fut: F) -> F::Output {
    tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap()
        .block_on(fut)
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        static_assertions::assert_impl_all,
        std::{
            io::{ErrorKind, Seek},
            time::{Duration, Instant},
        },
    };

    #[cfg(all(feature = "smol", not(feature = "tokio")))]
    use smol::io::{AsyncBufReadExt, AsyncReadExt};
    #[cfg(feature = "tokio")]
    use tokio::io::{AsyncBufReadExt, AsyncReadExt};

    assert_impl_all!(blocking::Unblock<std::fs::File>: Send, Unpin);
    assert_impl_all!(tokio::fs::File: Send, Unpin);
    assert_impl_all!(AsyncFile: Send, Unpin, crate::AsyncRead, crate::AsyncWrite);

    #[test]
    fn unblock_runs_on_other_thread() {
        let caller = std::thread::current().id();
        let worker = block_on(async { unblock(|| Ok(std::thread::current().id())).await }).unwrap();
        assert_ne!(caller, worker);
    }

    #[test]
    fn unblock_passes_error() {
        let err = block_on(async {
            unblock(|| -> io::Result<()> { Err(io::Error::new(ErrorKind::NotFound, "test error")) })
                .await
        })
        .unwrap_err();
        assert_eq!(err.kind(), ErrorKind::NotFound);
        assert_eq!(err.to_string(), "test error");
    }

    #[test]
    #[should_panic(expected = "closure panic")]
    fn unblock_resumes_panic() {
        let _ = block_on(async { unblock(|| -> io::Result<()> { panic!("closure panic") }).await });
    }

    #[test]
    fn unblock_starts_before_poll() {
        let (tx, rx) = std::sync::mpsc::channel();
        block_on(async {
            let fut = unblock(move || {
                tx.send(()).unwrap();
                Ok(())
            });
            rx.recv_timeout(Duration::from_secs(10))
                .expect("job did not start before the first poll");
            fut.await.unwrap();
        });
    }

    #[test]
    fn async_file_reads_file() {
        let path = concat!(env!("CARGO_MANIFEST_DIR"), "/Cargo.toml");
        let expected = std::fs::read(path).unwrap();
        block_on(async {
            let file = unblock(move || std::fs::File::open(path)).await.unwrap();
            let mut file = async_file(file);
            let mut data = Vec::new();
            file.read_to_end(&mut data).await.unwrap();
            assert_eq!(data, expected);
            let mut file = into_std(file).await;
            assert_eq!(file.stream_position().unwrap(), expected.len() as u64);
        });
    }

    /// Read `size` bytes of a file with `len` bytes through `file_reader`,
    /// with reads of `step` bytes.
    fn read_with_file_reader(len: usize, size: u64, step: usize) -> (Vec<u8>, Vec<u8>) {
        let tmp = crate::testutil::TempDir::new();
        let path = tmp.path().join("f");
        let data: Vec<u8> = (0..len).map(|i| (i % 251) as u8).collect();
        std::fs::write(&path, &data).unwrap();
        let read = block_on(async {
            let mut reader = file_reader(std::fs::File::open(&path).unwrap(), size);
            let mut read = Vec::new();
            let mut buf = vec![0; step];
            loop {
                let n = reader.read(&mut buf).await.unwrap();
                if n == 0 {
                    break;
                }
                read.extend_from_slice(&buf[..n]);
            }
            read
        });
        (data, read)
    }

    #[test]
    fn file_reader_reads_parts_of_chunks() {
        let len = 2 * 1024 * 1024 + 100;
        for step in [7000, 16 * 1024, 3 * 1024 * 1024] {
            let (data, read) = read_with_file_reader(len, len as u64, step);
            assert!(read == data, "step {step}");
        }
    }

    #[test]
    fn file_reader_gives_chunks_as_buffer() {
        let tmp = crate::testutil::TempDir::new();
        let path = tmp.path().join("f");
        let len = 2 * FILE_BUFFER_SIZE + 100;
        let data: Vec<u8> = (0..len).map(|i| (i % 251) as u8).collect();
        std::fs::write(&path, &data).unwrap();
        let (read, sizes) = block_on(async {
            let mut reader = file_reader(std::fs::File::open(&path).unwrap(), len as u64);
            let (mut read, mut sizes) = (Vec::new(), Vec::new());
            loop {
                let buf = reader.fill_buf().await.unwrap();
                if buf.is_empty() {
                    break;
                }
                sizes.push(buf.len());
                read.extend_from_slice(buf);
                let n = buf.len();
                reader.consume(n);
            }
            (read, sizes)
        });
        assert!(read == data);
        assert_eq!(sizes, [FILE_BUFFER_SIZE, FILE_BUFFER_SIZE, 100]);
    }

    #[test]
    fn file_reader_reads_next_chunk_ahead() {
        let tmp = crate::testutil::TempDir::new();
        let path = tmp.path().join("f");
        let len = 3 * FILE_BUFFER_SIZE;
        std::fs::write(&path, vec![1u8; len]).unwrap();
        let file = std::fs::File::open(&path).unwrap();
        // The clone shares the file offset.
        let mut probe = file.try_clone().unwrap();
        block_on(async {
            let mut reader = file_reader(file, len as u64);
            assert_eq!(reader.fill_buf().await.unwrap().len(), FILE_BUFFER_SIZE);
            // Do not consume. The read of the second chunk must run now.
            let deadline = Instant::now() + Duration::from_secs(10);
            while probe.stream_position().unwrap() < 2 * FILE_BUFFER_SIZE as u64 {
                assert!(Instant::now() < deadline, "no read-ahead");
                std::thread::sleep(Duration::from_millis(1));
            }
        });
    }

    #[test]
    fn file_reader_stops_at_size() {
        let len = 2 * 1024 * 1024 + 100;
        let size = len as u64 - 50;
        let (data, read) = read_with_file_reader(len, size, 64 * 1024);
        assert!(read == data[..size as usize]);
    }

    #[test]
    fn file_reader_errors_on_smaller_file() {
        // A short last chunk, and a file that ends at the end of a chunk.
        for len in [4 * FILE_BUFFER_SIZE + 100, 4 * FILE_BUFFER_SIZE] {
            let tmp = crate::testutil::TempDir::new();
            let path = tmp.path().join("f");
            let data: Vec<u8> = (0..len).map(|i| (i % 251) as u8).collect();
            std::fs::write(&path, &data).unwrap();
            block_on(async {
                let file = std::fs::File::open(&path).unwrap();
                let mut reader = file_reader(file, 3 * 1024 * 1024);
                let (mut read, mut buf) = (Vec::new(), vec![0; 64 * 1024]);
                let err = loop {
                    match reader.read(&mut buf).await {
                        Ok(0) => panic!("end of file before the error, {len}"),
                        Ok(n) => read.extend_from_slice(&buf[..n]),
                        Err(err) => break err,
                    }
                };
                assert_eq!(err.kind(), ErrorKind::InvalidData, "{len}");
                assert_eq!(err.to_string(), "file changed");
                // The data of the full chunks comes before the error.
                assert!(read == data[..4 * FILE_BUFFER_SIZE], "{len}");
                let err = reader.read(&mut buf).await.unwrap_err();
                assert_eq!(err.to_string(), "read failed");
                assert!(reader.fill_buf().await.is_err());
            });
        }
    }

    #[test]
    fn file_reader_error_is_sticky() {
        let tmp = crate::testutil::TempDir::new();
        let dir = std::fs::File::open(tmp.path()).unwrap();
        block_on(async {
            let mut reader = file_reader(dir, 2 * FILE_BUFFER_SIZE as u64);
            let mut buf = [0; 16];
            let err = reader.read(&mut buf).await.unwrap_err();
            assert_eq!(err.raw_os_error(), Some(libc::EISDIR));
            let err = reader.read(&mut buf).await.unwrap_err();
            assert_eq!(err.kind(), ErrorKind::Other);
            assert_eq!(err.to_string(), "read failed");
            assert!(reader.fill_buf().await.is_err());
        });
    }
}
