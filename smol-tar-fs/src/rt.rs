//! Runtime shim. It runs blocking work on the blocking thread pool of the
//! selected runtime and gives the async file type of that runtime.

use std::io;

/// Chunk size for file body I/O.
pub(crate) const FILE_BUFFER_SIZE: usize = 256 * 1024;

/// Async file type for file bodies.
#[cfg(all(feature = "smol", not(feature = "tokio")))]
pub(crate) type AsyncFile = blocking::Unblock<std::fs::File>;
/// Async file type for file bodies.
#[cfg(feature = "tokio")]
pub(crate) type AsyncFile = tokio::fs::File;

/// Run a blocking closure on the blocking thread pool.
///
/// A panic in the closure continues in the caller.
#[cfg(all(feature = "smol", not(feature = "tokio")))]
pub(crate) async fn unblock<T, F>(f: F) -> io::Result<T>
where
    F: FnOnce() -> io::Result<T> + Send + 'static,
    T: Send + 'static,
{
    blocking::unblock(f).await
}

/// Run a blocking closure on the blocking thread pool.
///
/// The call must run inside a Tokio runtime context, because
/// `spawn_blocking` panics outside of it.
///
/// A panic in the closure continues in the caller. A cancelled task gives an
/// error.
#[cfg(feature = "tokio")]
pub(crate) async fn unblock<T, F>(f: F) -> io::Result<T>
where
    F: FnOnce() -> io::Result<T> + Send + 'static,
    T: Send + 'static,
{
    match tokio::task::spawn_blocking(f).await {
        Ok(result) => result,
        Err(err) => match err.try_into_panic() {
            Ok(payload) => std::panic::resume_unwind(payload),
            Err(err) => Err(io::Error::other(err)),
        },
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
/// `Unblock` reads ahead up to `FILE_BUFFER_SIZE` bytes. `Take` stops the
/// reader at the size that the source read, so the reader does no read at
/// the end of the file.
#[cfg(all(feature = "smol", not(feature = "tokio")))]
pub(crate) type FileReader = blocking::Unblock<std::io::Take<std::fs::File>>;
/// Buffered reader for file bodies.
///
/// `tokio::fs::File` does one round trip to the blocking thread pool for each
/// read and does no read-ahead. Thus the reader reads the file in chunks of
/// `FILE_BUFFER_SIZE`. `Take` stops the reader at the size that the source
/// read, so the reader does no read at the end of the file.
#[cfg(feature = "tokio")]
pub(crate) type FileReader = tokio::io::BufReader<tokio::io::Take<AsyncFile>>;

/// Wrap a blocking file in the buffered reader for file bodies. The reader
/// gives no more than `size` bytes.
#[cfg(all(feature = "smol", not(feature = "tokio")))]
pub(crate) fn file_reader(file: std::fs::File, size: u64) -> FileReader {
    blocking::Unblock::with_capacity(FILE_BUFFER_SIZE, std::io::Read::take(file, size))
}

/// Wrap a blocking file in the buffered reader for file bodies. The reader
/// gives no more than `size` bytes.
#[cfg(feature = "tokio")]
pub(crate) fn file_reader(file: std::fs::File, size: u64) -> FileReader {
    let file = tokio::io::AsyncReadExt::take(async_file(file), size);
    tokio::io::BufReader::with_capacity(FILE_BUFFER_SIZE, file)
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
        std::io::{ErrorKind, Seek},
    };

    #[cfg(all(feature = "smol", not(feature = "tokio")))]
    use smol::io::AsyncReadExt;
    #[cfg(feature = "tokio")]
    use tokio::io::AsyncReadExt;

    assert_impl_all!(blocking::Unblock<std::fs::File>: Send, Unpin);
    assert_impl_all!(tokio::fs::File: Send, Unpin);
    assert_impl_all!(AsyncFile: Send, Unpin, crate::AsyncRead, crate::AsyncWrite);

    #[test]
    fn unblock_runs_on_other_thread() {
        let caller = std::thread::current().id();
        let worker = block_on(unblock(|| Ok(std::thread::current().id()))).unwrap();
        assert_ne!(caller, worker);
    }

    #[test]
    fn unblock_passes_error() {
        let err = block_on(unblock(|| -> io::Result<()> {
            Err(io::Error::new(ErrorKind::NotFound, "test error"))
        }))
        .unwrap_err();
        assert_eq!(err.kind(), ErrorKind::NotFound);
        assert_eq!(err.to_string(), "test error");
    }

    #[test]
    #[should_panic(expected = "closure panic")]
    fn unblock_resumes_panic() {
        let _ = block_on(unblock(|| -> io::Result<()> { panic!("closure panic") }));
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
}
