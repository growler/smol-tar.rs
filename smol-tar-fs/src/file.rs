//! Body of a regular file from the source. The file opens on its first read.

use {
    super::{rt, sys, with_path},
    std::{
        future::Future,
        io::{self, Read},
        mem,
        pin::Pin,
        sync::Arc,
        task::{ready, Context, Poll},
    },
};

#[cfg(all(feature = "smol", not(feature = "tokio")))]
use futures_io::AsyncRead;
#[cfg(feature = "tokio")]
use tokio::io::{AsyncRead, ReadBuf};

/// Body of a regular file from [`FsSource`](super::FsSource).
///
/// The type implements `AsyncRead`. The file opens on the first read. If the
/// `FsFile` is dropped before the first read, the file is not opened. A read
/// into an empty buffer gives 0 bytes and does not open the file.
///
/// The open does not follow a symlink. The open compares the identity and the
/// size of the file with the values that the source read. A difference gives
/// an [`InvalidData`] error with the message "file changed".
///
/// The size that the source read controls how the file is read. A file of
/// up to 256 KiB is read in one job at the first read, then the file closes.
/// The data stays in memory until the `FsFile` is dropped. A larger file
/// stays open until the `FsFile` is dropped, and the data is read in parts.
/// The reads do not go past the size that the source read. If a file of up
/// to 256 KiB has less data than that size, the first read gives an
/// [`InvalidData`] error with the message "file changed". If a larger file
/// gets smaller after the open, the reads end before that size.
///
/// An error from the open has the archive path of the entry. For a file of
/// up to 256 KiB, the read is part of the open, thus a read error also has
/// the archive path. After an error from the open, each read gives an error.
///
/// The open and the reads run on the blocking thread pool of the runtime.
/// The close of the file can occur on the thread that drops the `FsFile`.
/// With the `tokio` feature, read the file inside a Tokio runtime context.
///
/// # Panics
///
/// With the `tokio` feature, a read outside a Tokio runtime context panics.
/// After that panic, each read panics.
///
/// [`InvalidData`]: std::io::ErrorKind::InvalidData
pub struct FsFile {
    state: FileState,
    /// Archive path, for errors.
    path: Box<str>,
}

/// Open of the file on the blocking thread pool.
type OpenFuture = Pin<Box<dyn Future<Output = io::Result<Opened>> + Send + Sync>>;

/// Result of the open.
enum Opened {
    /// File larger than `FILE_BUFFER_SIZE`, to read in chunks.
    File(std::fs::File),
    /// All data of a file of up to `FILE_BUFFER_SIZE`.
    Bytes(Vec<u8>),
}

enum FileState {
    /// The file is not open yet.
    Closed {
        /// Directory that holds the file.
        parent: Arc<sys::Dir>,
        /// Name in `parent`.
        name: Box<str>,
        /// Identity that the source read.
        id: sys::FileId,
        /// Size that the source read.
        size: u64,
    },
    /// The open runs.
    Opening {
        /// Open on the blocking thread pool.
        fut: OpenFuture,
        /// Size that the source read.
        size: u64,
    },
    /// The file is open.
    Open(rt::FileReader),
    /// All data of the file is in memory.
    Memory(io::Cursor<Vec<u8>>),
    /// The open gave an error.
    Failed,
}

/// Open `name` in `parent`. If `size` is not more than `FILE_BUFFER_SIZE`,
/// read all data of the file and close it.
fn open(parent: &sys::Dir, name: &str, id: sys::FileId, size: u64) -> io::Result<Opened> {
    let mut file = sys::open_file(parent, name, id, size)?;
    if size > rt::FILE_BUFFER_SIZE as u64 {
        return Ok(Opened::File(file));
    }
    let mut buf = vec![0; size as usize];
    match file.read_exact(&mut buf) {
        Ok(()) => Ok(Opened::Bytes(buf)),
        // The file got smaller after the open.
        Err(err) if err.kind() == io::ErrorKind::UnexpectedEof => {
            Err(io::Error::new(io::ErrorKind::InvalidData, "file changed"))
        }
        Err(err) => Err(err),
    }
}

impl FsFile {
    /// Make a file that opens `name` in `parent` on the first read. `path` is
    /// the archive path.
    pub(crate) fn new(
        parent: Arc<sys::Dir>,
        name: Box<str>,
        id: sys::FileId,
        size: u64,
        path: Box<str>,
    ) -> Self {
        Self {
            state: FileState::Closed {
                parent,
                name,
                id,
                size,
            },
            path,
        }
    }

    /// Open the file if it is not open. On success, the state is `Open` or
    /// `Memory`.
    fn poll_open(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        loop {
            match &mut self.state {
                FileState::Closed { .. } => {
                    let FileState::Closed {
                        parent,
                        name,
                        id,
                        size,
                    } = mem::replace(&mut self.state, FileState::Failed)
                    else {
                        unreachable!()
                    };
                    self.state = FileState::Opening {
                        fut: Box::pin(rt::unblock(move || open(&parent, &name, id, size))),
                        size,
                    };
                }
                FileState::Opening { fut, size } => {
                    let size = *size;
                    match ready!(fut.as_mut().poll(cx)) {
                        Ok(Opened::File(file)) => {
                            self.state = FileState::Open(rt::file_reader(file, size));
                        }
                        Ok(Opened::Bytes(buf)) => {
                            self.state = FileState::Memory(io::Cursor::new(buf));
                        }
                        Err(err) => {
                            self.state = FileState::Failed;
                            return Poll::Ready(Err(with_path(err, &self.path)));
                        }
                    }
                }
                FileState::Open(_) | FileState::Memory(_) => return Poll::Ready(Ok(())),
                FileState::Failed => {
                    let err = io::Error::other("open failed");
                    return Poll::Ready(Err(with_path(err, &self.path)));
                }
            }
        }
    }
}

#[cfg(all(feature = "smol", not(feature = "tokio")))]
impl AsyncRead for FsFile {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<io::Result<usize>> {
        // `blocking::Unblock` takes a result of 0 bytes as the end of the
        // file. Then it drops the data that it read ahead, or it stops when
        // its pipe is full. Thus an empty buffer does not go to the reader.
        if buf.is_empty() {
            return Poll::Ready(Ok(0));
        }
        let this = self.get_mut();
        ready!(this.poll_open(cx))?;
        match &mut this.state {
            FileState::Open(reader) => Pin::new(reader).poll_read(cx, buf),
            FileState::Memory(data) => Poll::Ready(data.read(buf)),
            _ => unreachable!(),
        }
    }
}

#[cfg(feature = "tokio")]
impl AsyncRead for FsFile {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        // An empty buffer does not open the file.
        if buf.remaining() == 0 {
            return Poll::Ready(Ok(()));
        }
        let this = self.get_mut();
        ready!(this.poll_open(cx))?;
        match &mut this.state {
            FileState::Open(reader) => Pin::new(reader).poll_read(cx, buf),
            FileState::Memory(data) => Pin::new(data).poll_read(cx, buf),
            _ => unreachable!(),
        }
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::{rt::block_on, testutil::TempDir, FsEntry, FsSource},
        futures::TryStreamExt,
        static_assertions::assert_impl_all,
        std::{
            fs::OpenOptions,
            io::{ErrorKind, Write},
            os::unix::fs::PermissionsExt,
            path::Path,
        },
    };

    #[cfg(all(feature = "smol", not(feature = "tokio")))]
    use smol::io::AsyncReadExt;
    #[cfg(feature = "tokio")]
    use tokio::io::AsyncReadExt;

    assert_impl_all!(FsFile: Send, Sync, Unpin, crate::AsyncRead);

    /// Walk `root` with the prefix `pkg` and give the entry `pkg/f`.
    fn entry(root: &Path) -> FsEntry {
        let entries: Vec<FsEntry> =
            block_on(FsSource::new(root).with_prefix("pkg").try_collect()).unwrap();
        entries
            .into_iter()
            .find(|entry| entry.path() == "pkg/f")
            .unwrap()
    }

    /// Read all of `file`.
    fn read_all(mut file: FsFile) -> io::Result<Vec<u8>> {
        block_on(async {
            let mut data = Vec::new();
            file.read_to_end(&mut data).await?;
            Ok(data)
        })
    }

    /// Data of `len` bytes.
    fn data(len: usize) -> Vec<u8> {
        (0..len).map(|i| (i % 251) as u8).collect()
    }

    #[test]
    fn body_is_read() {
        for len in [100, rt::FILE_BUFFER_SIZE, rt::FILE_BUFFER_SIZE + 100] {
            let tmp = TempDir::new();
            let data = data(len);
            std::fs::write(tmp.path().join("f"), &data).unwrap();
            let entry = entry(tmp.path());
            let mut file = entry.file();
            let mut read = Vec::new();
            block_on(file.read_to_end(&mut read)).unwrap();
            assert_eq!(read, data, "{len}");
            if len <= rt::FILE_BUFFER_SIZE {
                assert!(matches!(file.state, FileState::Memory(_)), "{len}");
            } else {
                assert!(matches!(file.state, FileState::Open(_)), "{len}");
            }
        }
    }

    #[test]
    fn empty_read_keeps_data() {
        for len in [100, rt::FILE_BUFFER_SIZE + 100] {
            let tmp = TempDir::new();
            let data = data(len);
            std::fs::write(tmp.path().join("f"), &data).unwrap();
            let entry = entry(tmp.path());
            let mut file = entry.file();
            let read = block_on(async {
                assert_eq!(file.read(&mut []).await.unwrap(), 0);
                assert!(matches!(file.state, FileState::Closed { .. }));
                let mut read = vec![0; 10];
                file.read_exact(&mut read).await.unwrap();
                assert_eq!(file.read(&mut []).await.unwrap(), 0);
                file.read_to_end(&mut read).await.unwrap();
                read
            });
            assert_eq!(read, data, "{len}");
        }
    }

    #[test]
    fn file_changed_size() {
        let tmp = TempDir::new();
        let path = tmp.path().join("f");
        std::fs::write(&path, b"data").unwrap();
        let entry = entry(tmp.path());
        let mut file = OpenOptions::new().append(true).open(&path).unwrap();
        file.write_all(b"more").unwrap();
        let err = read_all(entry.file()).unwrap_err();
        assert_eq!(err.kind(), ErrorKind::InvalidData);
        assert_eq!(err.to_string(), "pkg/f: file changed");
    }

    #[test]
    fn file_replaced() {
        let tmp = TempDir::new();
        std::fs::write(tmp.path().join("f"), b"data").unwrap();
        let entry = entry(tmp.path());
        std::fs::write(tmp.path().join("new"), b"atad").unwrap();
        std::fs::rename(tmp.path().join("new"), tmp.path().join("f")).unwrap();
        let err = read_all(entry.file()).unwrap_err();
        assert_eq!(err.kind(), ErrorKind::InvalidData);
        assert_eq!(err.to_string(), "pkg/f: file changed");
    }

    #[test]
    fn file_replaced_by_symlink() {
        let tmp = TempDir::new();
        std::fs::write(tmp.path().join("f"), b"data").unwrap();
        let entry = entry(tmp.path());
        std::fs::rename(tmp.path().join("f"), tmp.path().join("g")).unwrap();
        std::os::unix::fs::symlink("g", tmp.path().join("f")).unwrap();
        let err = read_all(entry.file()).unwrap_err();
        assert!(err.to_string().starts_with("pkg/f: "), "{err}");
    }

    #[test]
    fn read_after_failed_open() {
        let tmp = TempDir::new();
        let path = tmp.path().join("f");
        std::fs::write(&path, b"data").unwrap();
        let entry = entry(tmp.path());
        std::fs::write(&path, b"other").unwrap();
        let mut file = entry.file();
        block_on(async {
            let mut buf = [0; 16];
            let err = file.read(&mut buf).await.unwrap_err();
            assert_eq!(err.kind(), ErrorKind::InvalidData);
            assert_eq!(err.to_string(), "pkg/f: file changed");
            let err = file.read(&mut buf).await.unwrap_err();
            assert_eq!(err.kind(), ErrorKind::Other);
            assert_eq!(err.to_string(), "pkg/f: open failed");
        });
    }

    #[test]
    fn dropped_file_is_not_opened() {
        let tmp = TempDir::new();
        let path = tmp.path().join("f");
        std::fs::write(&path, b"data").unwrap();
        let entry = entry(tmp.path());
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o000)).unwrap();
        let file = entry.file();
        assert!(matches!(file.state, FileState::Closed { .. }));
        drop(file);
        // A process that bypasses permission checks (for example, root) can
        // open the file.
        if std::fs::File::open(&path).is_ok() {
            eprintln!("skip: permission checks are bypassed");
            return;
        }
        let err = read_all(entry.file()).unwrap_err();
        assert_eq!(err.kind(), ErrorKind::PermissionDenied);
        assert!(err.to_string().starts_with("pkg/f: "), "{err}");
    }
}
