//! Body of a regular file from the source. The file opens on its first read.

use {
    super::{rt, sys, with_path},
    std::{
        future::Future,
        io::{self, BufRead, Read},
        mem,
        pin::Pin,
        sync::Arc,
        task::{ready, Context, Poll},
    },
};

#[cfg(all(feature = "smol", not(feature = "tokio")))]
use futures_io::{AsyncBufRead, AsyncRead};
#[cfg(feature = "tokio")]
use tokio::io::{AsyncBufRead, AsyncRead, ReadBuf};

/// Body of a regular file from [`FsSource`](super::FsSource).
///
/// The type implements `AsyncRead` and `AsyncBufRead`. [`IntoTar`](crate::IntoTar) reads a
/// file that is not empty and that has up to 256 KiB before the first read,
/// together with other files. See [Read ahead](crate::IntoTar#read-ahead).
/// An empty file or a larger file opens on the first read. If its `FsFile`
/// is dropped before the first read, the file is not opened. A read into an
/// empty buffer gives 0 bytes and does not open the file.
///
/// The open does not follow a symlink. The open compares the identity and the
/// size of the file with the values that the source read. A difference gives
/// an [`InvalidData`] error with the message "file changed".
///
/// The size that the source read controls how the file is read. A file of
/// up to 256 KiB is read in one job, then the file closes. The data stays
/// in memory until the `FsFile` is dropped. A larger file
/// stays open until the `FsFile` is dropped, and the data is read in parts
/// of up to 256 KiB. `poll_fill_buf` gives the data that is in memory, or
/// the data of the last part, without a copy.
/// The reads do not go past the size that the source read. If a file gets
/// smaller after the open, the read gives an [`InvalidData`] error with the
/// message "file changed".
///
/// An error from the open has the archive path of the entry. The open also
/// reads all data of a file of up to 256 KiB, and the first 256 KiB of a
/// larger file. Thus an error from these reads also has the archive path.
/// The first read gives the error of the open. After an error from the open
/// or from a read, each read gives an error.
///
/// The open and the reads run on the blocking thread pool of the runtime.
/// The close of the file can occur on the thread that drops the `FsFile`.
/// With the `tokio` feature, read the file inside a Tokio runtime context.
///
/// # Panics
///
/// With the `tokio` feature, a read outside a Tokio runtime context panics.
/// After that panic, do not use the `FsFile` again.
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
    /// File larger than `FILE_BUFFER_SIZE`, to read in chunks. The reader
    /// has the data of the first chunk.
    File(rt::FileReader),
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
    Opening(OpenFuture),
    /// The file is open.
    Open(rt::FileReader),
    /// All data of the file is in memory.
    Memory(io::Cursor<Vec<u8>>),
    /// The read ahead gave an error. The first read gives it.
    Error(io::Error),
    /// The open gave an error.
    Failed,
}

/// Open `name` in `parent`. If `size` is not more than `FILE_BUFFER_SIZE`,
/// read all data of the file and close it. Else read the first chunk.
fn open(parent: &sys::Dir, name: &str, id: sys::FileId, size: u64) -> io::Result<Opened> {
    let file = sys::open_file(parent, name, id, size)?;
    if size > rt::FILE_BUFFER_SIZE as u64 {
        return rt::file_reader_with_first_chunk(file, size).map(Opened::File);
    }
    read_data(file, size).map(Opened::Bytes)
}

/// Open `name` in `parent` and read all data of the file. `size` must not be
/// more than `FILE_BUFFER_SIZE`.
pub(crate) fn read_small(
    parent: &sys::Dir,
    name: &str,
    id: sys::FileId,
    size: u64,
) -> io::Result<Vec<u8>> {
    read_data(sys::open_file(parent, name, id, size)?, size)
}

/// Read all `size` bytes of the open file `file`.
fn read_data(mut file: std::fs::File, size: u64) -> io::Result<Vec<u8>> {
    let mut buf = vec![0; size as usize];
    match file.read_exact(&mut buf) {
        Ok(()) => Ok(buf),
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

    /// Make a file with the result of a read ahead of all its data. `path` is
    /// the archive path.
    pub(crate) fn loaded(data: io::Result<Vec<u8>>, path: Box<str>) -> Self {
        let state = match data {
            Ok(data) => FileState::Memory(io::Cursor::new(data)),
            Err(err) => FileState::Error(err),
        };
        Self { state, path }
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
                    self.state = FileState::Opening(Box::pin(rt::unblock(move || {
                        open(&parent, &name, id, size)
                    })));
                }
                FileState::Opening(fut) => match ready!(fut.as_mut().poll(cx)) {
                    Ok(Opened::File(reader)) => {
                        self.state = FileState::Open(reader);
                    }
                    Ok(Opened::Bytes(buf)) => {
                        self.state = FileState::Memory(io::Cursor::new(buf));
                    }
                    Err(err) => {
                        self.state = FileState::Failed;
                        return Poll::Ready(Err(with_path(err, &self.path)));
                    }
                },
                FileState::Open(_) | FileState::Memory(_) => return Poll::Ready(Ok(())),
                FileState::Error(_) => {
                    let FileState::Error(err) = mem::replace(&mut self.state, FileState::Failed)
                    else {
                        unreachable!()
                    };
                    return Poll::Ready(Err(with_path(err, &self.path)));
                }
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
        // An empty buffer does not open the file.
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

impl AsyncBufRead for FsFile {
    fn poll_fill_buf(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<&[u8]>> {
        let this = self.get_mut();
        ready!(this.poll_open(cx))?;
        match &mut this.state {
            FileState::Open(reader) => Pin::new(reader).poll_fill_buf(cx),
            FileState::Memory(data) => Poll::Ready(data.fill_buf()),
            _ => unreachable!(),
        }
    }

    fn consume(self: Pin<&mut Self>, amt: usize) {
        match &mut self.get_mut().state {
            FileState::Open(reader) => Pin::new(reader).consume(amt),
            FileState::Memory(data) => data.consume(amt),
            _ => {}
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
            time::Duration,
        },
    };

    #[cfg(all(feature = "smol", not(feature = "tokio")))]
    use smol::io::{AsyncBufReadExt, AsyncReadExt};
    #[cfg(feature = "tokio")]
    use tokio::io::{AsyncBufReadExt, AsyncReadExt};

    assert_impl_all!(FsFile: Send, Sync, Unpin, crate::AsyncRead, AsyncBufRead);

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

    /// The open of a larger file also reads the first part. The poll that
    /// ends the open gives the data, and the read of the second part runs.
    #[test]
    fn open_reads_first_part() {
        let tmp = TempDir::new();
        let path = tmp.path().join("f");
        std::fs::write(&path, data(3 * rt::FILE_BUFFER_SIZE)).unwrap();
        let entry = entry(tmp.path());
        let mut file = entry.file();
        block_on(async {
            let mut cx = Context::from_waker(futures::task::noop_waker_ref());
            let first = loop {
                match Pin::new(&mut file).poll_fill_buf(&mut cx) {
                    Poll::Ready(buf) => break buf.unwrap().len(),
                    Poll::Pending => {
                        let opening = matches!(file.state, FileState::Opening(_));
                        assert!(opening, "open ended without data");
                        std::thread::sleep(Duration::from_millis(1));
                    }
                }
            };
            assert_eq!(first, rt::FILE_BUFFER_SIZE);
            // Do not consume. The file offset shows the read of the second
            // part.
            #[cfg(target_os = "linux")]
            {
                use std::time::Instant;
                let fd = std::fs::read_dir("/proc/self/fd")
                    .unwrap()
                    .map(|fd| fd.unwrap().path())
                    .find(|fd| std::fs::read_link(fd).is_ok_and(|target| target == path))
                    .unwrap();
                let info = Path::new("/proc/self/fdinfo").join(fd.file_name().unwrap());
                let deadline = Instant::now() + Duration::from_secs(10);
                loop {
                    let info = std::fs::read_to_string(&info).unwrap();
                    let pos: u64 = info
                        .lines()
                        .find_map(|line| line.strip_prefix("pos:"))
                        .unwrap()
                        .trim()
                        .parse()
                        .unwrap();
                    if pos >= 2 * rt::FILE_BUFFER_SIZE as u64 {
                        break;
                    }
                    assert!(Instant::now() < deadline, "no read ahead");
                    std::thread::sleep(Duration::from_millis(1));
                }
            }
        });
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

    /// A file larger than 256 KiB that gets smaller after the open gives the
    /// same error kind as a file of up to 256 KiB.
    #[test]
    fn large_file_gets_smaller_after_open() {
        let tmp = TempDir::new();
        let path = tmp.path().join("f");
        let data = data(3 * rt::FILE_BUFFER_SIZE + 100);
        std::fs::write(&path, &data).unwrap();
        let entry = entry(tmp.path());
        let mut file = entry.file();
        block_on(async {
            // The open compares the size and reads the first part.
            assert_eq!(file.fill_buf().await.unwrap().len(), rt::FILE_BUFFER_SIZE);
            OpenOptions::new()
                .write(true)
                .open(&path)
                .unwrap()
                .set_len(rt::FILE_BUFFER_SIZE as u64)
                .unwrap();
            let mut read = Vec::new();
            let err = file.read_to_end(&mut read).await.unwrap_err();
            assert_eq!(err.kind(), ErrorKind::InvalidData);
            assert_eq!(err.to_string(), "file changed");
            assert!(data.starts_with(&read));
            assert!(file.read(&mut [0; 16]).await.is_err());
        });
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
