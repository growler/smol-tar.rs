//! Conversion of filesystem entries to tar entries, with hard link detection.

use {
    super::{file, rt, sys, FsEntry, FsFile, FsKind},
    futures_core::stream::{Stream, TryStream},
    pin_project_lite::pin_project,
    smol_tar::{TarDevice, TarDirectory, TarEntry, TarFifo, TarLink, TarRegularFile, TarSymlink},
    std::{
        collections::{hash_map::Entry, HashMap, VecDeque},
        future::Future,
        io, mem,
        pin::Pin,
        sync::Arc,
        task::{Context, Poll},
        time::{Duration, SystemTime, UNIX_EPOCH},
    },
};

/// Extension of a stream of [`FsEntry`] values.
pub trait FsStreamExt: TryStream<Ok = FsEntry, Error = io::Error> + Sized {
    /// Convert each entry to a [`TarEntry`]. See [`IntoTar`].
    fn into_tar(self) -> IntoTar<Self> {
        IntoTar {
            inner: self,
            links: HashMap::new(),
            queue: VecDeque::new(),
            queue_bytes: 0,
            load: None,
            done: false,
        }
    }
}

impl<S: TryStream<Ok = FsEntry, Error = io::Error>> FsStreamExt for S {}

pin_project! {
    /// Stream of tar entries from a stream of [`FsEntry`] values.
    ///
    /// Use [`FsStreamExt::into_tar`] to make it. Send the entries to a
    /// [`TarWriter`](smol_tar::TarWriter). Each full entry has the archive path,
    /// the mode, the owner, the modification time and the extended attributes
    /// of the node. The body of a regular file is an [`FsFile`].
    ///
    /// # Hard links
    ///
    /// The stream finds the paths of each node that is not a directory and
    /// that has more than one link. The first of these paths gives the full
    /// entry. Each later path gives a [`TarLink`](smol_tar::TarLink) to the
    /// first path. A `TarLink` has no metadata and no extended attributes.
    ///
    /// Filter the entries before `into_tar`. A filter after `into_tar` can
    /// drop the full entry and keep a link to it. Then the archive has a link
    /// to a path that is not in the archive.
    ///
    /// The stream keeps one row in memory for each node with more than one
    /// link. The row has the first path of the node. The stream removes the
    /// row when it gets the last link of the node. If a link of the node is
    /// outside the walked tree, or a filter drops it, the row stays until the
    /// stream is dropped. A row with a 70-byte path uses about 140 bytes, thus
    /// about 140 MB for each million such nodes. This occurs when each file of
    /// the tree has a hard link outside the walked root, for example in an
    /// OSTree deployment checkout or in an optimised Nix store subtree.
    ///
    /// # Read ahead
    ///
    /// The stream takes the entries that the inner stream has ready, up to
    /// 128 entries. It stops at 64 regular files that are not empty and that
    /// have up to 256 KiB, or when the data of these files is 1 MiB or more.
    /// Then one job on the blocking thread pool opens and reads these files.
    /// If the job fails, each of these files opens on its first read. Each
    /// [`FsFile`] of such a file has the data, or the error of the open or the
    /// read. The stream reads a file also if a later filter drops its entry.
    /// A larger file opens on its first read. An empty file does not open.
    ///
    /// The stream gives the entries before the first file that the job reads
    /// while the job runs. When the job ends, the stream starts the next read
    /// ahead if the data of the files that it holds is less than 1 MiB and it
    /// holds no error from the inner stream. Thus the stream holds the data of
    /// no more than two jobs.
    ///
    /// # Modification time
    ///
    /// The entry keeps only the whole seconds of the modification time. A
    /// time before 1970 goes down to the previous whole second.
    /// [`FsEntry::mtime`] has the full precision. The tar writer adds a pax
    /// record for each time with a part smaller than one second. A time before
    /// 1970, or a time that does not fit in the 12-byte octal field of the
    /// header (2^33 seconds after 1970 or later), also gets a pax record.
    ///
    /// # Errors
    ///
    /// An error from the inner stream passes through without change. The
    /// stream does not poll the inner stream after an error until the stream
    /// gives the error.
    ///
    /// With the `tokio` feature, poll the stream and read the file bodies
    /// inside a Tokio runtime context.
    pub struct IntoTar<S> {
        #[pin]
        inner: S,
        // File identity to the first archive path and the number of links
        // that did not come yet.
        links: HashMap<sys::FileId, (Box<str>, u64)>,
        // Entries that the stream read ahead, in order.
        queue: VecDeque<Ahead>,
        // Sum of the sizes of the `Ahead::Small` entries in `queue`.
        queue_bytes: u64,
        // Read of the small files in `queue` on the blocking thread pool.
        load: Option<LoadFuture>,
        // The inner stream ended.
        done: bool,
    }
}

/// Entry that the stream read ahead.
enum Ahead {
    Entry(TarEntry<'static, FsFile>),
    /// Regular file of 1 byte up to `FILE_BUFFER_SIZE`, and the data or the
    /// error of its read. `None` until the load job ends.
    Small(FsEntry, Option<io::Result<Vec<u8>>>),
    Err(io::Error),
}

/// Small file to read: the directory that holds it, its name, its identity
/// and its size.
type SmallFile = (Arc<sys::Dir>, Box<str>, sys::FileId, u64);

/// Read of small files on the blocking thread pool. It gives one result for
/// each file, in order.
type LoadFuture = Pin<Box<dyn Future<Output = io::Result<Vec<io::Result<Vec<u8>>>>> + Send>>;

/// Maximum number of entries in the queue of `IntoTar`.
const MAX_AHEAD: usize = 2 * rt::BATCH_FILES;

impl<S: TryStream<Ok = FsEntry, Error = io::Error>> Stream for IntoTar<S> {
    type Item = io::Result<TarEntry<'static, FsFile>>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.project();
        loop {
            if let Some(load) = this.load.as_mut() {
                if let Poll::Ready(result) = load.as_mut().poll(cx) {
                    *this.load = None;
                    fill(this.queue, this.queue_bytes, result);
                }
            }
            // Do not poll the inner stream past an error that the consumer
            // did not take. An error is always the last item in the queue.
            if this.load.is_none()
                && !*this.done
                && *this.queue_bytes < rt::BATCH_BYTES as u64
                && !matches!(this.queue.back(), Some(Ahead::Err(_)))
            {
                let small = read_ahead(
                    this.inner.as_mut(),
                    this.links,
                    this.queue,
                    this.queue_bytes,
                    this.done,
                    cx,
                );
                if !small.is_empty() {
                    // The job starts on the first poll, after the store in
                    // `load`. With `tokio`, the start panics outside a
                    // runtime. Then each later poll panics again, and the
                    // files in the queue do not get the data of other files.
                    *this.load = Some(Box::pin(async move {
                        rt::unblock(move || {
                            Ok(small
                                .into_iter()
                                .map(|(parent, name, id, size)| {
                                    file::read_small(&parent, &name, id, size)
                                })
                                .collect())
                        })
                        .await
                    }));
                    // Poll the new job, so that its end wakes the task.
                    continue;
                }
            }
            // A file that waits for the load job is in the queue only while
            // the job runs, and the poll above registered the task.
            return match this.queue.front() {
                None if *this.done => Poll::Ready(None),
                None | Some(Ahead::Small(_, None)) => Poll::Pending,
                Some(_) => Poll::Ready(this.queue.pop_front().map(|item| match item {
                    Ahead::Entry(entry) => Ok(entry),
                    Ahead::Small(entry, data) => {
                        *this.queue_bytes -= entry.meta.size;
                        Ok(tar_entry(entry, data))
                    }
                    Ahead::Err(err) => Err(err),
                })),
            };
        }
    }
}

/// Give the result of a load job to the files in `queue` that wait for it.
/// If the job failed, each of these files opens on its first read.
fn fill(
    queue: &mut VecDeque<Ahead>,
    queue_bytes: &mut u64,
    result: io::Result<Vec<io::Result<Vec<u8>>>>,
) {
    match result {
        Ok(data) => {
            let mut data = data.into_iter();
            for item in queue.iter_mut() {
                if let Ahead::Small(_, slot @ None) = item {
                    *slot = data.next();
                }
            }
        }
        Err(_) => {
            *queue = mem::take(queue)
                .into_iter()
                .map(|item| match item {
                    Ahead::Small(entry, None) => {
                        *queue_bytes -= entry.meta.size;
                        Ahead::Entry(tar_entry(entry, None))
                    }
                    item => item,
                })
                .collect();
        }
    }
}

/// Move the entries that `inner` has ready to `queue`. Stop at an error, at
/// the end of `inner`, at `MAX_AHEAD` entries in `queue`, at `BATCH_FILES`
/// small files, or when the small files have `BATCH_BYTES` or more. Add the
/// sizes of the small files to `queue_bytes`. Give the small files to read.
fn read_ahead<S: TryStream<Ok = FsEntry, Error = io::Error>>(
    mut inner: Pin<&mut S>,
    links: &mut HashMap<sys::FileId, (Box<str>, u64)>,
    queue: &mut VecDeque<Ahead>,
    queue_bytes: &mut u64,
    done: &mut bool,
    cx: &mut Context<'_>,
) -> Vec<SmallFile> {
    let mut small = Vec::new();
    let mut bytes = 0;
    while queue.len() < MAX_AHEAD && small.len() < rt::BATCH_FILES && bytes < rt::BATCH_BYTES as u64
    {
        let entry = match inner.as_mut().try_poll_next(cx) {
            Poll::Pending => break,
            Poll::Ready(None) => {
                *done = true;
                break;
            }
            Poll::Ready(Some(Err(err))) => {
                queue.push_back(Ahead::Err(err));
                break;
            }
            Poll::Ready(Some(Ok(entry))) => entry,
        };
        let size = entry.meta.size;
        if let Some(link) = link_entry(links, &entry) {
            queue.push_back(Ahead::Entry(link));
        } else if entry.meta.kind == FsKind::File && size > 0 && size <= rt::FILE_BUFFER_SIZE as u64
        {
            bytes += size;
            small.push((
                Arc::clone(&entry.parent),
                entry.name.clone(),
                entry.meta.id,
                size,
            ));
            queue.push_back(Ahead::Small(entry, None));
        } else {
            queue.push_back(Ahead::Entry(tar_entry(entry, None)));
        }
    }
    *queue_bytes += bytes;
    small
}

/// Give a `TarLink` if `entry` is a later path of a node with more than one
/// link. Keep the first path of each such node in `links`.
fn link_entry(
    links: &mut HashMap<sys::FileId, (Box<str>, u64)>,
    entry: &FsEntry,
) -> Option<TarEntry<'static, FsFile>> {
    if entry.meta.kind == FsKind::Directory || entry.meta.nlink <= 1 {
        return None;
    }
    match links.entry(entry.meta.id) {
        Entry::Occupied(mut row) => {
            let first = if row.get().1 <= 1 {
                row.remove().0
            } else {
                let (first, remaining) = row.get_mut();
                *remaining -= 1;
                first.clone()
            };
            Some(TarLink::new(entry.path.clone(), first).into())
        }
        Entry::Vacant(row) => {
            row.insert((entry.path.clone(), entry.meta.nlink - 1));
            None
        }
    }
}

/// Set the mode, the owner, the modification time and the extended
/// attributes of the `Meta` value `$meta` on the tar entry `$tar`. Give a
/// `TarEntry`.
macro_rules! with_meta {
    ($tar:expr, $meta:expr) => {{
        let tar = $tar;
        let meta = $meta;
        let tar = tar
            .with_mode(meta.mode)
            .with_uid(meta.uid)
            .with_gid(meta.gid)
            .with_mtime(whole_seconds(meta.mtime));
        if meta.xattrs.is_empty() {
            tar.into()
        } else {
            tar.with_attrs(meta.xattrs).into()
        }
    }};
}

/// Make the full tar entry of `entry`. `data` is the result of a read ahead
/// of the file data. Without it, the file opens on its first read.
fn tar_entry(entry: FsEntry, data: Option<io::Result<Vec<u8>>>) -> TarEntry<'static, FsFile> {
    // The source gives a link for each symlink and device numbers for each
    // device, thus the defaults are not used.
    match entry.meta.kind {
        FsKind::File => {
            let FsEntry {
                path,
                meta,
                parent,
                name,
                ..
            } = entry;
            let body = match data {
                Some(data) => FsFile::loaded(data, path.clone()),
                None => FsFile::new(parent, name, meta.id, meta.size, path.clone()),
            };
            with_meta!(TarRegularFile::new(path, meta.size, body), meta)
        }
        FsKind::Directory => with_meta!(TarDirectory::new(entry.path), entry.meta),
        FsKind::Symlink => with_meta!(
            TarSymlink::new(entry.path, entry.link.unwrap_or_default()),
            entry.meta
        ),
        FsKind::Fifo => with_meta!(TarFifo::new(entry.path), entry.meta),
        FsKind::CharDevice => {
            let (major, minor) = entry.meta.device.unwrap_or_default();
            with_meta!(TarDevice::new_char(entry.path, major, minor), entry.meta)
        }
        FsKind::BlockDevice => {
            let (major, minor) = entry.meta.device.unwrap_or_default();
            with_meta!(TarDevice::new_block(entry.path, major, minor), entry.meta)
        }
    }
}

/// Remove the part of `time` that is smaller than one second. A time before
/// the epoch goes down to the previous whole second.
fn whole_seconds(time: SystemTime) -> SystemTime {
    let whole = match time.duration_since(UNIX_EPOCH) {
        Ok(after) => UNIX_EPOCH.checked_add(Duration::from_secs(after.as_secs())),
        Err(err) => {
            let before = err.duration();
            before
                .as_secs()
                .checked_add(u64::from(before.subsec_nanos() != 0))
                .and_then(|secs| UNIX_EPOCH.checked_sub(Duration::from_secs(secs)))
        }
    };
    // A whole second that the platform cannot hold keeps the time.
    whole.unwrap_or(time)
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::{rt::block_on, testutil::TempDir, FsSource},
        futures::{stream, task::noop_waker_ref, StreamExt, TryStreamExt},
        static_assertions::assert_impl_all,
        std::{fs, path::Path},
    };

    #[cfg(all(feature = "smol", not(feature = "tokio")))]
    use smol::io::AsyncReadExt;
    #[cfg(feature = "tokio")]
    use tokio::io::AsyncReadExt;

    /// Give the entries of the walk of `root` with the prefix `pkg`.
    fn source_entries(root: &Path) -> Vec<FsEntry> {
        block_on(FsSource::new(root).with_prefix("pkg").try_collect()).unwrap()
    }

    /// Give the archive paths of the walk of `root` with the prefix `pkg`.
    fn source_paths(root: &Path) -> Vec<String> {
        source_entries(root)
            .iter()
            .map(|entry| entry.path().to_owned())
            .collect()
    }

    /// Data of `len` bytes that depends on `seed`.
    fn data(len: usize, seed: usize) -> Vec<u8> {
        (0..len).map(|i| ((i + seed) % 251) as u8).collect()
    }

    assert_impl_all!(IntoTar<FsSource>: Send, Unpin, Stream);

    #[test]
    fn links_table_empty_after_last_link() {
        let tmp = TempDir::new();
        std::fs::write(tmp.path().join("a"), b"data").unwrap();
        std::fs::hard_link(tmp.path().join("a"), tmp.path().join("b")).unwrap();
        std::fs::hard_link(tmp.path().join("a"), tmp.path().join("c")).unwrap();
        let mut tar = FsSource::new(tmp.path()).into_tar();
        let mut kinds = Vec::new();
        block_on(async {
            while let Some(entry) = tar.next().await {
                kinds.push(matches!(entry.unwrap(), TarEntry::Link(_)));
            }
        });
        assert_eq!(kinds, [false, true, true]);
        assert!(tar.links.is_empty());
    }

    #[test]
    fn read_ahead_keeps_order_and_data() {
        let tmp = TempDir::new();
        let root = tmp.path();
        // More small files than one batch holds, more bytes than one batch
        // holds, a file larger than the buffer, and a hard link.
        for i in 0..150 {
            let dir = root.join(format!("d{}", i % 3));
            fs::create_dir_all(&dir).unwrap();
            fs::write(dir.join(format!("f{i:03}")), data(1 + i % 7, i)).unwrap();
        }
        for i in 0..6 {
            fs::write(root.join(format!("big{i}")), data(200 * 1024, i)).unwrap();
        }
        fs::write(root.join("huge"), data(rt::FILE_BUFFER_SIZE + 1, 0)).unwrap();
        fs::hard_link(root.join("big0"), root.join("d0/link")).unwrap();
        let expected = source_paths(root);
        let mut paths = Vec::new();
        block_on(async {
            let mut tar = FsSource::new(root).with_prefix("pkg").into_tar();
            while let Some(entry) = tar.next().await {
                let entry = entry.unwrap();
                let path = entry.path().to_owned();
                if let TarEntry::File(mut file) = entry {
                    let mut body = Vec::new();
                    file.read_to_end(&mut body).await.unwrap();
                    let fs_path = root.join(path.strip_prefix("pkg/").unwrap());
                    assert_eq!(body, fs::read(fs_path).unwrap(), "{path}");
                }
                paths.push(path);
            }
        });
        assert_eq!(paths, expected);
    }

    #[test]
    fn read_ahead_error_has_path() {
        let tmp = TempDir::new();
        let path = tmp.path().join("f");
        fs::write(&path, b"data").unwrap();
        let entries = source_entries(tmp.path());
        fs::write(&path, b"other").unwrap();
        block_on(async {
            let mut tar = stream::iter(entries.into_iter().map(Ok::<_, io::Error>)).into_tar();
            let mut file = loop {
                if let TarEntry::File(file) = tar.next().await.unwrap().unwrap() {
                    break file;
                }
            };
            let mut buf = [0; 16];
            let err = file.read(&mut buf).await.unwrap_err();
            assert_eq!(err.kind(), io::ErrorKind::InvalidData);
            assert_eq!(err.to_string(), "pkg/f: file changed");
            let err = file.read(&mut buf).await.unwrap_err();
            assert_eq!(err.to_string(), "pkg/f: open failed");
        });
    }

    /// A failed read-ahead job keeps the entries in the queue. Each small
    /// file opens on its first read.
    #[test]
    fn failed_load_keeps_entries() {
        let tmp = TempDir::new();
        let root = tmp.path();
        fs::create_dir(root.join("d")).unwrap();
        fs::write(root.join("a"), data(5, 1)).unwrap();
        fs::write(root.join("d/b"), data(7, 2)).unwrap();
        fs::write(root.join("d/c"), data(9, 3)).unwrap();
        let entries = source_entries(root);
        let expected: Vec<String> = entries.iter().map(|e| e.path().to_owned()).collect();
        let queue_bytes = entries.iter().map(|entry| entry.meta.size).sum();
        // The read-ahead job fails, for example when the runtime cancels it.
        let queue = entries
            .into_iter()
            .map(|entry| {
                if entry.meta.kind == FsKind::File {
                    Ahead::Small(entry, None)
                } else {
                    Ahead::Entry(tar_entry(entry, None))
                }
            })
            .collect();
        let load: LoadFuture = Box::pin(async { Err(io::Error::other("cancelled")) });
        let mut tar = IntoTar {
            inner: stream::empty::<io::Result<FsEntry>>(),
            links: HashMap::new(),
            queue,
            queue_bytes,
            load: Some(load),
            done: true,
        };
        let mut paths = Vec::new();
        block_on(async {
            while let Some(entry) = tar.next().await {
                let entry = entry.unwrap();
                let path = entry.path().to_owned();
                if let TarEntry::File(mut file) = entry {
                    let mut body = Vec::new();
                    file.read_to_end(&mut body).await.unwrap();
                    let fs_path = root.join(path.strip_prefix("pkg/").unwrap());
                    assert_eq!(body, fs::read(fs_path).unwrap(), "{path}");
                }
                paths.push(path);
            }
        });
        assert_eq!(paths, expected);
    }

    /// An empty file is not read ahead, because the tar writer does not read
    /// its body.
    #[test]
    fn empty_file_is_not_read_ahead() {
        let tmp = TempDir::new();
        fs::write(tmp.path().join("empty"), b"").unwrap();
        fs::write(tmp.path().join("full"), b"data").unwrap();
        let mut inner = stream::iter(source_entries(tmp.path()).into_iter().map(Ok));
        let (mut queue, mut queue_bytes, mut done) = (VecDeque::new(), 0, false);
        let small = read_ahead(
            Pin::new(&mut inner),
            &mut HashMap::new(),
            &mut queue,
            &mut queue_bytes,
            &mut done,
            &mut Context::from_waker(noop_waker_ref()),
        );
        let names: Vec<&str> = small.iter().map(|file| &*file.1).collect();
        assert_eq!(names, ["full"]);
        assert_eq!(queue_bytes, 4);
        assert!(done);
    }

    /// The queue has no more than `MAX_AHEAD` entries, also when the inner
    /// stream has more entries ready and none of them is a small file.
    #[test]
    fn read_ahead_limits_queue() {
        let tmp = TempDir::new();
        for i in 0..MAX_AHEAD * 2 {
            fs::create_dir(tmp.path().join(format!("d{i}"))).unwrap();
        }
        let mut inner = stream::iter(source_entries(tmp.path()).into_iter().map(Ok));
        let (mut queue, mut queue_bytes, mut done) = (VecDeque::new(), 0, false);
        let small = read_ahead(
            Pin::new(&mut inner),
            &mut HashMap::new(),
            &mut queue,
            &mut queue_bytes,
            &mut done,
            &mut Context::from_waker(noop_waker_ref()),
        );
        assert!(small.is_empty());
        assert_eq!(queue.len(), MAX_AHEAD);
        assert!(!done);
    }

    /// The stream gives the entries before the first file of a load job
    /// while the job runs.
    #[test]
    fn entries_before_load_do_not_wait() {
        let tmp = TempDir::new();
        fs::create_dir(tmp.path().join("d")).unwrap();
        fs::write(tmp.path().join("d/f"), b"data").unwrap();
        let mut entries = source_entries(tmp.path());
        let file = entries.pop().unwrap();
        let queue = entries
            .into_iter()
            .map(|entry| Ahead::Entry(tar_entry(entry, None)))
            .chain([Ahead::Small(file, None)])
            .collect::<VecDeque<_>>();
        let count = queue.len() - 1;
        // The job does not end.
        let load: LoadFuture = Box::pin(std::future::pending());
        let mut tar = IntoTar {
            inner: stream::empty::<io::Result<FsEntry>>(),
            links: HashMap::new(),
            queue,
            queue_bytes: 4,
            load: Some(load),
            done: true,
        };
        let mut cx = Context::from_waker(noop_waker_ref());
        for _ in 0..count {
            let entry = Pin::new(&mut tar).poll_next(&mut cx);
            assert!(matches!(
                entry,
                Poll::Ready(Some(Ok(TarEntry::Directory(_))))
            ));
        }
        assert!(Pin::new(&mut tar).poll_next(&mut cx).is_pending());
    }

    /// A poll outside a Tokio runtime panics. A later poll inside a runtime
    /// panics again or gives the correct data.
    #[cfg(feature = "tokio")]
    #[test]
    fn poll_after_panic_outside_runtime() {
        use std::panic::{catch_unwind, AssertUnwindSafe};
        let tmp = TempDir::new();
        // More small files than one load job reads. The body of each file
        // is its name.
        for i in 0..rt::BATCH_FILES + 6 {
            fs::write(tmp.path().join(format!("f{i:03}")), format!("f{i:03}")).unwrap();
        }
        let entries = source_entries(tmp.path());
        let mut tar = stream::iter(entries.into_iter().map(Ok::<_, io::Error>)).into_tar();
        let mut cx = Context::from_waker(noop_waker_ref());
        let first = catch_unwind(AssertUnwindSafe(|| Pin::new(&mut tar).poll_next(&mut cx)));
        assert!(first.is_err());
        let again = catch_unwind(AssertUnwindSafe(|| {
            block_on(async {
                loop {
                    if let TarEntry::File(mut file) = tar.next().await.unwrap().unwrap() {
                        let path = file.path().to_owned();
                        let mut body = String::new();
                        file.read_to_string(&mut body).await.unwrap();
                        break (path, body);
                    }
                }
            })
        }));
        if let Ok((path, body)) = again {
            assert_eq!(path.strip_prefix("pkg/"), Some(body.as_str()));
        }
    }

    #[test]
    fn whole_seconds_floors() {
        let cases = [
            (
                UNIX_EPOCH + Duration::new(5, 500_000_000),
                UNIX_EPOCH + Duration::from_secs(5),
            ),
            (
                UNIX_EPOCH - Duration::new(1, 500_000_000),
                UNIX_EPOCH - Duration::from_secs(2),
            ),
            (
                UNIX_EPOCH + Duration::from_secs(7),
                UNIX_EPOCH + Duration::from_secs(7),
            ),
            (
                UNIX_EPOCH - Duration::from_secs(3),
                UNIX_EPOCH - Duration::from_secs(3),
            ),
        ];
        for (time, expected) in cases {
            assert_eq!(whole_seconds(time), expected, "{time:?}");
        }
    }

    /// The stream does not poll the inner stream past an error until it
    /// gives the error. Then it continues with the next entries.
    #[test]
    fn read_ahead_stops_at_error() {
        let tmp = TempDir::new();
        for i in 0..4 {
            fs::create_dir(tmp.path().join(format!("d{i}"))).unwrap();
        }
        let mut entries = source_entries(tmp.path()).into_iter().map(Ok);
        let mut items: Vec<io::Result<FsEntry>> = entries.by_ref().take(2).collect();
        items.push(Err(io::Error::other("test")));
        items.extend(entries);
        let pulled = std::cell::Cell::new(0);
        let mut tar = stream::iter(items)
            .inspect(|_| pulled.set(pulled.get() + 1))
            .into_tar();
        let mut cx = Context::from_waker(noop_waker_ref());
        for _ in 0..2 {
            let entry = Pin::new(&mut tar).poll_next(&mut cx);
            assert!(matches!(entry, Poll::Ready(Some(Ok(_)))));
        }
        let entry = Pin::new(&mut tar).poll_next(&mut cx);
        assert!(matches!(entry, Poll::Ready(Some(Err(_)))));
        assert_eq!(pulled.get(), 3);
        let mut rest = 0;
        while let Poll::Ready(Some(entry)) = Pin::new(&mut tar).poll_next(&mut cx) {
            entry.unwrap();
            rest += 1;
        }
        assert_eq!(rest, 3);
    }

    /// The stream does not poll the inner stream past an error after a load
    /// job ends. Then it continues with the next entries.
    #[test]
    fn load_stops_at_error() {
        let tmp = TempDir::new();
        fs::write(tmp.path().join("f"), b"data").unwrap();
        for i in 0..2 {
            fs::create_dir(tmp.path().join(format!("d{i}"))).unwrap();
        }
        let (files, dirs): (Vec<_>, Vec<_>) = source_entries(tmp.path())
            .into_iter()
            .partition(|entry| entry.meta.kind == FsKind::File);
        let rest = dirs.len();
        let items: Vec<io::Result<FsEntry>> = files
            .into_iter()
            .map(Ok)
            .chain([Err(io::Error::other("test"))])
            .chain(dirs.into_iter().map(Ok))
            .collect();
        let pulled = std::cell::Cell::new(0);
        let mut tar = stream::iter(items)
            .inspect(|_| pulled.set(pulled.get() + 1))
            .into_tar();
        block_on(async {
            assert!(matches!(tar.next().await, Some(Ok(TarEntry::File(_)))));
            assert!(matches!(tar.next().await, Some(Err(_))));
            assert_eq!(pulled.get(), 2);
            assert_eq!(tar.count().await, rest);
        });
    }
}
