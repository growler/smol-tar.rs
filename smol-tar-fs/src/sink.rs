//! Filesystem sink: writes tar entries to a directory tree.

use {
    super::{
        resolve, rt, sys, with_path, DeviceNodes, Existing, FsKind, MissingLinkTarget, SetMeta,
        UnsupportedXattr,
    },
    crate::{poll_read_compat, AsyncRead, AsyncWrite},
    futures_sink::Sink,
    smol_tar::{AttrList, TarEntry, TarRegularFile},
    std::{
        borrow::Cow,
        collections::HashSet,
        future::{poll_fn, Future},
        io::{self, Write},
        marker::PhantomData,
        mem,
        path::{Path, PathBuf},
        pin::Pin,
        task::{ready, Context, Poll},
        time::SystemTime,
    },
};

/// Sink that writes tar entries to a directory tree.
///
/// The sink takes the [`TarEntry`] values of an archive, for example from a
/// [`TarReader`](smol_tar::TarReader), and creates one node in the destination
/// directory for each entry. Use it with `forward`, or call
/// [`write`](Self::write) for each entry and then [`finish`](Self::finish).
///
/// # Paths
///
/// - The destination directory must exist. It can be a symlink to a
///   directory.
/// - The sink removes empty components and `.` components from each entry
///   path.
/// - A path that starts with `/`, or a path with a `..` component, gives an
///   [`InvalidData`] error.
/// - A directory entry with no components, for example `./`, is the
///   destination itself. The sink does not change the destination for it.
///   Another entry kind with no components gives an [`InvalidData`] error.
/// - The sink creates missing parent directories with mode 0o755. If the
///   parent of such a directory has the setgid bit, the new directory also
///   gets this bit and keeps it.
///
/// # Safety rules
///
/// - The sink opens each parent component without following symlinks. A
///   symlink in a parent component gives an error.
/// - If a node that is not a directory has the name of the entry, the sink
///   removes it and creates a new node. The sink never writes through an
///   existing node.
/// - If a directory has the name of the entry, a directory entry uses it.
///   Another entry kind gives an [`AlreadyExists`] error.
/// - The sink stores the target of a symlink without change. It never
///   follows a symlink target.
/// - The sink creates a hard link only to a path that it created before from
///   the same archive. The target must not be a directory. For another
///   target, [`with_missing_link_target`](Self::with_missing_link_target)
///   sets the action. A link to its own path does nothing.
///
/// # Metadata
///
/// - The sink sets the mode, the modification time and the owner of each
///   node after it writes the data. The umask does not change the mode.
/// - The mode keeps the permission bits (0o777). With
///   [`with_special_bits`](Self::with_special_bits), it keeps the setuid,
///   setgid and sticky bits (0o7777). Without it, the directory of a
///   directory entry loses at [`finish`](Self::finish) the setgid bit that
///   it gets from its parent.
/// - A new directory has mode 0o700 until [`finish`](Self::finish). Then
///   the sink sets the metadata of each directory, deepest first. Thus a
///   read-only directory does not stop the extraction of its children, and
///   the children do not change the modification time of the directory. If
///   the archive has more than one entry for a directory, the last entry
///   sets the metadata.
/// - The sink sets the owner only with [`with_owner`](Self::with_owner).
///   The owner change usually needs privileges.
/// - The sink writes extended attributes only with
///   [`with_xattrs`](Self::with_xattrs).
///   [`with_unsupported_xattr`](Self::with_unsupported_xattr) sets the
///   action for an attribute that the sink cannot write.
/// - A hard link gets no metadata and no extended attributes, because it
///   shares the node of its target.
///
/// # Finish
///
/// Call [`finish`](Self::finish) or `close` after the last entry. `forward`
/// does this. If the sink is dropped before, the directories keep mode
/// 0o700 and the modification time of the extraction.
///
/// # Memory
///
/// The sink keeps the path of each node that is not a directory, for the
/// hard link check. It keeps the metadata and the extended attributes of
/// each directory entry until [`finish`](Self::finish). A path of 70 bytes
/// uses about 100 bytes, thus about 100 MB for each million nodes. The sink
/// also has one buffer of 256 KiB for file bodies. A file body of up to
/// 256 KiB is read into this buffer and written to the file in one job.
///
/// # Runtime
///
/// The sink runs the filesystem calls on the blocking thread pool of the
/// runtime. The close of a file or a directory can occur on the thread that
/// polls or drops the sink. With the `tokio` feature, poll the sink inside a
/// Tokio runtime context.
///
/// # Errors
///
/// - [`InvalidData`]: the entry path starts with `/`, has a `..`
///   component, or has no components.
/// - [`InvalidInput`]: the entry path or the link target has a NUL byte,
///   or the platform cannot encode the device numbers or the modification
///   time.
/// - [`AlreadyExists`]: a directory has the name of an entry that is not a
///   directory.
/// - [`NotFound`]: the target of a hard link is not available, and the
///   policy is [`MissingLinkTarget::Error`].
/// - [`PermissionDenied`]: the sink cannot set the owner, create a device,
///   or write an extended attribute.
/// - [`Unsupported`]: the filesystem does not support extended attributes,
///   or the platform cannot write extended attributes on the kind of the
///   node.
/// - [`UnexpectedEof`]: the file body ends before the size of the entry.
/// - Another error from the system or from the body reader.
///
/// Each error message starts with the entry path, without its empty and `.`
/// components. An error of the entry path itself shows the path from the
/// archive. An error from the open of the destination starts with the
/// destination path. After an error, the sink stops: each later call gives
/// an error. A file that was partly written when the error occurred stays on
/// disk with mode 0o600.
///
/// [`InvalidData`]: io::ErrorKind::InvalidData
/// [`InvalidInput`]: io::ErrorKind::InvalidInput
/// [`AlreadyExists`]: io::ErrorKind::AlreadyExists
/// [`NotFound`]: io::ErrorKind::NotFound
/// [`PermissionDenied`]: io::ErrorKind::PermissionDenied
/// [`Unsupported`]: io::ErrorKind::Unsupported
/// [`UnexpectedEof`]: io::ErrorKind::UnexpectedEof
pub struct FsSink<'a, R> {
    state: SinkState<'a>,
    // The sink uses `R` only in the job future. `fn(R)` keeps `Send` and
    // `Unpin` independent of `R`.
    marker: PhantomData<fn(R)>,
}

/// Work on one entry or on the finish. It gives back the sink state.
type Job<'a> = Pin<Box<dyn Future<Output = io::Result<Box<Inner>>> + Send + 'a>>;

enum SinkState<'a> {
    /// No work runs.
    Idle(Box<Inner>),
    /// A job owns the state.
    Busy(Job<'a>),
    /// An error occurred.
    Poisoned,
}

/// State and options of the sink.
struct Inner {
    dest: PathBuf,
    /// Destination directory. It opens on the first entry.
    root: Option<sys::Dir>,
    /// Normalized paths of the nodes that are not directories and that the
    /// sink created.
    created: HashSet<Box<str>>,
    /// Metadata of the directory entries, for the finish.
    dirs: Vec<DirMeta>,
    missing_link: MissingLinkTarget,
    device_nodes: DeviceNodes,
    owner: bool,
    special_bits: bool,
    xattrs: bool,
    xattr_policy: UnsupportedXattr,
    /// Gives `true` for an attribute name that the sink writes.
    xattr_filter: Box<dyn Fn(&str) -> bool + Send + Sync>,
    finished: bool,
    /// Buffer for file bodies. It is empty until the first file body that
    /// is not empty.
    buf: Box<[u8]>,
}

/// Metadata of a directory entry.
struct DirMeta {
    /// Normalized path.
    path: Box<str>,
    /// Number of components of `path`.
    depth: u32,
    /// Position of the entry among the directory entries.
    seq: u32,
    meta: SetMeta,
}

impl<'a, R: AsyncRead + Unpin + Send + 'a> FsSink<'a, R> {
    /// Make a sink that writes to the directory `dest`. The sink opens the
    /// directory on the first entry.
    pub fn new(dest: impl Into<PathBuf>) -> Self {
        let inner = Inner {
            dest: dest.into(),
            root: None,
            created: HashSet::new(),
            dirs: Vec::new(),
            missing_link: MissingLinkTarget::default(),
            device_nodes: DeviceNodes::default(),
            owner: false,
            special_bits: false,
            xattrs: false,
            xattr_policy: UnsupportedXattr::default(),
            xattr_filter: Box::new(default_xattr_filter),
            finished: false,
            buf: Box::default(),
        };
        Self {
            state: SinkState::Idle(Box::new(inner)),
            marker: PhantomData,
        }
    }

    /// Set the action for a hard link whose target the sink did not create.
    /// The default is [`MissingLinkTarget::Error`].
    ///
    /// Call the options before the first entry.
    pub fn with_missing_link_target(mut self, policy: MissingLinkTarget) -> Self {
        if let SinkState::Idle(inner) = &mut self.state {
            inner.missing_link = policy;
        }
        self
    }

    /// Set the action for a character or block device entry. The default is
    /// [`DeviceNodes::Skip`].
    ///
    /// Call the options before the first entry.
    pub fn with_device_nodes(mut self, policy: DeviceNodes) -> Self {
        if let SinkState::Idle(inner) = &mut self.state {
            inner.device_nodes = policy;
        }
        self
    }

    /// Set the owner of each node to the uid and gid of the entry. The
    /// default is `false`: the nodes belong to the user of the process.
    ///
    /// Call the options before the first entry.
    pub fn with_owner(mut self, preserve: bool) -> Self {
        if let SinkState::Idle(inner) = &mut self.state {
            inner.owner = preserve;
        }
        self
    }

    /// Keep the setuid, setgid and sticky bits of the entry mode. The
    /// default is `false`: the sink removes these bits.
    ///
    /// Call the options before the first entry.
    pub fn with_special_bits(mut self, preserve: bool) -> Self {
        if let SinkState::Idle(inner) = &mut self.state {
            inner.special_bits = preserve;
        }
        self
    }

    /// Write the extended attributes of each entry. The default is `false`:
    /// the sink does not write extended attributes. The sink writes only the
    /// names that the filter of [`with_xattr_filter`](Self::with_xattr_filter)
    /// accepts.
    ///
    /// Call the options before the first entry.
    pub fn with_xattrs(mut self, restore: bool) -> Self {
        if let SinkState::Idle(inner) = &mut self.state {
            inner.xattrs = restore;
        }
        self
    }

    /// Set the action for an extended attribute that the sink cannot write.
    /// The default is [`UnsupportedXattr::Error`].
    ///
    /// Call the options before the first entry.
    pub fn with_unsupported_xattr(mut self, policy: UnsupportedXattr) -> Self {
        if let SinkState::Idle(inner) = &mut self.state {
            inner.xattr_policy = policy;
        }
        self
    }

    /// Set the filter for the extended attribute names. With
    /// [`with_xattrs(true)`](Self::with_xattrs), the sink calls `filter` for
    /// each attribute name of an entry that it writes. It writes only the
    /// attributes for which `filter` gives `true`. The policy of
    /// [`with_unsupported_xattr`](Self::with_unsupported_xattr) does not apply
    /// to an attribute that the filter removes.
    ///
    /// The default filter:
    ///
    /// - Apple platforms: all names. Apple names have no namespace prefix, for
    ///   example `com.apple.FinderInfo`.
    /// - Other platforms: the names that start with `user.`. Thus the sink
    ///   does not write `security.*`, `system.*` and `trusted.*` attributes,
    ///   for example file capabilities and ACLs.
    ///
    /// Call the options before the first entry.
    pub fn with_xattr_filter<F>(mut self, filter: F) -> Self
    where
        F: Fn(&str) -> bool + Send + Sync + 'static,
    {
        if let SinkState::Idle(inner) = &mut self.state {
            inner.xattr_filter = Box::new(filter);
        }
        self
    }

    /// Write one entry to the destination.
    ///
    /// # Errors
    ///
    /// See [`FsSink`]. A call after [`finish`](Self::finish) gives an error.
    pub async fn write(&mut self, entry: TarEntry<'a, R>) -> io::Result<()> {
        poll_fn(|cx| self.poll_wait(cx)).await?;
        self.start(entry)?;
        poll_fn(|cx| self.poll_wait(cx)).await
    }

    /// Set the metadata of the directories. A second call does nothing.
    ///
    /// # Errors
    ///
    /// See [`FsSink`].
    pub async fn finish(&mut self) -> io::Result<()> {
        poll_fn(|cx| self.poll_finish(cx)).await
    }

    /// Run the current job to completion.
    fn poll_wait(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let inner = ready!(self.poll_idle(cx))?;
        self.state = SinkState::Idle(inner);
        Poll::Ready(Ok(()))
    }

    /// Run the current job to completion and take the state. The sink is
    /// `Poisoned` until the caller puts a state back. Thus an error or a
    /// panic stops the sink.
    fn poll_idle(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<Box<Inner>>> {
        match mem::replace(&mut self.state, SinkState::Poisoned) {
            SinkState::Idle(inner) => Poll::Ready(Ok(inner)),
            SinkState::Busy(mut job) => {
                let result = job.as_mut().poll(cx);
                if result.is_pending() {
                    self.state = SinkState::Busy(job);
                }
                result
            }
            SinkState::Poisoned => Poll::Ready(Err(poisoned())),
        }
    }

    /// Start the job for `entry`.
    fn start(&mut self, entry: TarEntry<'a, R>) -> io::Result<()> {
        match mem::replace(&mut self.state, SinkState::Poisoned) {
            SinkState::Idle(inner) if inner.finished => {
                self.state = SinkState::Idle(inner);
                Err(io::Error::other("the sink is finished"))
            }
            SinkState::Idle(inner) => {
                self.state = SinkState::Busy(Box::pin(process(inner, entry)));
                Ok(())
            }
            SinkState::Busy(job) => {
                self.state = SinkState::Busy(job);
                Err(io::Error::other(
                    "start_send called while previous entry still in progress",
                ))
            }
            SinkState::Poisoned => Err(poisoned()),
        }
    }

    /// Run the current job, then the finish.
    fn poll_finish(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        loop {
            let mut inner = ready!(self.poll_idle(cx))?;
            if inner.finished {
                self.state = SinkState::Idle(inner);
                return Poll::Ready(Ok(()));
            }
            self.state = SinkState::Busy(Box::pin(rt::unblock(move || {
                inner.finish()?;
                Ok(inner)
            })));
        }
    }
}

impl<'a, R: AsyncRead + Unpin + Send + 'a> Sink<TarEntry<'a, R>> for FsSink<'a, R> {
    type Error = io::Error;

    fn poll_ready(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.get_mut().poll_wait(cx)
    }

    fn start_send(self: Pin<&mut Self>, entry: TarEntry<'a, R>) -> io::Result<()> {
        self.get_mut().start(entry)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.get_mut().poll_wait(cx)
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.get_mut().poll_finish(cx)
    }
}

/// Error of a sink that stopped after an error.
fn poisoned() -> io::Error {
    io::Error::other("the sink stopped after an error")
}

impl Inner {
    /// Make the metadata that the sink sets on a node.
    fn set_meta(
        &self,
        mode: u32,
        uid: u32,
        gid: u32,
        mtime: SystemTime,
        attrs: &mut AttrList,
    ) -> SetMeta {
        let mask = if self.special_bits { 0o7777 } else { 0o777 };
        SetMeta {
            mode: Some(mode & mask),
            owner: self.owner.then_some((uid, gid)),
            mtime,
            xattrs: if self.xattrs {
                let mut attrs = mem::take(attrs);
                attrs.retain(|name, _| (self.xattr_filter)(name));
                if attrs.is_empty() {
                    AttrList::new()
                } else {
                    attrs
                }
            } else {
                AttrList::new()
            },
            xattr_policy: self.xattr_policy,
        }
    }

    /// Set the metadata of the directories, deepest first. Then free the
    /// memory that the entries used.
    fn finish(&mut self) -> io::Result<()> {
        if let Some(root) = &self.root {
            let mut dirs = mem::take(&mut self.dirs);
            // The last entry of a directory comes first in its run of equal
            // paths.
            dirs.sort_unstable_by(|a, b| {
                b.depth
                    .cmp(&a.depth)
                    .then_with(|| a.path.cmp(&b.path))
                    .then_with(|| b.seq.cmp(&a.seq))
            });
            for (i, dir) in dirs.iter().enumerate() {
                if i > 0 && dirs[i - 1].path == dir.path {
                    continue;
                }
                let parts: Vec<&str> = dir.path.split('/').collect();
                let parent = resolve::open_parent(root, &parts, false)?;
                let name = parts[parts.len() - 1];
                sys::open_child_dir(&parent, name, false)
                    .and_then(|handle| sys::set_meta_dir(&handle, &dir.meta))
                    .map_err(|err| with_path(err, &dir.path))?;
            }
        }
        self.finished = true;
        self.root = None;
        self.created = HashSet::new();
        self.buf = Box::default();
        Ok(())
    }
}

/// Write one entry. Give back the state.
async fn process<R: AsyncRead + Unpin + Send>(
    mut inner: Box<Inner>,
    entry: TarEntry<'_, R>,
) -> io::Result<Box<Inner>> {
    if matches!(entry, TarEntry::Directory(_)) && is_root_path(entry.path()) {
        return Ok(inner);
    }
    let key = entry_key(entry.path())?;
    match entry {
        TarEntry::File(file) => write_file(inner, key, file).await,
        TarEntry::Directory(mut dir) => {
            let meta = inner.set_meta(
                dir.mode(),
                dir.uid(),
                dir.gid(),
                dir.mtime(),
                dir.attrs_mut(),
            );
            rt::unblock(move || {
                let state = &mut *inner;
                place(&mut state.root, &state.dest, &key, |_, parent, name| {
                    make_dir(parent, name)
                })?;
                state.created.remove(&key);
                let depth = key.split('/').count() as u32;
                let seq = state.dirs.len() as u32;
                state.dirs.push(DirMeta {
                    path: key,
                    depth,
                    seq,
                    meta,
                });
                Ok(inner)
            })
            .await
        }
        TarEntry::Symlink(mut link) => {
            let meta = inner.set_meta(
                link.mode(),
                link.uid(),
                link.gid(),
                link.mtime(),
                link.attrs_mut(),
            );
            rt::unblock(move || {
                let state = &mut *inner;
                place(&mut state.root, &state.dest, &key, |_, parent, name| {
                    make_symlink(parent, name, link.link(), &meta)
                })?;
                state.created.insert(key);
                Ok(inner)
            })
            .await
        }
        TarEntry::Link(link) => {
            rt::unblock(move || {
                let state = &mut *inner;
                // Check the target before any change at the entry path.
                let target = link_target(link.link());
                let Some(target) = target.filter(|target| state.created.contains(&**target)) else {
                    return match state.missing_link {
                        MissingLinkTarget::Error => Err(io::Error::new(
                            io::ErrorKind::NotFound,
                            format!("{key}: link target {} is not in the archive", link.link()),
                        )),
                        MissingLinkTarget::Skip => Ok(inner),
                    };
                };
                if *target == *key {
                    return Ok(inner);
                }
                place(&mut state.root, &state.dest, &key, |root, parent, name| {
                    make_link(root, &target, parent, &key, name)
                })?;
                state.created.insert(key);
                Ok(inner)
            })
            .await
        }
        TarEntry::Fifo(mut fifo) => {
            let meta = inner.set_meta(
                fifo.mode(),
                fifo.uid(),
                fifo.gid(),
                fifo.mtime(),
                fifo.attrs_mut(),
            );
            rt::unblock(move || {
                let state = &mut *inner;
                place(&mut state.root, &state.dest, &key, |_, parent, name| {
                    make_special(parent, name, FsKind::Fifo, None, &meta)
                })?;
                state.created.insert(key);
                Ok(inner)
            })
            .await
        }
        TarEntry::Device(mut dev) => {
            match inner.device_nodes {
                DeviceNodes::Create => {}
                DeviceNodes::Skip => {
                    inner.created.remove(&key);
                    return Ok(inner);
                }
                DeviceNodes::Error => {
                    return Err(io::Error::new(
                        io::ErrorKind::PermissionDenied,
                        format!("{key}: device entries are not allowed"),
                    ));
                }
            }
            let meta = inner.set_meta(
                dev.mode(),
                dev.uid(),
                dev.gid(),
                dev.mtime(),
                dev.attrs_mut(),
            );
            let kind = if dev.is_char() {
                FsKind::CharDevice
            } else {
                FsKind::BlockDevice
            };
            let device = Some((dev.major(), dev.minor()));
            rt::unblock(move || {
                let state = &mut *inner;
                place(&mut state.root, &state.dest, &key, |_, parent, name| {
                    make_special(parent, name, kind, device, &meta)
                })?;
                state.created.insert(key);
                Ok(inner)
            })
            .await
        }
    }
}

/// Write a regular file. Give back the state.
///
/// A body of up to the buffer size goes to the buffer first. Then one job
/// creates the file, writes the body and sets the metadata. A larger body
/// goes to the file in chunks of the buffer size.
async fn write_file<R: AsyncRead + Unpin + Send>(
    mut inner: Box<Inner>,
    key: Box<str>,
    mut file: TarRegularFile<'_, R>,
) -> io::Result<Box<Inner>> {
    let size = file.size();
    let meta = inner.set_meta(
        file.mode(),
        file.uid(),
        file.gid(),
        file.mtime(),
        file.attrs_mut(),
    );
    if size > 0 && inner.buf.is_empty() {
        inner.buf = vec![0; rt::FILE_BUFFER_SIZE].into_boxed_slice();
    }
    if size <= rt::FILE_BUFFER_SIZE as u64 {
        let len = size as usize;
        read_body(&mut file, &mut inner.buf[..len])
            .await
            .map_err(|err| with_path(err, &key))?;
        drop(file);
        return rt::unblock(move || {
            let state = &mut *inner;
            let body = &state.buf[..len];
            place(&mut state.root, &state.dest, &key, |_, parent, name| {
                let mut out = make_file(parent, name)?;
                out.write_all(body)?;
                sys::set_meta_file(&out, &meta)
            })?;
            state.created.insert(key);
            Ok(inner)
        })
        .await;
    }
    let (mut inner, key, out) = rt::unblock(move || {
        let state = &mut *inner;
        let out = place(&mut state.root, &state.dest, &key, |_, parent, name| {
            make_file(parent, name)
        })?;
        Ok((inner, key, out))
    })
    .await?;
    let mut out = rt::async_file(out);
    copy_body(&mut file, &mut out, &mut inner.buf, size)
        .await
        .map_err(|err| with_path(err, &key))?;
    drop(file);
    let out = rt::into_std(out).await;
    rt::unblock(move || {
        sys::set_meta_file(&out, &meta).map_err(|err| with_path(err, &key))?;
        drop(out);
        inner.created.insert(key);
        Ok(inner)
    })
    .await
}

/// Read exactly `buf.len()` bytes of a file body.
async fn read_body<R: AsyncRead + Unpin>(body: &mut R, buf: &mut [u8]) -> io::Result<()> {
    if buf.is_empty() {
        // A read at the end of the body moves the tar reader to the next
        // header and wakes the task. A body that is dropped without a read
        // does not wake the task.
        poll_fn(|cx| poll_read_compat(Pin::new(&mut *body), cx, &mut [])).await?;
        return Ok(());
    }
    let mut filled = 0;
    while filled < buf.len() {
        let n =
            poll_fn(|cx| poll_read_compat(Pin::new(&mut *body), cx, &mut buf[filled..])).await?;
        if n == 0 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "unexpected EOF while reading file",
            ));
        }
        filled += n;
    }
    Ok(())
}

/// Copy `size` bytes of a file body to `out` in chunks of `buf.len()`
/// bytes. Then flush `out`.
async fn copy_body<R: AsyncRead + Unpin, W: AsyncWrite + Unpin>(
    body: &mut R,
    out: &mut W,
    buf: &mut [u8],
    size: u64,
) -> io::Result<()> {
    let mut remain = size;
    while remain > 0 {
        let len = remain.min(buf.len() as u64) as usize;
        let chunk = &mut buf[..len];
        read_body(body, chunk).await?;
        let mut pos = 0;
        while pos < len {
            let n = poll_fn(|cx| Pin::new(&mut *out).poll_write(cx, &chunk[pos..])).await?;
            if n == 0 {
                return Err(io::Error::new(
                    io::ErrorKind::WriteZero,
                    "error writing file",
                ));
            }
            pos += n;
        }
        remain -= len as u64;
    }
    poll_fn(|cx| Pin::new(&mut *out).poll_flush(cx)).await
}

/// Give `true` for an extended attribute name that the sink writes by default.
fn default_xattr_filter(name: &str) -> bool {
    cfg!(target_vendor = "apple") || name.starts_with("user.")
}

/// Give `true` for a path with no components: each component is empty or
/// `.`.
fn is_root_path(path: &str) -> bool {
    !path.starts_with('/') && path.split('/').all(|part| part.is_empty() || part == ".")
}

/// Give `path` without a trailing `/` if it has no empty, `.` or `..`
/// component.
fn as_normalized(path: &str) -> Option<&str> {
    let path = path.strip_suffix('/').unwrap_or(path);
    path.split('/')
        .all(|part| !matches!(part, "" | "." | ".."))
        .then_some(path)
}

/// Give the normalized entry path. An error has the entry path.
fn entry_key(path: &str) -> io::Result<Box<str>> {
    if let Some(key) = as_normalized(path) {
        return Ok(Box::from(key));
    }
    match resolve::normalize(path) {
        Ok(parts) => Ok(parts.join("/").into_boxed_str()),
        Err(err) => Err(with_path(err, path)),
    }
}

/// Give the normalized path of a hard link target, or `None` if the path is
/// not valid.
fn link_target(path: &str) -> Option<Cow<'_, str>> {
    match as_normalized(path) {
        Some(target) => Some(Cow::Borrowed(target)),
        None => resolve::normalize(path)
            .ok()
            .map(|parts| Cow::Owned(parts.join("/"))),
    }
}

/// Open the destination and the parent of `key`, then call `f` with the
/// destination, the parent and the last component. An error from `f` gets
/// `key` as context.
fn place<T>(
    root: &mut Option<sys::Dir>,
    dest: &Path,
    key: &str,
    f: impl FnOnce(&sys::Dir, &sys::Dir, &str) -> io::Result<T>,
) -> io::Result<T> {
    let root = match root {
        Some(root) => &*root,
        None => &*root.insert(sys::open_dest(dest)?),
    };
    let parts: Vec<&str> = key.split('/').collect();
    let parent = resolve::open_parent(root, &parts, true)?;
    let name = parts[parts.len() - 1];
    f(root, &parent, name).map_err(|err| with_path(err, key))
}

/// Make room at `name` after a create failed with `AlreadyExists`. Remove a
/// node that is not a directory. Give `true` if a directory has the name.
fn clear(parent: &sys::Dir, name: &str) -> io::Result<bool> {
    match sys::lookup(parent, name)? {
        Existing::Directory => Ok(true),
        Existing::Other => {
            sys::remove(parent, name)?;
            Ok(false)
        }
        Existing::Missing => Ok(false),
    }
}

/// Create a node that is not a directory with `make`. If the name exists,
/// remove the node and call `make` again. A directory at the name gives
/// `AlreadyExists`.
fn create<T>(
    parent: &sys::Dir,
    name: &str,
    mut make: impl FnMut() -> io::Result<T>,
) -> io::Result<T> {
    match make() {
        Err(err) if err.kind() == io::ErrorKind::AlreadyExists => {
            if clear(parent, name)? {
                return Err(io::Error::new(
                    io::ErrorKind::AlreadyExists,
                    "a directory exists at the path",
                ));
            }
            make()
        }
        result => result,
    }
}

/// Create the directory `name` in `parent`. An existing directory stays.
fn make_dir(parent: &sys::Dir, name: &str) -> io::Result<()> {
    match sys::create_dir(parent, name) {
        Err(err) if err.kind() == io::ErrorKind::AlreadyExists => {
            if clear(parent, name)? {
                Ok(())
            } else {
                sys::create_dir(parent, name)
            }
        }
        result => result,
    }
}

/// Create the regular file `name` in `parent` and open it for writing.
fn make_file(parent: &sys::Dir, name: &str) -> io::Result<std::fs::File> {
    create(parent, name, || sys::create_file(parent, name))
}

/// Create the symlink `name` in `parent` and set its metadata.
fn make_symlink(parent: &sys::Dir, name: &str, target: &str, meta: &SetMeta) -> io::Result<()> {
    create(parent, name, || sys::create_symlink(parent, name, target))?;
    sys::set_meta_at(parent, name, FsKind::Symlink, meta)
}

/// Create the FIFO or device `name` in `parent` and set its metadata.
fn make_special(
    parent: &sys::Dir,
    name: &str,
    kind: FsKind,
    device: Option<(u32, u32)>,
    meta: &SetMeta,
) -> io::Result<()> {
    create(parent, name, || {
        sys::create_special(parent, name, kind, device)
    })?;
    sys::set_meta_at(parent, name, kind, meta)
}

/// Create the name `name` in `parent` for the node at the normalized path
/// `target` in `root`. `key` is the normalized path of `name`. If `target`
/// is in the same directory, the link uses `parent` for the target.
fn make_link(
    root: &sys::Dir,
    target: &str,
    parent: &sys::Dir,
    key: &str,
    name: &str,
) -> io::Result<()> {
    let (target_dir, from_name) = match target.rsplit_once('/') {
        Some((dir, from_name)) => (Some(dir), from_name),
        None => (None, target),
    };
    let opened;
    let from = if target_dir == key.rsplit_once('/').map(|(dir, _)| dir) {
        parent
    } else {
        let parts: Vec<&str> = target.split('/').collect();
        // The error of `open_parent` starts with the target path.
        opened = resolve::open_parent(root, &parts, false)
            .map_err(|err| io::Error::new(err.kind(), format!("link target {err}")))?;
        &*opened
    };
    create(parent, name, || {
        sys::create_link(from, from_name, parent, name)
    })
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::{rt::block_on, testutil::TempDir, FsFile},
        futures::StreamExt,
        smol_tar::{TarDirectory, TarLink, TarReader, TarRegularFileReader, TarWriter},
        static_assertions::assert_impl_all,
        std::{
            fs,
            io::ErrorKind,
            os::unix::fs::{MetadataExt, PermissionsExt},
            pin::pin,
            sync::{
                atomic::{AtomicUsize, Ordering},
                Arc,
            },
            task::{Wake, Waker},
            time::{Duration, UNIX_EPOCH},
        },
    };

    #[cfg(all(feature = "smol", not(feature = "tokio")))]
    type Body = smol::io::Empty;
    #[cfg(feature = "tokio")]
    type Body = tokio::io::Empty;

    assert_impl_all!(FsSink<'static, FsFile>: Send, Unpin);
    assert_impl_all!(FsSink<'static, TarRegularFileReader<'static, Body>>: Send, Unpin);

    /// Write `entries` to an archive in memory.
    fn archive(entries: Vec<TarEntry<'static, &'static [u8]>>) -> Vec<u8> {
        let mut data = Vec::new();
        block_on(async {
            let mut writer = TarWriter::new(&mut data);
            for entry in entries {
                writer.write(entry).await.unwrap();
            }
            writer.finish().await.unwrap();
        });
        data
    }

    /// Forward the entries of the archive `data` into a sink for `dest`.
    fn extract(data: &[u8], dest: &Path) -> io::Result<()> {
        block_on(TarReader::new(data).forward(FsSink::new(dest)))
    }

    /// Regular file `path` with the body `body`.
    fn file(path: &str, body: &'static [u8]) -> TarEntry<'static, &'static [u8]> {
        TarRegularFile::new(path, body.len() as u64, body).into()
    }

    #[test]
    fn pax_archive_is_extracted() {
        let tmp = TempDir::new();
        let data = fs::read(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../smol-tar/tests/data/pax.tar"
        ))
        .unwrap();
        extract(&data, tmp.path()).unwrap();
        let mut dir = tmp.path().join("pax");
        for _ in 0..10 {
            dir.push("deep");
        }
        let name = format!("{}.txt", "c".repeat(200));
        assert_eq!(fs::read(dir.join(&name)).unwrap(), b"pax payload");
        let link = tmp.path().join(format!("pax/link-{}", "d".repeat(190)));
        let target = format!("pax/{}/{name}", ["deep"; 10].join("/"));
        assert_eq!(fs::read_link(link).unwrap(), Path::new(&target));
        for parent in [tmp.path().join("pax"), dir] {
            let meta = fs::symlink_metadata(parent).unwrap();
            assert!(meta.is_dir());
            assert_eq!(meta.permissions().mode() & 0o777, 0o755);
        }
    }

    #[test]
    fn root_directory_entry_is_skipped() {
        let tmp = TempDir::new();
        let before = fs::metadata(tmp.path()).unwrap();
        let mtime = UNIX_EPOCH + Duration::from_secs(1_000_000);
        let root = TarDirectory::new("./").with_mode(0o700).with_mtime(mtime);
        let data = archive(vec![root.into(), file("./a", b"a")]);
        extract(&data, tmp.path()).unwrap();
        assert_eq!(fs::read(tmp.path().join("a")).unwrap(), b"a");
        let after = fs::metadata(tmp.path()).unwrap();
        assert_eq!(after.permissions().mode(), before.permissions().mode());
        assert_ne!(after.modified().unwrap(), mtime);
    }

    #[test]
    fn root_path_for_file_fails() {
        let tmp = TempDir::new();
        let data = archive(vec![file(".", b"x")]);
        let err = extract(&data, tmp.path()).unwrap_err();
        assert_eq!(err.kind(), ErrorKind::InvalidData);
        assert!(err.to_string().starts_with(".: "), "{err}");
    }

    /// An archive with an empty file extracts completely. The test
    /// `empty_body_read_wakes_reader` checks the wake of the reader.
    #[test]
    fn forward_after_empty_file() {
        let tmp = TempDir::new();
        let data = archive(vec![file("e", b""), file("f", b"data")]);
        extract(&data, tmp.path()).unwrap();
        assert_eq!(fs::read(tmp.path().join("e")).unwrap(), b"");
        assert_eq!(fs::read(tmp.path().join("f")).unwrap(), b"data");
    }

    /// Waker that counts its wakes.
    struct CountWaker(AtomicUsize);

    impl Wake for CountWaker {
        fn wake(self: Arc<Self>) {
            self.wake_by_ref();
        }

        fn wake_by_ref(self: &Arc<Self>) {
            self.0.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// `forward` polls the reader after the sink completes an entry. The read
    /// of an empty body moves the reader to the next header and wakes the
    /// task.
    #[test]
    fn empty_body_read_wakes_reader() {
        let data = archive(vec![file("e", b""), file("f", b"data")]);
        let mut reader = TarReader::new(&data[..]);
        let Some(Ok(TarEntry::File(mut body))) = block_on(reader.next()) else {
            panic!("no file entry");
        };
        let count = Arc::new(CountWaker(AtomicUsize::new(0)));
        let waker = Waker::from(Arc::clone(&count));
        let mut cx = Context::from_waker(&waker);
        {
            let read = pin!(read_body(&mut body, &mut []));
            assert!(matches!(read.poll(&mut cx), Poll::Ready(Ok(()))));
        }
        assert!(count.0.load(Ordering::Relaxed) > 0);
        drop(body);
        let Some(Ok(entry)) = block_on(reader.next()) else {
            panic!("no second entry");
        };
        assert_eq!(entry.path(), "f");
    }

    #[test]
    fn large_and_boundary_bodies() {
        let tmp = TempDir::new();
        let mtime = UNIX_EPOCH + Duration::from_secs(1_000_000);
        let sizes = [rt::FILE_BUFFER_SIZE, 2 * rt::FILE_BUFFER_SIZE + 100];
        let bodies: Vec<&'static [u8]> = sizes
            .iter()
            .map(|&size| {
                let body: Vec<u8> = (0..size).map(|i| (i % 251) as u8).collect();
                &*Vec::leak(body)
            })
            .collect();
        let entries = bodies
            .iter()
            .enumerate()
            .map(|(i, body)| {
                TarRegularFile::new(format!("f{i}"), body.len() as u64, *body)
                    .with_mode(0o444)
                    .with_mtime(mtime)
                    .into()
            })
            .collect();
        let data = archive(entries);
        extract(&data, tmp.path()).unwrap();
        for (i, body) in bodies.iter().enumerate() {
            let path = tmp.path().join(format!("f{i}"));
            assert_eq!(fs::read(&path).unwrap(), *body);
            let meta = fs::metadata(&path).unwrap();
            assert_eq!(meta.permissions().mode() & 0o7777, 0o444);
            assert_eq!(meta.mtime(), 1_000_000);
        }
    }

    #[test]
    fn last_directory_entry_sets_metadata() {
        let tmp = TempDir::new();
        let first = UNIX_EPOCH + Duration::from_secs(1_000_000);
        let last = UNIX_EPOCH + Duration::from_secs(2_000_000);
        let data = archive(vec![
            TarDirectory::new("d")
                .with_mode(0o700)
                .with_mtime(first)
                .into(),
            TarDirectory::new("./d/")
                .with_mode(0o750)
                .with_mtime(last)
                .into(),
            TarDirectory::new("d")
                .with_mode(0o711)
                .with_mtime(first)
                .into(),
            TarDirectory::new("d/")
                .with_mode(0o751)
                .with_mtime(last)
                .into(),
        ]);
        extract(&data, tmp.path()).unwrap();
        let meta = fs::metadata(tmp.path().join("d")).unwrap();
        assert_eq!(meta.permissions().mode() & 0o777, 0o751);
        assert_eq!(meta.mtime(), 2_000_000);
    }

    #[test]
    fn hard_links_share_the_node() {
        let tmp = TempDir::new();
        let data = archive(vec![
            file("a/f", b"data"),
            TarLink::new("a/same", "a/f").into(),
            TarLink::new("b/other", "./a//f").into(),
        ]);
        extract(&data, tmp.path()).unwrap();
        let ino = fs::metadata(tmp.path().join("a/f")).unwrap().ino();
        for path in ["a/same", "b/other"] {
            assert_eq!(fs::metadata(tmp.path().join(path)).unwrap().ino(), ino);
        }
    }

    #[test]
    fn keys_are_normalized() {
        assert_eq!(&*entry_key("a/b/").unwrap(), "a/b");
        assert_eq!(&*entry_key("./a//b/.").unwrap(), "a/b");
        assert!(entry_key("a/../b").is_err());
        assert!(matches!(link_target("a/b"), Some(Cow::Borrowed("a/b"))));
        assert_eq!(link_target("./a//b").as_deref(), Some("a/b"));
        assert_eq!(link_target("/a"), None);
        assert_eq!(link_target("a/.."), None);
    }

    #[test]
    fn default_xattr_filter_names() {
        assert!(default_xattr_filter("user.a"));
        for name in [
            "trusted.a",
            "security.capability",
            "system.posix_acl_access",
            "com.apple.FinderInfo",
        ] {
            assert_eq!(
                default_xattr_filter(name),
                cfg!(target_vendor = "apple"),
                "{name}"
            );
        }
    }

    #[test]
    fn short_body_gives_unexpected_eof() {
        for size in [10, rt::FILE_BUFFER_SIZE as u64 + 10] {
            let tmp = TempDir::new();
            let err = block_on(async {
                let mut sink = FsSink::new(tmp.path());
                sink.write(TarRegularFile::new("f", size, &b"12345"[..]).into())
                    .await
                    .unwrap_err()
            });
            assert_eq!(err.kind(), ErrorKind::UnexpectedEof, "{size}");
            assert!(err.to_string().starts_with("f: "), "{err}");
        }
    }
}
