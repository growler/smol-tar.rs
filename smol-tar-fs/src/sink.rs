//! Filesystem sink: writes tar entries to a directory tree.

use {
    super::{
        resolve, rt, sys, with_path, DeviceNodes, Existing, FsKind, MissingLinkTarget, ModePolicy,
        OwnerErrorFn, SetMeta, UnsupportedXattr, XattrFilter,
    },
    crate::{poll_read_compat, AsyncRead, AsyncWrite},
    futures_core::Stream,
    futures_sink::Sink,
    rustix::io::Errno,
    smol_tar::{AttrList, TarDirectory, TarEntry, TarRegularFile},
    std::{
        borrow::Cow,
        collections::HashMap,
        future::{poll_fn, Future},
        io::{self, Write},
        marker::PhantomData,
        mem,
        path::PathBuf,
        pin::{pin, Pin},
        sync::Arc,
        task::{ready, Context, Poll},
        time::SystemTime,
    },
};

/// Sink that writes tar entries to a directory tree.
///
/// The sink takes the [`TarEntry`] values of an archive, for example from a
/// [`TarReader`](smol_tar::TarReader), and creates one node in the destination
/// directory for each entry. Use [`extract`](Self::extract) with a stream
/// of entries, or call [`write`](Self::write) for each entry and then
/// [`finish`](Self::finish). The sink also implements [`Sink`] for
/// `forward`. See [Batches](#batches) for the difference.
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
/// - The sink creates a missing parent directory with mode 0o777 less the
///   umask, with each mode policy. With [`ModePolicy::Mask`], the directory
///   also does not get the bits of the mask. The sink does not change this
///   mode later. A later directory entry for the path sets the metadata of
///   the directory by the rules of [Metadata](#metadata). If the parent of
///   a new directory has the setgid bit, the new directory also gets this
///   bit. After a later directory entry, the directory keeps this bit only
///   if the process is a member of the group of the directory or has the
///   `CAP_FSETID` capability. Root has this capability. With
///   [`with_special_bits(true)`](Self::with_special_bits), a later
///   directory entry sets the exact mode of the entry. If the umask or the
///   mask removes the read, write or search bit of the owner, a process
///   without privileges cannot create nodes in such a directory.
///
/// # Safety rules
///
/// - The sink follows a symlink in a parent component if the symlink target
///   stays inside the destination. This rule applies to entry paths and to
///   hard link targets. For example, with `lib -> usr/lib` in the
///   destination, the entry `lib/f` goes to `usr/lib/f`. The rule applies
///   to the symlinks of the archive and to the symlinks that are in the
///   destination before the extraction.
/// - A parent symlink that leads outside the destination gives a
///   [`PermissionDenied`] error. An absolute parent symlink also gives this
///   error when its target is in the destination. For a parent symlink of
///   the entry path, the sink gives the error before it changes the
///   destination. For a parent symlink of a hard link target, the sink can
///   create the missing parent directories of the link path before it gives
///   the error.
/// - If a node that is not a directory has the name of the entry, the sink
///   removes it and creates a new node. The sink never writes through an
///   existing node.
/// - If a directory has the name of the entry, a directory entry uses it.
///   Another entry kind gives an [`AlreadyExists`] error.
/// - The sink stores the target of a symlink entry without change. It does
///   not resolve the target when it creates the symlink.
/// - The sink creates a hard link only to a node that it created before
///   from the same archive. The target must not be a directory. For each
///   node that is not a directory, the sink stores the identity of the
///   node: the device and the inode number. The node at the target path of
///   a hard link must have the stored identity. A target path can lead to
///   another node or to no node, for example if a later entry changes a
///   parent symlink of the path.
///   [`with_missing_link_target`](Self::with_missing_link_target) sets the
///   action if the target is not in the archive, or if the target path
///   leads to another node or to no node. A link to its own path does
///   nothing.
/// - By default, the sink skips character and block device entries and
///   creates no device nodes. A device node gives access to the device
///   through the extracted tree.
///   [`with_device_nodes`](Self::with_device_nodes) sets the action for a
///   device entry. The policy does not apply to FIFO entries. A device entry
///   with a path that is not valid gives an [`InvalidData`] error with each
///   policy.
/// - With [`DeviceNodes::Skip`], the sink does not change the destination
///   for a device entry. A later hard link to the path of the entry has no
///   target. [`with_missing_link_target`](Self::with_missing_link_target)
///   sets the action for that link.
/// - With [`with_xattrs(true)`](Self::with_xattrs), the sink writes only the
///   extended attributes that the filter of
///   [`with_xattr_filter`](Self::with_xattr_filter) accepts. On Apple
///   platforms, the default filter accepts all names. On Linux and Android,
///   it accepts only the names that start with `user.`. Thus the sink does
///   not write `security.*`, `system.*` and `trusted.*` attributes, for
///   example file capabilities and ACLs. On other Unix platforms, the sink
///   cannot write extended attributes.
///   [`with_unsupported_xattr`](Self::with_unsupported_xattr) sets the
///   action for them.
///
/// # Metadata
///
/// - The sink sets the mode, the modification time and the owner of each
///   node after it writes the data. A regular file gets the mode that the
///   mode policy gives, less the umask, when the sink creates it, if the mode
///   has no special bits and the sink writes no extended attributes on the
///   file. With [`with_owner(true)`](Self::with_owner), the group and other
///   bits of this create mode are only the bits that the owner, group and
///   other classes all have. Thus no user can read the data before the owner
///   change that cannot read it after the change. After the write, the sink
///   sets the mode only if the file does not have it.
/// - [`with_mode`](Self::with_mode) sets the action for the permission bits
///   (0o777). With [`ModePolicy::Apply`], the default, the node gets the
///   bits of the entry. With [`ModePolicy::Mask`], it gets the bits of the
///   entry without the bits of the mask. The umask of the process does not
///   change the mode of an entry with these two policies. With
///   [`ModePolicy::Skip`], a new node gets the default mode less the umask,
///   and the sink does not change the mode of an existing directory. The
///   default mode of a device node is 0o600. [Paths](#paths) gives the mode
///   of a missing parent directory.
/// - With [`with_special_bits`](Self::with_special_bits), the mode has the
///   setuid, setgid and sticky bits (0o7000) of the entry and no other
///   special bits. Without it, the sink sets no special bits from the entry.
///   A directory keeps the special bits that it has at
///   [`finish`](Self::finish), for example the setgid bit that a new
///   directory gets from its parent. The directory keeps the setgid bit only
///   if the process is a member of the group of the directory or has the
///   `CAP_FSETID` capability. Root has this capability. A new file, FIFO or
///   device node has no special bits. With [`ModePolicy::Skip`], the option
///   has no effect, and a new directory keeps the setgid bit that it gets
///   from its parent.
/// - The sink sets the modification time only with
///   [`with_mtime(true)`](Self::with_mtime), the default. Without it, each
///   node keeps the time of the extraction.
/// - Without [`ModePolicy::Skip`], a new directory of a directory entry has
///   mode 0o700 until [`finish`](Self::finish). If the directory keeps the
///   setgid bit of its parent, the mode is 0o2700. Then the sink sets the
///   metadata of each directory, deepest first. Thus a read-only directory
///   does not stop the extraction of its children, and the children do not
///   change the modification time of the directory. If
///   the archive has more than one entry for a directory, the last entry
///   sets the metadata. This rule also applies to entries that reach one
///   directory through a parent symlink.
/// - The finish sorts the directories by their depth on disk below the
///   destination. It opens each directory by its entry path. A symlink in
///   the entry path can lead up from a directory that is not a parent of
///   the directory on disk. The finish can set the metadata of that
///   directory first. Example: the archive has `p/q/` with mode 000,
///   `p/q/l -> ../../s`, `s/` and `p/q/l/t/`. On disk, `p/q` and `s/t` both
///   have depth 2. The finish sets mode 000 on `p/q` first. Then the open of
///   `p/q/l/t` fails for a user without privileges, and the finish stops
///   with this error.
/// - With [`ModePolicy::Skip`], a new directory gets its final mode when the
///   sink creates it. With [`ModePolicy::Skip`] and
///   [`with_mtime(false)`](Self::with_mtime), the sink also sets the owner
///   and the extended attributes of a directory entry when it creates the
///   directory, and does not keep its metadata for
///   [`finish`](Self::finish).
/// - The sink sets the owner only with [`with_owner`](Self::with_owner).
///   The owner change usually needs privileges. If the system refuses the
///   owner change, the handler of
///   [`with_owner_error`](Self::with_owner_error) selects if the sink stops
///   or continues.
/// - The sink writes extended attributes only with
///   [`with_xattrs`](Self::with_xattrs).
///   [`with_unsupported_xattr`](Self::with_unsupported_xattr) sets the
///   action for an attribute that the sink cannot write. This action does
///   not apply to an attribute that the filter of
///   [`with_xattr_filter`](Self::with_xattr_filter) removes.
/// - With [`DeviceNodes::Create`], the sink sets the metadata of a device
///   node by the same rules as for other nodes. With [`ModePolicy::Skip`], a
///   new device node has mode 0o600 less the umask. The sink cannot write
///   extended attributes on a device node.
///   [`with_unsupported_xattr`](Self::with_unsupported_xattr) sets the
///   action for the attributes of a device entry.
/// - A hard link gets no metadata and no extended attributes, because it
///   shares the node of its target.
///
/// # Finish
///
/// Call [`finish`](Self::finish) or `close` after the last entry.
/// [`extract`](Self::extract) and `forward` do this. A drop of the
/// [`extract`](Self::extract) future, for example at a timeout, also drops
/// the sink. If the sink is dropped before the end of the finish, these
/// rules apply:
///
/// - If the sink has no job on the blocking thread pool, the directories
///   keep mode 0o700, or 0o2700 with the setgid bit of the parent, and the
///   modification time of the extraction. With [`ModePolicy::Skip`], the
///   directories have their final mode. The entries of the batch are not
///   written, and no error comes back.
/// - A job that runs on the blocking thread pool continues after the drop.
///   Thus a job that writes the batch or runs the finish can write the
///   entries of the batch and set the metadata of the directories after the
///   drop returns.
/// - With the `smol` feature, the drop cancels a job that waits for a
///   thread. With the `tokio` feature, a job that waits for a thread runs
///   after the drop.
///
/// See [Batches](#batches).
///
/// At the finish, the sink opens the directory of each kept directory entry
/// again, by the entry path. The open does not follow a symlink at the last
/// component. The directory must have the identity that the sink stored
/// when it wrote the entry. If the path leads to another directory, the
/// finish stops with an [`InvalidData`] error "PATH: file changed". For
/// example, a later entry can change a parent symlink of the path. If the
/// path leads to no node or to a node that is not a directory, the finish
/// also stops with an error.
///
/// # Batches
///
/// The sink writes a regular file of up to 256 KiB and a directory entry in
/// a batch. It reads the body of such a file into memory and writes the file
/// later. One job on the blocking thread pool writes all entries of a batch,
/// in archive order. The sink writes the batch:
///
/// - when the batch has 64 entries or 1 MiB of bodies,
/// - before a file that makes the bodies of the batch larger than 1 MiB,
/// - before an entry that is not such a file or a directory entry,
/// - at [`flush`](Self::flush), `poll_flush`, [`finish`](Self::finish) and
///   `close`.
///
/// Thus [`write`](Self::write) for such an entry can return before the entry
/// is on disk. `start_send` and `feed` return before the sink writes the
/// entry. Until the sink runs the job, the job keeps the entry. The body of
/// a regular file from a [`TarReader`](smol_tar::TarReader) keeps the source
/// of the reader. Thus the reader cannot give the next entry. A `next` after
/// `feed` does not return until the sink runs the job. After `feed`, call
/// `flush` before the next `next`, or use `send`. `send` and `flush` write
/// the batch. Thus each batch has one entry. [`write`](Self::write) runs the
/// job before it returns. `write` does not flush the batch. Thus the batches
/// fill up to the limits. For full batches, use [`extract`](Self::extract)
/// or [`write`](Self::write). Call [`finish`](Self::finish) or `close` one
/// time, after the last entry. A flush after each entry writes each entry in
/// its own job. If you stop before the end of the entries, call
/// [`flush`](Self::flush) to write the entries of the batch.
/// [`finish`](Self::finish) also sets the metadata of the directories. If
/// the sink is dropped while the entries wait in the batch, these entries
/// are not written, and no error comes back. A job that writes the batch
/// continues after the drop. See [Finish](#finish). An error of an entry in
/// the batch comes from a later call, and the error has the path of that
/// entry. Each entry sees the entries before it. For example, a hard link
/// sees its target.
///
/// `forward` calls `poll_flush` each time that the stream is not ready. A
/// [`TarReader`](smol_tar::TarReader) stream is not ready until the sink
/// reads the body of the current entry. Thus with a `TarReader` stream,
/// `forward` flushes the sink after each regular file. As a result, each
/// batch has at most one file. [`extract`](Self::extract) does not call
/// `poll_flush`, and it writes full batches. After an error from the stream,
/// `forward` does not flush or close the sink. To write the last entry and
/// the batch after such an error, give `&mut sink` to `forward`, then call
/// `flush` or `close`.
///
/// # Memory
///
/// The sink keeps the path and the identity of each node that is not a
/// directory, for the hard link check. It keeps the metadata, the identity
/// and the extended attributes of each directory entry until
/// [`finish`](Self::finish), except with [`ModePolicy::Skip`] and
/// [`with_mtime(false)`](Self::with_mtime). For the same entries, it keeps
/// the identity and the depth on disk of the directory and of each parent
/// directory on disk, about 28 bytes for each directory. The identity uses
/// 16 bytes. A path of 70 bytes and its identity use about 116 bytes, thus
/// about 116 MB for each million nodes. The sink has one buffer of 1 MiB
/// for the bodies of a batch, and one buffer of 256 KiB for the bodies of
/// larger files. Each buffer is allocated at the first file body that uses
/// it. The sink keeps the destination and up to 16 other directories open.
/// See [Runtime](#runtime).
///
/// # Runtime
///
/// The sink runs the filesystem calls on the blocking thread pool of the
/// runtime. The close of a file or a directory can occur on the thread that
/// polls or drops the sink. With the `tokio` feature, poll the sink inside a
/// Tokio runtime context.
///
/// The sink keeps up to 16 directories in the destination open. These are
/// parent directories of earlier entries and new directories of directory
/// entries. The path of each one is an ancestor of the path of the next one.
/// The sink uses an open directory for each later entry with the same parent
/// path. For another parent path, the sink resolves the rest of the parent
/// path beneath the nearest open ancestor directory. If this fails, it
/// resolves the parent path from the destination again. The sink closes all
/// of them when it removes a node, at [`finish`](Self::finish), and when it
/// is dropped. If another process moves one of these directories during the
/// extraction, the entries with that parent path or with a parent path
/// below it go to the moved directory or to its subdirectories. The moved
/// directory can be outside the destination. New directories of directory
/// entries in it also stay open for later entries.
///
/// The limit of 40 symlink expansions applies separately to the part of the
/// parent path beneath the open ancestor. Thus a parent path with many
/// symlinks can resolve when an ancestor is open, and fail when no ancestor
/// is open. In both cases, the result stays inside the directory where the
/// resolution starts.
///
/// # Errors
///
/// - [`InvalidData`]: the entry path starts with `/`, has a `..`
///   component, or has no components. At [`finish`](Self::finish), the path
///   of a directory entry leads to another directory. The message is "PATH:
///   file changed".
/// - [`InvalidInput`]: the entry path or the link target has a NUL byte,
///   or the platform cannot encode the device numbers. With
///   [`with_mtime(true)`](Self::with_mtime), also if the platform cannot
///   encode the modification time.
/// - [`AlreadyExists`]: a directory has the name of an entry that is not a
///   directory.
/// - [`NotFound`]: the target of a hard link is not available, and the
///   policy is [`MissingLinkTarget::Error`]. For example, the target is not
///   in the archive, the target path leads to another node or to no node,
///   or the target path has a symlink loop. The message is "PATH: link
///   target TARGET is not in the archive".
/// - [`PermissionDenied`]: a parent symlink of the entry path or of a hard
///   link target leads outside the destination or is absolute. The message
///   has "a path led outside of the filesystem". The sink also gives this
///   error if it cannot create a device or write an extended attribute. A
///   device entry with the policy
///   [`DeviceNodes::Error`] also gives this error. The message is
///   "PATH: device entries are not allowed".
/// - [`Unsupported`]: the filesystem does not support extended attributes,
///   the platform cannot write extended attributes, or the platform cannot
///   write extended attributes on the kind of the node.
/// - [`UnexpectedEof`]: the file body ends before the size of the entry.
/// - The sink cannot set the owner. The kind comes from the system error.
///   Examples are [`PermissionDenied`] for a user without privileges and,
///   on Linux, [`InvalidInput`] for an id that the user namespace does not
///   map. The message is "PATH: cannot set the owner to uid UID, gid GID:
///   ERROR". The handler of [`with_owner_error`](Self::with_owner_error)
///   can let the sink continue.
/// - Another error from the system or from the body reader.
///
/// Each error message starts with the entry path, without its empty and `.`
/// components. An error of the entry path itself shows the path from the
/// archive. An error from the open of the destination starts with the
/// destination path. After an error, the sink stops: each later call gives
/// an error. A file that the sink created when the error occurred stays on
/// disk. The sink can have written all or part of its data. If the error
/// occurred before the sink set the mode, the mode of the file is the mode
/// that the mode policy gives less the umask, or 0o600 if the mode has
/// special bits or the sink writes extended attributes on the file. With
/// [`with_owner(true)`](Self::with_owner), the group and other bits of this
/// mode are only the bits that the owner, group and other classes all have.
/// With [`ModePolicy::Skip`], the file has mode 0o666 less the umask.
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
    /// Destination directory. It opens on the first entry that the sink
    /// writes.
    root: Option<sys::Dir>,
    /// Identity of `root`, when it is known.
    root_id: Option<sys::FileId>,
    /// Open parent directories of the last entries, at most `OPEN_DIRS`.
    /// Each directory is an ancestor of the next one on the entry paths. The
    /// last one is the parent directory of an earlier entry or a new
    /// directory of a directory entry. The sink opens the rest of a parent
    /// path in the nearest of them that is an ancestor of this path, and
    /// opens the parent path from `root` if that fails. A removal of a node
    /// clears all of them, because the removal of a symlink can change the
    /// directory that a path leads to.
    parents: Vec<OpenDir>,
    /// Normalized paths of the nodes that are not directories and that the
    /// sink created, with the identity of each node.
    created: HashMap<Box<str>, sys::FileId>,
    /// Metadata of the directory entries, for the finish.
    dirs: Vec<DirMeta>,
    /// Depth on disk below the destination of each directory that the sink
    /// passed to find the depth of a directory entry. The destination has
    /// depth 0.
    depths: HashMap<sys::FileId, u32>,
    missing_link: MissingLinkTarget,
    device_nodes: DeviceNodes,
    owner: bool,
    /// Handler of an owner change that failed.
    owner_error: OwnerErrorFn,
    mode: ModePolicy,
    special_bits: bool,
    mtime: bool,
    xattrs: bool,
    xattr_policy: UnsupportedXattr,
    /// Gives `true` for an attribute name that the sink writes.
    xattr_filter: XattrFilter,
    finished: bool,
    /// Buffer for the bodies of files larger than `FILE_BUFFER_SIZE`. It is
    /// empty until the first such file.
    buf: Box<[u8]>,
    /// Files of up to `FILE_BUFFER_SIZE` and directory entries that wait for
    /// the next batch job, in archive order.
    batch: Vec<QueuedEntry>,
    /// Buffer of `BATCH_BYTES` for the bodies of the files in `batch`, one
    /// after the other. It is empty until the first such file that is not
    /// empty.
    batch_data: Box<[u8]>,
    /// Number of bytes in `batch_data` that the bodies use.
    batch_len: usize,
}

/// Most open directories that the sink keeps for later entries.
const OPEN_DIRS: usize = 16;

/// Open directory that the sink keeps for later entries.
struct OpenDir {
    /// Normalized path.
    path: Box<str>,
    dir: sys::Dir,
    /// Identity of the directory, when it is known.
    id: Option<sys::FileId>,
}

/// Entry in the batch of the sink.
struct QueuedEntry {
    /// Normalized path.
    key: Box<str>,
    /// Length of the body in `batch_data` for a regular file. `None` for a
    /// directory entry.
    len: Option<usize>,
    meta: SetMeta,
}

/// Metadata of a directory entry.
struct DirMeta {
    /// Normalized path.
    path: Box<str>,
    /// Depth of the directory on disk below the destination.
    depth: u32,
    /// Position of the entry among the directory entries.
    seq: u32,
    /// Identity of the directory.
    id: sys::FileId,
    meta: SetMeta,
}

impl<'a, R: AsyncRead + Unpin + Send + 'a> FsSink<'a, R> {
    /// Make a sink that writes to the directory `dest`. The sink opens the
    /// directory on the first entry that it writes.
    pub fn new(dest: impl Into<PathBuf>) -> Self {
        let inner = Inner {
            dest: dest.into(),
            root: None,
            root_id: None,
            parents: Vec::new(),
            created: HashMap::new(),
            dirs: Vec::new(),
            depths: HashMap::new(),
            missing_link: MissingLinkTarget::default(),
            device_nodes: DeviceNodes::default(),
            owner: false,
            owner_error: Arc::new(|_, err| Err(err)),
            mode: ModePolicy::default(),
            special_bits: false,
            mtime: true,
            xattrs: false,
            xattr_policy: UnsupportedXattr::default(),
            xattr_filter: Arc::new(default_xattr_filter),
            finished: false,
            buf: Box::default(),
            batch: Vec::new(),
            batch_data: Box::default(),
            batch_len: 0,
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
    /// When the sink creates a regular file, the group and other bits of the
    /// file are only the bits that the owner, group and other classes of its
    /// mode all have. The file keeps these bits until the sink sets the mode
    /// after the owner change. A mode with special bits or extended
    /// attributes to write gives mode 0o600 at the create. With
    /// [`ModePolicy::Skip`], the sink does not change the mode. Thus the file
    /// keeps the mode 0o666 less the umask from the create.
    ///
    /// The owner change usually needs privileges. After the owner change, the
    /// sink can set the mode of the file. For this step, the process must
    /// have CAP_FOWNER or must be the owner of the file after the change.
    ///
    /// If the system refuses the owner change, the sink stops with an error.
    /// [`with_owner_error`](Self::with_owner_error) sets a handler that can
    /// let the sink continue.
    ///
    /// Call the options before the first entry.
    pub fn with_owner(mut self, preserve: bool) -> Self {
        if let SinkState::Idle(inner) = &mut self.state {
            inner.owner = preserve;
        }
        self
    }

    /// Set the handler for an owner change that the system refuses. With
    /// [`with_owner(true)`](Self::with_owner), the sink calls `handler` with
    /// the entry path and the error. The entry path has no empty and `.`
    /// components. The error message has the uid and the gid of the entry.
    /// The sink calls `handler` on the blocking thread pool.
    ///
    /// If `handler` gives an error, the sink stops with this error and adds
    /// the entry path to the message. If `handler` gives `Ok`, the node keeps
    /// its owner, usually the user of the process. The sink sets the other
    /// metadata of the node and continues. Then a node that is not a
    /// directory does not get the setuid and setgid bits of the entry. It
    /// gets the sticky bit. The sink also does not write the
    /// `security.capability` attribute on this node.
    ///
    /// The default handler gives the error back, thus the sink stops.
    ///
    /// Call the options before the first entry.
    pub fn with_owner_error<F>(mut self, handler: F) -> Self
    where
        F: Fn(&str, io::Error) -> io::Result<()> + Send + Sync + 'static,
    {
        if let SinkState::Idle(inner) = &mut self.state {
            inner.owner_error = Arc::new(handler);
        }
        self
    }

    /// Set the action for the permission bits (0o777) of each entry. The
    /// default is [`ModePolicy::Apply`].
    ///
    /// Call the options before the first entry.
    pub fn with_mode(mut self, policy: ModePolicy) -> Self {
        if let SinkState::Idle(inner) = &mut self.state {
            inner.mode = policy;
        }
        self
    }

    /// Set the setuid, setgid and sticky bits of the entry mode. Then the
    /// mode of a directory has no other special bits. The default is
    /// `false`: the sink sets no special bits from the entry, and a
    /// directory keeps the special bits that it has on disk. Examples are the
    /// setgid bit that a new directory gets from its parent and the bits that
    /// the owner of the directory set before the extraction. A new directory
    /// keeps the setgid bit only if the process is a member of the group of
    /// the directory or has the `CAP_FSETID` capability. Root has this
    /// capability. A caller that runs as root and needs the exact modes of
    /// the archive sets this option. With [`ModePolicy::Skip`], the option
    /// has no effect.
    ///
    /// Call the options before the first entry.
    pub fn with_special_bits(mut self, preserve: bool) -> Self {
        if let SinkState::Idle(inner) = &mut self.state {
            inner.special_bits = preserve;
        }
        self
    }

    /// Set the modification time of each node to the time of the entry. The
    /// default is `true`. With `false`, each node keeps the time of the
    /// extraction.
    ///
    /// Call the options before the first entry.
    pub fn with_mtime(mut self, restore: bool) -> Self {
        if let SinkState::Idle(inner) = &mut self.state {
            inner.mtime = restore;
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
    /// attributes for which `filter` gives `true`. The sink can call `filter`
    /// more than one time for a name, so `filter` must give the same result
    /// each time. The policy of
    /// [`with_unsupported_xattr`](Self::with_unsupported_xattr) does not apply
    /// to an attribute that the filter removes.
    ///
    /// The default filter:
    ///
    /// - Apple platforms: all names. Apple names have no namespace prefix, for
    ///   example `com.apple.FinderInfo`.
    /// - Linux and Android: the names that start with `user.`. Thus the sink
    ///   does not write `security.*`, `system.*` and `trusted.*` attributes,
    ///   for example file capabilities and ACLs.
    /// - Other Unix platforms: the names that start with `user.`. The sink
    ///   cannot write extended attributes on these platforms.
    ///   [`with_unsupported_xattr`](Self::with_unsupported_xattr) sets the
    ///   action for them.
    ///
    /// Call the options before the first entry.
    pub fn with_xattr_filter<F>(mut self, filter: F) -> Self
    where
        F: Fn(&str) -> bool + Send + Sync + 'static,
    {
        if let SinkState::Idle(inner) = &mut self.state {
            inner.xattr_filter = Arc::new(filter);
        }
        self
    }

    /// Write one entry to the destination. A regular file of up to 256 KiB
    /// and a directory entry go to the batch. See [Batches](FsSink#batches).
    ///
    /// The method can return before the entry is on disk. Call
    /// [`finish`](Self::finish) after the last entry. If you stop before the
    /// end of the entries, call [`flush`](Self::flush). If the sink is
    /// dropped before, the entries that wait in the batch are not written,
    /// and no error comes back. A job that writes the batch or runs the
    /// finish continues after the drop. See [Finish](FsSink#finish).
    ///
    /// # Errors
    ///
    /// See [`FsSink`]. A call after [`finish`](Self::finish) gives an error.
    pub async fn write(&mut self, entry: TarEntry<'a, R>) -> io::Result<()> {
        poll_fn(|cx| self.poll_wait(cx)).await?;
        self.start(entry)?;
        poll_fn(|cx| self.poll_wait(cx)).await
    }

    /// Write the entries of the batch to the destination.
    ///
    /// # Errors
    ///
    /// See [`FsSink`].
    pub async fn flush(&mut self) -> io::Result<()> {
        poll_fn(|cx| self.poll_flush_batch(cx)).await
    }

    /// Write each entry of `entries` to the destination, then call
    /// [`finish`](Self::finish). The method writes full batches. See
    /// [Batches](FsSink#batches).
    ///
    /// # Errors
    ///
    /// See [`FsSink`]. After an error from `entries`, the method writes the
    /// batch. Then it gives the error from the batch if there is one, or else
    /// the error from `entries`. After an error, the method does not call
    /// [`finish`](Self::finish).
    pub async fn extract<S>(mut self, entries: S) -> io::Result<()>
    where
        S: Stream<Item = io::Result<TarEntry<'a, R>>>,
    {
        let mut entries = pin!(entries);
        while let Some(entry) = poll_fn(|cx| entries.as_mut().poll_next(cx)).await {
            match entry {
                Ok(entry) => self.write(entry).await?,
                Err(err) => {
                    self.flush().await?;
                    return Err(err);
                }
            }
        }
        self.finish().await
    }

    /// Write the entries of the batch, then set the metadata of the
    /// directories. After a call that succeeds, a second call does nothing.
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

    /// Run the current job, then write the batch.
    fn poll_flush_batch(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        loop {
            let inner = ready!(self.poll_idle(cx))?;
            if inner.batch.is_empty() {
                self.state = SinkState::Idle(inner);
                return Poll::Ready(Ok(()));
            }
            self.state = SinkState::Busy(Box::pin(flush(inner)));
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
                // The explicit deref lets rust-analyzer see the mutation.
                (*inner).finish()?;
                Ok(inner)
            })));
        }
    }
}

/// `start_send` only prepares the job for the entry. The sink runs the job
/// at the next `poll_ready`, `poll_flush` or `poll_close`. A regular file of
/// up to 256 KiB and a directory entry can also wait in the batch. After the
/// last entry, call `poll_close`, for example with `SinkExt::close`. If the
/// sink is dropped before, the last entry and the entries that wait in the
/// batch are not written, and no error comes back. A job that runs on the
/// blocking thread pool continues after the drop. See
/// [Finish](FsSink#finish). A `next` on a [`TarReader`](smol_tar::TarReader)
/// after `feed` of a regular file does not return until the sink runs the
/// job. For full batches, use [`write`](FsSink::write) or
/// [`extract`](FsSink::extract). You can also use `send` or `forward`, or
/// call `flush` after `feed`. A flush after each entry writes each entry in
/// its own job. See [Batches](FsSink#batches).
impl<'a, R: AsyncRead + Unpin + Send + 'a> Sink<TarEntry<'a, R>> for FsSink<'a, R> {
    type Error = io::Error;

    fn poll_ready(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.get_mut().poll_wait(cx)
    }

    fn start_send(self: Pin<&mut Self>, entry: TarEntry<'a, R>) -> io::Result<()> {
        self.get_mut().start(entry)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.get_mut().poll_flush_batch(cx)
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.get_mut().poll_finish(cx)
    }
}

/// Give the mode for the create of a regular file with the metadata `meta`.
/// The umask applies to it.
///
/// With [`ModePolicy::Skip`], the mode is 0o666. This is the final mode.
///
/// A mode without special bits and without xattrs to write is the final
/// mode. If the sink sets the owner, the group and other bits of the create
/// mode are only the bits that the owner, group and other classes all have.
/// Before the owner change, the file can have a different group. Then a user
/// other than the creator gets at most these common bits. After the owner
/// change, that user gets the bits of one class. Thus the user gets no access
/// before the change that the user does not get after it. A file with mode
/// 0o644 or 0o755 thus needs no mode change after the owner change.
/// [`sys::finish_file`] sets the final mode if the umask or this rule removed
/// bits.
///
/// Else the mode is 0o600, and the sink sets the final mode later.
fn file_create_mode(meta: &SetMeta) -> u32 {
    match meta.mode {
        None => 0o666,
        Some(mode) if mode & 0o7000 == 0 && meta.xattrs.is_empty() => {
            if meta.owner.is_some() {
                let common = (mode >> 6) & (mode >> 3) & mode & 0o7;
                (mode & 0o700) | (common << 3) | common
            } else {
                mode
            }
        }
        Some(_) => 0o600,
    }
}

/// Give the mode for the create of a FIFO or a device of the kind `kind`
/// with the mode policy `policy`. The umask applies to it. With
/// [`ModePolicy::Skip`], it is the final mode: 0o666 for a FIFO and 0o600
/// for a device. A device node with mode 0o666 gives each local user access
/// to the device. With the other policies, the mode is 0o600, and the sink
/// sets the final mode later.
fn special_create_mode(policy: ModePolicy, kind: FsKind) -> u32 {
    match (policy, kind) {
        (ModePolicy::Skip, FsKind::Fifo) => 0o666,
        _ => 0o600,
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
        let special = if self.special_bits { mode & 0o7000 } else { 0 };
        let mode = match self.mode {
            ModePolicy::Apply => Some((mode & 0o777) | special),
            ModePolicy::Mask(mask) => Some((mode & 0o777 & !mask) | special),
            ModePolicy::Skip => None,
        };
        SetMeta {
            mode,
            owner: self.owner.then_some((uid, gid)),
            mtime: self.mtime.then_some(mtime),
            // The list keeps the names that the filter removes. A list from
            // `TarReader` shares the global PAX records, and a copy would
            // not. The writes apply the filter.
            xattrs: if self.xattrs && attrs.iter().any(|(name, _)| (self.xattr_filter)(name)) {
                mem::take(attrs)
            } else {
                AttrList::new()
            },
            xattr_filter: Arc::clone(&self.xattr_filter),
            xattr_policy: self.xattr_policy,
        }
    }

    /// Set the metadata of the directories, deepest first. Then free the
    /// memory that the entries used.
    fn finish(&mut self) -> io::Result<()> {
        self.write_batch()?;
        self.parents = Vec::new();
        if let Some(root) = &self.root {
            let mut dirs = mem::take(&mut self.dirs);
            // Keep the last entry of each directory. Entries with different
            // paths can reach one directory through a parent symlink.
            dirs.sort_unstable_by(|a, b| a.id.cmp(&b.id).then_with(|| b.seq.cmp(&a.seq)));
            dirs.dedup_by_key(|dir| dir.id);
            dirs.sort_unstable_by(|a, b| b.depth.cmp(&a.depth).then_with(|| a.path.cmp(&b.path)));
            // Parent path and open parent of the directory before. The next
            // directory with the same parent path uses it.
            let mut last: Option<(&str, sys::Dir)> = None;
            for dir in &dirs {
                // The open of the last component does not follow a symlink,
                // thus the metadata does not go to the node behind it. The
                // open also checks that the path leads to the directory of
                // the entry.
                let (parent, name) = match dir.path.rsplit_once('/') {
                    Some((path, name)) => {
                        if last
                            .as_ref()
                            .is_none_or(|(last_path, _)| *last_path != path)
                        {
                            let opened = resolve::open_dir(root, path, false, None)
                                .map_err(|err| with_path(err, &dir.path))?;
                            last = Some((path, opened));
                        }
                        (&last.as_ref().expect("the parent is open").1, name)
                    }
                    None => (root, &*dir.path),
                };
                sys::finish_dir(
                    parent,
                    name,
                    dir.id,
                    &dir.meta,
                    !self.special_bits,
                    &|err| (self.owner_error)(&dir.path, err),
                )
                .map_err(|err| with_path(err, &dir.path))?;
            }
        }
        self.finished = true;
        self.root = None;
        self.root_id = None;
        self.created = HashMap::new();
        self.depths = HashMap::new();
        self.buf = Box::default();
        self.batch_data = Box::default();
        Ok(())
    }

    /// Create the files and the directories of the batch, in archive order.
    /// Write the body and set the metadata of each file. Keep the metadata of
    /// each directory for the finish, as [`make_entry_dir`] gives it.
    fn write_batch(&mut self) -> io::Result<()> {
        if self.batch.is_empty() {
            return Ok(());
        }
        let batch = mem::take(&mut self.batch);
        let mode = parent_mode(self.mode);
        let umask = self.mode == ModePolicy::Skip;
        let root = match &self.root {
            Some(root) => root,
            None => self.root.insert(sys::open_dest(&self.dest)?),
        };
        let mut pos = 0;
        for QueuedEntry { key, len, meta } in batch {
            // `parent_id` is `None` for the destination.
            let (parent, parent_id, name) = match key.rsplit_once('/') {
                Some((dir, name)) => {
                    let OpenDir { dir, id, .. } = parent_dir(root, &mut self.parents, dir, mode)
                        .map_err(|err| with_path(err, &key))?;
                    (&*dir, Some(id), name)
                }
                None => (root, None, &*key),
            };
            let mut removed = false;
            let Some(len) = len else {
                let mut created = None;
                let placed = make_entry_dir(
                    root,
                    &mut self.root_id,
                    parent,
                    parent_id,
                    name,
                    &meta,
                    umask,
                    &mut self.depths,
                    &|err| (self.owner_error)(&key, err),
                    &mut removed,
                    &mut created,
                );
                if removed {
                    self.parents.clear();
                }
                let placed = placed.map_err(|err| with_path(err, &key))?;
                self.created.remove(&key);
                // A removal clears the stack, and the new directory also does
                // not go on it. The next entry opens its parent path from the
                // destination.
                if let (false, Some((dir, id))) = (removed, created) {
                    let open = OpenDir {
                        path: key.clone(),
                        dir,
                        id: Some(id),
                    };
                    push_dir(&mut self.parents, open);
                }
                if let Some((id, depth)) = placed {
                    let seq = self.dirs.len() as u32;
                    self.dirs.push(DirMeta {
                        path: key,
                        depth,
                        seq,
                        id,
                        meta,
                    });
                }
                continue;
            };
            let body = &self.batch_data[pos..pos + len];
            pos += len;
            let out = make_file(parent, name, file_create_mode(&meta), &mut removed);
            if removed {
                self.parents.clear();
            }
            let mut out = out.map_err(|err| with_path(err, &key))?;
            let id = out
                .write_all(body)
                .and_then(|()| sys::finish_file(&out, &meta, &|err| (self.owner_error)(&key, err)))
                .map_err(|err| with_path(err, &key))?;
            self.created.insert(key, id);
        }
        self.batch_len = 0;
        Ok(())
    }
}

/// Write the batch of the sink on the blocking thread pool. Give back the
/// state.
async fn flush(mut inner: Box<Inner>) -> io::Result<Box<Inner>> {
    if inner.batch.is_empty() {
        return Ok(inner);
    }
    rt::unblock(move || {
        inner.write_batch()?;
        Ok(inner)
    })
    .await
}

/// Add a file of up to `FILE_BUFFER_SIZE` to the batch. Write the batch if
/// it is full. Give back the state.
///
/// An error from the entry writes the files before it, then the sink gives
/// the error.
async fn queue_file<R: AsyncRead + Unpin + Send>(
    mut inner: Box<Inner>,
    mut file: TarRegularFile<'_, R>,
) -> io::Result<Box<Inner>> {
    let len = file.size() as usize;
    if !inner.batch.is_empty() && inner.batch_len + len > rt::BATCH_BYTES {
        inner = flush(inner).await?;
    }
    let meta = inner.set_meta(
        file.mode(),
        file.uid(),
        file.gid(),
        file.mtime(),
        file.attrs_mut(),
    );
    if len > 0 && inner.batch_data.is_empty() {
        inner.batch_data = vec![0; rt::BATCH_BYTES].into_boxed_slice();
    }
    // A batch that is not empty has room for the body, because of the flush
    // above. A body is not larger than the buffer.
    let start = inner.batch_len;
    let read = match entry_key(file.path()) {
        Ok(key) => match read_body(&mut file, &mut inner.batch_data[start..start + len]).await {
            Ok(()) => Ok(key),
            Err(err) => Err(with_path(err, &key)),
        },
        Err(err) => Err(err),
    };
    // If the body is empty or the read failed, the drop gives the source back
    // to the reader and wakes the task that waits for the next entry. A body
    // that was read to the end gave the source back already. The drop comes
    // before the flush, so the reader can continue while the flush runs.
    drop(file);
    let key = match read {
        Ok(key) => key,
        Err(err) => {
            flush(inner).await?;
            return Err(err);
        }
    };
    inner.batch.push(QueuedEntry {
        key,
        len: Some(len),
        meta,
    });
    inner.batch_len += len;
    if inner.batch.len() >= rt::BATCH_FILES || inner.batch_len >= rt::BATCH_BYTES {
        inner = flush(inner).await?;
    }
    Ok(inner)
}

/// Add a directory entry to the batch. Write the batch if it is full. Give
/// back the state.
///
/// An error from the entry path writes the entries before it, then the sink
/// gives the error.
async fn queue_dir(mut inner: Box<Inner>, mut dir: TarDirectory) -> io::Result<Box<Inner>> {
    if is_root_path(dir.path()) {
        return Ok(inner);
    }
    let key = match entry_key(dir.path()) {
        Ok(key) => key,
        Err(err) => {
            flush(inner).await?;
            return Err(err);
        }
    };
    let meta = inner.set_meta(
        dir.mode(),
        dir.uid(),
        dir.gid(),
        dir.mtime(),
        dir.attrs_mut(),
    );
    inner.batch.push(QueuedEntry {
        key,
        len: None,
        meta,
    });
    if inner.batch.len() >= rt::BATCH_FILES {
        inner = flush(inner).await?;
    }
    Ok(inner)
}

/// Write one entry. Give back the state.
async fn process<R: AsyncRead + Unpin + Send>(
    mut inner: Box<Inner>,
    entry: TarEntry<'_, R>,
) -> io::Result<Box<Inner>> {
    let entry = match entry {
        TarEntry::File(file) if file.size() <= rt::FILE_BUFFER_SIZE as u64 => {
            return queue_file(inner, file).await;
        }
        TarEntry::Directory(dir) => return queue_dir(inner, dir).await,
        entry => entry,
    };
    // Write the batch before each other entry. Then each entry sees the
    // entries before it, for example a hard link sees its target. An error
    // from the entry comes after the sink wrote the entries before it.
    inner = flush(inner).await?;
    let key = entry_key(entry.path())?;
    match entry {
        TarEntry::File(file) => write_file(inner, key, file).await,
        TarEntry::Directory(_) => unreachable!("the batch takes the directory entries"),
        TarEntry::Symlink(mut link) => {
            let meta = inner.set_meta(
                link.mode(),
                link.uid(),
                link.gid(),
                link.mtime(),
                link.attrs_mut(),
            );
            rt::unblock(move || {
                let owner_error = Arc::clone(&inner.owner_error);
                let state = &mut *inner;
                let id = place(state, &key, |_, parent, name, removed| {
                    make_symlink(
                        parent,
                        name,
                        link.link(),
                        &meta,
                        &|err| owner_error(&key, err),
                        removed,
                    )
                })?;
                state.created.insert(key, id);
                Ok(inner)
            })
            .await
        }
        TarEntry::Link(link) => {
            rt::unblock(move || {
                let state = &mut *inner;
                // Check the target before any change at the entry path.
                let target = link_target(link.link());
                let Some((target, id)) = target.and_then(|target| {
                    let id = *state.created.get(&*target)?;
                    Some((target, id))
                }) else {
                    return missing_link_target(state.missing_link, &key, link.link())
                        .map(|()| inner);
                };
                if *target == *key {
                    return Ok(inner);
                }
                let linked = place(state, &key, |root, parent, name, removed| {
                    make_link(root, &target, id, parent, &key, name, removed)
                })?;
                if !linked {
                    return missing_link_target(state.missing_link, &key, link.link())
                        .map(|()| inner);
                }
                state.created.insert(key, id);
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
            let create_mode = special_create_mode(inner.mode, FsKind::Fifo);
            rt::unblock(move || {
                let owner_error = Arc::clone(&inner.owner_error);
                let state = &mut *inner;
                let id = place(state, &key, |_, parent, name, removed| {
                    make_special(
                        parent,
                        name,
                        FsKind::Fifo,
                        None,
                        create_mode,
                        &meta,
                        &|err| owner_error(&key, err),
                        removed,
                    )
                })?;
                state.created.insert(key, id);
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
            let create_mode = special_create_mode(inner.mode, kind);
            rt::unblock(move || {
                let owner_error = Arc::clone(&inner.owner_error);
                let state = &mut *inner;
                let id = place(state, &key, |_, parent, name, removed| {
                    make_special(
                        parent,
                        name,
                        kind,
                        device,
                        create_mode,
                        &meta,
                        &|err| owner_error(&key, err),
                        removed,
                    )
                })?;
                state.created.insert(key, id);
                Ok(inner)
            })
            .await
        }
    }
}

/// Write a regular file larger than `FILE_BUFFER_SIZE`. Give back the state.
///
/// The body goes to the file in chunks of the buffer size.
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
    if inner.buf.is_empty() {
        inner.buf = vec![0; rt::FILE_BUFFER_SIZE].into_boxed_slice();
    }
    let create_mode = file_create_mode(&meta);
    let (mut inner, key, out) = rt::unblock(move || {
        let state = &mut *inner;
        let out = place(state, &key, |_, parent, name, removed| {
            make_file(parent, name, create_mode, removed)
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
        let id = sys::finish_file(&out, &meta, &|err| (inner.owner_error)(&key, err))
            .map_err(|err| with_path(err, &key))?;
        drop(out);
        inner.created.insert(key, id);
        Ok(inner)
    })
    .await
}

/// Read exactly `buf.len()` bytes of a file body.
async fn read_body<R: AsyncRead + Unpin>(body: &mut R, buf: &mut [u8]) -> io::Result<()> {
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

/// Apply `policy` for the hard link `key` with a target that the sink did
/// not create.
fn missing_link_target(policy: MissingLinkTarget, key: &str, target: &str) -> io::Result<()> {
    match policy {
        MissingLinkTarget::Error => Err(io::Error::new(
            io::ErrorKind::NotFound,
            format!("{key}: link target {target} is not in the archive"),
        )),
        MissingLinkTarget::Skip => Ok(()),
    }
}

/// Open the destination and the parent of `key`, then call `f` with the
/// destination, the parent, the last component and a flag that `f` sets
/// when it removes a node. Create the missing parent directories. An error
/// gets `key` as context.
fn place<T>(
    state: &mut Inner,
    key: &str,
    f: impl FnOnce(&sys::Dir, &sys::Dir, &str, &mut bool) -> io::Result<T>,
) -> io::Result<T> {
    let mode = parent_mode(state.mode);
    let root = match &mut state.root {
        Some(root) => &*root,
        None => &*state.root.insert(sys::open_dest(&state.dest)?),
    };
    let (parent, name) = match key.rsplit_once('/') {
        Some((dir, name)) => (
            &parent_dir(root, &mut state.parents, dir, mode)
                .map_err(|err| with_path(err, key))?
                .dir,
            name,
        ),
        None => (root, key),
    };
    let mut removed = false;
    let result = f(root, parent, name, &mut removed);
    if removed {
        state.parents.clear();
    }
    result.map_err(|err| with_path(err, key))
}

/// Give the mode for the create of a missing parent directory with the
/// mode policy `policy`. The umask applies to this mode, and the sink does
/// not change the mode later. `None` is the mode 0o777 less the umask. With
/// [`ModePolicy::Mask`], the mode is 0o777 without the bits of the mask,
/// less the umask.
fn parent_mode(policy: ModePolicy) -> Option<u32> {
    match policy {
        ModePolicy::Mask(mask) => Some(0o777 & !mask),
        ModePolicy::Apply | ModePolicy::Skip => None,
    }
}

/// Give the directory at the normalized path `dir` in `root`. Remove the
/// directories from the end of `stack` that are not `dir` and not an
/// ancestor of `dir`. If the last directory of `stack` has the path `dir`,
/// it is the result. Else open `dir`, create the missing directories with
/// the mode `mode`, as [`resolve::open_dir`] does, and push the directory on
/// `stack` for the next entries. The rules of the open are in
/// `parent_dir_with`.
fn parent_dir<'c>(
    root: &sys::Dir,
    stack: &'c mut Vec<OpenDir>,
    dir: &str,
    mode: Option<u32>,
) -> io::Result<&'c mut OpenDir> {
    parent_dir_with(root, stack, dir, |base, path| {
        resolve::open_dir(base, path, true, mode)
    })
}

/// Give the directory at the normalized path `dir` in `root`, by the rules
/// of `parent_dir`. `open(base, path)` opens the directory at `path` in
/// `base`. If the last directory of `stack` is an ancestor of `dir`, open
/// the rest of `dir` in this ancestor. If this open fails, open `dir` in
/// `root`, and use that result. Thus a symlink in the rest can lead outside
/// the ancestor if its target stays inside `root`. If no directory of
/// `stack` is an ancestor of `dir`, open `dir` in `root`.
///
/// An open in the ancestor creates directories only below the ancestor.
/// These are the directories that an open of `dir` from `root` through the
/// same ancestor also creates.
fn parent_dir_with<'c>(
    root: &sys::Dir,
    stack: &'c mut Vec<OpenDir>,
    dir: &str,
    mut open: impl FnMut(&sys::Dir, &str) -> io::Result<sys::Dir>,
) -> io::Result<&'c mut OpenDir> {
    while stack
        .last()
        .is_some_and(|top| *top.path != *dir && !is_below(dir, &top.path))
    {
        stack.pop();
    }
    if stack.last().is_none_or(|top| *top.path != *dir) {
        let opened = match stack.last() {
            Some(top) => open(&top.dir, &dir[top.path.len() + 1..]).or_else(|_| open(root, dir)),
            None => open(root, dir),
        }?;
        push_dir(
            stack,
            OpenDir {
                path: dir.into(),
                dir: opened,
                id: None,
            },
        );
    }
    Ok(stack.last_mut().expect("the stack has a directory"))
}

/// Push `open` on `stack`. Remove the directories from the end of `stack`
/// that are not an ancestor of `open` first. If `stack` has `OPEN_DIRS`
/// directories, remove the first one.
fn push_dir(stack: &mut Vec<OpenDir>, open: OpenDir) {
    while stack
        .last()
        .is_some_and(|top| !is_below(&open.path, &top.path))
    {
        stack.pop();
    }
    if stack.len() == OPEN_DIRS {
        stack.remove(0);
    }
    stack.push(open);
}

/// Give `true` if the normalized path `path` is below the normalized path
/// `dir`.
fn is_below(path: &str, dir: &str) -> bool {
    path.strip_prefix(dir)
        .is_some_and(|rest| rest.starts_with('/'))
}

/// Make room at `name` after a create failed with `AlreadyExists`. Remove a
/// node that is not a directory, and set `removed`. If a directory has the
/// name, give its identity.
fn clear(parent: &sys::Dir, name: &str, removed: &mut bool) -> io::Result<Option<sys::FileId>> {
    match sys::lookup(parent, name)? {
        Existing::Directory(id) => Ok(Some(id)),
        Existing::Other => {
            *removed = true;
            sys::remove(parent, name)?;
            Ok(None)
        }
        Existing::Missing => Ok(None),
    }
}

/// Create a node that is not a directory with `make`. If the name exists,
/// remove the node, set `removed` and call `make` again. A directory at the
/// name gives `AlreadyExists`.
fn create<T>(
    parent: &sys::Dir,
    name: &str,
    removed: &mut bool,
    mut make: impl FnMut() -> io::Result<T>,
) -> io::Result<T> {
    match make() {
        Err(err) if err.kind() == io::ErrorKind::AlreadyExists => {
            replace(parent, name, removed, make)
        }
        result => result,
    }
}

/// Create a node that is not a directory with `make` after a create failed
/// with `AlreadyExists`. Remove the node at the name, set `removed` and call
/// `make`. A directory at the name gives `AlreadyExists`.
fn replace<T>(
    parent: &sys::Dir,
    name: &str,
    removed: &mut bool,
    make: impl FnOnce() -> io::Result<T>,
) -> io::Result<T> {
    if clear(parent, name, removed)?.is_some() {
        return Err(io::Error::new(
            io::ErrorKind::AlreadyExists,
            "a directory exists at the path",
        ));
    }
    make()
}

/// Create the directory `name` in `parent`. With `umask` set, use the mode
/// 0o777 less the umask, else 0o700. Give the open new directory and its
/// identity, as [`sys::create_dir`] does. An existing directory stays and
/// gives only its identity. If a node that is not a directory has the name,
/// remove it and set `removed`.
fn make_dir(
    parent: &sys::Dir,
    name: &str,
    umask: bool,
    removed: &mut bool,
) -> io::Result<(Option<sys::Dir>, Option<sys::FileId>)> {
    let created = match sys::create_dir(parent, name, umask) {
        Err(err) if err.kind() == io::ErrorKind::AlreadyExists => {
            if let Some(id) = clear(parent, name, removed)? {
                return Ok((None, Some(id)));
            }
            sys::create_dir(parent, name, umask)?
        }
        result => result?,
    };
    Ok(created.unzip())
}

/// Create the directory `name` in `parent` for a directory entry with the
/// metadata `meta`, as [`make_dir`] does. If the finish does not set the
/// metadata of the directory, set the owner and the extended attributes now
/// with `on_owner_error` and give `None`. Else give the identity of the
/// directory and its depth on disk below `root`. `root_id` and `parent_id`
/// keep the identities of `root` and `parent` when they are known.
/// `parent_id` is `None` if `parent` is `root`. `depths` is the cache of
/// [`disk_depth`]. Set `created` to the open new directory and its identity,
/// as [`make_dir`] gives them.
#[allow(clippy::too_many_arguments)]
fn make_entry_dir(
    root: &sys::Dir,
    root_id: &mut Option<sys::FileId>,
    parent: &sys::Dir,
    parent_id: Option<&mut Option<sys::FileId>>,
    name: &str,
    meta: &SetMeta,
    umask: bool,
    depths: &mut HashMap<sys::FileId, u32>,
    on_owner_error: &dyn Fn(io::Error) -> io::Result<()>,
    removed: &mut bool,
    created: &mut Option<(sys::Dir, sys::FileId)>,
) -> io::Result<Option<(sys::FileId, u32)>> {
    let (dir, id) = make_dir(parent, name, umask, removed)?;
    *created = dir.zip(id);
    // Without a mode and a time to set, the children have no effect on the
    // metadata. Thus the sink sets it now.
    if meta.mode.is_none() && meta.mtime.is_none() {
        if meta.owner.is_some() || !meta.xattrs.is_empty() {
            sys::set_meta_dir(&sys::open_child_dir(parent, name)?, meta, on_owner_error)?;
        }
        return Ok(None);
    }
    // The finish uses the identity to find the directory, and the depth to
    // set the metadata of the directories below it first.
    let parent_id = match parent_id {
        Some(id) => known_id(parent, id)?,
        None => known_id(root, root_id)?,
    };
    let depth = disk_depth(root, root_id, parent, parent_id, depths)? + 1;
    let id = match id {
        Some(id) => id,
        None => sys::id_at(parent, name)?,
    };
    depths.insert(id, depth);
    Ok(Some((id, depth)))
}

/// Give the identity of the open directory `dir`. Keep it in `id`. If `id`
/// has an identity, give it.
fn known_id(dir: &sys::Dir, id: &mut Option<sys::FileId>) -> io::Result<sys::FileId> {
    match *id {
        Some(id) => Ok(id),
        None => Ok(*id.insert(sys::dir_id(dir)?)),
    }
}

/// Give the depth on disk of the directory `dir` with the identity `dir_id`
/// below `root`. `root` has depth 0. `root_id` keeps the identity of `root`
/// when it is known. Go up from `dir` until a directory in `depths` or
/// `root`, and keep the depth of each directory on the way in `depths`. The
/// walk needs search permission on each directory on the way. A directory
/// that is not below `root` gives an
/// [`InvalidData`](io::ErrorKind::InvalidData) error "file changed".
fn disk_depth(
    root: &sys::Dir,
    root_id: &mut Option<sys::FileId>,
    dir: &sys::Dir,
    dir_id: sys::FileId,
    depths: &mut HashMap<sys::FileId, u32>,
) -> io::Result<u32> {
    if depths.is_empty() {
        depths.insert(known_id(root, root_id)?, 0);
    }
    let mut ids = Vec::new();
    let mut id = dir_id;
    let mut opened = None;
    let base = loop {
        if let Some(&depth) = depths.get(&id) {
            break depth;
        }
        ids.push(id);
        let parent = sys::open_parent(opened.as_ref().unwrap_or(dir))?;
        id = sys::dir_id(&parent)?;
        // The root of the filesystem is its own parent.
        if ids.last() == Some(&id) {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "file changed"));
        }
        opened = Some(parent);
    };
    for (depth, id) in (base + 1..).zip(ids.iter().rev()) {
        depths.insert(*id, depth);
    }
    Ok(base + ids.len() as u32)
}

/// Create the regular file `name` in `parent` with `mode` less the umask
/// and open it for writing. Set `removed` if a node is removed.
fn make_file(
    parent: &sys::Dir,
    name: &str,
    mode: u32,
    removed: &mut bool,
) -> io::Result<std::fs::File> {
    create(parent, name, removed, || {
        sys::create_file(parent, name, mode)
    })
}

/// Create the symlink `name` in `parent` and set its metadata with
/// `on_owner_error`. Give the identity of the symlink. Set `removed` if a
/// node is removed.
fn make_symlink(
    parent: &sys::Dir,
    name: &str,
    target: &str,
    meta: &SetMeta,
    on_owner_error: &dyn Fn(io::Error) -> io::Result<()>,
    removed: &mut bool,
) -> io::Result<sys::FileId> {
    create(parent, name, removed, || {
        sys::create_symlink(parent, name, target)
    })?;
    let id = sys::id_at(parent, name)?;
    sys::set_meta_at(parent, name, FsKind::Symlink, meta, on_owner_error)?;
    Ok(id)
}

/// Create the FIFO or device `name` in `parent` with `mode` less the umask
/// and set its metadata with `on_owner_error`. Give the identity of the
/// node. Set `removed` if a node is removed.
#[allow(clippy::too_many_arguments)]
fn make_special(
    parent: &sys::Dir,
    name: &str,
    kind: FsKind,
    device: Option<(u32, u32)>,
    mode: u32,
    meta: &SetMeta,
    on_owner_error: &dyn Fn(io::Error) -> io::Result<()>,
    removed: &mut bool,
) -> io::Result<sys::FileId> {
    create(parent, name, removed, || {
        sys::create_special(parent, name, kind, device, mode)
    })?;
    let id = sys::id_at(parent, name)?;
    sys::set_meta_at(parent, name, kind, meta, on_owner_error)?;
    Ok(id)
}

/// Create the name `name` in `parent` for the node at the normalized path
/// `target` in `root`. `key` is the normalized path of `name`. If `target`
/// is in the same directory, the link uses `parent` for the target. If
/// `target` leads to no node or to a node without the identity `id`, give
/// `false` and create nothing. If `name` is a link to the node already,
/// change nothing and give `true`. Set `removed` if a node is removed.
fn make_link(
    root: &sys::Dir,
    target: &str,
    id: sys::FileId,
    parent: &sys::Dir,
    key: &str,
    name: &str,
    removed: &mut bool,
) -> io::Result<bool> {
    let (target_dir, from_name) = match target.rsplit_once('/') {
        Some((dir, from_name)) => (Some(dir), from_name),
        None => (None, target),
    };
    let opened;
    let from = if target_dir == key.rsplit_once('/').map(|(dir, _)| dir) {
        parent
    } else if let Some(dir) = target_dir {
        opened = match resolve::open_dir(root, dir, false, None) {
            Ok(opened) => opened,
            // A missing component, a component that is not a directory and a
            // symlink loop: the target path leads to no node.
            Err(err)
                if matches!(
                    err.kind(),
                    io::ErrorKind::NotFound | io::ErrorKind::NotADirectory
                ) || err.raw_os_error() == Some(Errno::LOOP.raw_os_error()) =>
            {
                return Ok(false)
            }
            Err(err) => return Err(with_path(err, &format!("link target {target}"))),
        };
        &opened
    } else {
        root
    };
    // A parent symlink that changed after the create of the target can lead
    // `target` to another node or to no node.
    match sys::id_at(from, from_name) {
        Ok(found) if found == id => {}
        Ok(_) => return Ok(false),
        Err(err) if err.kind() == io::ErrorKind::NotFound => return Ok(false),
        Err(err) => return Err(err),
    }
    let link = || sys::create_link(from, from_name, parent, name);
    match link() {
        Ok(()) => {}
        Err(err) if err.kind() == io::ErrorKind::AlreadyExists => {
            // Two paths can lead to one directory entry, for example through
            // a parent symlink. Then `name` is the target, and a remove of
            // `name` deletes the target.
            match sys::id_at(parent, name) {
                Ok(found) if found == id => {}
                _ => replace(parent, name, removed, link)?,
            }
        }
        Err(err) => return Err(err),
    }
    Ok(true)
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::{rt::block_on, testutil::TempDir, FsFile},
        futures::{task::noop_waker_ref, SinkExt, StreamExt},
        smol_tar::{TarDirectory, TarLink, TarReader, TarRegularFileReader, TarWriter},
        static_assertions::assert_impl_all,
        std::{
            fs,
            io::ErrorKind,
            os::unix::fs::{symlink, MetadataExt, PermissionsExt},
            path::Path,
            sync::{
                atomic::{AtomicUsize, Ordering},
                mpsc::{self, RecvTimeoutError},
                Arc,
            },
            task::{Wake, Waker},
            thread,
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
        // A directory that the process creates with the mode 0o777 and its
        // umask.
        fs::create_dir(tmp.path().join("ref")).unwrap();
        let ref_meta = fs::metadata(tmp.path().join("ref")).unwrap();
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
            assert_eq!(meta.mode() & 0o777, ref_meta.mode() & 0o777);
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

    /// An archive with an empty file extracts completely.
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

    impl CountWaker {
        /// Make a counter and a waker that increments it.
        fn new() -> (Arc<Self>, Waker) {
            let count = Arc::new(Self(AtomicUsize::new(0)));
            (count.clone(), Waker::from(count))
        }

        /// Number of wakes.
        fn get(&self) -> usize {
            self.0.load(Ordering::SeqCst)
        }
    }

    impl Wake for CountWaker {
        fn wake(self: Arc<Self>) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    /// The `poll_ready` that queues an empty file gives the source back to
    /// the reader and wakes the reader task. The sink is idle after the call
    /// and does not wake its own task.
    #[test]
    fn empty_body_drop_wakes_reader() {
        let tmp = TempDir::new();
        let data = archive(vec![file("e", b""), file("f", b"data")]);
        let mut reader = TarReader::new(&data[..]);
        let mut sink = FsSink::new(tmp.path());
        let entry = block_on(reader.next()).unwrap().unwrap();
        Pin::new(&mut sink).start_send(entry).unwrap();
        let (stream_wakes, stream_waker) = CountWaker::new();
        let mut stream_cx = Context::from_waker(&stream_waker);
        assert!(Pin::new(&mut reader).poll_next(&mut stream_cx).is_pending());
        assert_eq!(stream_wakes.get(), 0);
        let (sink_wakes, sink_waker) = CountWaker::new();
        let poll = Pin::new(&mut sink).poll_ready(&mut Context::from_waker(&sink_waker));
        assert!(matches!(poll, Poll::Ready(Ok(()))));
        assert!(matches!(sink.state, SinkState::Idle(_)));
        assert_eq!(sink_wakes.get(), 0);
        assert_eq!(stream_wakes.get(), 1);
        let Poll::Ready(Some(Ok(entry))) = Pin::new(&mut reader).poll_next(&mut stream_cx) else {
            panic!("the reader does not give the second entry");
        };
        assert_eq!(entry.path(), "f");
    }

    /// The first `poll_flush` after the `start_send` of an empty file wakes
    /// the reader task. This is the call order of `StreamExt::forward`.
    #[test]
    fn empty_body_flush_wakes_reader() {
        let tmp = TempDir::new();
        let data = archive(vec![file("e", b""), file("f", b"data")]);
        let mut reader = TarReader::new(&data[..]);
        let mut sink = FsSink::new(tmp.path());
        // The flush job uses the blocking thread pool of the runtime. Thus
        // the test polls the sink in `block_on`.
        block_on(async {
            let entry = reader.next().await.unwrap()?;
            Pin::new(&mut sink).start_send(entry)?;
            let (stream_wakes, stream_waker) = CountWaker::new();
            let mut stream_cx = Context::from_waker(&stream_waker);
            assert!(Pin::new(&mut reader).poll_next(&mut stream_cx).is_pending());
            assert_eq!(stream_wakes.get(), 0);
            let mut sink_cx = Context::from_waker(noop_waker_ref());
            if let Poll::Ready(result) = Pin::new(&mut sink).poll_flush(&mut sink_cx) {
                result?;
            }
            assert_eq!(stream_wakes.get(), 1);
            sink.flush().await?;
            let entry = reader.next().await.unwrap()?;
            assert_eq!(entry.path(), "f");
            Ok::<_, io::Error>(())
        })
        .unwrap();
    }

    /// After `feed` of a file with a body, the reader does not give the next
    /// entry. The `flush` runs the job, and the job gives the source back to
    /// the reader and wakes the reader task.
    #[test]
    fn feed_then_next_waits_for_sink() {
        let tmp = TempDir::new();
        let data = archive(vec![file("f", b"data"), file("g", b"more")]);
        let mut reader = TarReader::new(&data[..]);
        let mut sink = FsSink::new(tmp.path());
        block_on(async {
            let entry = reader.next().await.unwrap()?;
            sink.feed(entry).await?;
            let (stream_wakes, stream_waker) = CountWaker::new();
            let mut stream_cx = Context::from_waker(&stream_waker);
            assert!(reader.poll_next_unpin(&mut stream_cx).is_pending());
            assert_eq!(stream_wakes.get(), 0);
            SinkExt::flush(&mut sink).await?;
            assert_eq!(stream_wakes.get(), 1);
            let entry = reader.next().await.unwrap()?;
            assert_eq!(entry.path(), "g");
            Ok::<_, io::Error>(())
        })
        .unwrap();
    }

    /// Run `f` on a new thread and give its result. Panic if `f` does not
    /// return in 10 seconds.
    fn within_timeout<T: Send + 'static>(f: impl FnOnce() -> T + Send + 'static) -> T {
        let (tx, rx) = mpsc::channel();
        let handle = thread::spawn(move || {
            let _ = tx.send(f());
        });
        match rx.recv_timeout(Duration::from_secs(10)) {
            Ok(value) => value,
            Err(RecvTimeoutError::Disconnected) => {
                std::panic::resume_unwind(handle.join().unwrap_err())
            }
            Err(RecvTimeoutError::Timeout) => panic!("the call does not return in 10 seconds"),
        }
    }

    /// A `flush` after each `feed` lets the reader give each entry.
    #[test]
    fn feed_flush_then_next_extracts_all() {
        let tmp = TempDir::new();
        let big = (0..rt::FILE_BUFFER_SIZE + 100).map(|i| (i % 251) as u8);
        let big: &'static [u8] = Vec::leak(big.collect());
        let data = archive(vec![
            TarDirectory::new("d/").into(),
            file("d/e", b""),
            file("d/big", big),
            file("d/f", b"data"),
        ]);
        let dest = tmp.path().to_path_buf();
        within_timeout(move || {
            block_on(async {
                let mut reader = TarReader::new(&data[..]);
                let mut sink = FsSink::new(dest);
                while let Some(entry) = reader.next().await {
                    sink.feed(entry?).await?;
                    SinkExt::flush(&mut sink).await?;
                }
                SinkExt::close(&mut sink).await
            })
        })
        .unwrap();
        assert_eq!(fs::read(tmp.path().join("d/e")).unwrap(), b"");
        assert_eq!(fs::read(tmp.path().join("d/big")).unwrap(), big);
        assert_eq!(fs::read(tmp.path().join("d/f")).unwrap(), b"data");
    }

    /// `write` runs the job before it returns. Thus the reader gives each
    /// entry. The small entries wait in one batch until the large file.
    #[test]
    fn write_then_next_keeps_batch() {
        let tmp = TempDir::new();
        let big = (0..rt::FILE_BUFFER_SIZE + 100).map(|i| (i % 251) as u8);
        let big: &'static [u8] = Vec::leak(big.collect());
        let data = archive(vec![
            TarDirectory::new("d/").into(),
            file("d/e", b""),
            file("d/f", b"data"),
            file("d/big", big),
        ]);
        let dest = tmp.path().to_path_buf();
        let batch_len = within_timeout(move || {
            block_on(async {
                let mut reader = TarReader::new(&data[..]);
                let mut sink = FsSink::new(dest);
                let mut batch_len = None;
                while let Some(entry) = reader.next().await {
                    let entry = entry?;
                    if entry.path() == "d/big" {
                        let SinkState::Idle(inner) = &sink.state else {
                            panic!("the sink is not idle");
                        };
                        batch_len = Some(inner.batch.len());
                    }
                    sink.write(entry).await?;
                }
                sink.finish().await?;
                Ok::<_, io::Error>(batch_len)
            })
        })
        .unwrap();
        assert_eq!(batch_len, Some(3));
        assert!(fs::metadata(tmp.path().join("d")).unwrap().is_dir());
        assert_eq!(fs::read(tmp.path().join("d/e")).unwrap(), b"");
        assert_eq!(fs::read(tmp.path().join("d/f")).unwrap(), b"data");
        assert_eq!(fs::read(tmp.path().join("d/big")).unwrap(), big);
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

    #[test]
    fn file_create_mode_with_owner_has_common_bits() {
        let meta = |mode: Option<u32>, owner: Option<(u32, u32)>| SetMeta {
            mode,
            owner,
            mtime: None,
            xattrs: AttrList::new(),
            xattr_filter: Arc::new(|_| true),
            xattr_policy: UnsupportedXattr::Error,
        };
        assert_eq!(file_create_mode(&meta(Some(0o640), None)), 0o640);
        assert_eq!(file_create_mode(&meta(Some(0o640), Some((1, 2)))), 0o600);
        assert_eq!(file_create_mode(&meta(Some(0o644), Some((1, 2)))), 0o644);
        assert_eq!(file_create_mode(&meta(Some(0o755), Some((1, 2)))), 0o755);
        assert_eq!(file_create_mode(&meta(Some(0o750), Some((1, 2)))), 0o700);
        assert_eq!(file_create_mode(&meta(Some(0o604), Some((1, 2)))), 0o600);
        assert_eq!(file_create_mode(&meta(Some(0o477), Some((1, 2)))), 0o444);
        assert_eq!(file_create_mode(&meta(Some(0o4755), Some((1, 2)))), 0o600);
        // Without a mode to set, the sink does not change the mode later.
        assert_eq!(file_create_mode(&meta(None, Some((1, 2)))), 0o666);
    }

    #[test]
    fn special_create_mode_is_owner_only_except_skipped_fifo() {
        let policies = [
            (ModePolicy::Skip, 0o666),
            (ModePolicy::Apply, 0o600),
            (ModePolicy::Mask(0o022), 0o600),
        ];
        for (policy, fifo) in policies {
            let kinds = [
                (FsKind::Fifo, fifo),
                (FsKind::CharDevice, 0o600),
                (FsKind::BlockDevice, 0o600),
            ];
            for (kind, mode) in kinds {
                let got = special_create_mode(policy, kind);
                assert_eq!(got, mode, "{policy:?} {kind:?}");
            }
        }
    }

    /// With the owner option, a partly written file with mode 0o640 has no
    /// group and other bits. The body ends early, thus the sink does not set
    /// the owner.
    #[test]
    fn partial_file_with_owner_hides_group_bits() {
        let tmp = TempDir::new();
        let size = rt::FILE_BUFFER_SIZE as u64 + 10;
        let err = block_on(async {
            let mut sink = FsSink::new(tmp.path()).with_owner(true);
            let entry = TarRegularFile::new("f", size, &b"12345"[..]).with_mode(0o640);
            sink.write(entry.into()).await.unwrap_err()
        });
        assert_eq!(err.kind(), ErrorKind::UnexpectedEof);
        let mode = fs::metadata(tmp.path().join("f"))
            .unwrap()
            .permissions()
            .mode();
        assert_eq!(mode & 0o077, 0);
    }

    /// Files of size 0 do not allocate the buffer of the batch.
    #[test]
    fn empty_files_do_not_allocate_batch_buffer() {
        let tmp = TempDir::new();
        block_on(async {
            let mut sink = FsSink::new(tmp.path());
            for name in ["e1", "e2", "e3"] {
                sink.write(file(name, b"")).await.unwrap();
            }
            match &sink.state {
                SinkState::Idle(inner) => assert!(inner.batch_data.is_empty()),
                _ => panic!("the sink is not idle"),
            }
            sink.finish().await.unwrap();
        });
        for name in ["e1", "e2", "e3"] {
            assert_eq!(fs::metadata(tmp.path().join(name)).unwrap().len(), 0);
        }
    }

    /// Give the paths of the open directories of the idle sink `sink`.
    fn open_paths<'s, R>(sink: &'s FsSink<'_, R>) -> Vec<&'s str> {
        match &sink.state {
            SinkState::Idle(inner) => inner.parents.iter().map(|open| &*open.path).collect(),
            _ => panic!("the sink is not idle"),
        }
    }

    /// The open directories of the sink are a chain of ancestors. A new
    /// directory goes on the stack with its identity.
    #[test]
    fn open_parents_are_a_chain() {
        let tmp = TempDir::new();
        block_on(async {
            let mut sink = FsSink::new(tmp.path());
            sink.write(TarDirectory::new("a/").into()).await.unwrap();
            sink.write(TarDirectory::new("a/b1/").into()).await.unwrap();
            sink.write(file("a/b1/f", b"f")).await.unwrap();
            sink.flush().await.unwrap();
            assert_eq!(open_paths(&sink), ["a", "a/b1"]);
            sink.write(TarDirectory::new("a/b2/").into()).await.unwrap();
            sink.flush().await.unwrap();
            assert_eq!(open_paths(&sink), ["a", "a/b2"]);
            // A chain deeper than `OPEN_DIRS` keeps the deepest directories.
            let paths: Vec<String> = (1..=20).map(|depth| vec!["c"; depth].join("/")).collect();
            for path in &paths {
                sink.write(TarDirectory::new(path.as_str()).into())
                    .await
                    .unwrap();
            }
            sink.flush().await.unwrap();
            assert_eq!(open_paths(&sink), paths[20 - OPEN_DIRS..]);
            match &sink.state {
                SinkState::Idle(inner) => {
                    for open in &inner.parents {
                        let meta = fs::metadata(tmp.path().join(&*open.path)).unwrap();
                        assert_eq!(open.id, Some((meta.dev(), meta.ino())), "{}", open.path);
                    }
                }
                _ => panic!("the sink is not idle"),
            }
            sink.finish().await.unwrap();
            assert!(open_paths(&sink).is_empty());
        });
        assert_eq!(fs::read(tmp.path().join("a/b1/f")).unwrap(), b"f");
        assert!(tmp.path().join(["c"; 20].join("/")).is_dir());
    }

    /// With [`ModePolicy::Skip`], the sink does not open a new directory. A
    /// later entry opens its parent path.
    #[test]
    fn skipped_mode_does_not_keep_new_directory() {
        let tmp = TempDir::new();
        block_on(async {
            let mut sink = FsSink::new(tmp.path()).with_mode(ModePolicy::Skip);
            sink.write(TarDirectory::new("a/").into()).await.unwrap();
            sink.flush().await.unwrap();
            assert!(open_paths(&sink).is_empty());
            sink.write(file("a/f", b"f")).await.unwrap();
            sink.flush().await.unwrap();
            assert_eq!(open_paths(&sink), ["a"]);
            sink.finish().await.unwrap();
        });
        assert_eq!(fs::read(tmp.path().join("a/f")).unwrap(), b"f");
    }

    /// Give the identity of the directory at `path`.
    fn id_of(path: &Path) -> sys::FileId {
        let meta = fs::metadata(path).unwrap();
        (meta.dev(), meta.ino())
    }

    /// Give a stack with the directory `path` in `root`.
    fn stack_with(root: &sys::Dir, path: &str) -> Vec<OpenDir> {
        let dir = resolve::open_dir(root, path, false, None).unwrap();
        let path = path.into();
        vec![OpenDir {
            path,
            dir,
            id: None,
        }]
    }

    /// Give the directory at `dir` in `root` with `parent_dir_with` and
    /// `stack`. Give the paths that `open` got and the identity of the
    /// result. `open` creates the missing directories.
    fn parent_with(
        root: &sys::Dir,
        stack: &mut Vec<OpenDir>,
        dir: &str,
    ) -> (Vec<String>, io::Result<sys::FileId>) {
        let mut calls = Vec::new();
        let result = parent_dir_with(root, stack, dir, |base, path| {
            calls.push(path.to_owned());
            resolve::open_dir(base, path, true, None)
        })
        .and_then(|open| sys::dir_id(&open.dir));
        (calls, result)
    }

    /// Give the paths of the directories of `stack`.
    fn stack_paths(stack: &[OpenDir]) -> Vec<&str> {
        stack.iter().map(|open| &*open.path).collect()
    }

    /// An open ancestor gives the start of the open of the rest of the
    /// path. An open directory with the path gives the result without an
    /// open. With no open ancestor, the open starts at the destination.
    #[test]
    fn parent_below_open_ancestor_resolves_rest() {
        let tmp = TempDir::new();
        fs::create_dir(tmp.path().join("a")).unwrap();
        let root = sys::open_dest(tmp.path()).unwrap();
        let mut stack = stack_with(&root, "a");
        let (calls, id) = parent_with(&root, &mut stack, "a/c");
        assert_eq!(calls, ["c"]);
        assert!(tmp.path().join("a/c").is_dir());
        assert_eq!(id.unwrap(), id_of(&tmp.path().join("a/c")));
        assert_eq!(stack_paths(&stack), ["a", "a/c"]);
        let (calls, id) = parent_with(&root, &mut stack, "a/c");
        assert!(calls.is_empty());
        assert_eq!(id.unwrap(), id_of(&tmp.path().join("a/c")));
        let mut stack = Vec::new();
        let (calls, id) = parent_with(&root, &mut stack, "a/c");
        assert_eq!(calls, ["a/c"]);
        assert_eq!(id.unwrap(), id_of(&tmp.path().join("a/c")));
        assert_eq!(stack_paths(&stack), ["a/c"]);
    }

    /// A symlink below an open ancestor leads outside the ancestor and
    /// stays inside the destination. The open from the destination gives
    /// the result.
    #[test]
    fn parent_symlink_outside_ancestor_resolves_from_root() {
        let tmp = TempDir::new();
        fs::create_dir(tmp.path().join("a")).unwrap();
        fs::create_dir(tmp.path().join("b")).unwrap();
        symlink("../b", tmp.path().join("a/c")).unwrap();
        let root = sys::open_dest(tmp.path()).unwrap();
        let mut stack = stack_with(&root, "a");
        let (calls, id) = parent_with(&root, &mut stack, "a/c");
        assert_eq!(calls, ["c", "a/c"]);
        assert_eq!(id.unwrap(), id_of(&tmp.path().join("b")));
        assert_eq!(stack_paths(&stack), ["a", "a/c"]);
    }

    /// An absolute symlink below an open ancestor gives the escape error
    /// from the ancestor and from the destination.
    #[test]
    fn parent_absolute_symlink_below_ancestor_fails() {
        let tmp = TempDir::new();
        fs::create_dir(tmp.path().join("a")).unwrap();
        fs::create_dir(tmp.path().join("b")).unwrap();
        symlink(tmp.path().join("b"), tmp.path().join("a/abs")).unwrap();
        let root = sys::open_dest(tmp.path()).unwrap();
        let mut stack = stack_with(&root, "a");
        let (calls, id) = parent_with(&root, &mut stack, "a/abs");
        assert_eq!(calls, ["abs", "a/abs"]);
        let err = id.unwrap_err();
        assert_eq!(err.kind(), ErrorKind::PermissionDenied, "{err}");
        assert_eq!(err.to_string(), "a path led outside of the filesystem");
        assert_eq!(fs::read_dir(tmp.path().join("b")).unwrap().count(), 0);
    }

    /// A symlink below an open ancestor that leads outside the destination
    /// gives the escape error from the ancestor and from the destination.
    #[test]
    fn parent_symlink_outside_root_fails() {
        let tmp = TempDir::new();
        let dest = tmp.path().join("dest");
        let out = tmp.path().join("out");
        fs::create_dir_all(dest.join("a")).unwrap();
        fs::create_dir(&out).unwrap();
        symlink("../../out", dest.join("a/c")).unwrap();
        let root = sys::open_dest(&dest).unwrap();
        let mut stack = stack_with(&root, "a");
        let (calls, id) = parent_with(&root, &mut stack, "a/c");
        assert_eq!(calls, ["c", "a/c"]);
        let err = id.unwrap_err();
        assert_eq!(err.kind(), ErrorKind::PermissionDenied, "{err}");
        assert_eq!(err.to_string(), "a path led outside of the filesystem");
        assert_eq!(fs::read_dir(&out).unwrap().count(), 0);
    }
}
