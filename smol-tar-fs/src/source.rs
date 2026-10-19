//! Filesystem source: a stream of the nodes in a directory tree.

use {
    super::{rt, sys, walk, with_path, Child, FsFile, FsKind, Meta, Root},
    futures_core::stream::Stream,
    smol_tar::AttrList,
    std::{
        collections::VecDeque,
        future::Future,
        io, mem,
        path::{Path, PathBuf},
        pin::Pin,
        sync::Arc,
        task::{Context, Poll},
        time::SystemTime,
    },
};

/// Stream of the nodes in a directory tree.
///
/// The stream gives one [`FsEntry`] for each node. The walk is depth first.
/// In each directory, the stream gives the directory entry, then the
/// non-directory children sorted by name, then each subdirectory sorted by
/// name. Names are sorted in the order of their UTF-8 bytes. Thus the order
/// is the same on each run.
///
/// The archive path of an entry is the prefix, a `/`, and the path relative
/// to the root. The archive path of a directory ends with `/`. The prefix
/// loses its trailing `/` characters. The source does not check the prefix
/// in any other way.
///
/// - Root directory and a prefix: the first entry is the root directory,
///   with the path `prefix/`.
/// - Root directory and an empty prefix: the stream gives no entry for the
///   root. The paths are relative to the root.
/// - Root that is not a directory: the stream gives one entry with the path
///   `prefix`. An empty prefix gives an [`InvalidInput`] error.
///
/// The source does not follow symlinks. A symlink gives one entry of the
/// kind [`FsKind::Symlink`], with its target in [`FsEntry::link`]. Below
/// the root, the source skips nodes that have no [`FsKind`], for example
/// sockets. A root that has no [`FsKind`] gives an [`InvalidInput`] error.
/// A name, a symlink target, or an extended attribute name that is not
/// UTF-8 gives an [`InvalidData`] error. A modification time that is out of
/// range gives an [`InvalidData`] error. A node that changes after the
/// source read its metadata gives an [`InvalidData`] error with the message
/// "file changed". For example, another directory replaces a directory, or
/// a node of a different kind replaces a symlink. With
/// [`with_xattrs`](Self::with_xattrs), a different node at the name of a
/// node gives the same error. Other changes give the error from the system.
/// The source does not stop at mount points.
///
/// The source does not read extended attributes by default. Use
/// [`with_xattrs`](Self::with_xattrs) to read them.
///
/// The source runs the filesystem calls on the blocking thread pool of the
/// runtime. The close of a directory can occur on the thread that drops it.
/// With the `tokio` feature, poll the stream inside a Tokio runtime context.
///
/// An error from the filesystem has the path of the node. After an error,
/// the stream ends.
///
/// [`InvalidInput`]: std::io::ErrorKind::InvalidInput
/// [`InvalidData`]: std::io::ErrorKind::InvalidData
pub struct FsSource {
    root: PathBuf,
    prefix: String,
    xattrs: bool,
    stack: Vec<Frame>,
    state: State,
}

/// Directory that the walk is in.
struct Frame {
    /// Open directory.
    dir: Arc<sys::Dir>,
    /// Archive path of the directory. It ends with `/` or is empty.
    path: String,
    fs_path: PathBuf,
    /// Non-directories, sorted.
    files: VecDeque<Child>,
    /// Directories, sorted.
    dirs: VecDeque<Child>,
}

/// Open directory and its sorted children.
struct Listing {
    dir: sys::Dir,
    files: VecDeque<Child>,
    dirs: VecDeque<Child>,
}

impl Listing {
    /// Read and sort the children of `dir`.
    fn read(dir: sys::Dir, fs_path: &Path, xattrs: bool) -> io::Result<Self> {
        let (files, dirs) = walk::read_sorted(&dir, fs_path, xattrs)?;
        Ok(Self { dir, files, dirs })
    }
}

/// Result of one call on the blocking thread pool.
enum Step {
    /// The root is a directory.
    RootDir {
        root: PathBuf,
        meta: Meta,
        listing: Listing,
    },
    /// The root is not a directory.
    RootOther {
        root: PathBuf,
        parent: sys::Dir,
        child: Child,
    },
    /// The subdirectory `child` of `parent` is open and read.
    Dir {
        parent: Arc<sys::Dir>,
        child: Child,
        path: String,
        fs_path: PathBuf,
        listing: Listing,
    },
}

/// Call on the blocking thread pool.
type StepFuture = Pin<Box<dyn Future<Output = io::Result<Step>> + Send>>;

enum State {
    /// The root is not read yet.
    Start,
    /// The stack has the next entry, or the stream ends if it is empty.
    Walk,
    /// A call on the blocking thread pool runs.
    Busy(StepFuture),
    /// The stream ended.
    Done,
}

impl FsSource {
    /// Make a source that walks the tree at `root`, with an empty prefix.
    ///
    /// The source does no I/O before the first poll.
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self {
            root: root.into(),
            prefix: String::new(),
            xattrs: false,
            stack: Vec::new(),
            state: State::Start,
        }
    }

    /// Set the prefix of the archive paths. The prefix loses its trailing
    /// `/` characters.
    pub fn with_prefix(mut self, prefix: impl Into<String>) -> Self {
        let mut prefix = prefix.into();
        prefix.truncate(prefix.trim_end_matches('/').len());
        self.prefix = prefix;
        self
    }

    /// Set if the source reads the extended attributes of each node. The
    /// default is `false`.
    ///
    /// With `true`, the source opens each regular file and each FIFO one
    /// time to read its attributes, also if a later filter drops the entry.
    /// A regular file that the process cannot open gives an error.
    pub fn with_xattrs(mut self, read: bool) -> Self {
        self.xattrs = read;
        self
    }

    /// Start the read of the root on the blocking thread pool.
    fn read_root(&mut self) -> StepFuture {
        let root = mem::take(&mut self.root);
        let xattrs = self.xattrs;
        // Without a prefix, the root gives no entry.
        let root_xattrs = xattrs && !self.prefix.is_empty();
        Box::pin(rt::unblock(move || {
            Ok(match sys::open_root(&root, root_xattrs)? {
                Root::Dir { dir, meta } => {
                    let listing = Listing::read(dir, &root, xattrs)?;
                    Step::RootDir {
                        root,
                        meta,
                        listing,
                    }
                }
                Root::Other { parent, child } => Step::RootOther {
                    root,
                    parent,
                    child,
                },
            })
        }))
    }

    /// Push the frame of a directory that `step` read. Give the entry of
    /// that directory or of the root, if it has one.
    fn apply(&mut self, step: Step) -> Option<io::Result<FsEntry>> {
        match step {
            Step::RootDir {
                root,
                meta,
                listing,
            } => {
                let dir = Arc::new(listing.dir);
                let mut path = mem::take(&mut self.prefix);
                let entry = (!path.is_empty()).then(|| {
                    path.push('/');
                    FsEntry {
                        path: path.as_str().into(),
                        fs_path: root.clone(),
                        meta,
                        link: None,
                        parent: Arc::clone(&dir),
                        name: ".".into(),
                    }
                });
                self.stack.push(Frame {
                    dir,
                    path,
                    fs_path: root,
                    files: listing.files,
                    dirs: listing.dirs,
                });
                entry.map(Ok)
            }
            Step::RootOther {
                root,
                parent,
                child,
            } => {
                let prefix = mem::take(&mut self.prefix);
                if prefix.is_empty() {
                    let err = io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "a root that is not a directory needs a prefix",
                    );
                    return Some(Err(with_path(err, &root.to_string_lossy())));
                }
                Some(Ok(FsEntry {
                    path: prefix.into_boxed_str(),
                    fs_path: root,
                    meta: child.meta,
                    link: child.link,
                    parent: Arc::new(parent),
                    name: child.name,
                }))
            }
            Step::Dir {
                parent,
                child,
                path,
                fs_path,
                listing,
            } => {
                self.stack.push(Frame {
                    dir: Arc::new(listing.dir),
                    path: path.clone(),
                    fs_path: fs_path.clone(),
                    files: listing.files,
                    dirs: listing.dirs,
                });
                Some(Ok(FsEntry {
                    path: path.into_boxed_str(),
                    fs_path,
                    meta: child.meta,
                    link: child.link,
                    parent,
                    name: child.name,
                }))
            }
        }
    }
}

impl Frame {
    /// Archive path of the child `name`, with `suffix` added.
    fn child_path(&self, name: &str, suffix: &str) -> String {
        let mut path = String::with_capacity(self.path.len() + name.len() + suffix.len());
        path.push_str(&self.path);
        path.push_str(name);
        path.push_str(suffix);
        path
    }

    /// Filesystem path of the child `name`.
    fn child_fs_path(&self, name: &str) -> PathBuf {
        let mut path = PathBuf::with_capacity(self.fs_path.as_os_str().len() + 1 + name.len());
        path.push(&self.fs_path);
        path.push(name);
        path
    }

    /// Make the entry of a non-directory child. No I/O.
    fn file_entry(&self, child: Child) -> FsEntry {
        FsEntry {
            path: self.child_path(&child.name, "").into_boxed_str(),
            fs_path: self.child_fs_path(&child.name),
            meta: child.meta,
            link: child.link,
            parent: Arc::clone(&self.dir),
            name: child.name,
        }
    }

    /// Start to open and read the subdirectory `child` on the blocking thread
    /// pool.
    fn read_dir(&self, mut child: Child, xattrs: bool) -> StepFuture {
        let path = self.child_path(&child.name, "/");
        let fs_path = self.child_fs_path(&child.name);
        let parent = Arc::clone(&self.dir);
        Box::pin(rt::unblock(move || {
            let context = |err| with_path(err, &fs_path.to_string_lossy());
            let dir = sys::open_dir(&parent, &child.name, child.meta.id).map_err(context)?;
            if xattrs {
                child.meta.xattrs = sys::read_xattrs_dir(&dir).map_err(context)?;
            }
            let listing = Listing::read(dir, &fs_path, xattrs)?;
            Ok(Step::Dir {
                parent,
                child,
                path,
                fs_path,
                listing,
            })
        }))
    }
}

impl Stream for FsSource {
    type Item = io::Result<FsEntry>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        loop {
            // The state is `Done` until a branch sets it. Thus an error or a
            // panic ends the stream.
            match mem::replace(&mut this.state, State::Done) {
                State::Start => this.state = State::Busy(this.read_root()),
                State::Walk => {
                    let Some(frame) = this.stack.last_mut() else {
                        return Poll::Ready(None);
                    };
                    if let Some(child) = frame.files.pop_front() {
                        this.state = State::Walk;
                        return Poll::Ready(Some(Ok(frame.file_entry(child))));
                    }
                    // Free the buffer of all children before the walk of the
                    // subdirectories.
                    if frame.files.capacity() != 0 {
                        frame.files = VecDeque::new();
                    }
                    if let Some(child) = frame.dirs.pop_front() {
                        this.state = State::Busy(frame.read_dir(child, this.xattrs));
                    } else {
                        this.stack.pop();
                        this.state = State::Walk;
                    }
                }
                State::Busy(mut fut) => match fut.as_mut().poll(cx) {
                    Poll::Pending => {
                        this.state = State::Busy(fut);
                        return Poll::Pending;
                    }
                    Poll::Ready(Err(err)) => return Poll::Ready(Some(Err(err))),
                    Poll::Ready(Ok(step)) => {
                        this.state = State::Walk;
                        if let Some(entry) = this.apply(step) {
                            return Poll::Ready(Some(entry));
                        }
                    }
                },
                State::Done => return Poll::Ready(None),
            }
        }
    }
}

/// Node that [`FsSource`] gives.
///
/// An entry keeps the directory that holds it open until the entry is
/// dropped.
#[derive(Debug)]
pub struct FsEntry {
    /// Archive path. The path of a directory ends with `/`.
    pub(super) path: Box<str>,
    fs_path: PathBuf,
    pub(super) meta: Meta,
    /// Symlink target.
    pub(super) link: Option<Box<str>>,
    /// Directory that holds the entry.
    pub(super) parent: Arc<sys::Dir>,
    /// Name in `parent`.
    pub(super) name: Box<str>,
}

impl FsEntry {
    /// Archive path of the entry: the prefix and the path relative to the
    /// root. The path of a directory ends with `/`.
    pub fn path(&self) -> &str {
        &self.path
    }

    /// Path of the node on the filesystem: the root joined with the path
    /// relative to the root.
    pub fn fs_path(&self) -> &Path {
        &self.fs_path
    }

    /// Kind of the node.
    pub fn kind(&self) -> FsKind {
        self.meta.kind
    }

    /// Size of a regular file in bytes. The size is 0 for all other kinds.
    pub fn size(&self) -> u64 {
        self.meta.size
    }

    /// Permission and special bits (`0o7777`).
    pub fn mode(&self) -> u32 {
        self.meta.mode
    }

    /// Numeric user ID of the owner.
    pub fn uid(&self) -> u32 {
        self.meta.uid
    }

    /// Numeric group ID of the owner.
    pub fn gid(&self) -> u32 {
        self.meta.gid
    }

    /// Modification time, with the full precision of the filesystem.
    pub fn mtime(&self) -> SystemTime {
        self.meta.mtime
    }

    /// Target of a symlink. `None` for all other kinds.
    pub fn link(&self) -> Option<&str> {
        self.link.as_deref()
    }

    /// Extended attributes, sorted by name. The list is empty if the source
    /// does not read extended attributes.
    pub fn xattrs(&self) -> &AttrList {
        &self.meta.xattrs
    }

    /// Body of the entry. The file opens on the first read. The caller makes
    /// the body only for a regular file.
    #[cfg_attr(not(test), expect(dead_code))]
    pub(crate) fn file(&self) -> FsFile {
        FsFile::new(
            Arc::clone(&self.parent),
            self.name.clone(),
            self.meta.id,
            self.meta.size,
            self.path.clone(),
        )
    }
}

#[cfg(test)]
mod tests {
    use {super::*, static_assertions::assert_impl_all};

    assert_impl_all!(FsSource: Send, Unpin);
    assert_impl_all!(FsEntry: Send, Sync, Unpin);
}
