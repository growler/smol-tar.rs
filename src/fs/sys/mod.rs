//! Platform backend.
//!
//! The backend is a set of free functions and types with the same names on
//! each platform. It has no trait, because only one backend is compiled at a
//! time. All functions block. The platform-independent code calls them only
//! inside `rt::unblock`.
//!
//! The shared types `Meta`, `Child`, `SetMeta`, `Root`, `Existing`, `FsKind`
//! and `AttrList` are platform-independent.
//!
//! # Types
//!
//! ```text
//! pub(crate) type Dir;                 // open directory handle; Send + Sync
//! pub(crate) type FileId;              // Copy + Eq + Hash + Send
//! ```
//!
//! # Source functions
//!
//! ```text
//! pub(crate) fn open_root(root: &Path, xattrs: bool) -> io::Result<Root>;
//! pub(crate) fn read_dir(dir: &Dir, fs_path: &Path, xattrs: bool) -> io::Result<Vec<Child>>;
//! pub(crate) fn open_dir(parent: &Dir, name: &str, expect: FileId) -> io::Result<Dir>;
//! pub(crate) fn open_file(parent: &Dir, name: &str, expect: FileId, size: u64)
//!     -> io::Result<std::fs::File>;
//! pub(crate) fn read_xattrs_dir(dir: &Dir) -> io::Result<AttrList>;
//! pub(crate) fn read_xattrs_at(parent: &Dir, name: &str, kind: FsKind, expect: FileId)
//!     -> io::Result<AttrList>;
//! ```
//!
//! # Sink functions
//!
//! ```text
//! pub(crate) fn open_dest(path: &Path) -> io::Result<Dir>;
//! pub(crate) fn open_child_dir(parent: &Dir, name: &str, create: bool) -> io::Result<Dir>;
//! pub(crate) fn lookup(parent: &Dir, name: &str) -> io::Result<Existing>;
//! pub(crate) fn remove(parent: &Dir, name: &str) -> io::Result<()>;
//! pub(crate) fn create_dir(parent: &Dir, name: &str) -> io::Result<()>;
//! pub(crate) fn create_file(parent: &Dir, name: &str) -> io::Result<std::fs::File>;
//! pub(crate) fn create_symlink(parent: &Dir, name: &str, target: &str) -> io::Result<()>;
//! pub(crate) fn create_link(from: &Dir, from_name: &str, parent: &Dir, name: &str)
//!     -> io::Result<()>;
//! pub(crate) fn create_special(parent: &Dir, name: &str, kind: FsKind,
//!     device: Option<(u32, u32)>) -> io::Result<()>;
//! pub(crate) fn set_meta_file(file: &std::fs::File, meta: &SetMeta) -> io::Result<()>;
//! pub(crate) fn set_meta_dir(dir: &Dir, meta: &SetMeta) -> io::Result<()>;
//! pub(crate) fn set_meta_at(parent: &Dir, name: &str, kind: FsKind, meta: &SetMeta)
//!     -> io::Result<()>;
//! ```
//!
//! # Rules
//!
//! Names and links:
//!
//! - Each function that takes a parent handle and a name works on the name in
//!   that directory. It never follows a symlink or another link type at the
//!   name, and it never resolves a path with more than one component.
//! - `read_dir` does not give `.` and `..`. It does not give entries that have
//!   no `FsKind` (for example, sockets). A name or symlink target that is not
//!   UTF-8 gives `InvalidData` with the path.
//!
//! Identity:
//!
//! - `open_dir` and `open_file` compare the identity of the opened object with
//!   `expect`. A difference gives `InvalidData`. `open_file` also compares the
//!   size.
//!
//! Creation:
//!
//! - `open_child_dir` with `create` set creates a missing directory with the
//!   default directory mode of the platform (Unix: 0o755).
//! - `create_dir` makes a directory that only the owner can change (Unix:
//!   0o700). The sink sets the final mode when it finishes.
//! - `create_file` creates a new file and fails if the name exists.
//! - `create_special` gives `Unsupported` on a platform that cannot create the
//!   kind.
//!
//! Extended attributes:
//!
//! - The backend reads and writes xattrs only through a file descriptor that
//!   it opened by name in the parent handle, without following links.
//! - `open_root` with `xattrs` set reads the xattrs of the root.
//! - `read_dir` with `xattrs` set reads the xattrs of each non-directory with
//!   `read_xattrs_at`. It gives each directory with an empty list. The walk
//!   reads the directory xattrs with `read_xattrs_dir` after `open_dir`.
//! - `read_xattrs_at` gives an empty list for a kind that the platform cannot
//!   open safely. It gives an empty list if the filesystem does not support
//!   xattrs.
//! - `read_xattrs_at` compares the identity of the opened object with
//!   `expect`. A difference gives `InvalidData` "file changed".
//! - A name that is not UTF-8 gives `InvalidData` with the path.
//! - The `set_meta_*` functions write `SetMeta::xattrs` and apply
//!   `SetMeta::xattr_policy`.

#[cfg(unix)]
mod unix;
#[cfg(unix)]
#[expect(unused_imports)]
pub(crate) use unix::*;
