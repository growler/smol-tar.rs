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
//! pub(crate) fn read_dir(dir: &Dir, fs_path: &Path, xattrs: bool,
//!     keep: &dyn Fn(&str, FsKind) -> bool) -> io::Result<Vec<Child>>;
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
//! pub(crate) fn open_beneath(root: &Dir, path: &str) -> io::Result<Dir>;
//! pub(crate) fn create_dirs_beneath(root: &Dir, path: &str, mode: u32) -> io::Result<Dir>;
//! pub(crate) fn open_child_dir(parent: &Dir, name: &str) -> io::Result<Dir>;
//! pub(crate) fn id_at(parent: &Dir, name: &str) -> io::Result<FileId>;
//! pub(crate) fn dir_id(dir: &Dir) -> io::Result<FileId>;
//! pub(crate) fn open_parent(dir: &Dir) -> io::Result<Dir>;
//! pub(crate) fn lookup(parent: &Dir, name: &str) -> io::Result<Existing>;
//! pub(crate) fn remove(parent: &Dir, name: &str) -> io::Result<()>;
//! pub(crate) fn create_dir(parent: &Dir, name: &str, umask: bool)
//!     -> io::Result<Option<(Dir, FileId)>>;
//! pub(crate) fn create_file(parent: &Dir, name: &str, mode: u32) -> io::Result<std::fs::File>;
//! pub(crate) fn create_symlink(parent: &Dir, name: &str, target: &str) -> io::Result<()>;
//! pub(crate) fn create_link(from: &Dir, from_name: &str, parent: &Dir, name: &str)
//!     -> io::Result<()>;
//! pub(crate) fn create_special(parent: &Dir, name: &str, kind: FsKind,
//!     device: Option<(u32, u32)>, mode: u32) -> io::Result<()>;
//! pub(crate) fn finish_file(file: &std::fs::File, meta: &SetMeta,
//!     on_owner_error: &dyn Fn(io::Error) -> io::Result<()>) -> io::Result<FileId>;
//! pub(crate) fn finish_dir(parent: &Dir, name: &str, expect: FileId, meta: &SetMeta,
//!     keep_special: bool, on_owner_error: &dyn Fn(io::Error) -> io::Result<()>)
//!     -> io::Result<()>;
//! pub(crate) fn set_meta_dir(dir: &Dir, meta: &SetMeta,
//!     on_owner_error: &dyn Fn(io::Error) -> io::Result<()>) -> io::Result<()>;
//! pub(crate) fn set_meta_at(parent: &Dir, name: &str, kind: FsKind, meta: &SetMeta,
//!     on_owner_error: &dyn Fn(io::Error) -> io::Result<()>) -> io::Result<()>;
//! ```
//!
//! # Rules
//!
//! Directory handles and opens:
//!
//! - On Unix, `Dir` is an `OwnedFd` of an open directory. The calls on a
//!   node in a `Dir` use `rustix` with this descriptor through `AsFd`.
//!   Examples are stat, create, link, remove, chown, chmod, times and
//!   xattrs. On Apple platforms, `create_special` uses `libc` with the raw
//!   descriptor.
//! - `open_dir`, `open_file`, `open_child_dir` and `finish_dir` open an
//!   existing name in a `Dir` with `openat` and `O_NOFOLLOW`. When the xattr
//!   functions open a file, a FIFO or a directory, they do the same. These
//!   opens give an error for a symlink at the name. The name must be one
//!   path component. An empty name, `.`, `..` and a name with `/` give
//!   `InvalidInput`. On Apple platforms, the xattr functions open a symlink
//!   with `openat` and `O_SYMLINK`. This name must also be one path
//!   component.
//! - `create_file` and `create_dir` without `umask` open the new node with
//!   `openat` and `O_NOFOLLOW`.
//! - `open_root` and `open_dest` open a path from the current directory with
//!   `openat`. `open_root` does not follow a symlink at the last component of
//!   the path. `open_dest` follows it.
//! - `open_beneath` opens the directory at a path in `root` for reading.
//!   `root` can be any open directory, for example the destination or a
//!   directory below it. The function follows a symlink in each component,
//!   also in the last component, if the target stays inside `root`. A `..`
//!   component goes to the parent directory. It needs search permission on
//!   the current directory. On Linux, Android and FreeBSD, an intermediate
//!   directory needs only search permission. On other targets, a `..`
//!   component also needs read permission on the parent directory. The last
//!   directory needs search and read permission.
//! - `create_dirs_beneath` creates each missing directory of a normalized
//!   path in `root` with `mode` less the umask. Then it gives the directory
//!   at the path, open for reading. If the path exists, the result and the
//!   permission rules are the same as for `open_beneath`. Else it finds the
//!   deepest existing directory, by the rules of `open_beneath`. The
//!   userspace resolver finds it with one walk of the path. On Linux,
//!   `openat2` finds it with a small number of opens of prefixes of the
//!   path. It does not resolve the path again for each missing component.
//!   On Linux, Android and FreeBSD, it opens the deepest existing directory
//!   with `O_PATH`. Then it creates each missing component with `mkdirat`
//!   and opens it with `O_NOFOLLOW`. A parent directory needs search and
//!   write permission. On other targets, it also needs read permission. If
//!   a node has the name of a missing component, it resolves the path up to
//!   this component again from `root`, by the rules of `open_beneath`. A
//!   missing component in a symlink target gives `NotFound`, also for a
//!   dangling symlink. Thus it creates no directory through a symlink. A
//!   new directory keeps the setgid bit that it gets from its parent.
//! - `open_parent` opens `..` in a `Dir`. It needs search permission on the
//!   `Dir`. On Linux, Android and FreeBSD, it opens the parent with `O_PATH`
//!   and needs no permission on the parent. This handle is only good for
//!   `dir_id` and as the `dir` of `open_parent`. On other targets, the
//!   parent needs read permission.
//! - On Linux, `open_beneath` and `create_dirs_beneath` resolve the path
//!   with `openat2` with `RESOLVE_BENEATH` and `RESOLVE_NO_MAGICLINKS`.
//!   `create_dirs_beneath` opens the full path first. If the path is
//!   missing, it opens the parent, then prefixes 3, 7, 15 and more
//!   components back, and then bisects. Thus the number of opens increases
//!   with the logarithm of the number of missing components. They use the
//!   userspace resolver if `openat2` gives `ENOSYS`, and then for each later
//!   call. They also use the userspace resolver for one call if `openat2`
//!   gives `EPERM`, `ENAMETOOLONG` or `ELOOP`, or gives `EAGAIN` 4 times. On
//!   other targets, they always use the userspace resolver.
//! - The userspace resolver resolves the path one component at a time with
//!   `openat` and `O_NOFOLLOW`. It keeps at most two open descriptors. It
//!   keeps the identity of each directory above the current directory. For
//!   `..`, it opens `..` in the current directory and compares the identity
//!   of the result with the kept identity. A different identity gives the
//!   error for a path that leads outside `root`.
//! - In `open_beneath` and `create_dirs_beneath`, a path or a symlink
//!   target that leads outside `root` gives `PermissionDenied` with the
//!   message "a path led outside of the filesystem". This is also true for
//!   a target outside `root` and inside the destination. An absolute path
//!   or symlink target gives the same error. On Linux, `EXDEV` from `openat2`
//!   gives this error. More than 40 symlink expansions give `ELOOP`. On
//!   Linux, `openat2` gives `ELOOP` for a magic link (procfs). The userspace
//!   resolver reads the target of a magic link as a path. In `open_beneath`,
//!   an empty path, a missing component and a dangling symlink give
//!   `NotFound`. A component that is not a directory gives `NotADirectory`.
//! - The sink opens the parent directory of an entry with `resolve::open_dir`.
//!   Without `create`, this function opens the path with `open_beneath`.
//!   With `create` set, it opens the path with `create_dirs_beneath`. The
//!   sink opens the rest of the parent path in the nearest open ancestor
//!   directory. If this open fails, it opens the full path from the
//!   destination. The sink also uses the open directory that `create_dir`
//!   gives as the parent directory of later entries.
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
//! - `open_dir`, `open_file` and `finish_dir` compare the identity and the
//!   kind of the opened object with `expect`. A difference gives
//!   `InvalidData` "file changed". `open_file` also compares the size.
//! - At the finish, the sink sets the metadata of the directory of each
//!   directory entry with `finish_dir` and the identity that it stored for
//!   the entry.
//! - `finish_file` gives the identity of the open file. `id_at` gives the
//!   identity of the node at the name. A symlink at the name gives the
//!   identity of the symlink. `lookup` gives the identity of a directory at
//!   the name. A symlink to a directory is not a directory for `lookup`.
//!
//! Creation:
//!
//! - `create_dir` makes a directory that only the owner can change (Unix:
//!   0o700). It gives the open directory and its identity. The sink sets the
//!   final mode when it finishes. With `umask` set, `create_dir` uses the
//!   default directory mode less the umask (Unix: 0o777 less the umask), and
//!   gives `None`.
//! - On Unix, the umask does not change the mode of a directory that
//!   `create_dir` creates without `umask`. The directory keeps the setgid bit
//!   that it gets from its parent only if the process is a member of the
//!   group of the directory or has the `CAP_FSETID` capability. Root has
//!   this capability.
//! - `create_file` creates a new file with `mode` less the umask and fails if
//!   the name exists.
//! - `create_special` creates a FIFO or a device with `mode` less the umask.
//!   A kind that is not a FIFO or a device with device numbers gives
//!   `InvalidInput`. On Apple platforms, device numbers that the platform
//!   cannot encode give `InvalidInput`.
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
//! - `read_xattrs_at` compares the identity and the kind of the opened object
//!   with `expect` and `kind`. A difference gives `InvalidData` "file
//!   changed".
//! - A name that is not UTF-8 gives `InvalidData` with the path.
//! - `finish_file`, `finish_dir` and the `set_meta_*` functions write
//!   `SetMeta::xattrs` and apply `SetMeta::xattr_policy`.
//!
//! Metadata:
//!
//! - `finish_file`, `finish_dir` and the `set_meta_*` functions do not
//!   change the mode if `SetMeta::mode` is `None`, and do not change the
//!   modification time if `SetMeta::mtime` is `None`.
//! - `finish_dir` with `keep_special` set adds the setuid, setgid and sticky
//!   bits that the directory has to `SetMeta::mode`.
//! - `finish_file` does not change the mode if the file has the mode of
//!   `SetMeta::mode` and the filter accepts no xattr of `SetMeta::xattrs`.
//! - `finish_dir` does not change the mode if the directory has the new mode,
//!   `SetMeta::owner` is `None` and the filter accepts no xattr of
//!   `SetMeta::xattrs`.
//! - If the owner change fails, `finish_file`, `finish_dir` and the
//!   `set_meta_*` functions call `on_owner_error` with an error of the same
//!   kind. The message is "cannot set the owner to uid UID, gid GID: ERROR".
//!   An error from `on_owner_error` stops the function. With `Ok`, the
//!   function sets the other metadata. Then the new mode of a node that is
//!   not a directory has no setuid and setgid bits, and the function does
//!   not write the `security.capability` xattr on this node.

#[cfg(unix)]
mod beneath;
#[cfg(unix)]
mod unix;
#[cfg(unix)]
pub(crate) use beneath::*;
#[cfg(unix)]
pub(crate) use unix::*;
