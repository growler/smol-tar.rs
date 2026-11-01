//! Resolution of a path beneath a root directory. The root can be any open
//! directory. A path that leads outside the root gives the escape error. On
//! Linux, the kernel resolves the path with `openat2`. On other targets,
//! and on Linux when `openat2` is not available, a userspace resolver
//! resolves the path one component at a time. Both follow a symlink only
//! if its target stays beneath the root. The module also creates the
//! missing directories of a path beneath the root.

use {
    super::{file_id, Dir, FileId},
    rustix::{
        fd::{AsFd, BorrowedFd, OwnedFd},
        fs::{fstat, mkdirat, openat, readlinkat, Mode, OFlags},
        io::Errno,
    },
    std::{borrow::Cow, io},
};
#[cfg(target_os = "linux")]
use {
    rustix::fs::{openat2, ResolveFlags},
    std::sync::atomic::{AtomicBool, Ordering},
};

/// Maximum number of symlink expansions in one path. Linux has the same
/// limit.
const MAX_SYMLINK_EXPANSIONS: u8 = 40;

/// Flags to open an intermediate directory. On Linux, Android and FreeBSD,
/// the flags include `O_PATH`. An open with `O_PATH` needs no permission on
/// the opened directory. Thus an intermediate directory needs only search
/// permission, for the lookup inside it. `open_parent` also uses these flags
/// to open the parent directory.
#[cfg(any(target_os = "linux", target_os = "android", target_os = "freebsd"))]
pub(super) const PATH_FLAGS: OFlags = OFlags::PATH.union(OFlags::DIRECTORY).union(OFlags::CLOEXEC);
#[cfg(not(any(target_os = "linux", target_os = "android", target_os = "freebsd")))]
pub(super) const PATH_FLAGS: OFlags = OFlags::RDONLY
    .union(OFlags::DIRECTORY)
    .union(OFlags::CLOEXEC);

/// Flags to open a directory for reading.
const READ_FLAGS: OFlags = OFlags::RDONLY
    .union(OFlags::DIRECTORY)
    .union(OFlags::CLOEXEC);

/// Number of `openat2` calls that can give `EAGAIN` before the resolution
/// uses the userspace resolver.
#[cfg(target_os = "linux")]
const OPENAT2_TRIES: usize = 4;

/// Set when `openat2` gave `ENOSYS`. Then each later resolution uses the
/// userspace resolver.
#[cfg(target_os = "linux")]
static OPENAT2_MISSING: AtomicBool = AtomicBool::new(false);

/// Open the directory at `path` in `root` for reading. The open follows a
/// symlink in each component, also in the last component, if the target
/// stays inside `root`.
pub(crate) fn open_beneath(root: &Dir, path: &str) -> io::Result<Dir> {
    resolve(root.as_fd(), path.as_bytes(), READ_FLAGS)
}

/// Open the directory at the normalized `path` in `root` for reading, by the
/// rules of `open_beneath`. First create each missing directory of `path`
/// with `mode` less the umask. To find the deepest existing directory, walk
/// `path` one time with the userspace resolver, or, on Linux, open `path`
/// and some of its prefixes with `openat2`. If `path` exists, the result
/// and the permission rules are the same as for `open_beneath`.
///
/// The caller gives a normalized path: no empty, `.` or `..` component, and
/// no leading or trailing `/`.
pub(crate) fn create_dirs_beneath(root: &Dir, path: &str, mode: u32) -> io::Result<Dir> {
    create_dirs_with(
        root.as_fd(),
        path,
        Mode::from_raw_mode(mode as _),
        open_prefix,
        resolve,
    )
}

/// Create each missing directory of `path` in `root` with `mode` less the
/// umask, and open `path` for reading. Open `path` or its deepest existing
/// directory with `prefix`. Then create each missing component with
/// `mkdirat` and open it with `O_NOFOLLOW`. If a node has the name of a
/// missing component, resolve the path up to this component from `root`
/// with `resolve`. The other rules are in `create_dirs_beneath`.
fn create_dirs_with(
    root: BorrowedFd<'_>,
    path: &str,
    mode: Mode,
    prefix: impl FnOnce(BorrowedFd<'_>, &str) -> io::Result<Found>,
    mut resolve: impl FnMut(BorrowedFd<'_>, &[u8], OFlags) -> io::Result<OwnedFd>,
) -> io::Result<OwnedFd> {
    let (mut opened, mut begin) = match prefix(root, path)? {
        Found::Path(dir) => return Ok(dir),
        Found::Prefix(opened, begin) => (opened, begin),
    };
    loop {
        let end = path[begin..].find('/').map_or(path.len(), |i| begin + i);
        let last = end == path.len();
        let flags = if last { READ_FLAGS } else { PATH_FLAGS };
        let name = &path[begin..end];
        let current = opened.as_ref().map_or(root, |dir| dir.as_fd());
        let dir = match mkdirat(current, name, mode) {
            Ok(()) => openat(current, name, flags | OFlags::NOFOLLOW, Mode::empty())?,
            // Another process created the node, or a symlink has the name.
            // The symlink can be dangling. Close `opened` first to keep at
            // most two open descriptors.
            Err(Errno::EXIST) => {
                drop(opened.take());
                resolve(root, &path.as_bytes()[..end], flags)?
            }
            Err(err) => return Err(err.into()),
        };
        if last {
            return Ok(dir);
        }
        opened = Some(dir);
        begin = end + 1;
    }
}

/// The result of `open_prefix`.
enum Found {
    /// The directory at the path, open for reading.
    Path(OwnedFd),
    /// The deepest existing directory of the path, open with `PATH_FLAGS`,
    /// or `None` for the root, and the offset of the first component after
    /// this directory.
    Prefix(Option<OwnedFd>, usize),
}

/// Open the directory at the normalized `path` in `root` for reading, by the
/// rules of `open_beneath`. If `path` is missing, open its deepest existing
/// directory with `PATH_FLAGS`, by the symlink rules of `open_beneath`. On
/// Linux, try `openat2` first. Use the userspace resolver if `openat2` gives
/// no result.
fn open_prefix(root: BorrowedFd<'_>, path: &str) -> io::Result<Found> {
    #[cfg(target_os = "linux")]
    if let Some(result) = open_prefix_openat2(root, path) {
        return result;
    }
    open_prefix_manual(root, path)
}

/// Open `path` in `root` with `resolve_openat2`. If `path` is missing, find
/// its deepest existing directory with `deepest`. The rules are in
/// `open_prefix`. Give `None` if the userspace resolver must do the work.
#[cfg(target_os = "linux")]
fn open_prefix_openat2(root: BorrowedFd<'_>, path: &str) -> Option<io::Result<Found>> {
    match resolve_openat2(root, path.as_bytes(), READ_FLAGS)? {
        Ok(dir) => return Some(Ok(Found::Path(dir))),
        Err(err) if err.kind() == io::ErrorKind::NotFound => {}
        Err(err) => return Some(Err(err)),
    }
    let slashes: Vec<usize> = path.match_indices('/').map(|(i, _)| i).collect();
    let found = deepest(slashes.len() + 1, |count| {
        resolve_openat2(root, &path.as_bytes()[..slashes[count - 1]], PATH_FLAGS)
    });
    Some(found?.map(|(dir, count)| match count {
        0 => Found::Prefix(dir, 0),
        count => Found::Prefix(dir, slashes[count - 1] + 1),
    }))
}

/// Find the number of existing components of a path with `n` components.
/// The path itself is missing. `probe(count)` opens the prefix with `count`
/// components, with `0 < count < n`. The prefix with no components is the
/// root, which exists. Give the result of the probe for the found prefix,
/// `None` for the root, and the number of components.
///
/// Probe the parent first. Then probe 3, 7, 15 and more components back,
/// until a prefix exists. Then bisect between the deepest existing prefix
/// and the shortest missing prefix. `NotFound` tells that a prefix is
/// missing. Other errors go to the caller. Give `None` if `probe` gives
/// `None`.
#[cfg(any(target_os = "linux", test))]
fn deepest<T>(
    n: usize,
    mut probe: impl FnMut(usize) -> Option<io::Result<T>>,
) -> Option<io::Result<(Option<T>, usize)>> {
    // The prefix with `found_at` components exists, and the prefix with
    // `missing_at` components is missing.
    let (mut found, mut found_at, mut missing_at) = (None, 0, n);
    let mut back = 1;
    while missing_at - found_at > 1 {
        let count = match found {
            None if back < n => n - back,
            _ => found_at + (missing_at - found_at) / 2,
        };
        match probe(count)? {
            Ok(value) => {
                found = Some(value);
                found_at = count;
            }
            Err(err) if err.kind() == io::ErrorKind::NotFound => missing_at = count,
            Err(err) => return Some(Err(err)),
        }
        back = (2 * back + 1).min(n);
    }
    Some(Ok((found, found_at)))
}

/// Open the directory at `path` in `root` with the flags `last`. On Linux,
/// try `openat2` first. Use the userspace resolver if `openat2` gives no
/// result.
fn resolve(root: BorrowedFd<'_>, path: &[u8], last: OFlags) -> io::Result<OwnedFd> {
    #[cfg(target_os = "linux")]
    if let Some(result) = resolve_openat2(root, path, last) {
        return result;
    }
    resolve_manual(root, path, last)
}

/// Open the directory at `path` in `root` with the flags `last` and
/// `openat2` with `RESOLVE_BENEATH` and `RESOLVE_NO_MAGICLINKS`. Give `None`
/// if the userspace resolver must resolve the path. The rules are in
/// `with_openat2`.
#[cfg(target_os = "linux")]
fn resolve_openat2(root: BorrowedFd<'_>, path: &[u8], last: OFlags) -> Option<io::Result<OwnedFd>> {
    let resolve = ResolveFlags::BENEATH | ResolveFlags::NO_MAGICLINKS;
    // The `/.` suffix makes the kernel look up `.` in the last directory.
    // Thus the last directory needs search permission, as in the userspace
    // resolver. An empty path stays empty and gives `ENOENT`. The first call
    // of `open` makes the suffixed path.
    let mut suffixed = None;
    with_openat2(&OPENAT2_MISSING, || {
        let path: &[u8] = if path.is_empty() {
            path
        } else {
            suffixed.get_or_insert_with(|| [path, b"/.".as_slice()].concat())
        };
        openat2(root, path, last, Mode::empty(), resolve)
    })
}

/// Call `open` with the fallback rules for `openat2`. Give `None` if the
/// userspace resolver must do the work:
///
/// - `missing` is set.
/// - `open` gives `ENOSYS`. Then set `missing`.
/// - `open` gives `EPERM`, for example from a seccomp filter.
/// - `open` gives `ENAMETOOLONG`. The kernel gives this error for a path
///   longer than `PATH_MAX`, also when only the `/.` suffix makes the path
///   too long. The userspace resolver opens one component at a time and can
///   resolve this path.
/// - `open` gives `EAGAIN` `OPENAT2_TRIES` times. The kernel gives `EAGAIN`
///   if a rename or a mount occurs on the system during the lookup of a path
///   that has `..`.
/// - `open` gives `ELOOP`. The kernel gives this error for a symlink loop,
///   for more than 40 symlink expansions, and, with
///   `RESOLVE_NO_MAGICLINKS`, for a magic link (procfs). The userspace
///   resolver reads the target of a magic link as a path. Thus an absolute
///   target gives the escape error. A symlink loop also gives `ELOOP` from
///   the userspace resolver.
///
/// `EXDEV` gives the escape error. Other errors go to the caller.
#[cfg(target_os = "linux")]
fn with_openat2<T>(
    missing: &AtomicBool,
    mut open: impl FnMut() -> rustix::io::Result<T>,
) -> Option<io::Result<T>> {
    if missing.load(Ordering::Relaxed) {
        return None;
    }
    for _ in 0..OPENAT2_TRIES {
        match open() {
            Ok(value) => return Some(Ok(value)),
            Err(Errno::AGAIN) => {}
            Err(Errno::PERM | Errno::NAMETOOLONG | Errno::LOOP) => return None,
            Err(Errno::NOSYS) => {
                missing.store(true, Ordering::Relaxed);
                return None;
            }
            Err(Errno::XDEV) => return Some(Err(escape_error())),
            Err(err) => return Some(Err(err.into())),
        }
    }
    None
}

/// Give the error for a path or a symlink target that leads outside the
/// root.
fn escape_error() -> io::Error {
    io::Error::new(
        io::ErrorKind::PermissionDenied,
        "a path led outside of the filesystem",
    )
}

/// Paths with components to resolve. Each item is a path and the offset of
/// its next component. The next component comes from the last item. Thus
/// the components of a symlink target come before the remaining components
/// of the path that contains the symlink.
struct Worklist<'a> {
    items: Vec<(Cow<'a, [u8]>, usize)>,
}

impl<'a> Worklist<'a> {
    /// Add `path` before the remaining components. An empty path gives
    /// `NotFound`. An absolute path gives the escape error.
    fn push(&mut self, path: Cow<'a, [u8]>) -> io::Result<()> {
        match path.first() {
            None => Err(Errno::NOENT.into()),
            Some(b'/') => Err(escape_error()),
            Some(_) => {
                self.items.push((path, 0));
                Ok(())
            }
        }
    }

    /// Tell if a component that is not empty and not `.` follows. Skip the
    /// empty and `.` components before it, and remove the finished items.
    fn has_next(&mut self) -> bool {
        while let Some((path, pos)) = self.items.last_mut() {
            if *pos >= path.len() {
                self.items.pop();
                continue;
            }
            let start = *pos;
            let end = path[start..]
                .iter()
                .position(|&b| b == b'/')
                .map_or(path.len(), |i| start + i);
            if !matches!(&path[start..end], b"" | b".") {
                return true;
            }
            *pos = end + 1;
        }
        false
    }

    /// Give the next component that is not empty and not `.`. Give `None`
    /// at the end.
    fn next_component(&mut self) -> Option<&[u8]> {
        if !self.has_next() {
            return None;
        }
        let (path, pos) = self.items.last_mut()?;
        let start = *pos;
        let end = path[start..]
            .iter()
            .position(|&b| b == b'/')
            .map_or(path.len(), |i| start + i);
        *pos = end + 1;
        Some(&path[start..end])
    }
}

/// Open the directory at `path` in `root` with the flags `last`. Resolve
/// `path` one component at a time. Open each component with `O_NOFOLLOW`
/// and follow a symlink in userspace. A path or a symlink target that leads
/// outside `root` gives the escape error. The resolution keeps the open
/// descriptor of the current directory and the identity of each directory
/// above it, up to `root`. For `..`, it opens `..` in the current directory
/// and compares the identity of the result with the kept identity. A
/// different identity gives the escape error, for example after a rename
/// moves the current directory. The resolution keeps at most two
/// descriptors open.
fn resolve_manual(root: BorrowedFd<'_>, path: &[u8], last: OFlags) -> io::Result<OwnedFd> {
    let (opened, _) = walk(root, path, false)?;
    let current = opened.as_ref().map_or(root, |dir| dir.as_fd());
    Ok(openat(current, ".", last, Mode::empty())?)
}

/// Open `path` or its deepest existing directory in `root` with one walk of
/// the userspace resolver. The rules are in `open_prefix`.
fn open_prefix_manual(root: BorrowedFd<'_>, path: &str) -> io::Result<Found> {
    let (opened, begin) = walk(root, path.as_bytes(), true)?;
    if begin < path.len() {
        return Ok(Found::Prefix(opened, begin));
    }
    let current = opened.as_ref().map_or(root, |dir| dir.as_fd());
    let dir = openat(current, ".", READ_FLAGS, Mode::empty())?;
    Ok(Found::Path(dir))
}

/// Open the directory at `path` in `root` with `PATH_FLAGS`, by the rules of
/// `resolve_manual`. Give the directory, or `None` for `root`, and
/// `path.len()`. With `stop` set, a missing component of `path` ends the
/// walk. Then give the last opened directory and the offset of the missing
/// component. A missing component in a symlink target always gives
/// `NotFound`.
fn walk(root: BorrowedFd<'_>, path: &[u8], stop: bool) -> io::Result<(Option<OwnedFd>, usize)> {
    let mut work = Worklist { items: Vec::new() };
    work.push(Cow::Borrowed(path))?;
    // The identity of each directory above the current directory, from
    // `root` down. The current directory is `opened`, or `root` if `ids` is
    // empty. The last descent does not push an identity, because no `..`
    // follows it. Then the loop ends.
    let mut ids: Vec<FileId> = Vec::new();
    let mut opened: Option<OwnedFd> = None;
    // The identity of the current directory, if it is known.
    let mut current_id: Option<FileId> = None;
    let mut expansions = 0;
    while let Some(name) = work.next_component() {
        let current = opened.as_ref().map_or(root, |dir| dir.as_fd());
        if name == b".." {
            let Some(expect) = ids.pop() else {
                return Err(escape_error());
            };
            // The open needs search permission on the current directory.
            let parent = openat(current, "..", PATH_FLAGS, Mode::empty())?;
            if fd_id(parent.as_fd())? != expect {
                return Err(escape_error());
            }
            opened = if ids.is_empty() { None } else { Some(parent) };
            current_id = Some(expect);
            continue;
        }
        let len = name.len();
        let open_err = match openat(current, name, PATH_FLAGS | OFlags::NOFOLLOW, Mode::empty()) {
            Ok(dir) => {
                let id = current_id.take();
                if work.has_next() {
                    ids.push(match id {
                        Some(id) => id,
                        None => fd_id(current)?,
                    });
                }
                opened = Some(dir);
                continue;
            }
            // The first item is `path`. With one item, the component comes
            // from `path` and not from a symlink target.
            Err(Errno::NOENT) if stop => {
                return match work.items[..] {
                    // `next` is one byte after the end of the component.
                    [(_, next)] => Ok((opened, next - 1 - len)),
                    _ => Err(Errno::NOENT.into()),
                };
            }
            Err(err) if may_be_symlink(err) => err,
            Err(err) => return Err(err.into()),
        };
        let target = match readlinkat(current, name, Vec::new()) {
            Ok(target) => target,
            // The node is not a symlink.
            Err(Errno::INVAL) => return Err(open_err.into()),
            Err(err) => return Err(err.into()),
        };
        // Count the expansion after the read. Thus a node that is not a
        // symlink after the last allowed expansion gives its own error.
        if expansions == MAX_SYMLINK_EXPANSIONS {
            return Err(Errno::LOOP.into());
        }
        expansions += 1;
        work.push(Cow::Owned(target.into_bytes()))?;
    }
    Ok((opened, path.len()))
}

/// Give the identity of the open directory `dir`.
fn fd_id(dir: BorrowedFd<'_>) -> io::Result<FileId> {
    Ok(file_id(&fstat(dir)?))
}

/// Tell if the error of an open with `O_NOFOLLOW` and `O_DIRECTORY` can come
/// from a symlink. Linux and Android give `ENOTDIR` for a symlink and for
/// other nodes that are not directories. Other targets give `ELOOP`,
/// FreeBSD gives `EMLINK` or `ENOTDIR`, DragonFly gives `EMLINK`, and NetBSD
/// gives `EFTYPE`.
fn may_be_symlink(err: Errno) -> bool {
    #[cfg(any(target_os = "freebsd", target_os = "dragonfly"))]
    if err == Errno::MLINK {
        return true;
    }
    #[cfg(target_os = "netbsd")]
    if err == Errno::FTYPE {
        return true;
    }
    matches!(err, Errno::LOOP | Errno::NOTDIR)
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::{
            sys::{open_dest, read_dir, FileId},
            testutil::TempDir,
        },
        std::{
            ffi::OsStr,
            fs::{self, File, Permissions},
            io::ErrorKind,
            os::unix::{
                ffi::OsStrExt,
                fs::{symlink, MetadataExt, PermissionsExt},
            },
            path::Path,
        },
    };

    /// A resolver with the signature of `resolve`.
    type Resolver = fn(BorrowedFd<'_>, &[u8], OFlags) -> io::Result<OwnedFd>;

    /// Resolve `path` in the directory `root` with the userspace resolver and
    /// give the identity of the result.
    fn resolve_id(root: &Path, path: impl AsRef<[u8]>) -> io::Result<FileId> {
        let dir = open_dest(root).unwrap();
        id_with(resolve_manual, &dir, path.as_ref(), READ_FLAGS)
    }

    /// Resolve `path` in `root` with `resolver` and the flags `last`. Give the
    /// identity of the result.
    fn id_with(resolver: Resolver, root: &Dir, path: &[u8], last: OFlags) -> io::Result<FileId> {
        let fd = resolver(root.as_fd(), path, last)?;
        let md = File::from(fd).metadata()?;
        Ok((md.dev(), md.ino()))
    }

    fn id_of(path: &Path) -> FileId {
        let md = fs::metadata(path).unwrap();
        (md.dev(), md.ino())
    }

    fn assert_escape(result: io::Result<FileId>, path: &str) {
        let err = result.unwrap_err();
        assert_eq!(err.kind(), ErrorKind::PermissionDenied, "{path}");
        assert_eq!(
            err.to_string(),
            "a path led outside of the filesystem",
            "{path}"
        );
    }

    fn assert_loop(result: io::Result<FileId>, path: &str) {
        let err = result.unwrap_err();
        assert_eq!(
            err.raw_os_error(),
            Some(Errno::LOOP.raw_os_error()),
            "{path}"
        );
    }

    fn is_root() -> bool {
        // SAFETY: `geteuid` has no preconditions.
        unsafe { libc::geteuid() == 0 }
    }

    /// Make a chain of `n` symlinks in `dir`: `{prefix}1` -> `{prefix}2` ->
    /// ... -> `{prefix}n` -> `target`. Give the name of the first symlink.
    fn chain(dir: &Path, prefix: &str, n: usize, target: &str) -> String {
        for i in 1..=n {
            let next = if i == n {
                target.to_owned()
            } else {
                format!("{prefix}{}", i + 1)
            };
            symlink(next, dir.join(format!("{prefix}{i}"))).unwrap();
        }
        format!("{prefix}1")
    }

    #[test]
    fn resolve_manual_without_symlink() {
        let tmp = TempDir::new();
        fs::create_dir_all(tmp.path().join("a/b")).unwrap();
        let expect = id_of(&tmp.path().join("a/b"));
        for path in ["a/b", "a//./b/", "./a/b"] {
            assert_eq!(resolve_id(tmp.path(), path).unwrap(), expect, "{path}");
        }
        assert_eq!(resolve_id(tmp.path(), ".").unwrap(), id_of(tmp.path()));
        #[cfg(any(target_os = "linux", target_os = "android", target_os = "freebsd"))]
        {
            let root = open_dest(tmp.path()).unwrap();
            let fd = resolve_manual(root.as_fd(), b"a/b", READ_FLAGS).unwrap();
            let flags = rustix::fs::fcntl_getfl(&fd).unwrap();
            assert!(!flags.contains(OFlags::PATH));
        }
    }

    #[test]
    fn resolve_manual_sets_cloexec() {
        let tmp = TempDir::new();
        fs::create_dir(tmp.path().join("a")).unwrap();
        let root = open_dest(tmp.path()).unwrap();
        let fd = resolve_manual(root.as_fd(), b"a", READ_FLAGS).unwrap();
        let flags = rustix::io::fcntl_getfd(&fd).unwrap();
        assert!(flags.contains(rustix::io::FdFlags::CLOEXEC));
    }

    #[test]
    fn resolve_manual_follows_relative_symlink() {
        let tmp = TempDir::new();
        fs::create_dir_all(tmp.path().join("d/e")).unwrap();
        symlink("d", tmp.path().join("l")).unwrap();
        symlink("e", tmp.path().join("d/m")).unwrap();
        let d = id_of(&tmp.path().join("d"));
        let e = id_of(&tmp.path().join("d/e"));
        assert_eq!(resolve_id(tmp.path(), "l").unwrap(), d);
        for path in ["l/e", "d/m", "l/m"] {
            assert_eq!(resolve_id(tmp.path(), path).unwrap(), e, "{path}");
        }
    }

    #[test]
    fn resolve_manual_dotdot_in_target_inside_root() {
        let tmp = TempDir::new();
        fs::create_dir_all(tmp.path().join("a/b")).unwrap();
        fs::create_dir(tmp.path().join("c")).unwrap();
        symlink("../c", tmp.path().join("a/l")).unwrap();
        symlink("../../c", tmp.path().join("a/b/l2")).unwrap();
        symlink("..", tmp.path().join("a/up")).unwrap();
        let c = id_of(&tmp.path().join("c"));
        assert_eq!(resolve_id(tmp.path(), "a/l").unwrap(), c);
        assert_eq!(resolve_id(tmp.path(), "a/b/l2").unwrap(), c);
        assert_eq!(resolve_id(tmp.path(), "a/up").unwrap(), id_of(tmp.path()));
    }

    #[test]
    fn resolve_manual_dotdot_in_target_outside_root() {
        let tmp = TempDir::new();
        let dest = tmp.path().join("dest");
        fs::create_dir_all(dest.join("a")).unwrap();
        fs::create_dir(tmp.path().join("out")).unwrap();
        symlink("../out", dest.join("l")).unwrap();
        symlink("../../out", dest.join("a/l")).unwrap();
        symlink("../dest", dest.join("self")).unwrap();
        for path in ["l", "l/x", "a/l", "self"] {
            assert_escape(resolve_id(&dest, path), path);
        }
    }

    #[test]
    fn resolve_manual_absolute_target() {
        let tmp = TempDir::new();
        let target = tmp.path().join("d");
        fs::create_dir(&target).unwrap();
        symlink(&target, tmp.path().join("l")).unwrap();
        for path in ["l", "l/x"] {
            assert_escape(resolve_id(tmp.path(), path), path);
        }
    }

    #[test]
    fn resolve_manual_absolute_or_empty_path() {
        let tmp = TempDir::new();
        fs::create_dir(tmp.path().join("d")).unwrap();
        assert_escape(resolve_id(tmp.path(), "/d"), "/d");
        let err = resolve_id(tmp.path(), "").unwrap_err();
        assert_eq!(err.kind(), ErrorKind::NotFound);
    }

    #[test]
    fn resolve_manual_dotdot_in_path() {
        let tmp = TempDir::new();
        fs::create_dir(tmp.path().join("a")).unwrap();
        for path in ["..", "a/../.."] {
            assert_escape(resolve_id(tmp.path(), path), path);
        }
        assert_eq!(resolve_id(tmp.path(), "a/..").unwrap(), id_of(tmp.path()));
    }

    #[test]
    fn resolve_manual_deep_dotdot() {
        let tmp = TempDir::new();
        let deep = tmp.path().join("d/".repeat(100));
        fs::create_dir_all(&deep).unwrap();
        let mid = tmp.path().join("d/".repeat(50));
        symlink(format!("{}d", "../".repeat(50)), mid.join("x")).unwrap();
        let up = format!("{}{}", "d/".repeat(100), "../".repeat(100));
        assert_eq!(resolve_id(tmp.path(), &up).unwrap(), id_of(tmp.path()));
        let over = format!("{up}..");
        assert_escape(resolve_id(tmp.path(), &over), &over);
        let link = format!("{}x", "d/".repeat(50));
        assert_eq!(
            resolve_id(tmp.path(), &link).unwrap(),
            id_of(&tmp.path().join("d"))
        );
    }

    #[test]
    fn resolve_manual_symlink_loop() {
        let tmp = TempDir::new();
        symlink("b", tmp.path().join("a")).unwrap();
        symlink("a", tmp.path().join("b")).unwrap();
        symlink("s", tmp.path().join("s")).unwrap();
        for path in ["a", "a/x", "s"] {
            assert_loop(resolve_id(tmp.path(), path), path);
        }
    }

    #[test]
    fn resolve_manual_symlink_chain_limit() {
        let tmp = TempDir::new();
        fs::create_dir(tmp.path().join("d")).unwrap();
        fs::write(tmp.path().join("f"), b"").unwrap();
        let max = usize::from(MAX_SYMLINK_EXPANSIONS);
        let ok = chain(tmp.path(), "a", max, "d");
        assert_eq!(
            resolve_id(tmp.path(), &ok).unwrap(),
            id_of(&tmp.path().join("d"))
        );
        let over = chain(tmp.path(), "b", max + 1, "d");
        assert_loop(resolve_id(tmp.path(), &over), &over);
        let file = chain(tmp.path(), "c", max, "f");
        let err = resolve_id(tmp.path(), &file).unwrap_err();
        assert_eq!(err.kind(), ErrorKind::NotADirectory);
    }

    #[test]
    fn resolve_manual_symlink_limit_before_more_components() {
        let tmp = TempDir::new();
        fs::create_dir(tmp.path().join("a")).unwrap();
        symlink(".", tmp.path().join("dot")).unwrap();
        let max = usize::from(MAX_SYMLINK_EXPANSIONS);
        let ok = format!("{}a", "dot/".repeat(max));
        assert_eq!(
            resolve_id(tmp.path(), &ok).unwrap(),
            id_of(&tmp.path().join("a"))
        );
        let over = format!("{}a", "dot/".repeat(max + 1));
        assert_loop(resolve_id(tmp.path(), &over), &over);
    }

    #[test]
    fn resolve_manual_symlink_to_file() {
        let tmp = TempDir::new();
        fs::write(tmp.path().join("f"), b"").unwrap();
        symlink("f", tmp.path().join("l")).unwrap();
        for path in ["f", "l", "l/x"] {
            let err = resolve_id(tmp.path(), path).unwrap_err();
            assert_eq!(err.kind(), ErrorKind::NotADirectory, "{path}");
        }
    }

    #[test]
    fn resolve_manual_dangling_symlink() {
        let tmp = TempDir::new();
        symlink("missing", tmp.path().join("l")).unwrap();
        for path in ["l", "l/x", "missing"] {
            let err = resolve_id(tmp.path(), path).unwrap_err();
            assert_eq!(err.kind(), ErrorKind::NotFound, "{path}");
        }
    }

    #[test]
    fn resolve_manual_non_utf8_target() {
        let tmp = TempDir::new();
        let name = OsStr::from_bytes(b"\xff");
        match fs::create_dir(tmp.path().join(name)) {
            Err(err) if err.raw_os_error() == Some(Errno::ILSEQ.raw_os_error()) => {
                eprintln!("skip: the filesystem does not allow names that are not UTF-8");
                return;
            }
            result => result.unwrap(),
        }
        symlink(name, tmp.path().join("l")).unwrap();
        assert_eq!(
            resolve_id(tmp.path(), "l").unwrap(),
            id_of(&tmp.path().join(name))
        );
    }

    #[cfg(any(target_os = "linux", target_os = "android", target_os = "freebsd"))]
    #[test]
    fn resolve_manual_intermediate_needs_only_search() {
        let tmp = TempDir::new();
        let x = tmp.path().join("x");
        fs::create_dir_all(x.join("y")).unwrap();
        let y = id_of(&x.join("y"));
        fs::set_permissions(&x, Permissions::from_mode(0o111)).unwrap();
        let inner = resolve_id(tmp.path(), "x/y");
        let outer = (!is_root()).then(|| resolve_id(tmp.path(), "x"));
        fs::set_permissions(&x, Permissions::from_mode(0o755)).unwrap();
        assert_eq!(inner.unwrap(), y);
        if let Some(outer) = outer {
            assert_eq!(outer.unwrap_err().kind(), ErrorKind::PermissionDenied);
        }
    }

    #[test]
    fn resolve_manual_dotdot_needs_search() {
        if is_root() {
            eprintln!("skip: root has search permission on each directory");
            return;
        }
        let tmp = TempDir::new();
        let a = tmp.path().join("a");
        fs::create_dir(&a).unwrap();
        symlink("a/..", tmp.path().join("l")).unwrap();
        fs::set_permissions(&a, Permissions::from_mode(0o600)).unwrap();
        let results = ["a/..", "l"].map(|path| resolve_id(tmp.path(), path));
        fs::set_permissions(&a, Permissions::from_mode(0o755)).unwrap();
        for (path, result) in ["a/..", "l"].into_iter().zip(results) {
            assert_eq!(
                result.unwrap_err().raw_os_error(),
                Some(Errno::ACCESS.raw_os_error()),
                "{path}"
            );
        }
    }

    /// Resolve `path` with `openat2` only. A missing result gives an error.
    #[cfg(target_os = "linux")]
    fn openat2_only(root: BorrowedFd<'_>, path: &[u8], last: OFlags) -> io::Result<OwnedFd> {
        resolve_openat2(root, path, last)
            .unwrap_or_else(|| Err(io::Error::other("openat2 gave no result")))
    }

    /// Tell if `openat2` resolves paths in `root`. If not, print a skip
    /// message.
    #[cfg(target_os = "linux")]
    fn openat2_works(root: &Dir) -> bool {
        if resolve_openat2(root.as_fd(), b".", READ_FLAGS).is_some() {
            return true;
        }
        eprintln!("skip: openat2 is not available");
        false
    }

    /// Expected result of a resolution.
    #[cfg(target_os = "linux")]
    #[derive(Clone, Copy, Debug)]
    enum Expect {
        Dir(FileId),
        Escape,
        Errno(Errno),
    }

    #[cfg(target_os = "linux")]
    fn assert_expect(result: &io::Result<FileId>, expect: Expect, path: &str) {
        match (result, expect) {
            (Ok(id), Expect::Dir(want)) => assert_eq!(*id, want, "{path}"),
            (Err(err), Expect::Escape) => {
                assert_eq!(err.kind(), ErrorKind::PermissionDenied, "{path}");
                assert_eq!(
                    err.to_string(),
                    "a path led outside of the filesystem",
                    "{path}"
                );
            }
            (Err(err), Expect::Errno(want)) => {
                assert_eq!(err.raw_os_error(), Some(want.raw_os_error()), "{path}");
            }
            (result, expect) => panic!("{path}: got {result:?}, expected {expect:?}"),
        }
    }

    #[cfg(target_os = "linux")]
    fn assert_same(kernel: &io::Result<FileId>, manual: &io::Result<FileId>, path: &str) {
        match (kernel, manual) {
            (Ok(kernel), Ok(manual)) => assert_eq!(kernel, manual, "{path}"),
            (Err(kernel), Err(manual)) => {
                assert_eq!(kernel.kind(), manual.kind(), "{path}");
                assert_eq!(kernel.raw_os_error(), manual.raw_os_error(), "{path}");
                assert_eq!(kernel.to_string(), manual.to_string(), "{path}");
            }
            _ => panic!("{path}: openat2 gave {kernel:?}, the userspace resolver gave {manual:?}"),
        }
    }

    /// `openat2` and the userspace resolver give the same result for each
    /// path. The fixture does not cover these known differences:
    ///
    /// - A path with a NUL byte gives `EINVAL` from `openat2`. The userspace
    ///   resolver gives the error of an earlier component that fails.
    /// - `openat2` can give `EAGAIN` or `ENAMETOOLONG`. Then `resolve` uses
    ///   the userspace resolver. `resolve_long_path` tests `ENAMETOOLONG`.
    ///
    /// `openat2` gives `ELOOP` for a symlink loop and for more than 40
    /// symlink expansions. Then `resolve` uses the userspace resolver, which
    /// also gives `ELOOP`. For each other path, `openat2` gives the result.
    #[cfg(target_os = "linux")]
    #[test]
    fn resolve_openat2_matches_manual() {
        let tmp = TempDir::new();
        let dest = tmp.path().join("dest");
        for dir in ["d/e", "c", "x/y", "p"] {
            fs::create_dir_all(dest.join(dir)).unwrap();
        }
        fs::create_dir(tmp.path().join("out")).unwrap();
        fs::write(dest.join("f"), b"").unwrap();
        for (target, name) in [
            ("d", "l"),
            ("e", "d/m"),
            ("..", "d/up"),
            ("../c", "d/l2"),
            ("../../c", "d/e/l3"),
            ("d/e", "lde"),
            ("../out", "esc"),
            ("../../out", "d/esc2"),
            ("../dest", "self"),
            ("loop_b", "loop_a"),
            ("loop_a", "loop_b"),
            ("s", "s"),
            (".", "dot"),
            ("f", "lf"),
            ("missing", "dangling"),
            ("p/..", "lp"),
        ] {
            symlink(target, dest.join(name)).unwrap();
        }
        symlink(dest.join("d"), dest.join("abs")).unwrap();
        let max = usize::from(MAX_SYMLINK_EXPANSIONS);
        let ca = chain(&dest, "ca", max, "d");
        let ca_e = format!("{ca}/e");
        let cb = chain(&dest, "cb", max + 1, "d");
        let cc = chain(&dest, "cc", max, "f");
        let dots = format!("{}d", "dot/".repeat(max));
        let dots_over = format!("{}d", "dot/".repeat(max + 1));
        let name = OsStr::from_bytes(b"\xff");
        let non_utf8 = match fs::create_dir(dest.join(name)) {
            Err(err) if err.raw_os_error() == Some(Errno::ILSEQ.raw_os_error()) => {
                eprintln!("skip: the filesystem does not allow names that are not UTF-8");
                None
            }
            result => {
                result.unwrap();
                symlink(name, dest.join("nu")).unwrap();
                Some(id_of(&dest.join(name)))
            }
        };
        let root = open_dest(&dest).unwrap();
        if !openat2_works(&root) {
            return;
        }

        let id = |path: &str| id_of(&dest.join(path));
        let (top, d, e, c) = (id(""), id("d"), id("d/e"), id("c"));
        let denied = |id: FileId| {
            if is_root() {
                Expect::Dir(id)
            } else {
                Expect::Errno(Errno::ACCESS)
            }
        };
        let mut cases: Vec<(Vec<u8>, Expect)> = Vec::new();
        let mut add = |paths: &[&str], expect: Expect| {
            cases.extend(paths.iter().map(|path| (path.as_bytes().to_vec(), expect)));
        };
        add(&["d/e", "./d//./e/", "d/e/"], Expect::Dir(e));
        add(&[".", "d/..", "d/up"], Expect::Dir(top));
        add(&["l", "lde/..", &ca, &dots], Expect::Dir(d));
        add(&["l/e", "d/m", "l/m", &ca_e], Expect::Dir(e));
        add(&["d/l2", "d/e/l3"], Expect::Dir(c));
        add(
            &[
                "esc", "esc/x", "d/esc2", "self", "..", "d/../..", "abs", "abs/x", "/d",
            ],
            Expect::Escape,
        );
        add(
            &["", "dangling", "dangling/x", "missing", "d/missing"],
            Expect::Errno(Errno::NOENT),
        );
        add(
            &["loop_a", "loop_a/x", "s", &cb, &dots_over],
            Expect::Errno(Errno::LOOP),
        );
        add(
            &[&cc, "f", "lf", "lf/x", "f/x", "lf/"],
            Expect::Errno(Errno::NOTDIR),
        );
        add(&["x/y"], Expect::Dir(id("x/y")));
        add(&["x"], denied(id("x")));
        add(&["p/..", "lp"], denied(top));
        add(&["p"], denied(id("p")));
        add(&["d\0e"], Expect::Errno(Errno::INVAL));
        if let Some(ff) = non_utf8 {
            cases.push((b"nu".to_vec(), Expect::Dir(ff)));
            cases.push((b"\xff".to_vec(), Expect::Dir(ff)));
        }

        fs::set_permissions(dest.join("x"), Permissions::from_mode(0o111)).unwrap();
        fs::set_permissions(dest.join("p"), Permissions::from_mode(0o600)).unwrap();
        let results: Vec<_> = cases
            .iter()
            .map(|(path, _)| {
                [READ_FLAGS, PATH_FLAGS].map(|last| {
                    (
                        id_with(resolve, &root, path, last),
                        id_with(resolve_manual, &root, path, last),
                        resolve_openat2(root.as_fd(), path, last).is_some(),
                    )
                })
            })
            .collect();
        for dir in ["x", "p"] {
            fs::set_permissions(dest.join(dir), Permissions::from_mode(0o755)).unwrap();
        }

        for ((path, expect), [read, path_flags]) in cases.iter().zip(&results) {
            let path = path.escape_ascii().to_string();
            let kernel = !matches!(expect, Expect::Errno(Errno::LOOP));
            assert_eq!(read.2, kernel, "{path}");
            assert_eq!(path_flags.2, kernel, "{path} (PATH_FLAGS)");
            assert_expect(&read.0, *expect, &path);
            assert_same(&read.0, &read.1, &path);
            assert_same(
                &path_flags.0,
                &path_flags.1,
                &format!("{path} (PATH_FLAGS)"),
            );
        }
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn with_openat2_fallback_rules() {
        /// Call `with_openat2` with `missing`. The fake `openat2` gives the
        /// items of `results` in order. Give the result and the number of
        /// calls.
        fn run(
            missing: &AtomicBool,
            results: &[rustix::io::Result<u32>],
        ) -> (Option<io::Result<u32>>, usize) {
            let mut calls = 0;
            let result = with_openat2(missing, || {
                calls += 1;
                results[calls - 1]
            });
            (result, calls)
        }

        let missing = AtomicBool::new(false);
        let (result, calls) = run(&missing, &[Err(Errno::AGAIN); OPENAT2_TRIES]);
        assert!(result.is_none());
        assert_eq!(calls, OPENAT2_TRIES);
        assert!(!missing.load(Ordering::Relaxed));

        let mut results = [Err(Errno::AGAIN); OPENAT2_TRIES];
        results[OPENAT2_TRIES - 1] = Ok(7);
        let (result, calls) = run(&missing, &results);
        assert_eq!(result.unwrap().unwrap(), 7);
        assert_eq!(calls, OPENAT2_TRIES);

        for errno in [Errno::PERM, Errno::NAMETOOLONG, Errno::LOOP] {
            let (result, calls) = run(&missing, &[Err(errno)]);
            assert!(result.is_none(), "{errno:?}");
            assert_eq!(calls, 1, "{errno:?}");
            assert!(!missing.load(Ordering::Relaxed), "{errno:?}");
        }

        for results in [
            &[Err(Errno::XDEV)][..],
            &[Err(Errno::AGAIN), Err(Errno::XDEV)],
        ] {
            let (result, calls) = run(&missing, results);
            assert_escape(result.unwrap().map(|_| (0, 0)), "EXDEV");
            assert_eq!(calls, results.len());
        }

        let (result, calls) = run(&missing, &[Err(Errno::NOENT)]);
        let err = result.unwrap().unwrap_err();
        assert_eq!(err.kind(), ErrorKind::NotFound);
        assert_eq!(err.raw_os_error(), Some(Errno::NOENT.raw_os_error()));
        assert_eq!(calls, 1);

        let (result, calls) = run(&missing, &[Err(Errno::NOSYS)]);
        assert!(result.is_none());
        assert_eq!(calls, 1);
        assert!(missing.load(Ordering::Relaxed));
        let (result, calls) = run(&missing, &[]);
        assert!(result.is_none());
        assert_eq!(calls, 0);
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn resolve_openat2_escape_message() {
        let tmp = TempDir::new();
        let dest = tmp.path().join("dest");
        fs::create_dir_all(dest.join("d")).unwrap();
        fs::create_dir(tmp.path().join("out")).unwrap();
        symlink("../out", dest.join("esc")).unwrap();
        symlink(dest.join("d"), dest.join("abs")).unwrap();
        let root = open_dest(&dest).unwrap();
        if !openat2_works(&root) {
            return;
        }
        // `with_openat2` gives the escape error only for `EXDEV`.
        for path in ["..", "esc", "esc/x", "abs", "/d"] {
            assert_escape(
                id_with(openat2_only, &root, path.as_bytes(), READ_FLAGS),
                path,
            );
            assert_escape(id_with(resolve, &root, path.as_bytes(), READ_FLAGS), path);
        }
    }

    /// A path of 4095 bytes fits in `PATH_MAX`, but the `/.` suffix makes it
    /// too long for `openat2`. Then `resolve` uses the userspace resolver. A
    /// component longer than `NAME_MAX` gives `ENAMETOOLONG` from both.
    #[cfg(target_os = "linux")]
    #[test]
    fn resolve_long_path() {
        let tmp = TempDir::new();
        fs::create_dir(tmp.path().join("d")).unwrap();
        let root = open_dest(tmp.path()).unwrap();
        let path = format!("{}d", "./".repeat(2047));
        assert_eq!(path.len(), 4095);
        let long = "a".repeat(256);
        assert_eq!(
            id_with(resolve, &root, path.as_bytes(), READ_FLAGS).unwrap(),
            id_of(&tmp.path().join("d"))
        );
        let err = id_with(resolve, &root, long.as_bytes(), READ_FLAGS).unwrap_err();
        assert_eq!(err.raw_os_error(), Some(Errno::NAMETOOLONG.raw_os_error()));
        if openat2_works(&root) {
            for path in [&path, &long] {
                assert!(resolve_openat2(root.as_fd(), path.as_bytes(), READ_FLAGS).is_none());
            }
        }
    }

    /// With `RESOLVE_NO_MAGICLINKS`, `openat2` gives `ELOOP` for the magic
    /// link `cwd` in `/proc/self`. Then `resolve` uses the userspace
    /// resolver. It reads the absolute link target and gives the escape
    /// error.
    #[cfg(target_os = "linux")]
    #[test]
    fn resolve_magic_link_gives_escape_error() {
        let root = match open_dest(Path::new("/proc/self")) {
            Ok(root) => root,
            Err(err) => {
                eprintln!("skip: cannot open /proc/self: {err}");
                return;
            }
        };
        if !openat2_works(&root) {
            return;
        }
        assert!(resolve_openat2(root.as_fd(), b"cwd", READ_FLAGS).is_none());
        for last in [READ_FLAGS, PATH_FLAGS] {
            assert_escape(id_with(resolve, &root, b"cwd", last), "cwd");
            assert_escape(id_with(resolve_manual, &root, b"cwd", last), "cwd");
        }
    }

    #[test]
    fn resolve_opens_for_reading_with_cloexec() {
        let tmp = TempDir::new();
        fs::create_dir(tmp.path().join("a")).unwrap();
        let root = open_dest(tmp.path()).unwrap();
        let fd = resolve(root.as_fd(), b"a", READ_FLAGS).unwrap();
        let flags = rustix::io::fcntl_getfd(&fd).unwrap();
        assert!(flags.contains(rustix::io::FdFlags::CLOEXEC));
        #[cfg(any(target_os = "linux", target_os = "android", target_os = "freebsd"))]
        assert!(!rustix::fs::fcntl_getfl(&fd).unwrap().contains(OFlags::PATH));
    }

    #[test]
    fn open_beneath_opens_for_reading() {
        let tmp = TempDir::new();
        fs::create_dir_all(tmp.path().join("d/x")).unwrap();
        symlink("d", tmp.path().join("l")).unwrap();
        let root = open_dest(tmp.path()).unwrap();
        let dir = open_beneath(&root, "l").unwrap();
        let children = read_dir(&dir, Path::new("l"), false, &|_, _| true).unwrap();
        assert_eq!(children.len(), 1);
        assert_eq!(&*children[0].name, "x");
    }

    /// A function with the signature of `open_prefix`.
    type Prefix = fn(BorrowedFd<'_>, &str) -> io::Result<Found>;

    /// Open the deepest existing directory with `openat2` only. A missing
    /// result gives an error.
    #[cfg(target_os = "linux")]
    fn openat2_prefix_only(root: BorrowedFd<'_>, path: &str) -> io::Result<Found> {
        open_prefix_openat2(root, path)
            .unwrap_or_else(|| Err(io::Error::other("openat2 gave no result")))
    }

    /// Give the functions for the create tests with their names: the
    /// userspace resolver and, on Linux, `openat2` if it works in `root`.
    fn resolvers(root: &Dir) -> Vec<(&'static str, Prefix, Resolver)> {
        #[cfg_attr(not(target_os = "linux"), allow(unused_mut))]
        let mut list: Vec<(&'static str, Prefix, Resolver)> =
            vec![("manual", open_prefix_manual, resolve_manual)];
        #[cfg(target_os = "linux")]
        if openat2_works(root) {
            list.push(("openat2", openat2_prefix_only, openat2_only));
        }
        #[cfg(not(target_os = "linux"))]
        let _ = root;
        list
    }

    /// Create the missing directories of `path` in `root` with the mode
    /// 0o755 with `prefix` and `resolver`. Give the identity of the result.
    /// Make sure that the result is open for reading.
    fn create_with(
        prefix: impl FnOnce(BorrowedFd<'_>, &str) -> io::Result<Found>,
        resolver: impl FnMut(BorrowedFd<'_>, &[u8], OFlags) -> io::Result<OwnedFd>,
        root: &Dir,
        path: &str,
    ) -> io::Result<FileId> {
        let mode = Mode::from_raw_mode(0o755);
        let fd = create_dirs_with(root.as_fd(), path, mode, prefix, resolver)?;
        #[cfg(any(target_os = "linux", target_os = "android", target_os = "freebsd"))]
        assert!(!rustix::fs::fcntl_getfl(&fd).unwrap().contains(OFlags::PATH));
        let md = File::from(fd).metadata()?;
        Ok((md.dev(), md.ino()))
    }

    #[test]
    fn create_dirs_beneath_symlink_outside_root() {
        let tmp = TempDir::new();
        let dest = tmp.path().join("dest");
        let out = tmp.path().join("out");
        fs::create_dir_all(dest.join("a")).unwrap();
        fs::create_dir(&out).unwrap();
        symlink("../out", dest.join("esc")).unwrap();
        symlink("../../out", dest.join("a/esc")).unwrap();
        symlink(&out, dest.join("abs")).unwrap();
        fs::create_dir(out.join("y")).unwrap();
        let root = open_dest(&dest).unwrap();
        for (name, prefix, resolver) in resolvers(&root) {
            for path in ["esc/x", "a/esc/x", "abs/x", "esc/y/x"] {
                let result = create_with(prefix, resolver, &root, path);
                assert_escape(result, &format!("{path} ({name})"));
            }
        }
        assert!(fs::symlink_metadata(out.join("x")).is_err());
        assert_eq!(fs::read_dir(out.join("y")).unwrap().count(), 0);
    }

    #[cfg(any(target_os = "linux", target_os = "android", target_os = "freebsd"))]
    #[test]
    fn create_dirs_beneath_parent_needs_write_and_search() {
        if is_root() {
            eprintln!("skip: root has each permission on each directory");
            return;
        }
        let tmp = TempDir::new();
        let p = tmp.path().join("p");
        fs::create_dir(&p).unwrap();
        let root = open_dest(tmp.path()).unwrap();
        let resolvers = resolvers(&root);
        fs::set_permissions(&p, Permissions::from_mode(0o300)).unwrap();
        let results: Vec<_> = resolvers
            .iter()
            .map(|&(name, prefix, resolver)| {
                let paths = [format!("p/{name}"), format!("p/{name}/q")];
                (
                    name,
                    paths.map(|path| create_with(prefix, resolver, &root, &path)),
                )
            })
            .collect();
        fs::set_permissions(&p, Permissions::from_mode(0o755)).unwrap();
        for (name, [dir, sub]) in results {
            assert_eq!(dir.unwrap(), id_of(&p.join(name)), "{name}");
            assert_eq!(sub.unwrap(), id_of(&p.join(name).join("q")), "{name}");
        }
    }

    #[test]
    fn create_dirs_beneath_last_symlink() {
        let tmp = TempDir::new();
        let dest = tmp.path().join("dest");
        let out = tmp.path().join("out");
        fs::create_dir_all(dest.join("d")).unwrap();
        fs::create_dir(&out).unwrap();
        symlink("../out/new", dest.join("l")).unwrap();
        symlink("../../out/new", dest.join("d/l")).unwrap();
        symlink("d", dest.join("m")).unwrap();
        let root = open_dest(&dest).unwrap();
        for (name, prefix, resolver) in resolvers(&root) {
            for path in ["l", "d/l"] {
                let result = create_with(prefix, resolver, &root, path);
                assert_escape(result, &format!("{path} ({name})"));
            }
            let result = create_with(prefix, resolver, &root, "m");
            assert_eq!(result.unwrap(), id_of(&dest.join("d")), "{name}");
        }
        assert!(fs::symlink_metadata(out.join("new")).is_err());
        assert_eq!(fs::read_dir(dest.join("d")).unwrap().count(), 1);
    }

    #[test]
    fn create_dirs_beneath_dangling_symlink() {
        let tmp = TempDir::new();
        fs::create_dir(tmp.path().join("d")).unwrap();
        symlink("new", tmp.path().join("n")).unwrap();
        symlink("d/missing", tmp.path().join("ld")).unwrap();
        let root = open_dest(tmp.path()).unwrap();
        for (name, prefix, resolver) in resolvers(&root) {
            for path in ["n", "n/x", "ld", "ld/x"] {
                let err = create_with(prefix, resolver, &root, path).unwrap_err();
                assert_eq!(err.kind(), ErrorKind::NotFound, "{path} ({name})");
            }
        }
        assert!(fs::symlink_metadata(tmp.path().join("new")).is_err());
        assert!(fs::symlink_metadata(tmp.path().join("d/missing")).is_err());
    }

    #[test]
    fn create_dirs_beneath_follows_symlink() {
        let tmp = TempDir::new();
        fs::create_dir(tmp.path().join("d")).unwrap();
        symlink("d", tmp.path().join("l")).unwrap();
        let root = open_dest(tmp.path()).unwrap();
        for (name, prefix, resolver) in resolvers(&root) {
            for (path, real) in [
                (format!("l/{name}/y"), format!("d/{name}/y")),
                (format!("n_{name}/y/z"), format!("n_{name}/y/z")),
            ] {
                let id = create_with(prefix, resolver, &root, &path).unwrap();
                assert_eq!(id, id_of(&tmp.path().join(real)), "{path}");
            }
        }
    }

    /// The root of the resolution is a directory below the destination. A
    /// symlink that leads outside this directory gives the escape error, also
    /// if its target is in the destination. Then the resolution creates no
    /// directory. A symlink that stays in the directory resolves.
    #[test]
    fn resolve_in_subdirectory_root() {
        let tmp = TempDir::new();
        let dest = tmp.path();
        fs::create_dir_all(dest.join("a/d")).unwrap();
        fs::create_dir(dest.join("b")).unwrap();
        symlink("../b", dest.join("a/x")).unwrap();
        symlink("../b/new", dest.join("a/y")).unwrap();
        symlink(dest.join("b"), dest.join("a/z")).unwrap();
        symlink("./d", dest.join("a/w")).unwrap();
        let root = open_dest(dest).unwrap();
        let base = open_beneath(&root, "a").unwrap();
        for (name, prefix, resolver) in resolvers(&base) {
            for path in ["x", "x/new", "y", "z", "z/new"] {
                let result = id_with(resolver, &base, path.as_bytes(), READ_FLAGS);
                assert_escape(result, &format!("{path} ({name})"));
                let result = create_with(prefix, resolver, &base, path);
                assert_escape(result, &format!("{path} ({name})"));
            }
            let id = id_with(resolver, &base, b"w", READ_FLAGS).unwrap();
            assert_eq!(id, id_of(&dest.join("a/d")), "{name}");
            let id = create_with(prefix, resolver, &base, &format!("w/{name}")).unwrap();
            assert_eq!(id, id_of(&dest.join("a/d").join(name)), "{name}");
        }
        assert_eq!(fs::read_dir(dest.join("b")).unwrap().count(), 0);
    }

    /// A node that another process creates after the call of `prefix` gives
    /// no error. A symlink at the name follows the rules of `open_beneath`.
    #[test]
    fn create_dirs_beneath_node_created_after_prefix() {
        let tmp = TempDir::new();
        let dest = tmp.path().join("dest");
        let out = tmp.path().join("out");
        fs::create_dir_all(dest.join("a")).unwrap();
        fs::create_dir(dest.join("d")).unwrap();
        fs::create_dir(&out).unwrap();
        symlink("d", dest.join("s")).unwrap();
        symlink("../out", dest.join("e")).unwrap();
        fs::write(dest.join("f"), b"").unwrap();
        let root = open_dest(&dest).unwrap();
        let none: Prefix = |_, _| Ok(Found::Prefix(None, 0));
        for (name, _, resolver) in resolvers(&root) {
            for (path, real) in [
                (format!("a/{name}"), format!("a/{name}")),
                (format!("s/{name}"), format!("d/{name}")),
            ] {
                let id = create_with(none, resolver, &root, &path).unwrap();
                assert_eq!(id, id_of(&dest.join(real)), "{path}");
            }
            assert_escape(create_with(none, resolver, &root, "e/b"), name);
            let err = create_with(none, resolver, &root, "f/b").unwrap_err();
            assert_eq!(err.kind(), ErrorKind::NotADirectory, "{name}");
            // The prefix is the full path.
            let id = create_with(open_prefix_manual, resolver, &root, "a").unwrap();
            assert_eq!(id, id_of(&dest.join("a")), "{name}");
        }
        assert_eq!(fs::read_dir(&out).unwrap().count(), 0);
    }

    /// `prefix` opens the path or its deepest existing directory, and no
    /// call of `resolve` follows. The paths are: all components missing, the
    /// same path when it exists, and a path with an existing prefix.
    #[test]
    fn create_dirs_beneath_resolves_one_time() {
        let tmp = TempDir::new();
        let root = open_dest(tmp.path()).unwrap();
        for (name, prefix, resolver) in resolvers(&root) {
            let path = format!("{name}{}", "/c".repeat(63));
            for path in [path.clone(), path.clone(), format!("{path}/x/y")] {
                let (mut prefixes, mut resolves) = (0, 0);
                let id = create_with(
                    |root, path| {
                        prefixes += 1;
                        prefix(root, path)
                    },
                    |root, path, last| {
                        resolves += 1;
                        resolver(root, path, last)
                    },
                    &root,
                    &path,
                )
                .unwrap();
                assert_eq!(id, id_of(&tmp.path().join(&path)), "{name}");
                assert_eq!((prefixes, resolves), (1, 0), "{name}");
            }
        }
    }

    /// For an existing path, `open_prefix_manual` gives the directory open
    /// for reading. A symlink in the last component goes to its target.
    #[test]
    fn open_prefix_manual_existing_path() {
        let tmp = TempDir::new();
        fs::create_dir_all(tmp.path().join("d/x")).unwrap();
        symlink("d", tmp.path().join("l")).unwrap();
        symlink(".", tmp.path().join("dot")).unwrap();
        let root = open_dest(tmp.path()).unwrap();
        for (path, real) in [("d/x", "d/x"), ("l", "d"), ("dot", "")] {
            let Found::Path(fd) = open_prefix_manual(root.as_fd(), path).unwrap() else {
                panic!("{path}: the path is missing");
            };
            #[cfg(any(target_os = "linux", target_os = "android", target_os = "freebsd"))]
            assert!(!rustix::fs::fcntl_getfl(&fd).unwrap().contains(OFlags::PATH));
            let md = File::from(fd).metadata().unwrap();
            let id = (md.dev(), md.ino());
            assert_eq!(id, id_of(&tmp.path().join(real)), "{path}");
        }
    }

    /// The path with `n` components is missing. Thus the number of existing
    /// components `e` is less than `n`.
    #[test]
    fn deepest_finds_existing_prefix() {
        for n in 1..=70 {
            for e in 0..n {
                let mut probes = 0;
                let (found, count) = deepest(n, |count| {
                    assert!(0 < count && count < n, "n {n}, e {e}, count {count}");
                    probes += 1;
                    Some(if count <= e {
                        Ok(count)
                    } else {
                        Err(io::Error::from(ErrorKind::NotFound))
                    })
                })
                .unwrap()
                .unwrap();
                assert_eq!(count, e, "n {n}");
                assert_eq!(found, (e > 0).then_some(e), "n {n}");
                if e == n - 1 {
                    // The parent needs one probe. The root needs no probe.
                    assert_eq!(probes, usize::from(e > 0), "n {n}, e {e}");
                } else {
                    let log = (usize::BITS - (n - e - 1).leading_zeros()) as usize;
                    assert!(probes <= 2 * log + 1, "n {n}, e {e}: {probes} probes");
                }
            }
        }
        assert!(deepest(5, |_| None::<io::Result<()>>).is_none());
        let err = deepest(5, |_| {
            Some(Err::<(), _>(io::Error::from(ErrorKind::PermissionDenied)))
        });
        assert_eq!(
            err.unwrap().unwrap_err().kind(),
            ErrorKind::PermissionDenied
        );
    }
}
