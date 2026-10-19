//! Unix backend. It uses `rustix` for all system calls, except `mkfifoat`
//! and `mknodat` on Apple platforms, which use `libc`.

#[cfg(target_vendor = "apple")]
use rustix::fd::AsRawFd;
#[cfg(not(any(target_os = "linux", target_os = "android")))]
use rustix::fs::Dir as DirReader;
#[cfg(any(target_os = "linux", target_os = "android", target_vendor = "apple"))]
use rustix::fs::{fgetxattr, flistxattr, fsetxattr, XattrFlags};
#[cfg(not(target_vendor = "apple"))]
use rustix::fs::{makedev, mknodat};
#[cfg(any(target_os = "linux", target_os = "android"))]
use rustix::fs::{seek, RawDir, SeekFrom};
use {
    crate::{with_path, Child, Existing, FsKind, Meta, Root, SetMeta, UnsupportedXattr},
    rustix::{
        fd::{AsFd, BorrowedFd, OwnedFd},
        fs::{
            chmodat, chownat, fchmod, fchown, fcntl_setfl, fstat, futimens, linkat, major, minor,
            mkdirat, openat, readlinkat, readlinkat_raw, statat, symlinkat, unlinkat, utimensat,
            AtFlags, FileType, Gid, Mode, OFlags, Stat, Timespec, Timestamps, Uid, CWD, UTIME_OMIT,
        },
        io::Errno,
        path::Arg,
    },
    smol_tar::AttrList,
    std::{
        ffi::{CStr, CString, OsStr},
        io,
        mem::MaybeUninit,
        os::unix::ffi::OsStrExt,
        path::Path,
        time::{Duration, SystemTime, UNIX_EPOCH},
    },
};

/// Open directory handle.
pub(crate) type Dir = rustix::fd::OwnedFd;

/// File identity: `(st_dev, st_ino)`.
pub(crate) type FileId = (u64, u64);

/// Flags to open a directory without following a symlink.
const DIR_FLAGS: OFlags = OFlags::RDONLY
    .union(OFlags::DIRECTORY)
    .union(OFlags::NOFOLLOW)
    .union(OFlags::CLOEXEC);

/// Flags to open a regular file or a FIFO without following a symlink. The
/// open does not wait for a FIFO writer, and a terminal does not become the
/// controlling terminal.
const FILE_FLAGS: OFlags = OFlags::RDONLY
    .union(OFlags::NONBLOCK)
    .union(OFlags::NOFOLLOW)
    .union(OFlags::NOCTTY)
    .union(OFlags::CLOEXEC);

/// Size of the stack buffer for a symlink target. Linux and Apple limit a
/// target to fewer bytes.
const LINK_BUF_LEN: usize = 4096;

/// Give the kind of a node. A node with no `FsKind` gives `None`.
fn kind_of(st: &Stat) -> Option<FsKind> {
    Some(match FileType::from_raw_mode(st.st_mode as _) {
        FileType::RegularFile => FsKind::File,
        FileType::Directory => FsKind::Directory,
        FileType::Symlink => FsKind::Symlink,
        FileType::Fifo => FsKind::Fifo,
        FileType::CharacterDevice => FsKind::CharDevice,
        FileType::BlockDevice => FsKind::BlockDevice,
        _ => return None,
    })
}

/// Make the metadata of a node from its `Stat`. A node with no `FsKind`
/// gives `None`. The xattr list is empty.
// The field types of `Stat` differ between targets.
#[allow(clippy::unnecessary_cast)]
fn meta_from_stat(st: &Stat) -> io::Result<Option<Meta>> {
    let Some(kind) = kind_of(st) else {
        return Ok(None);
    };
    let sec = st.st_mtime as i64;
    let nsec = Duration::from_nanos(st.st_mtime_nsec as u64);
    let mtime = if sec >= 0 {
        UNIX_EPOCH
            .checked_add(Duration::from_secs(sec as u64))
            .and_then(|t| t.checked_add(nsec))
    } else {
        UNIX_EPOCH
            .checked_sub(Duration::from_secs(sec.unsigned_abs()))
            .and_then(|t| t.checked_add(nsec))
    }
    .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "modification time out of range"))?;
    let device = match kind {
        FsKind::CharDevice | FsKind::BlockDevice => {
            Some((major(st.st_rdev as _), minor(st.st_rdev as _)))
        }
        _ => None,
    };
    Ok(Some(Meta {
        kind,
        size: if kind == FsKind::File {
            st.st_size as u64
        } else {
            0
        },
        mode: st.st_mode as u32 & 0o7777,
        uid: st.st_uid as u32,
        gid: st.st_gid as u32,
        mtime,
        id: file_id(st),
        nlink: st.st_nlink as u64,
        device,
        xattrs: AttrList::new(),
    }))
}

/// Give the identity of a node.
// The field types of `Stat` differ between targets.
#[allow(clippy::unnecessary_cast)]
fn file_id(st: &Stat) -> FileId {
    (st.st_dev as u64, st.st_ino as u64)
}

/// Error for a node that is different from the expected node.
fn file_changed() -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, "file changed")
}

/// Compare the kind and the identity of a node with `kind` and `expect`.
fn check_id(st: &Stat, kind: FsKind, expect: FileId) -> io::Result<()> {
    if kind_of(st) == Some(kind) && file_id(st) == expect {
        Ok(())
    } else {
        Err(file_changed())
    }
}

/// Buffers for `flistxattr` and `fgetxattr`. One set serves many nodes.
#[derive(Default)]
struct XattrBufs {
    /// Buffer for the name list.
    #[cfg(any(target_os = "linux", target_os = "android", target_vendor = "apple"))]
    list: Vec<u8>,
    /// Buffer for one value.
    #[cfg(any(target_os = "linux", target_os = "android", target_vendor = "apple"))]
    value: Vec<u8>,
}

/// Read the node `name` in `dir`. A node with no `FsKind` gives `None`.
/// With `expect` set, the kind and the identity of the node must be equal
/// to it. With `bufs` set, read the xattrs of a node that is not a
/// directory.
fn read_child(
    dir: &Dir,
    name: &CStr,
    expect: Option<(FsKind, FileId)>,
    bufs: Option<&mut XattrBufs>,
) -> io::Result<Option<Child>> {
    let st = statat(dir, name, AtFlags::SYMLINK_NOFOLLOW)?;
    if let Some((kind, id)) = expect {
        check_id(&st, kind, id)?;
    }
    let Some(mut meta) = meta_from_stat(&st)? else {
        return Ok(None);
    };
    let utf8_name = name
        .to_str()
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "name is not valid UTF-8"))?;
    let link = if meta.kind == FsKind::Symlink {
        Some(read_link(dir, name)?)
    } else {
        None
    };
    if let Some(bufs) = bufs {
        if meta.kind != FsKind::Directory {
            meta.xattrs = read_xattrs_at_with(dir, name, meta.kind, meta.id, bufs)?;
        }
    }
    Ok(Some(Child {
        name: utf8_name.into(),
        meta,
        link,
    }))
}

/// Read the target of the symlink `name` in `dir`.
fn read_link(dir: &Dir, name: &CStr) -> io::Result<Box<str>> {
    let not_utf8 = || {
        io::Error::new(
            io::ErrorKind::InvalidData,
            "symlink target is not valid UTF-8",
        )
    };
    // `EINVAL`: the node is no longer a symlink.
    let link_error = |err: Errno| {
        if err == Errno::INVAL {
            file_changed()
        } else {
            err.into()
        }
    };
    let mut buf = [MaybeUninit::<u8>::uninit(); LINK_BUF_LEN];
    let (target, _) = readlinkat_raw(dir, name, &mut buf).map_err(link_error)?;
    if target.len() < LINK_BUF_LEN {
        return std::str::from_utf8(target)
            .map(Box::from)
            .map_err(|_| not_utf8());
    }
    // A full buffer can hold a truncated target.
    readlinkat(dir, name, Vec::new())
        .map_err(link_error)?
        .into_string()
        .map(String::into_boxed_str)
        .map_err(|_| not_utf8())
}

/// Read the children of `dir`, without `.` and `..`, in directory order.
/// Skip nodes with no `FsKind`. `fs_path` is the path of `dir` for error
/// messages.
pub(crate) fn read_dir(dir: &Dir, fs_path: &Path, xattrs: bool) -> io::Result<Vec<Child>> {
    let mut bufs = XattrBufs::default();
    let mut bufs = xattrs.then_some(&mut bufs);
    let mut children = Vec::new();
    let mut add = |name: &CStr| -> io::Result<()> {
        let raw = name.to_bytes();
        if raw == b"." || raw == b".." {
            return Ok(());
        }
        let child = read_child(dir, name, None, bufs.as_deref_mut())
            .map_err(|err| path_error(err, &fs_path.join(OsStr::from_bytes(raw))))?;
        children.extend(child);
        Ok(())
    };
    let dir_error = |err: Errno| path_error(err.into(), fs_path);
    #[cfg(any(target_os = "linux", target_os = "android"))]
    {
        // `getdents64` reads from the current offset of `dir`.
        seek(dir, SeekFrom::Start(0)).map_err(dir_error)?;
        let mut buf = [MaybeUninit::<u8>::uninit(); 8192];
        let mut reader = RawDir::new(dir, &mut buf);
        while let Some(entry) = reader.next() {
            add(entry.map_err(dir_error)?.file_name())?;
        }
    }
    #[cfg(not(any(target_os = "linux", target_os = "android")))]
    for entry in DirReader::read_from(dir).map_err(dir_error)? {
        add(entry.map_err(dir_error)?.file_name())?;
    }
    Ok(children)
}

/// Add `path` to the message of an error.
fn path_error(err: io::Error, path: &Path) -> io::Error {
    with_path(err, &path.to_string_lossy())
}

/// Open the root of a source walk.
pub(crate) fn open_root(root: &Path, xattrs: bool) -> io::Result<Root> {
    open_root_inner(root, xattrs).map_err(|err| path_error(err, root))
}

fn open_root_inner(root: &Path, xattrs: bool) -> io::Result<Root> {
    let st = statat(CWD, root, AtFlags::SYMLINK_NOFOLLOW)?;
    let id = file_id(&st);
    let unsupported = || io::Error::new(io::ErrorKind::InvalidInput, "unsupported file type");
    if FileType::from_raw_mode(st.st_mode as _) == FileType::Directory {
        let dir = openat(CWD, root, DIR_FLAGS, Mode::empty())?;
        let st = fstat(&dir)?;
        if file_id(&st) != id {
            return Err(file_changed());
        }
        let Some(mut meta) = meta_from_stat(&st)? else {
            return Err(file_changed());
        };
        if xattrs {
            meta.xattrs = read_xattrs_dir(&dir)?;
        }
        return Ok(Root::Dir { dir, meta });
    }
    let kind = kind_of(&st).ok_or_else(unsupported)?;
    let name = root
        .file_name()
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "path has no file name"))?;
    let name = CString::new(name.as_bytes())?;
    let parent_path = match root.parent() {
        Some(parent) if !parent.as_os_str().is_empty() => parent,
        _ => Path::new("."),
    };
    // The parent is used only as the directory of `*at` calls. `O_PATH`
    // needs only search permission on it.
    #[cfg(any(target_os = "linux", target_os = "android"))]
    let flags = OFlags::PATH | OFlags::DIRECTORY | OFlags::CLOEXEC;
    #[cfg(not(any(target_os = "linux", target_os = "android")))]
    let flags = OFlags::RDONLY | OFlags::DIRECTORY | OFlags::CLOEXEC;
    let parent = openat(CWD, parent_path, flags, Mode::empty())?;
    let mut bufs = XattrBufs::default();
    let child = read_child(
        &parent,
        &name,
        Some((kind, id)),
        xattrs.then_some(&mut bufs),
    )?
    .ok_or_else(unsupported)?;
    Ok(Root::Other { parent, child })
}

/// Open the directory `name` in `parent`. The identity must be `expect`.
pub(crate) fn open_dir(parent: &Dir, name: &str, expect: FileId) -> io::Result<Dir> {
    let dir = openat(parent, name, DIR_FLAGS, Mode::empty())?;
    check_id(&fstat(&dir)?, FsKind::Directory, expect)?;
    Ok(dir)
}

/// Open the regular file `name` in `parent` for reading. The identity must
/// be `expect` and the size must be `size`.
pub(crate) fn open_file(
    parent: &Dir,
    name: &str,
    expect: FileId,
    size: u64,
) -> io::Result<std::fs::File> {
    let (fd, nonblock) = open_nonblock(parent, name)?;
    let st = fstat(&fd)?;
    check_id(&st, FsKind::File, expect)?;
    if st.st_size as u64 != size {
        return Err(file_changed());
    }
    if nonblock {
        // `F_SETFL` changes only the file status flags. `O_NONBLOCK` is the
        // only one of them in `FILE_FLAGS`.
        fcntl_setfl(&fd, OFlags::empty())?;
    }
    Ok(std::fs::File::from(fd))
}

/// Open the node `name` in `parent` with `FILE_FLAGS`. Give the descriptor,
/// and `true` if it has `O_NONBLOCK`.
///
/// An open with `O_NONBLOCK` of a file with a lease (for example, a Samba
/// oplock) gives `EAGAIN`. Then open the file again without `O_NONBLOCK`.
/// That open waits until the lease holder releases the lease. A FIFO opened
/// with `O_RDONLY | O_NONBLOCK` never gives `EAGAIN`, thus the second open
/// occurs only for a node that is not a FIFO.
fn open_nonblock<P: Arg + Copy>(parent: &Dir, name: P) -> io::Result<(OwnedFd, bool)> {
    match openat(parent, name, FILE_FLAGS, Mode::empty()) {
        Ok(fd) => Ok((fd, true)),
        Err(Errno::AGAIN) => {
            let fd = openat(parent, name, FILE_FLAGS - OFlags::NONBLOCK, Mode::empty())?;
            Ok((fd, false))
        }
        Err(err) => Err(err.into()),
    }
}

/// Read the xattrs of an open directory.
#[cfg(any(target_os = "linux", target_os = "android", target_vendor = "apple"))]
pub(crate) fn read_xattrs_dir(dir: &Dir) -> io::Result<AttrList> {
    read_fd_xattrs(dir.as_fd(), &mut XattrBufs::default())
}

/// Read the xattrs of an open directory. This target has no xattrs.
#[cfg(not(any(target_os = "linux", target_os = "android", target_vendor = "apple")))]
pub(crate) fn read_xattrs_dir(_dir: &Dir) -> io::Result<AttrList> {
    Ok(AttrList::new())
}

/// Read the xattrs of the node `name` in `parent`. The kind must be `kind`
/// and the identity must be `expect`. A kind that cannot be opened safely
/// gives an empty list.
#[cfg_attr(not(test), expect(dead_code))]
pub(crate) fn read_xattrs_at(
    parent: &Dir,
    name: &str,
    kind: FsKind,
    expect: FileId,
) -> io::Result<AttrList> {
    read_xattrs_at_with(parent, name, kind, expect, &mut XattrBufs::default())
}

/// Read the xattrs of the node `name` in `parent` with the buffers `bufs`.
/// See `read_xattrs_at`.
#[cfg(any(target_os = "linux", target_os = "android", target_vendor = "apple"))]
fn read_xattrs_at_with<P: Arg + Copy>(
    parent: &Dir,
    name: P,
    kind: FsKind,
    expect: FileId,
    bufs: &mut XattrBufs,
) -> io::Result<AttrList> {
    let fd = match open_for_xattrs(parent, name, kind) {
        Ok(Some(fd)) => fd,
        Ok(None) => return Ok(AttrList::new()),
        // A FIFO has no body. An empty list lets the walk keep a FIFO that it
        // cannot open.
        Err(err) if kind == FsKind::Fifo && Errno::from_io_error(&err) == Some(Errno::ACCESS) => {
            return Ok(AttrList::new())
        }
        Err(err) => return Err(err),
    };
    check_id(&fstat(&fd)?, kind, expect)?;
    read_fd_xattrs(fd.as_fd(), bufs)
}

/// Open the node `name` in `parent` to read or write its xattrs. `kind` is
/// the expected kind of the node. The caller checks the kind. A kind that
/// cannot be opened safely gives `None`.
#[cfg(any(target_os = "linux", target_os = "android", target_vendor = "apple"))]
fn open_for_xattrs<P: Arg + Copy>(
    parent: &Dir,
    name: P,
    kind: FsKind,
) -> io::Result<Option<OwnedFd>> {
    Ok(Some(match kind {
        FsKind::File => open_nonblock(parent, name)?.0,
        FsKind::Fifo => openat(parent, name, FILE_FLAGS, Mode::empty())?,
        FsKind::Directory => openat(parent, name, DIR_FLAGS, Mode::empty())?,
        // `O_SYMLINK` opens the symlink itself. If the node is no longer a
        // symlink, `O_NONBLOCK` and `O_NOCTTY` make the open safe for a FIFO
        // or a terminal.
        #[cfg(target_vendor = "apple")]
        FsKind::Symlink => openat(
            parent,
            name,
            OFlags::RDONLY | OFlags::SYMLINK | OFlags::NONBLOCK | OFlags::NOCTTY | OFlags::CLOEXEC,
            Mode::empty(),
        )?,
        // Linux: an `O_PATH` descriptor gives `EBADF` for `flistxattr`, and
        // `user.*` is not allowed on symlinks. The open of a device node can
        // have side effects.
        _ => return Ok(None),
    }))
}

/// Read the xattrs of the node `name` in `parent`. This target has no xattrs.
#[cfg(not(any(target_os = "linux", target_os = "android", target_vendor = "apple")))]
fn read_xattrs_at_with<P: Arg + Copy>(
    _parent: &Dir,
    _name: P,
    _kind: FsKind,
    _expect: FileId,
    _bufs: &mut XattrBufs,
) -> io::Result<AttrList> {
    Ok(AttrList::new())
}

/// Error of `fgetxattr` for a name that does not exist.
#[cfg(any(target_os = "linux", target_os = "android"))]
const NO_ATTR: Errno = Errno::NODATA;
/// Error of `fgetxattr` for a name that does not exist.
#[cfg(target_vendor = "apple")]
const NO_ATTR: Errno = Errno::NOATTR;

/// Read all xattrs of `fd` with the buffers `bufs`, sorted by name. A
/// filesystem without xattr support gives an empty list.
#[cfg(any(target_os = "linux", target_os = "android", target_vendor = "apple"))]
fn read_fd_xattrs(fd: BorrowedFd<'_>, bufs: &mut XattrBufs) -> io::Result<AttrList> {
    let XattrBufs { list, value } = bufs;
    let len = match read_sized(list, |buf| flistxattr(fd, buf)) {
        Ok(0) | Err(Errno::NOTSUP) => return Ok(AttrList::new()),
        Ok(len) => len,
        Err(err) => return Err(err.into()),
    };
    let list = &list[..len];
    let mut attrs = Vec::with_capacity(list.iter().filter(|&&b| b == 0).count());
    for name in list.split(|&b| b == 0).filter(|name| !name.is_empty()) {
        let name = std::str::from_utf8(name).map_err(|_| {
            io::Error::new(io::ErrorKind::InvalidData, "xattr name is not valid UTF-8")
        })?;
        match read_sized(value, |buf| fgetxattr(fd, name, buf)) {
            Ok(len) => attrs.push((Box::<str>::from(name), Box::<[u8]>::from(&value[..len]))),
            // The attribute was removed after the list was read.
            Err(NO_ATTR) => {}
            Err(err) => {
                let err = io::Error::from(err);
                return Err(io::Error::new(err.kind(), format!("xattr {name}: {err}")));
            }
        }
    }
    // The order of `str` is the order of the UTF-8 bytes.
    attrs.sort_unstable_by(|a, b| a.0.cmp(&b.0));
    Ok(AttrList::from(attrs))
}

/// Call `read` with `buf` and give the length of the result. An empty `buf`
/// grows to 256 bytes first. On `ERANGE`, get the size with an empty buffer,
/// grow `buf` and call again. `buf` keeps its length for the next call.
#[cfg(any(target_os = "linux", target_os = "android", target_vendor = "apple"))]
fn read_sized(
    buf: &mut Vec<u8>,
    mut read: impl FnMut(&mut [u8]) -> rustix::io::Result<usize>,
) -> rustix::io::Result<usize> {
    if buf.is_empty() {
        buf.resize(256, 0);
    }
    loop {
        match read(buf) {
            Err(Errno::RANGE) => {
                let need = read(&mut [])?;
                let len = need.max(buf.len() * 2);
                buf.resize(len, 0);
            }
            // Apple: `fgetxattr` of `com.apple.ResourceFork` fills the buffer
            // and gives no `ERANGE` if the value is larger. Get the size with
            // an empty buffer, and read again if it is larger.
            #[cfg(target_vendor = "apple")]
            Ok(len) if len == buf.len() => {
                let need = read(&mut [])?;
                if need <= len {
                    return Ok(len);
                }
                buf.resize(need, 0);
            }
            result => return result,
        }
    }
}

/// Open the destination directory of the sink. `path` can be a symlink to a
/// directory.
pub(crate) fn open_dest(path: &Path) -> io::Result<Dir> {
    let flags = OFlags::RDONLY | OFlags::DIRECTORY | OFlags::CLOEXEC;
    openat(CWD, path, flags, Mode::empty()).map_err(|err| path_error(err.into(), path))
}

/// Open the directory `name` in `parent`. With `create` set, create a
/// missing directory with mode 0o755. The new directory keeps the setgid bit
/// that it gets from `parent`.
pub(crate) fn open_child_dir(parent: &Dir, name: &str, create: bool) -> io::Result<Dir> {
    match openat(parent, name, DIR_FLAGS, Mode::empty()) {
        Err(Errno::NOENT) if create => {}
        result => return Ok(result?),
    }
    let mode = Mode::from_raw_mode(0o755);
    let created = match mkdirat(parent, name, mode) {
        Ok(()) => true,
        // Another process created the name after the first open.
        Err(Errno::EXIST) => false,
        Err(err) => return Err(err.into()),
    };
    let dir = openat(parent, name, DIR_FLAGS, Mode::empty())?;
    if created {
        set_new_dir_mode(&dir, mode)?;
    }
    Ok(dir)
}

/// Set the mode of the new directory `dir` to `mode`, because `mkdirat`
/// applies the umask. Keep the setgid bit of `dir`.
fn set_new_dir_mode(dir: &Dir, mode: Mode) -> io::Result<()> {
    let setgid = Mode::from_raw_mode(fstat(dir)?.st_mode as _) & Mode::SGID;
    Ok(fchmod(dir, mode | setgid)?)
}

/// Give the kind of the node `name` in `parent`. A symlink is `Other`.
pub(crate) fn lookup(parent: &Dir, name: &str) -> io::Result<Existing> {
    match statat(parent, name, AtFlags::SYMLINK_NOFOLLOW) {
        Ok(st) if kind_of(&st) == Some(FsKind::Directory) => Ok(Existing::Directory),
        Ok(_) => Ok(Existing::Other),
        Err(Errno::NOENT) => Ok(Existing::Missing),
        Err(err) => Err(err.into()),
    }
}

/// Remove the node `name` in `parent`. The node must not be a directory.
pub(crate) fn remove(parent: &Dir, name: &str) -> io::Result<()> {
    Ok(unlinkat(parent, name, AtFlags::empty())?)
}

/// Create the directory `name` in `parent` with mode 0o700. The directory
/// keeps the setgid bit that it gets from `parent`.
pub(crate) fn create_dir(parent: &Dir, name: &str) -> io::Result<()> {
    let mode = Mode::from_raw_mode(0o700);
    mkdirat(parent, name, mode)?;
    let dir = openat(parent, name, DIR_FLAGS, Mode::empty())?;
    set_new_dir_mode(&dir, mode)
}

/// Create the regular file `name` in `parent` with mode 0o600 and open it
/// for writing. An existing name gives `AlreadyExists`.
pub(crate) fn create_file(parent: &Dir, name: &str) -> io::Result<std::fs::File> {
    let flags = OFlags::WRONLY | OFlags::CREATE | OFlags::EXCL | OFlags::NOFOLLOW | OFlags::CLOEXEC;
    let fd = openat(parent, name, flags, Mode::from_raw_mode(0o600))?;
    Ok(std::fs::File::from(fd))
}

/// Create the symlink `name` in `parent` with the target `target`.
pub(crate) fn create_symlink(parent: &Dir, name: &str, target: &str) -> io::Result<()> {
    Ok(symlinkat(target, parent, name)?)
}

/// Create the name `name` in `parent` for the node `from_name` in `from`.
pub(crate) fn create_link(from: &Dir, from_name: &str, parent: &Dir, name: &str) -> io::Result<()> {
    Ok(linkat(from, from_name, parent, name, AtFlags::empty())?)
}

/// Create the FIFO or device `name` in `parent` with mode 0o600. A device
/// needs `device`, the `(major, minor)` numbers. Other kinds give
/// `InvalidInput`.
#[cfg(not(target_vendor = "apple"))]
pub(crate) fn create_special(
    parent: &Dir,
    name: &str,
    kind: FsKind,
    device: Option<(u32, u32)>,
) -> io::Result<()> {
    let (file_type, dev) = match (kind, device) {
        (FsKind::Fifo, _) => (FileType::Fifo, 0),
        (FsKind::CharDevice, Some((major, minor))) => {
            (FileType::CharacterDevice, makedev(major, minor))
        }
        (FsKind::BlockDevice, Some((major, minor))) => {
            (FileType::BlockDevice, makedev(major, minor))
        }
        _ => {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "not a FIFO or a device with device numbers",
            ))
        }
    };
    let mode = Mode::from_raw_mode(0o600);
    Ok(mknodat(parent, name, file_type, mode, dev)?)
}

/// Create the FIFO or device `name` in `parent` with mode 0o600. A device
/// needs `device`, the `(major, minor)` numbers. A major number above 0xff
/// or a minor number above 0xffffff gives `InvalidInput`. The numbers
/// (0xff, 0xffffff) also give `InvalidInput`. A FIFO uses `mkfifoat`. A
/// device uses `mknodat`, which needs root privileges. Both functions need
/// macOS 13 or later. Other kinds give `InvalidInput`.
#[cfg(target_vendor = "apple")]
pub(crate) fn create_special(
    parent: &Dir,
    name: &str,
    kind: FsKind,
    device: Option<(u32, u32)>,
) -> io::Result<()> {
    let makedev = |(major, minor): (u32, u32)| {
        // `makedev(0xff, 0xffffff)` gives -1. -1 is `NODEV`, the value for
        // "no device".
        if major > 0xff || minor > 0xff_ffff || (major, minor) == (0xff, 0xff_ffff) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "device numbers out of range",
            ));
        }
        Ok(libc::makedev(major as i32, minor as i32))
    };
    let (file_type, dev) = match (kind, device) {
        (FsKind::Fifo, _) => (libc::S_IFIFO, 0),
        (FsKind::CharDevice, Some(device)) => (libc::S_IFCHR, makedev(device)?),
        (FsKind::BlockDevice, Some(device)) => (libc::S_IFBLK, makedev(device)?),
        _ => {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "not a FIFO or a device with device numbers",
            ))
        }
    };
    // A NUL byte in `name` gives `EINVAL`.
    name.into_with_c_str(|name| {
        // SAFETY: `parent` is an open descriptor and `name` is a
        // NUL-terminated string. Both outlive the call.
        let ret = unsafe {
            if file_type == libc::S_IFIFO {
                libc::mkfifoat(parent.as_raw_fd(), name.as_ptr(), 0o600)
            } else {
                libc::mknodat(parent.as_raw_fd(), name.as_ptr(), file_type | 0o600, dev)
            }
        };
        Ok(if ret == 0 {
            Ok(())
        } else {
            Err(io::Error::last_os_error())
        })
    })?
}

/// Set the metadata of an open regular file.
pub(crate) fn set_meta_file(file: &std::fs::File, meta: &SetMeta) -> io::Result<()> {
    set_meta_fd(file.as_fd(), meta)
}

/// Set the metadata of an open directory.
pub(crate) fn set_meta_dir(dir: &Dir, meta: &SetMeta) -> io::Result<()> {
    set_meta_fd(dir.as_fd(), meta)
}

/// Set the metadata of an open node. The xattrs come before the mode, thus a
/// read-only mode does not stop the xattr writes.
fn set_meta_fd(fd: BorrowedFd<'_>, meta: &SetMeta) -> io::Result<()> {
    if let Some(owner) = meta.owner {
        let (uid, gid) = owner_ids(owner);
        fchown(fd, uid, gid)?;
    }
    set_fd_xattrs(fd, &meta.xattrs, meta.xattr_policy)?;
    if let Some(mode) = meta.mode {
        fchmod(fd, Mode::from_raw_mode(mode as _))?;
    }
    futimens(fd, &timestamps(meta.mtime)?)?;
    Ok(())
}

/// Set the metadata of the symlink, FIFO or device `name` in `parent`. The
/// node must have the kind `kind`. Other kinds give `InvalidInput`. The mode
/// of a symlink does not change.
pub(crate) fn set_meta_at(
    parent: &Dir,
    name: &str,
    kind: FsKind,
    meta: &SetMeta,
) -> io::Result<()> {
    if matches!(kind, FsKind::File | FsKind::Directory) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "not a symlink, a FIFO or a device",
        ));
    }
    let times = timestamps(meta.mtime)?;
    utimensat(parent, name, &times, AtFlags::SYMLINK_NOFOLLOW)?;
    if let Some(owner) = meta.owner {
        let (uid, gid) = owner_ids(owner);
        chownat(parent, name, uid, gid, AtFlags::SYMLINK_NOFOLLOW)?;
    }
    set_xattrs_at(parent, name, kind, meta)?;
    if let Some(mode) = meta.mode {
        if kind != FsKind::Symlink {
            // Linux does not support `SYMLINK_NOFOLLOW` here on all kernels.
            // The node is not a symlink: the sink created it immediately
            // before, in a directory that it opened without following links.
            let mode = Mode::from_raw_mode(mode as _);
            chmodat(parent, name, mode, AtFlags::empty())?;
        }
    }
    Ok(())
}

/// Give the `chown` arguments for `(uid, gid)`.
fn owner_ids((uid, gid): (u32, u32)) -> (Option<Uid>, Option<Gid>) {
    // `from_raw` rejects `u32::MAX` in debug builds. For `chown`, `u32::MAX`
    // means "no change".
    (
        Some(Uid::from_raw_unchecked(uid)),
        Some(Gid::from_raw_unchecked(gid)),
    )
}

/// Give the timestamps that set the mtime to `mtime` and do not change the
/// atime. A time out of range gives `InvalidInput`.
fn timestamps(mtime: SystemTime) -> io::Result<Timestamps> {
    let out_of_range = || {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "modification time out of range",
        )
    };
    let (sec, nsec) = match mtime.duration_since(UNIX_EPOCH) {
        Ok(after) => (i64::try_from(after.as_secs()), after.subsec_nanos()),
        Err(err) => {
            // `tv_nsec` is not negative: round `tv_sec` down.
            let before = err.duration();
            let sec = i64::try_from(before.as_secs()).map(|sec| -sec);
            match before.subsec_nanos() {
                0 => (sec, 0),
                nsec => (sec.map(|sec| sec - 1), 1_000_000_000 - nsec),
            }
        }
    };
    Ok(Timestamps {
        last_access: Timespec {
            tv_sec: 0,
            tv_nsec: UTIME_OMIT,
        },
        last_modification: Timespec {
            tv_sec: sec.map_err(|_| out_of_range())?,
            tv_nsec: nsec as _,
        },
    })
}

/// Write the xattrs of `meta` on the node `name` in `parent`. The node must
/// have the kind `kind`. A different kind gives `InvalidData`.
#[cfg(any(target_os = "linux", target_os = "android", target_vendor = "apple"))]
fn set_xattrs_at(parent: &Dir, name: &str, kind: FsKind, meta: &SetMeta) -> io::Result<()> {
    if meta.xattrs.is_empty() {
        return Ok(());
    }
    let Some(fd) = open_for_xattrs(parent, name, kind)? else {
        return no_xattrs(&meta.xattrs, meta.xattr_policy);
    };
    if kind_of(&fstat(&fd)?) != Some(kind) {
        return Err(file_changed());
    }
    set_fd_xattrs(fd.as_fd(), &meta.xattrs, meta.xattr_policy)
}

/// Write the xattrs of `meta` on a node. This target has no xattrs.
#[cfg(not(any(target_os = "linux", target_os = "android", target_vendor = "apple")))]
fn set_xattrs_at(_parent: &Dir, _name: &str, _kind: FsKind, meta: &SetMeta) -> io::Result<()> {
    no_xattrs(&meta.xattrs, meta.xattr_policy)
}

/// Write `attrs` on `fd`. `EPERM` and `ENOTSUP` apply `policy`. With
/// `UnsupportedXattr::Error`, `ENOTSUP` gives `Unsupported`.
#[cfg(any(target_os = "linux", target_os = "android", target_vendor = "apple"))]
fn set_fd_xattrs(fd: BorrowedFd<'_>, attrs: &AttrList, policy: UnsupportedXattr) -> io::Result<()> {
    for (name, value) in attrs.iter() {
        match fsetxattr(fd, name, value, XattrFlags::empty()) {
            Ok(()) => {}
            Err(Errno::PERM | Errno::NOTSUP) if policy == UnsupportedXattr::Skip => {}
            Err(errno) => {
                let err = io::Error::from(errno);
                // On Apple platforms, std gives `Uncategorized` for `ENOTSUP`.
                let kind = match errno {
                    Errno::NOTSUP => io::ErrorKind::Unsupported,
                    _ => err.kind(),
                };
                return Err(io::Error::new(kind, format!("xattr {name}: {err}")));
            }
        }
    }
    Ok(())
}

/// Write `attrs` on `fd`. This target has no xattrs.
#[cfg(not(any(target_os = "linux", target_os = "android", target_vendor = "apple")))]
fn set_fd_xattrs(
    _fd: BorrowedFd<'_>,
    attrs: &AttrList,
    policy: UnsupportedXattr,
) -> io::Result<()> {
    no_xattrs(attrs, policy)
}

/// Apply `policy` to `attrs` for a node that cannot have xattrs. A list that
/// is not empty gives `Unsupported` with `UnsupportedXattr::Error`.
fn no_xattrs(attrs: &AttrList, policy: UnsupportedXattr) -> io::Result<()> {
    match attrs.iter().next() {
        Some((name, _)) if policy == UnsupportedXattr::Error => Err(io::Error::new(
            io::ErrorKind::Unsupported,
            format!("xattr {name}: extended attributes are not supported on this file type"),
        )),
        _ => Ok(()),
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::testutil::TempDir,
        rustix::fs::fcntl_getfl,
        static_assertions::assert_impl_all,
        std::{
            io::{ErrorKind, Read, Write},
            os::unix::fs::MetadataExt,
        },
    };

    assert_impl_all!(Dir: Send, Sync);
    assert_impl_all!(FileId: Copy, Eq, std::hash::Hash, Send);

    fn open_dir_root(path: &Path) -> Dir {
        match open_root(path, false).unwrap() {
            Root::Dir { dir, .. } => dir,
            Root::Other { .. } => panic!("root is not a directory"),
        }
    }

    fn id_of(path: &Path) -> FileId {
        let md = std::fs::symlink_metadata(path).unwrap();
        (md.dev(), md.ino())
    }

    fn find<'a>(children: &'a [Child], name: &str) -> &'a Child {
        children
            .iter()
            .find(|child| &*child.name == name)
            .unwrap_or_else(|| panic!("no child {name}"))
    }

    /// Give the result of the creation of a node with a name that is not
    /// UTF-8. Give `None` if the filesystem does not allow such names (Apple
    /// APFS gives `EILSEQ`).
    fn non_utf8_node<T>(result: io::Result<T>) -> Option<T> {
        match result {
            Ok(node) => Some(node),
            Err(err) if err.raw_os_error() == Some(Errno::ILSEQ.raw_os_error()) => {
                eprintln!("skip: the filesystem does not allow names that are not UTF-8");
                None
            }
            Err(err) => panic!("create a node with a name that is not UTF-8: {err}"),
        }
    }

    #[test]
    fn read_dir_skips_socket() {
        let tmp = TempDir::new();
        std::fs::write(tmp.path().join("f"), b"").unwrap();
        let _sock = std::os::unix::net::UnixListener::bind(tmp.path().join("s")).unwrap();
        // A node with no kind is skipped before its name is checked.
        let _bad = non_utf8_node(std::os::unix::net::UnixListener::bind(
            tmp.path().join(OsStr::from_bytes(b"\xff")),
        ));
        let children = read_dir(&open_dir_root(tmp.path()), tmp.path(), false).unwrap();
        assert_eq!(children.len(), 1);
        assert_eq!(&*children[0].name, "f");
    }

    #[test]
    fn read_dir_gives_symlink_target() {
        let tmp = TempDir::new();
        std::os::unix::fs::symlink("some/target", tmp.path().join("l")).unwrap();
        let children = read_dir(&open_dir_root(tmp.path()), tmp.path(), false).unwrap();
        let child = find(&children, "l");
        assert_eq!(child.meta.kind, FsKind::Symlink);
        assert_eq!(child.meta.size, 0);
        assert_eq!(child.link.as_deref(), Some("some/target"));
    }

    #[test]
    fn read_dir_rejects_non_utf8_name() {
        let tmp = TempDir::new();
        let path = tmp.path().join(OsStr::from_bytes(b"\xff"));
        if non_utf8_node(std::fs::write(path, b"")).is_none() {
            return;
        }
        let err = read_dir(&open_dir_root(tmp.path()), tmp.path(), false).unwrap_err();
        assert_eq!(err.kind(), ErrorKind::InvalidData);
        let msg = err.to_string();
        assert!(msg.contains(&*tmp.path().to_string_lossy()), "{msg}");
    }

    #[test]
    fn open_dir_does_not_follow_symlink() {
        let tmp = TempDir::new();
        std::fs::create_dir(tmp.path().join("d")).unwrap();
        std::os::unix::fs::symlink("d", tmp.path().join("l")).unwrap();
        let dir = open_dir_root(tmp.path());
        let err = open_dir(&dir, "l", id_of(&tmp.path().join("d"))).unwrap_err();
        assert_ne!(err.kind(), ErrorKind::InvalidData);
    }

    #[test]
    fn open_dir_checks_id() {
        let tmp = TempDir::new();
        std::fs::create_dir(tmp.path().join("a")).unwrap();
        std::fs::create_dir(tmp.path().join("b")).unwrap();
        let dir = open_dir_root(tmp.path());
        open_dir(&dir, "a", id_of(&tmp.path().join("a"))).unwrap();
        let err = open_dir(&dir, "a", id_of(&tmp.path().join("b"))).unwrap_err();
        assert_eq!(err.kind(), ErrorKind::InvalidData);
        assert_eq!(err.to_string(), "file changed");
    }

    #[test]
    fn open_file_checks_size() {
        let tmp = TempDir::new();
        let path = tmp.path().join("f");
        std::fs::write(&path, b"abc").unwrap();
        let id = id_of(&path);
        let dir = open_dir_root(tmp.path());
        open_file(&dir, "f", id, 3).unwrap();
        std::fs::OpenOptions::new()
            .append(true)
            .open(&path)
            .unwrap()
            .write_all(b"d")
            .unwrap();
        let err = open_file(&dir, "f", id, 3).unwrap_err();
        assert_eq!(err.kind(), ErrorKind::InvalidData);
        assert_eq!(err.to_string(), "file changed");
    }

    #[test]
    fn open_file_does_not_follow_symlink() {
        let tmp = TempDir::new();
        std::fs::write(tmp.path().join("t"), b"abc").unwrap();
        std::os::unix::fs::symlink("t", tmp.path().join("l")).unwrap();
        let dir = open_dir_root(tmp.path());
        let err = open_file(&dir, "l", id_of(&tmp.path().join("t")), 3).unwrap_err();
        assert_ne!(err.kind(), ErrorKind::InvalidData);
    }

    #[cfg(not(target_vendor = "apple"))]
    #[test]
    fn open_file_checks_kind() {
        let tmp = TempDir::new();
        let dir = open_dir_root(tmp.path());
        rustix::fs::mkfifoat(&dir, "p", Mode::RUSR | Mode::WUSR).unwrap();
        let err = open_file(&dir, "p", id_of(&tmp.path().join("p")), 0).unwrap_err();
        assert_eq!(err.kind(), ErrorKind::InvalidData);
        assert_eq!(err.to_string(), "file changed");
    }

    #[test]
    fn open_file_is_blocking() {
        let tmp = TempDir::new();
        std::fs::write(tmp.path().join("f"), b"abc").unwrap();
        let dir = open_dir_root(tmp.path());
        let file = open_file(&dir, "f", id_of(&tmp.path().join("f")), 3).unwrap();
        assert!(!fcntl_getfl(&file).unwrap().contains(OFlags::NONBLOCK));
    }

    #[test]
    fn open_root_gives_variant() {
        let tmp = TempDir::new();
        std::fs::write(tmp.path().join("f"), b"abc").unwrap();
        std::os::unix::fs::symlink("f", tmp.path().join("l")).unwrap();

        match open_root(tmp.path(), false).unwrap() {
            Root::Dir { meta, .. } => assert_eq!(meta.kind, FsKind::Directory),
            Root::Other { .. } => panic!("root is not a directory"),
        }

        let Root::Other { parent, child } = open_root(&tmp.path().join("f"), false).unwrap() else {
            panic!("root is a directory");
        };
        assert_eq!(&*child.name, "f");
        assert_eq!(child.meta.kind, FsKind::File);
        let mut data = String::new();
        open_file(&parent, &child.name, child.meta.id, child.meta.size)
            .unwrap()
            .read_to_string(&mut data)
            .unwrap();
        assert_eq!(data, "abc");

        let Root::Other { child, .. } = open_root(&tmp.path().join("l"), false).unwrap() else {
            panic!("root is a directory");
        };
        assert_eq!(&*child.name, "l");
        assert_eq!(child.meta.kind, FsKind::Symlink);
        assert_eq!(child.link.as_deref(), Some("f"));
    }

    #[cfg(any(target_os = "linux", target_os = "android"))]
    #[test]
    fn open_root_parent_needs_only_search() {
        use std::{fs::Permissions, os::unix::fs::PermissionsExt};
        let tmp = TempDir::new();
        let parent = tmp.path().join("d");
        std::fs::create_dir(&parent).unwrap();
        let path = parent.join("f");
        std::fs::write(&path, b"abc").unwrap();
        std::fs::set_permissions(&parent, Permissions::from_mode(0o311)).unwrap();
        let result = open_root(&path, true).and_then(|root| match root {
            Root::Other { parent, child } => {
                let mut data = String::new();
                open_file(&parent, &child.name, child.meta.id, child.meta.size)?
                    .read_to_string(&mut data)?;
                Ok(data)
            }
            Root::Dir { .. } => Err(io::Error::other("root is a directory")),
        });
        std::fs::set_permissions(&parent, Permissions::from_mode(0o755)).unwrap();
        assert_eq!(result.unwrap(), "abc");
    }

    #[test]
    fn open_root_rejects_socket() {
        let tmp = TempDir::new();
        let path = tmp.path().join("s");
        let _sock = std::os::unix::net::UnixListener::bind(&path).unwrap();
        let err = open_root(&path, false).unwrap_err();
        assert_eq!(err.kind(), ErrorKind::InvalidInput);
        assert_eq!(
            err.to_string(),
            format!("{}: unsupported file type", path.display())
        );
    }

    #[test]
    fn open_root_missing_gives_path() {
        let tmp = TempDir::new();
        let path = tmp.path().join("missing");
        let err = open_root(&path, false).unwrap_err();
        assert_eq!(err.kind(), ErrorKind::NotFound);
        assert!(err.to_string().starts_with(&*path.to_string_lossy()));
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn open_root_dev_null() {
        let Root::Other { child, .. } = open_root(Path::new("/dev/null"), false).unwrap() else {
            panic!("root is a directory");
        };
        assert_eq!(&*child.name, "null");
        assert_eq!(child.meta.kind, FsKind::CharDevice);
        assert_eq!(child.meta.device, Some((1, 3)));
        assert_eq!(child.meta.size, 0);
    }

    #[test]
    fn meta_matches_std() {
        let tmp = TempDir::new();
        let path = tmp.path().join("f");
        std::fs::write(&path, b"hello").unwrap();
        std::fs::hard_link(&path, tmp.path().join("g")).unwrap();
        let children = read_dir(&open_dir_root(tmp.path()), tmp.path(), false).unwrap();
        let meta = &find(&children, "f").meta;
        let md = std::fs::symlink_metadata(&path).unwrap();
        assert_eq!(meta.kind, FsKind::File);
        assert_eq!(meta.size, md.size());
        assert_eq!(meta.mode, md.mode() & 0o7777);
        assert_eq!(meta.uid, md.uid());
        assert_eq!(meta.gid, md.gid());
        assert_eq!(meta.mtime, md.modified().unwrap());
        assert_eq!(meta.nlink, md.nlink());
        assert_eq!(meta.nlink, 2);
        assert_eq!(meta.id, (md.dev(), md.ino()));
        assert_eq!(meta.device, None);
        assert!(meta.xattrs.is_empty());
    }

    #[test]
    fn meta_mtime_before_epoch() {
        let tmp = TempDir::new();
        let path = tmp.path().join("f");
        let mtime = UNIX_EPOCH - Duration::from_millis(1500);
        std::fs::File::create(&path)
            .unwrap()
            .set_modified(mtime)
            .unwrap();
        let children = read_dir(&open_dir_root(tmp.path()), tmp.path(), false).unwrap();
        assert_eq!(find(&children, "f").meta.mtime, mtime);
    }

    #[cfg(not(target_vendor = "apple"))]
    #[test]
    fn read_xattrs_at_fifo_does_not_block() {
        let tmp = TempDir::new();
        let dir = open_dir_root(tmp.path());
        rustix::fs::mkfifoat(&dir, "p", Mode::RUSR | Mode::WUSR).unwrap();
        let id = id_of(&tmp.path().join("p"));
        let (tx, rx) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            let _ = tx.send(read_xattrs_at(&dir, "p", FsKind::Fifo, id));
        });
        rx.recv_timeout(std::time::Duration::from_secs(5))
            .expect("read_xattrs_at blocks on a FIFO")
            .unwrap();
    }

    #[cfg(not(target_vendor = "apple"))]
    #[test]
    fn read_dir_unreadable_fifo_has_no_xattrs() {
        let tmp = TempDir::new();
        let dir = open_dir_root(tmp.path());
        rustix::fs::mkfifoat(&dir, "p", Mode::empty()).unwrap();
        // A process that bypasses permission checks (for example, root) can
        // open the FIFO.
        if openat(&dir, "p", FILE_FLAGS, Mode::empty()).is_ok() {
            eprintln!("skip: permission checks are bypassed");
            return;
        }
        let children = read_dir(&dir, tmp.path(), true).unwrap();
        assert!(find(&children, "p").meta.xattrs.is_empty());
    }

    /// Give a `SetMeta` with no owner and no xattrs.
    fn set_meta(mode: Option<u32>, mtime: SystemTime) -> SetMeta {
        SetMeta {
            mode,
            owner: None,
            mtime,
            xattrs: AttrList::new(),
            xattr_policy: UnsupportedXattr::Error,
        }
    }

    fn mode_of(path: &Path) -> u32 {
        std::fs::symlink_metadata(path).unwrap().mode() & 0o7777
    }

    #[test]
    fn open_child_dir_creates_with_mode_0o755() {
        // The umask 0o022 does not change 0o755. Only a umask that removes
        // more bits, for example 0o077, tests the `fchmod`.
        let tmp = TempDir::new();
        let root = open_dest(tmp.path()).unwrap();
        open_child_dir(&root, "d", true).unwrap();
        assert_eq!(mode_of(&tmp.path().join("d")), 0o755);
    }

    #[test]
    fn open_child_dir_missing_gives_not_found() {
        let tmp = TempDir::new();
        let root = open_dest(tmp.path()).unwrap();
        let err = open_child_dir(&root, "d", false).unwrap_err();
        assert_eq!(err.kind(), ErrorKind::NotFound);
    }

    #[test]
    fn open_child_dir_does_not_follow_symlink() {
        let tmp = TempDir::new();
        std::fs::create_dir(tmp.path().join("d")).unwrap();
        std::os::unix::fs::symlink("d", tmp.path().join("l")).unwrap();
        std::os::unix::fs::symlink("missing", tmp.path().join("dangling")).unwrap();
        let root = open_dest(tmp.path()).unwrap();
        open_child_dir(&root, "l", true).unwrap_err();
        open_child_dir(&root, "dangling", true).unwrap_err();
        assert_eq!(std::fs::read_dir(tmp.path().join("d")).unwrap().count(), 0);
        assert!(std::fs::symlink_metadata(tmp.path().join("missing")).is_err());
    }

    #[test]
    fn open_child_dir_on_file_gives_enotdir() {
        let tmp = TempDir::new();
        std::fs::write(tmp.path().join("f"), b"").unwrap();
        let root = open_dest(tmp.path()).unwrap();
        let err = open_child_dir(&root, "f", true).unwrap_err();
        assert_eq!(err.raw_os_error(), Some(Errno::NOTDIR.raw_os_error()));
    }

    #[test]
    fn lookup_kinds() {
        let tmp = TempDir::new();
        std::fs::create_dir(tmp.path().join("d")).unwrap();
        std::fs::write(tmp.path().join("f"), b"").unwrap();
        std::os::unix::fs::symlink("d", tmp.path().join("l")).unwrap();
        let root = open_dest(tmp.path()).unwrap();
        assert_eq!(lookup(&root, "missing").unwrap(), Existing::Missing);
        assert_eq!(lookup(&root, "d").unwrap(), Existing::Directory);
        assert_eq!(lookup(&root, "f").unwrap(), Existing::Other);
        assert_eq!(lookup(&root, "l").unwrap(), Existing::Other);
    }

    #[test]
    fn remove_does_not_follow_symlink() {
        let tmp = TempDir::new();
        std::fs::write(tmp.path().join("t"), b"").unwrap();
        std::os::unix::fs::symlink("t", tmp.path().join("l")).unwrap();
        let root = open_dest(tmp.path()).unwrap();
        remove(&root, "l").unwrap();
        assert!(std::fs::symlink_metadata(tmp.path().join("l")).is_err());
        assert!(tmp.path().join("t").is_file());
    }

    #[test]
    fn create_dir_mode_0o700() {
        let tmp = TempDir::new();
        let root = open_dest(tmp.path()).unwrap();
        create_dir(&root, "d").unwrap();
        assert_eq!(mode_of(&tmp.path().join("d")), 0o700);
    }

    #[test]
    fn open_child_dir_and_create_dir_keep_setgid() {
        use std::{fs::Permissions, os::unix::fs::PermissionsExt};
        let tmp = TempDir::new();
        let parent = tmp.path().join("p");
        std::fs::create_dir(&parent).unwrap();
        std::fs::set_permissions(&parent, Permissions::from_mode(0o2775)).unwrap();
        if mode_of(&parent) != 0o2775 {
            eprintln!("skip: the process cannot set the setgid bit");
            return;
        }
        std::fs::create_dir(parent.join("probe")).unwrap();
        if mode_of(&parent.join("probe")) & 0o2000 == 0 {
            eprintln!("skip: a new directory does not get the setgid bit");
            return;
        }
        let dir = open_dest(&parent).unwrap();
        open_child_dir(&dir, "a", true).unwrap();
        create_dir(&dir, "b").unwrap();
        assert_eq!(mode_of(&parent.join("a")), 0o2755);
        assert_eq!(mode_of(&parent.join("b")), 0o2700);
    }

    #[test]
    fn create_file_existing_gives_already_exists() {
        let tmp = TempDir::new();
        std::fs::write(tmp.path().join("f"), b"").unwrap();
        let root = open_dest(tmp.path()).unwrap();
        let err = create_file(&root, "f").unwrap_err();
        assert_eq!(err.kind(), ErrorKind::AlreadyExists);
    }

    #[test]
    fn create_file_does_not_follow_dangling_symlink() {
        let tmp = TempDir::new();
        std::os::unix::fs::symlink("t", tmp.path().join("l")).unwrap();
        let root = open_dest(tmp.path()).unwrap();
        create_file(&root, "l").unwrap_err();
        assert!(std::fs::symlink_metadata(tmp.path().join("t")).is_err());
    }

    #[test]
    fn create_symlink_target() {
        let tmp = TempDir::new();
        let root = open_dest(tmp.path()).unwrap();
        create_symlink(&root, "l", "some/target").unwrap();
        let target = std::fs::read_link(tmp.path().join("l")).unwrap();
        assert_eq!(target, Path::new("some/target"));
    }

    #[test]
    fn create_link_shares_inode() {
        let tmp = TempDir::new();
        std::fs::write(tmp.path().join("f"), b"").unwrap();
        let root = open_dest(tmp.path()).unwrap();
        create_link(&root, "f", &root, "g").unwrap();
        assert_eq!(id_of(&tmp.path().join("f")), id_of(&tmp.path().join("g")));
    }

    #[test]
    fn create_special_fifo() {
        use std::os::unix::fs::FileTypeExt;
        let tmp = TempDir::new();
        let root = open_dest(tmp.path()).unwrap();
        create_special(&root, "p", FsKind::Fifo, None).unwrap();
        let md = std::fs::symlink_metadata(tmp.path().join("p")).unwrap();
        assert!(md.file_type().is_fifo());
    }

    #[test]
    fn create_special_other_kind_invalid_input() {
        let tmp = TempDir::new();
        let root = open_dest(tmp.path()).unwrap();
        let err = create_special(&root, "c", FsKind::CharDevice, None).unwrap_err();
        assert_eq!(err.kind(), ErrorKind::InvalidInput);
        let err = create_special(&root, "f", FsKind::File, None).unwrap_err();
        assert_eq!(err.kind(), ErrorKind::InvalidInput);
    }

    #[test]
    fn create_special_interior_nul_gives_einval() {
        let tmp = TempDir::new();
        let root = open_dest(tmp.path()).unwrap();
        let err = create_special(&root, "a\0b", FsKind::Fifo, None).unwrap_err();
        assert_eq!(err.raw_os_error(), Some(libc::EINVAL));
    }

    #[cfg(target_vendor = "apple")]
    #[test]
    fn create_special_device() {
        use std::os::unix::fs::FileTypeExt;
        let tmp = TempDir::new();
        let root = open_dest(tmp.path()).unwrap();
        // SAFETY: `geteuid` has no preconditions.
        let is_root = unsafe { libc::geteuid() } == 0;
        for (name, kind) in [("c", FsKind::CharDevice), ("b", FsKind::BlockDevice)] {
            let result = create_special(&root, name, kind, Some((3, 2)));
            if !is_root {
                assert_eq!(result.unwrap_err().kind(), ErrorKind::PermissionDenied);
                continue;
            }
            result.unwrap();
            let md = std::fs::symlink_metadata(tmp.path().join(name)).unwrap();
            let file_type = md.file_type();
            match kind {
                FsKind::CharDevice => assert!(file_type.is_char_device()),
                _ => assert!(file_type.is_block_device()),
            }
            assert_eq!((major(md.rdev() as _), minor(md.rdev() as _)), (3, 2));
        }
    }

    #[cfg(target_vendor = "apple")]
    #[test]
    fn create_special_device_out_of_range() {
        let tmp = TempDir::new();
        let root = open_dest(tmp.path()).unwrap();
        for device in [(0x100, 0), (0, 0x100_0000), (0xff, 0xff_ffff)] {
            let err = create_special(&root, "c", FsKind::CharDevice, Some(device)).unwrap_err();
            assert_eq!(err.kind(), ErrorKind::InvalidInput);
        }
    }

    #[test]
    fn set_meta_at_symlink_sets_mtime() {
        use std::{fs::Permissions, os::unix::fs::PermissionsExt};
        let tmp = TempDir::new();
        let target = tmp.path().join("t");
        std::fs::write(&target, b"").unwrap();
        std::fs::set_permissions(&target, Permissions::from_mode(0o644)).unwrap();
        let target_mtime = std::fs::metadata(&target).unwrap().modified().unwrap();
        std::os::unix::fs::symlink("t", tmp.path().join("l")).unwrap();
        let root = open_dest(tmp.path()).unwrap();
        let mtime = UNIX_EPOCH + Duration::new(1_000_000, 500);
        set_meta_at(&root, "l", FsKind::Symlink, &set_meta(Some(0o600), mtime)).unwrap();
        let md = std::fs::symlink_metadata(tmp.path().join("l")).unwrap();
        assert_eq!(md.modified().unwrap(), mtime);
        let md = std::fs::metadata(&target).unwrap();
        assert_eq!(md.modified().unwrap(), target_mtime);
        assert_eq!(md.mode() & 0o7777, 0o644);
    }

    #[test]
    fn set_meta_file_sets_mode_and_mtime() {
        let tmp = TempDir::new();
        let root = open_dest(tmp.path()).unwrap();
        // `create_file` gives mode 0o600. No umask gives 0o640 from it.
        let file = create_file(&root, "f").unwrap();
        let mtime = UNIX_EPOCH + Duration::new(1_000_000, 500);
        set_meta_file(&file, &set_meta(Some(0o640), mtime)).unwrap();
        let md = std::fs::symlink_metadata(tmp.path().join("f")).unwrap();
        assert_eq!(md.mode() & 0o7777, 0o640);
        assert_eq!(md.modified().unwrap(), mtime);
    }

    #[test]
    fn set_meta_dir_sets_mode_and_mtime() {
        let tmp = TempDir::new();
        let root = open_dest(tmp.path()).unwrap();
        create_dir(&root, "d").unwrap();
        let dir = open_child_dir(&root, "d", false).unwrap();
        let mtime = UNIX_EPOCH - Duration::from_millis(1500);
        // The directory stays empty, thus the removal of the temporary
        // directory does not need write permission on it.
        set_meta_dir(&dir, &set_meta(Some(0o555), mtime)).unwrap();
        let md = std::fs::symlink_metadata(tmp.path().join("d")).unwrap();
        assert_eq!(md.mode() & 0o7777, 0o555);
        assert_eq!(md.modified().unwrap(), mtime);
    }

    #[test]
    fn timestamps_before_epoch() {
        let times = timestamps(UNIX_EPOCH - Duration::from_millis(1500)).unwrap();
        assert_eq!(times.last_modification.tv_sec, -2);
        assert_eq!(times.last_modification.tv_nsec, 500_000_000);
        assert_eq!(times.last_access.tv_nsec, UTIME_OMIT);
    }

    #[cfg(any(target_os = "linux", target_os = "android", target_vendor = "apple"))]
    mod xattr {
        use {
            super::*,
            rustix::fs::{setxattr, XattrFlags},
        };

        /// Set an xattr. Give `false` if the filesystem does not support
        /// `user.*` xattrs.
        fn set_xattr(path: &Path, name: &str, value: &[u8]) -> bool {
            match setxattr(path, name, value, XattrFlags::empty()) {
                Ok(()) => true,
                Err(Errno::NOTSUP) => {
                    eprintln!("skip: no user xattrs in {}", path.display());
                    false
                }
                Err(err) => panic!("setxattr {name}: {err}"),
            }
        }

        /// Give the `user.*` attributes. Check that the full list is sorted.
        fn user_attrs(list: &AttrList) -> Vec<(&str, &[u8])> {
            let names: Vec<&str> = list.iter().map(|(name, _)| name).collect();
            assert!(names.windows(2).all(|w| w[0] < w[1]), "{names:?}");
            list.iter()
                .filter(|(name, _)| name.starts_with("user."))
                .collect()
        }

        #[test]
        fn read_xattrs_at_sorts_names() {
            let tmp = TempDir::new();
            let path = tmp.path().join("f");
            std::fs::write(&path, b"").unwrap();
            if !set_xattr(&path, "user.b", b"2") || !set_xattr(&path, "user.a", b"1") {
                return;
            }
            let dir = open_dir_root(tmp.path());
            let list = read_xattrs_at(&dir, "f", FsKind::File, id_of(&path)).unwrap();
            assert_eq!(
                user_attrs(&list),
                [("user.a", &b"1"[..]), ("user.b", &b"2"[..])]
            );
        }

        #[test]
        fn read_xattrs_at_keeps_values() {
            let tmp = TempDir::new();
            let path = tmp.path().join("f");
            std::fs::write(&path, b"").unwrap();
            let long: Vec<u8> = (0..2000).map(|i| i as u8).collect();
            if !set_xattr(&path, "user.bin", b"\x00\xff\x80\n")
                || !set_xattr(&path, "user.long", &long)
            {
                return;
            }
            let dir = open_dir_root(tmp.path());
            let list = read_xattrs_at(&dir, "f", FsKind::File, id_of(&path)).unwrap();
            assert_eq!(
                user_attrs(&list),
                [
                    ("user.bin", &b"\x00\xff\x80\n"[..]),
                    ("user.long", &long[..])
                ]
            );
        }

        #[test]
        fn read_xattrs_dir_reads_directory() {
            let tmp = TempDir::new();
            let path = tmp.path().join("d");
            std::fs::create_dir(&path).unwrap();
            if !set_xattr(&path, "user.a", b"1") {
                return;
            }
            let root = open_dir_root(tmp.path());
            let children = read_dir(&root, tmp.path(), true).unwrap();
            assert!(find(&children, "d").meta.xattrs.is_empty());
            let dir = open_dir(&root, "d", id_of(&path)).unwrap();
            let list = read_xattrs_dir(&dir).unwrap();
            assert_eq!(user_attrs(&list), [("user.a", &b"1"[..])]);
        }

        #[test]
        fn read_dir_reads_xattrs_only_when_asked() {
            let tmp = TempDir::new();
            let path = tmp.path().join("f");
            std::fs::write(&path, b"").unwrap();
            if !set_xattr(&path, "user.a", b"1") {
                return;
            }
            let dir = open_dir_root(tmp.path());
            let children = read_dir(&dir, tmp.path(), false).unwrap();
            assert!(find(&children, "f").meta.xattrs.is_empty());
            let children = read_dir(&dir, tmp.path(), true).unwrap();
            assert_eq!(
                user_attrs(&find(&children, "f").meta.xattrs),
                [("user.a", &b"1"[..])]
            );
        }

        #[cfg(target_os = "linux")]
        #[test]
        fn read_xattrs_at_symlink_is_empty() {
            let tmp = TempDir::new();
            let target = tmp.path().join("t");
            std::fs::write(&target, b"").unwrap();
            if !set_xattr(&target, "user.a", b"1") {
                return;
            }
            std::os::unix::fs::symlink("t", tmp.path().join("l")).unwrap();
            let dir = open_dir_root(tmp.path());
            let id = id_of(&tmp.path().join("l"));
            let list = read_xattrs_at(&dir, "l", FsKind::Symlink, id).unwrap();
            assert!(list.is_empty());
        }

        #[cfg(target_os = "linux")]
        #[test]
        fn read_xattrs_at_dev_null_is_empty() {
            let Root::Other { parent, child } = open_root(Path::new("/dev/null"), true).unwrap()
            else {
                panic!("root is a directory");
            };
            assert!(child.meta.xattrs.is_empty());
            let list = read_xattrs_at(&parent, "null", FsKind::CharDevice, child.meta.id).unwrap();
            assert!(list.is_empty());
        }

        #[test]
        fn read_xattrs_at_checks_id() {
            let tmp = TempDir::new();
            let path = tmp.path().join("f");
            std::fs::write(&path, b"").unwrap();
            let id = id_of(&path);
            let new = tmp.path().join("f.new");
            std::fs::write(&new, b"").unwrap();
            std::fs::rename(&new, &path).unwrap();
            let dir = open_dir_root(tmp.path());
            let err = read_xattrs_at(&dir, "f", FsKind::File, id).unwrap_err();
            assert_eq!(err.kind(), ErrorKind::InvalidData);
            assert_eq!(err.to_string(), "file changed");
        }

        #[test]
        fn read_xattrs_at_checks_kind() {
            let tmp = TempDir::new();
            let path = tmp.path().join("f");
            std::fs::write(&path, b"").unwrap();
            let dir = open_dir_root(tmp.path());
            let err = read_xattrs_at(&dir, "f", FsKind::Fifo, id_of(&path)).unwrap_err();
            assert_eq!(err.kind(), ErrorKind::InvalidData);
            assert_eq!(err.to_string(), "file changed");
        }

        /// Give `true` if the filesystem of `dir` supports `user.*` xattrs.
        fn user_xattrs_supported(dir: &Path) -> bool {
            let probe = dir.join("probe");
            std::fs::write(&probe, b"").unwrap();
            let supported = set_xattr(&probe, "user.probe", b"");
            std::fs::remove_file(&probe).unwrap();
            supported
        }

        /// Give `true` if the process can set the xattr `name` on a new file
        /// in `dir`.
        #[cfg(target_os = "linux")]
        fn can_set_xattr(dir: &Path, name: &str, value: &[u8]) -> bool {
            let probe = dir.join("probe");
            std::fs::write(&probe, b"").unwrap();
            let allowed = match setxattr(&probe, name, value, XattrFlags::empty()) {
                Ok(()) => true,
                Err(Errno::PERM) => false,
                Err(err) => panic!("setxattr {name}: {err}"),
            };
            std::fs::remove_file(&probe).unwrap();
            allowed
        }

        fn set_meta_xattrs(
            mode: Option<u32>,
            xattrs: AttrList,
            xattr_policy: UnsupportedXattr,
        ) -> SetMeta {
            SetMeta {
                xattrs,
                xattr_policy,
                ..set_meta(mode, UNIX_EPOCH)
            }
        }

        #[test]
        fn set_meta_writes_user_xattrs() {
            let tmp = TempDir::new();
            if !user_xattrs_supported(tmp.path()) {
                return;
            }
            let root = open_dest(tmp.path()).unwrap();
            let attrs = AttrList::new().with("user.a", &b"1"[..]);

            let file = create_file(&root, "f").unwrap();
            let meta = set_meta_xattrs(Some(0o444), attrs.clone(), UnsupportedXattr::Error);
            set_meta_file(&file, &meta).unwrap();
            let id = id_of(&tmp.path().join("f"));
            let list = read_xattrs_at(&root, "f", FsKind::File, id).unwrap();
            assert_eq!(user_attrs(&list), [("user.a", &b"1"[..])]);

            // A process without the right to bypass permission checks cannot
            // write `user.*` xattrs on a directory with mode 0o555. The
            // directory stays empty for the removal of the temporary
            // directory.
            create_dir(&root, "d").unwrap();
            let dir = open_child_dir(&root, "d", false).unwrap();
            let meta = set_meta_xattrs(Some(0o555), attrs, UnsupportedXattr::Error);
            set_meta_dir(&dir, &meta).unwrap();
            let list = read_xattrs_dir(&dir).unwrap();
            assert_eq!(user_attrs(&list), [("user.a", &b"1"[..])]);
            assert_eq!(mode_of(&tmp.path().join("d")), 0o555);
        }

        #[cfg(target_os = "linux")]
        #[test]
        fn set_meta_file_trusted_xattr_policy() {
            let tmp = TempDir::new();
            if !user_xattrs_supported(tmp.path()) {
                return;
            }
            if can_set_xattr(tmp.path(), "trusted.x", b"1") {
                eprintln!("skip: the process can set trusted xattrs");
                return;
            }
            let root = open_dest(tmp.path()).unwrap();
            let file = create_file(&root, "f").unwrap();
            let attrs = AttrList::new()
                .with("trusted.x", &b"1"[..])
                .with("user.a", &b"1"[..]);

            let meta = set_meta_xattrs(None, attrs.clone(), UnsupportedXattr::Error);
            let err = set_meta_file(&file, &meta).unwrap_err();
            assert_eq!(err.kind(), ErrorKind::PermissionDenied);
            assert!(err.to_string().starts_with("xattr trusted.x: "), "{err}");

            let meta = set_meta_xattrs(None, attrs, UnsupportedXattr::Skip);
            set_meta_file(&file, &meta).unwrap();
            let id = id_of(&tmp.path().join("f"));
            let list = read_xattrs_at(&root, "f", FsKind::File, id).unwrap();
            assert_eq!(user_attrs(&list), [("user.a", &b"1"[..])]);
        }

        #[cfg(target_os = "linux")]
        #[test]
        fn set_meta_at_symlink_xattr_policy() {
            let tmp = TempDir::new();
            std::os::unix::fs::symlink("t", tmp.path().join("l")).unwrap();
            let root = open_dest(tmp.path()).unwrap();
            let attrs = AttrList::new().with("user.a", &b"1"[..]);

            let meta = set_meta_xattrs(None, attrs.clone(), UnsupportedXattr::Error);
            let err = set_meta_at(&root, "l", FsKind::Symlink, &meta).unwrap_err();
            assert_eq!(err.kind(), ErrorKind::Unsupported);
            assert!(err.to_string().starts_with("xattr user.a: "), "{err}");

            let meta = set_meta_xattrs(None, attrs, UnsupportedXattr::Skip);
            set_meta_at(&root, "l", FsKind::Symlink, &meta).unwrap();
        }

        #[cfg(target_os = "linux")]
        #[test]
        fn set_meta_file_keeps_capability_after_owner() {
            // VFS capability version 2: magic and flags, then the permitted
            // set with `CAP_NET_BIND_SERVICE`, then the inheritable set and
            // the upper 32 bits.
            let mut cap = [0u8; 20];
            cap[..8].copy_from_slice(&[0x01, 0x00, 0x00, 0x02, 0x00, 0x04, 0x00, 0x00]);
            let tmp = TempDir::new();
            if !can_set_xattr(tmp.path(), "security.capability", &cap) {
                eprintln!("skip: the process cannot set security.capability");
                return;
            }
            let root = open_dest(tmp.path()).unwrap();
            let file = create_file(&root, "f").unwrap();
            let md = file.metadata().unwrap();
            let meta = SetMeta {
                owner: Some((md.uid(), md.gid())),
                ..set_meta_xattrs(
                    Some(0o755),
                    AttrList::new().with("security.capability", &cap[..]),
                    UnsupportedXattr::Error,
                )
            };
            set_meta_file(&file, &meta).unwrap();
            let id = id_of(&tmp.path().join("f"));
            let list = read_xattrs_at(&root, "f", FsKind::File, id).unwrap();
            let value = list
                .iter()
                .find(|(name, _)| *name == "security.capability")
                .map(|(_, value)| value);
            assert_eq!(value, Some(&cap[..]));
        }
    }
}
