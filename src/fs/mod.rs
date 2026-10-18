//! Filesystem source and sink for tar archives.
//!
//! This module is available only on Unix, with the `fs` feature.

#[cfg_attr(not(test), expect(dead_code))]
mod rt;
#[cfg_attr(not(test), expect(dead_code))]
mod sys;
#[cfg(test)]
mod testutil;
#[cfg_attr(not(test), expect(dead_code))]
mod walk;

use {
    crate::AttrList,
    std::{io, time::SystemTime},
};

/// Kind of a filesystem node.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum FsKind {
    /// Regular file.
    File,
    /// Directory.
    Directory,
    /// Symbolic link.
    Symlink,
    /// Named pipe (FIFO).
    Fifo,
    /// Character device.
    CharDevice,
    /// Block device.
    BlockDevice,
}

/// Metadata of a filesystem node.
#[cfg_attr(not(test), expect(dead_code))]
#[derive(Debug)]
pub(crate) struct Meta {
    kind: FsKind,
    /// 0 for all kinds except `File`.
    size: u64,
    /// Permission and special bits (0o7777).
    mode: u32,
    uid: u32,
    gid: u32,
    mtime: SystemTime,
    /// File identity for hard links.
    id: sys::FileId,
    nlink: u64,
    /// `(major, minor)` for `CharDevice` and `BlockDevice`.
    device: Option<(u32, u32)>,
    /// Sorted by name. Empty if not read.
    xattrs: AttrList,
}

/// Node in a directory.
#[cfg_attr(not(test), expect(dead_code))]
#[derive(Debug)]
pub(crate) struct Child {
    /// UTF-8 name in its parent directory.
    name: Box<str>,
    meta: Meta,
    /// Symlink target.
    link: Option<Box<str>>,
}

/// Root of a source walk.
#[cfg_attr(not(test), expect(dead_code))]
#[derive(Debug)]
pub(crate) enum Root {
    /// The root is a directory.
    Dir { dir: sys::Dir, meta: Meta },
    /// The root is not a directory. `parent` is the directory that holds it.
    Other { parent: sys::Dir, child: Child },
}

/// Add a path to the message of an error. The error kind does not change.
pub(crate) fn with_path(err: io::Error, path: &str) -> io::Error {
    io::Error::new(err.kind(), format!("{path}: {err}"))
}

#[cfg(test)]
mod tests {
    use {super::*, std::io::ErrorKind};

    #[test]
    fn with_path_keeps_kind() {
        let err = with_path(io::Error::new(ErrorKind::NotFound, "gone"), "a/b");
        assert_eq!(err.kind(), ErrorKind::NotFound);
        assert_eq!(err.to_string(), "a/b: gone");
    }
}
