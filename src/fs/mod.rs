//! Filesystem source and sink for tar archives.
//!
//! This module is available only on Unix, with the `fs` feature.

mod file;
mod into_tar;
mod resolve;
mod rt;
mod sink;
mod source;
mod sys;
#[cfg(test)]
mod testutil;
mod walk;

pub use {
    file::FsFile,
    into_tar::{FsStreamExt, IntoTar},
    sink::FsSink,
    source::{FsEntry, FsSource},
};

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

/// Action of the sink for a hard link whose target the sink did not create.
///
/// The sink creates a hard link only to a path that it created before from
/// the same archive. The target is not available in these conditions:
///
/// - The archive has the link before its target.
/// - A filter removed the target entry.
/// - The target is a directory, or a later entry replaced it with a
///   directory.
/// - The target path starts with `/` or has a `..` component.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub enum MissingLinkTarget {
    /// Stop with a [`NotFound`](std::io::ErrorKind::NotFound) error. The
    /// error has the entry path and the link target.
    #[default]
    Error,
    /// Do not create the link, and continue.
    Skip,
}

/// Action of the sink when it cannot write an extended attribute.
///
/// The sink cannot write an attribute in these conditions:
///
/// - The system refuses the attribute (`EPERM` or `ENOTSUP`). For example,
///   `security.*` and `trusted.*` attributes need privileges, and some
///   filesystems do not support extended attributes.
/// - The platform cannot write extended attributes on the kind of the node.
///   For example, Linux does not write them on symbolic links and devices.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub enum UnsupportedXattr {
    /// Stop with an error. The error has the entry path and the attribute
    /// name.
    #[default]
    Error,
    /// Do not write the attribute, and continue.
    Skip,
}

/// Metadata of a filesystem node.
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
#[derive(Debug)]
pub(crate) struct Child {
    /// UTF-8 name in its parent directory.
    name: Box<str>,
    meta: Meta,
    /// Symlink target.
    link: Option<Box<str>>,
}

/// Metadata that the sink sets on a node.
pub(crate) struct SetMeta {
    /// `None`: do not change the mode.
    mode: Option<u32>,
    /// `(uid, gid)`. `None`: do not change the owner.
    owner: Option<(u32, u32)>,
    mtime: SystemTime,
    /// Empty: do not write xattrs.
    xattrs: AttrList,
    xattr_policy: UnsupportedXattr,
}

/// Root of a source walk.
#[derive(Debug)]
pub(crate) enum Root {
    /// The root is a directory.
    Dir { dir: sys::Dir, meta: Meta },
    /// The root is not a directory. `parent` is the directory that holds it.
    Other { parent: sys::Dir, child: Child },
}

/// Node at a name in the destination of the sink.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum Existing {
    /// No node has the name.
    Missing,
    /// A directory. A symlink to a directory is `Other`.
    Directory,
    /// A node of another kind.
    Other,
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
