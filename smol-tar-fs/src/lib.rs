#![cfg(unix)]
#![deny(missing_docs)]
#![deny(rustdoc::broken_intra_doc_links)]
#![deny(rustdoc::invalid_rust_codeblocks)]

//! Filesystem source and sink for [`smol_tar`] archives.
//!
//! The crate gives a `Stream` of tar entries from a directory tree and a
//! `Sink` that writes a stream of tar entries to a directory. Use it with the
//! [`smol_tar`] reader and writer. The crate is available only on Unix. On other
//! platforms, the crate is empty. On Apple platforms, the crate needs
//! macOS 13, iOS 16, tvOS 16, or watchOS 9 or later.
//!
//! - [`FsSource`] is a stream of the nodes in a directory tree.
//!   [`FsStreamExt::into_tar`] converts the nodes to [`TarEntry`] values
//!   for [`TarWriter`].
//! - [`FsSink`] is a sink of [`TarEntry`] values, for example from
//!   [`TarReader`]. It writes each entry to a directory.
//!
//! The source and the sink run the filesystem calls on the blocking thread
//! pool of the runtime. The close of a file or a directory can occur on the
//! thread that drops it.
//!
//! # Feature flags
//!
//! - `smol` (default): use the `futures-io` traits `AsyncRead` and
//!   `AsyncWrite`, and the thread pool of the `blocking` crate. This feature
//!   enables the `smol` feature of `smol-tar`.
//! - `tokio`: use the `tokio::io` traits `AsyncRead` and `AsyncWrite`, and
//!   the blocking thread pool of Tokio. This feature enables the `tokio`
//!   feature of `smol-tar`.
//!
//! Enable `smol` or `tokio`. If you enable neither feature, the build fails.
//! If you enable both features, the crate uses `tokio`. Enable the same
//! runtime feature for `smol-tar` and `smol-tar-fs`. If `smol-tar` uses
//! `tokio` and `smol-tar-fs` uses `smol`, the build fails.
//!
//! [`TarEntry`]: smol_tar::TarEntry
//! [`TarReader`]: smol_tar::TarReader
//! [`TarWriter`]: smol_tar::TarWriter

#[cfg(not(any(feature = "smol", feature = "tokio")))]
compile_error!("either feature `smol` or `tokio` must be enabled");

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
    smol_tar::AttrList,
    std::{
        io,
        pin::Pin,
        sync::Arc,
        task::{Context, Poll},
        time::SystemTime,
    },
};

#[cfg(all(feature = "smol", not(feature = "tokio")))]
use futures_io::{AsyncRead, AsyncWrite};
#[cfg(feature = "tokio")]
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

#[cfg(all(feature = "smol", not(feature = "tokio")))]
fn poll_read_compat<R: AsyncRead + ?Sized>(
    reader: Pin<&mut R>,
    cx: &mut Context<'_>,
    buf: &mut [u8],
) -> Poll<io::Result<usize>> {
    reader.poll_read(cx, buf)
}

#[cfg(feature = "tokio")]
fn poll_read_compat<R: AsyncRead + ?Sized>(
    reader: Pin<&mut R>,
    cx: &mut Context<'_>,
    buf: &mut [u8],
) -> Poll<io::Result<usize>> {
    let mut read_buf = ReadBuf::new(buf);
    match reader.poll_read(cx, &mut read_buf) {
        Poll::Ready(Ok(())) => Poll::Ready(Ok(read_buf.filled().len())),
        Poll::Ready(Err(err)) => Poll::Ready(Err(err)),
        Poll::Pending => Poll::Pending,
    }
}

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
/// - The last entry at the target path is a device entry that the sink
///   skipped with [`DeviceNodes::Skip`].
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub enum MissingLinkTarget {
    /// Stop with a [`NotFound`](std::io::ErrorKind::NotFound) error. The
    /// error has the entry path and the link target.
    #[default]
    Error,
    /// Do not create the link, and continue.
    Skip,
}

/// Action of the sink for a character or block device entry.
///
/// A device node gives access to the device through the extracted tree.
/// The policy does not apply to FIFO entries. A device entry with a path
/// that is not valid gives an [`InvalidData`](std::io::ErrorKind::InvalidData)
/// error with each policy.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub enum DeviceNodes {
    /// Create the device node. Without privileges, the entry gives a
    /// [`PermissionDenied`](std::io::ErrorKind::PermissionDenied) error.
    Create,
    /// Do not create the node, and continue.
    #[default]
    Skip,
    /// Stop with a [`PermissionDenied`](std::io::ErrorKind::PermissionDenied)
    /// error. The error message starts with the entry path.
    Error,
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
///
/// The policy does not apply to an attribute that the filter of
/// [`FsSink::with_xattr_filter`] removes.
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
    /// Gives `true` for an attribute name that the sink writes.
    xattr_filter: XattrFilter,
    xattr_policy: UnsupportedXattr,
}

impl SetMeta {
    /// Give the xattrs that the filter accepts.
    fn filtered_xattrs(&self) -> impl Iterator<Item = (&str, &[u8])> {
        self.xattrs
            .iter()
            .filter(|(name, _)| (self.xattr_filter)(name))
    }
}

/// Filter of xattr names. It gives `true` for a name that the sink writes.
pub(crate) type XattrFilter = Arc<dyn Fn(&str) -> bool + Send + Sync>;

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
