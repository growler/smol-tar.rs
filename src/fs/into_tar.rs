//! Conversion of filesystem entries to tar entries, with hard link detection.

use {
    super::{sys, FsEntry, FsFile, FsKind},
    crate::{TarDevice, TarDirectory, TarEntry, TarFifo, TarLink, TarRegularFile, TarSymlink},
    futures_core::stream::{Stream, TryStream},
    pin_project_lite::pin_project,
    std::{
        collections::{hash_map::Entry, HashMap},
        io,
        pin::Pin,
        task::{ready, Context, Poll},
        time::{Duration, SystemTime, UNIX_EPOCH},
    },
};

/// Extension of a stream of [`FsEntry`] values.
pub trait FsStreamExt: TryStream<Ok = FsEntry, Error = io::Error> + Sized {
    /// Convert each entry to a [`TarEntry`]. See [`IntoTar`].
    fn into_tar(self) -> IntoTar<Self> {
        IntoTar {
            inner: self,
            links: HashMap::new(),
        }
    }
}

impl<S: TryStream<Ok = FsEntry, Error = io::Error>> FsStreamExt for S {}

pin_project! {
    /// Stream of tar entries from a stream of [`FsEntry`] values.
    ///
    /// Use [`FsStreamExt::into_tar`] to make it. Send the entries to a
    /// [`TarWriter`](crate::TarWriter). Each full entry has the archive path,
    /// the mode, the owner, the modification time and the extended attributes
    /// of the node. The body of a regular file is an [`FsFile`].
    ///
    /// # Hard links
    ///
    /// The stream finds the paths of each node that is not a directory and
    /// that has more than one link. The first of these paths gives the full
    /// entry. Each later path gives a [`TarLink`](crate::TarLink) to the
    /// first path. A `TarLink` has no metadata and no extended attributes.
    ///
    /// Filter the entries before `into_tar`. A filter after `into_tar` can
    /// drop the full entry and keep a link to it. Then the archive has a link
    /// to a path that is not in the archive.
    ///
    /// The stream keeps one row in memory for each node with more than one
    /// link. The row has the first path of the node. The stream removes the
    /// row when it gets the last link of the node. If a link of the node is
    /// outside the walked tree, or a filter drops it, the row stays until the
    /// stream is dropped. A row with a 70-byte path uses about 140 bytes, thus
    /// about 140 MB for each million such nodes. This occurs when each file of
    /// the tree has a hard link outside the walked root, for example in an
    /// OSTree deployment checkout or in an optimised Nix store subtree.
    ///
    /// # Modification time
    ///
    /// The entry keeps only the whole seconds of the modification time. A
    /// time before 1970 goes down to the previous whole second.
    /// [`FsEntry::mtime`] has the full precision. The tar writer adds a pax
    /// record for each time with a part smaller than one second. A time before
    /// 1970, or a time that does not fit in the 12-byte octal field of the
    /// header (2^33 seconds after 1970 or later), also gets a pax record.
    ///
    /// # Errors
    ///
    /// An error from the inner stream passes through without change.
    ///
    /// With the `tokio` feature, poll the stream and read the file bodies
    /// inside a Tokio runtime context.
    pub struct IntoTar<S> {
        #[pin]
        inner: S,
        // File identity to the first archive path and the number of links
        // that did not come yet.
        links: HashMap<sys::FileId, (Box<str>, u64)>,
    }
}

impl<S: TryStream<Ok = FsEntry, Error = io::Error>> Stream for IntoTar<S> {
    type Item = io::Result<TarEntry<'static, FsFile>>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.project();
        let entry = match ready!(this.inner.try_poll_next(cx)) {
            None => return Poll::Ready(None),
            Some(Err(err)) => return Poll::Ready(Some(Err(err))),
            Some(Ok(entry)) => entry,
        };
        if entry.meta.kind != FsKind::Directory && entry.meta.nlink > 1 {
            match this.links.entry(entry.meta.id) {
                Entry::Occupied(mut row) => {
                    let first = if row.get().1 <= 1 {
                        row.remove().0
                    } else {
                        let (first, remaining) = row.get_mut();
                        *remaining -= 1;
                        first.clone()
                    };
                    return Poll::Ready(Some(Ok(TarLink::new(entry.path, first).into())));
                }
                Entry::Vacant(row) => {
                    row.insert((entry.path.clone(), entry.meta.nlink - 1));
                }
            }
        }
        Poll::Ready(Some(Ok(tar_entry(entry))))
    }
}

/// Set the mode, the owner, the modification time and the extended
/// attributes of the `Meta` value `$meta` on the tar entry `$tar`. Give a
/// `TarEntry`.
macro_rules! with_meta {
    ($tar:expr, $meta:expr) => {{
        let tar = $tar;
        let meta = $meta;
        let tar = tar
            .with_mode(meta.mode)
            .with_uid(meta.uid)
            .with_gid(meta.gid)
            .with_mtime(whole_seconds(meta.mtime));
        if meta.xattrs.is_empty() {
            tar.into()
        } else {
            tar.with_attrs(meta.xattrs).into()
        }
    }};
}

/// Make the full tar entry of `entry`.
fn tar_entry(entry: FsEntry) -> TarEntry<'static, FsFile> {
    // The source gives a link for each symlink and device numbers for each
    // device, thus the defaults are not used.
    match entry.meta.kind {
        FsKind::File => {
            let FsEntry {
                path,
                meta,
                parent,
                name,
                ..
            } = entry;
            let body = FsFile::new(parent, name, meta.id, meta.size, path.clone());
            with_meta!(TarRegularFile::new(path, meta.size, body), meta)
        }
        FsKind::Directory => with_meta!(TarDirectory::new(entry.path), entry.meta),
        FsKind::Symlink => with_meta!(
            TarSymlink::new(entry.path, entry.link.unwrap_or_default()),
            entry.meta
        ),
        FsKind::Fifo => with_meta!(TarFifo::new(entry.path), entry.meta),
        FsKind::CharDevice => {
            let (major, minor) = entry.meta.device.unwrap_or_default();
            with_meta!(TarDevice::new_char(entry.path, major, minor), entry.meta)
        }
        FsKind::BlockDevice => {
            let (major, minor) = entry.meta.device.unwrap_or_default();
            with_meta!(TarDevice::new_block(entry.path, major, minor), entry.meta)
        }
    }
}

/// Remove the part of `time` that is smaller than one second. A time before
/// the epoch goes down to the previous whole second.
fn whole_seconds(time: SystemTime) -> SystemTime {
    let whole = match time.duration_since(UNIX_EPOCH) {
        Ok(after) => UNIX_EPOCH.checked_add(Duration::from_secs(after.as_secs())),
        Err(err) => {
            let before = err.duration();
            before
                .as_secs()
                .checked_add(u64::from(before.subsec_nanos() != 0))
                .and_then(|secs| UNIX_EPOCH.checked_sub(Duration::from_secs(secs)))
        }
    };
    // A whole second that the platform cannot hold keeps the time.
    whole.unwrap_or(time)
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::fs::{rt::block_on, testutil::TempDir, FsSource},
        futures::StreamExt,
        static_assertions::assert_impl_all,
    };

    assert_impl_all!(IntoTar<FsSource>: Send, Unpin, Stream);

    #[test]
    fn links_table_empty_after_last_link() {
        let tmp = TempDir::new();
        std::fs::write(tmp.path().join("a"), b"data").unwrap();
        std::fs::hard_link(tmp.path().join("a"), tmp.path().join("b")).unwrap();
        std::fs::hard_link(tmp.path().join("a"), tmp.path().join("c")).unwrap();
        let mut tar = FsSource::new(tmp.path()).into_tar();
        let mut kinds = Vec::new();
        let mut rows = Vec::new();
        block_on(async {
            while let Some(entry) = tar.next().await {
                kinds.push(matches!(entry.unwrap(), TarEntry::Link(_)));
                rows.push(tar.links.len());
            }
        });
        assert_eq!(kinds, [false, true, true]);
        assert_eq!(rows, [1, 1, 0]);
    }

    #[test]
    fn whole_seconds_floors() {
        let cases = [
            (
                UNIX_EPOCH + Duration::new(5, 500_000_000),
                UNIX_EPOCH + Duration::from_secs(5),
            ),
            (
                UNIX_EPOCH - Duration::new(1, 500_000_000),
                UNIX_EPOCH - Duration::from_secs(2),
            ),
            (
                UNIX_EPOCH + Duration::from_secs(7),
                UNIX_EPOCH + Duration::from_secs(7),
            ),
            (
                UNIX_EPOCH - Duration::from_secs(3),
                UNIX_EPOCH - Duration::from_secs(3),
            ),
        ];
        for (time, expected) in cases {
            assert_eq!(whole_seconds(time), expected, "{time:?}");
        }
    }
}
