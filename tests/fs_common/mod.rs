//! Helpers for the filesystem tests.

// Each test crate uses a part of the helpers.
#![allow(dead_code)]

use {
    futures::{Stream, StreamExt},
    std::{
        fs::{self, Permissions},
        future::Future,
        os::unix::fs::PermissionsExt,
        path::{Path, PathBuf},
        sync::atomic::{AtomicU64, Ordering},
        time::{SystemTime, UNIX_EPOCH},
    },
};

/// Temporary directory. Drop removes it with all its contents.
pub struct TempDir {
    path: PathBuf,
}

impl TempDir {
    /// Create a directory with a unique name in the system temporary
    /// directory.
    pub fn new() -> Self {
        static NEXT_ID: AtomicU64 = AtomicU64::new(0);
        let unique = format!(
            "smol-tar-fs-{}-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos(),
            NEXT_ID.fetch_add(1, Ordering::Relaxed)
        );
        let path = std::env::temp_dir().join(unique);
        std::fs::create_dir_all(&path).unwrap();
        Self { path }
    }

    /// Path of the directory.
    pub fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        if fs::remove_dir_all(&self.path).is_err() {
            make_writable(&self.path);
            let _ = fs::remove_dir_all(&self.path);
        }
    }
}

/// Give the owner all permissions on the directory `dir` and on each
/// directory in it. Do not follow symlinks. Ignore errors.
fn make_writable(dir: &Path) {
    let _ = fs::set_permissions(dir, Permissions::from_mode(0o700));
    let Ok(entries) = fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        if entry.file_type().is_ok_and(|kind| kind.is_dir()) {
            make_writable(&entry.path());
        }
    }
}

/// Run a future to completion on the selected runtime.
#[cfg(all(feature = "smol", not(feature = "tokio")))]
pub fn block_on<F: Future>(fut: F) -> F::Output {
    smol::block_on(fut)
}

/// Run a future to completion on the selected runtime.
#[cfg(feature = "tokio")]
pub fn block_on<F: Future>(fut: F) -> F::Output {
    tokio::runtime::Builder::new_current_thread()
        .build()
        .expect("build tokio runtime")
        .block_on(fut)
}

/// Give all items of a stream.
pub fn collect<S: Stream>(s: S) -> Vec<S::Item> {
    block_on(s.collect())
}
