//! Helpers for the filesystem tests.

use {
    futures::{Stream, StreamExt},
    std::{
        future::Future,
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
        let _ = std::fs::remove_dir_all(&self.path);
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
