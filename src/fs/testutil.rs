//! Helpers for unit tests.

use std::{
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
    time::{SystemTime, UNIX_EPOCH},
};

/// Temporary directory. Drop removes it with all its contents.
pub(crate) struct TempDir {
    path: PathBuf,
}

impl TempDir {
    /// Create a directory with a unique name in the system temporary
    /// directory.
    pub(crate) fn new() -> Self {
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let n = COUNTER.fetch_add(1, Ordering::Relaxed);
        let path =
            std::env::temp_dir().join(format!("smol-tar-{}-{nanos}-{n}", std::process::id()));
        std::fs::create_dir(&path).unwrap();
        Self { path }
    }

    /// Path of the directory.
    pub(crate) fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.path);
    }
}
