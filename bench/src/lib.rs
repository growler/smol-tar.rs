//! Corpus generation and per-library adapters for the smol-tar benchmarks.
//!
//! Every adapter does the same work for its library:
//!
//! - `read_all` reads every entry and every file body into a caller buffer.
//! - `read_skip` reads every entry header and skips every file body.
//! - `write` encodes a list of [`Spec`] values into a caller `Vec<u8>`.
//! - `filter` copies every second entry of an archive into a caller `Vec<u8>`.
//! - `encode_dir` encodes a directory tree on disk into a caller writer.
//! - `extract` extracts an archive into a new directory on disk.

#[cfg(all(feature = "smol", feature = "tokio"))]
compile_error!("enable exactly one of `smol` or `tokio`, see bench/README.md");
#[cfg(not(any(feature = "smol", feature = "tokio")))]
compile_error!("enable exactly one of `smol` or `tokio`, see bench/README.md");

use std::{
    fmt, fs,
    future::Future,
    io,
    path::PathBuf,
    pin::Pin,
    sync::OnceLock,
    task::{Context, Poll},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

/// Size of the buffer that `read_all` adapters read file bodies into.
pub const BUF_SIZE: usize = 64 * 1024;

const MTIME: u64 = 1_700_000_000;
const LARGE_FILE_SIZE: usize = 64 << 20;
const TREE_LARGE_FILE_SIZE: usize = 16 << 20;
const LONG_NAME: &str = "nnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnn";

/// Entry and body byte counts from a `read_all` adapter.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Counts {
    /// Number of entries.
    pub entries: u64,
    /// Number of file body bytes.
    pub bytes: u64,
}

/// Kind of one archive entry.
#[derive(Clone, Debug)]
pub enum Kind {
    /// A directory.
    Dir,
    /// A regular file with this body size.
    File(usize),
    /// A symbolic link to this target.
    Symlink(String),
}

/// Description of one archive entry.
#[derive(Clone, Debug)]
pub struct Spec {
    /// Entry path.
    pub path: String,
    /// Entry kind.
    pub kind: Kind,
    /// Attach two extended attributes to the entry (PAX only).
    pub xattrs: bool,
}

/// `n` regular files of `size` bytes with short paths.
pub fn files(n: usize, size: usize) -> Vec<Spec> {
    (0..n)
        .map(|i| Spec {
            path: format!("d{:02}/f{i:05}.txt", i % 100),
            kind: Kind::File(size),
            xattrs: false,
        })
        .collect()
}

/// `n` regular files of 1 KiB with paths that do not fit a ustar header.
pub fn long_paths(n: usize, xattrs: bool) -> Vec<Spec> {
    (0..n)
        .map(|i| Spec {
            path: format!("long/{LONG_NAME}{i:05}.bin"),
            kind: Kind::File(1024),
            xattrs,
        })
        .collect()
}

/// A tree of `dirs` directories. Each directory holds 90 files of 0 to
/// 16 KiB and 10 symbolic links.
pub fn mixed(dirs: usize) -> Vec<Spec> {
    let mut state = 0x2545_f491_4f6c_dd1du64;
    let mut specs = Vec::with_capacity(dirs * 101);
    for d in 0..dirs {
        specs.push(Spec {
            path: format!("m/d{d:03}/"),
            kind: Kind::Dir,
            xattrs: false,
        });
        for f in 0..90 {
            state = state
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            specs.push(Spec {
                path: format!("m/d{d:03}/f{f:02}"),
                kind: Kind::File((state >> 33) as usize % (16 * 1024 + 1)),
                xattrs: false,
            });
        }
        for l in 0..10 {
            specs.push(Spec {
                path: format!("m/d{d:03}/l{l:02}"),
                kind: Kind::Symlink(format!("f{l:02}")),
                xattrs: false,
            });
        }
    }
    specs
}

/// Body bytes for a file of `size` bytes.
pub fn body(size: usize) -> &'static [u8] {
    static BODY: OnceLock<Vec<u8>> = OnceLock::new();
    &BODY.get_or_init(|| {
        (0..LARGE_FILE_SIZE as u32)
            .map(|i| (i.wrapping_mul(2654435761) >> 24) as u8)
            .collect()
    })[..size]
}

/// A ustar archive of `specs`, written by the `tar` crate.
pub fn ustar_archive(specs: &[Spec]) -> Vec<u8> {
    let mut out = Vec::new();
    adapters::tar::write_with(specs, &mut out, false).unwrap();
    out
}

/// A GNU archive of `specs` with GNU long-name headers, written by the `tar`
/// crate.
pub fn gnu_archive(specs: &[Spec]) -> Vec<u8> {
    let mut out = Vec::new();
    adapters::tar::write_with(specs, &mut out, true).unwrap();
    out
}

/// A PAX archive of `specs`, written by smol-tar.
pub fn pax_archive(specs: &[Spec]) -> Vec<u8> {
    let mut out = Vec::new();
    Runtime::new()
        .block_on(adapters::smol_tar::write(specs, &mut out))
        .unwrap();
    out
}

fn mtime() -> SystemTime {
    UNIX_EPOCH + Duration::from_secs(MTIME)
}

/// A benchmark corpus.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Corpus {
    /// 10,000 files of 1 KiB, ustar.
    Small,
    /// 4 files of 64 MiB, ustar.
    Large,
    /// 10,000 files of 1 KiB with long paths and two xattrs each, PAX.
    Pax,
    /// 10,000 files of 1 KiB with long paths, GNU long-name headers.
    GnuLong,
    /// 100 directories, 9,000 files of 0 to 16 KiB, 1,000 symlinks, ustar.
    Mixed,
}

/// The data of one corpus.
pub struct Data {
    /// The entries of the archive.
    pub specs: Vec<Spec>,
    /// The archive bytes.
    pub archive: Vec<u8>,
    /// Number of entries in the archive.
    pub entries: u64,
    /// Number of file body bytes in the archive.
    pub body_bytes: u64,
}

impl Corpus {
    /// All corpora.
    pub const ALL: [Corpus; 5] = [
        Corpus::Small,
        Corpus::Large,
        Corpus::Pax,
        Corpus::GnuLong,
        Corpus::Mixed,
    ];
    /// The corpora that every writer encodes as plain ustar.
    pub const USTAR: [Corpus; 3] = [Corpus::Small, Corpus::Large, Corpus::Mixed];
    /// The corpora used by the filter benchmarks.
    pub const FILTER: [Corpus; 2] = [Corpus::Small, Corpus::Mixed];

    /// The data of this corpus. The first call generates it.
    pub fn data(self) -> &'static Data {
        static DATA: [OnceLock<Data>; 5] = [const { OnceLock::new() }; 5];
        DATA[self as usize].get_or_init(|| {
            let specs = match self {
                Corpus::Small => files(10_000, 1024),
                Corpus::Large => files(4, LARGE_FILE_SIZE),
                Corpus::Pax => long_paths(10_000, true),
                Corpus::GnuLong => long_paths(10_000, false),
                Corpus::Mixed => mixed(100),
            };
            let archive = match self {
                Corpus::Pax => pax_archive(&specs),
                Corpus::GnuLong => gnu_archive(&specs),
                _ => ustar_archive(&specs),
            };
            let body_bytes = specs
                .iter()
                .map(|s| match s.kind {
                    Kind::File(size) => size as u64,
                    _ => 0,
                })
                .sum();
            Data {
                entries: specs.len() as u64,
                specs,
                archive,
                body_bytes,
            }
        })
    }
}

impl fmt::Display for Corpus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Corpus::Small => "small",
            Corpus::Large => "large",
            Corpus::Pax => "pax",
            Corpus::GnuLong => "gnu-long",
            Corpus::Mixed => "mixed",
        })
    }
}

/// A directory tree for the disk benchmarks.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Tree {
    /// 1 file of 16 MiB.
    Large,
    /// 1,024 files of 1 KiB in 32 directories.
    ManySmall,
    /// The entries of the mixed corpus.
    Mixed,
}

/// A tree on disk and a ustar archive of its entries.
pub struct Fixture {
    _dir: tempfile::TempDir,
    /// The root directory of the tree.
    pub root: PathBuf,
    /// The entries of the tree. The archive has the same entries.
    pub specs: Vec<Spec>,
    /// The archive of the entries, written by the `tar` crate.
    pub archive: Vec<u8>,
    /// Number of file body bytes in the tree.
    pub body_bytes: u64,
}

impl Tree {
    /// All trees.
    pub const ALL: [Tree; 3] = [Tree::Large, Tree::ManySmall, Tree::Mixed];

    /// The name of the root directory. Each entry path starts with this
    /// name.
    pub fn root(self) -> &'static str {
        match self {
            Tree::Large => "large",
            Tree::ManySmall => "many-small",
            Tree::Mixed => "m",
        }
    }

    /// The entries of the tree. The large and many-small trees have no
    /// directory entries.
    pub fn specs(self) -> Vec<Spec> {
        let file = |path, size| Spec {
            path,
            kind: Kind::File(size),
            xattrs: false,
        };
        match self {
            Tree::Large => vec![file("large/payload.bin".into(), TREE_LARGE_FILE_SIZE)],
            Tree::ManySmall => (0..1024)
                .map(|i| file(format!("many-small/d{:02}/f{i:04}.txt", i % 32), 1024))
                .collect(),
            Tree::Mixed => mixed(100),
        }
    }

    /// Make the tree in a new temporary directory, and make an archive of
    /// its entries.
    pub fn fixture(self) -> Fixture {
        let dir = tempfile::tempdir().unwrap();
        let specs = self.specs();
        let mut body_bytes = 0;
        for spec in &specs {
            let path = dir.path().join(&spec.path);
            match &spec.kind {
                Kind::Dir => fs::create_dir_all(&path).unwrap(),
                Kind::File(size) => {
                    fs::create_dir_all(path.parent().unwrap()).unwrap();
                    fs::write(&path, body(*size)).unwrap();
                    body_bytes += *size as u64;
                }
                Kind::Symlink(target) => std::os::unix::fs::symlink(target, &path).unwrap(),
            }
        }
        Fixture {
            root: dir.path().join(self.root()),
            archive: ustar_archive(&specs),
            specs,
            body_bytes,
            _dir: dir,
        }
    }
}

impl fmt::Display for Tree {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Tree::Large => "large-16m",
            Tree::ManySmall => "many-small",
            Tree::Mixed => "mixed",
        })
    }
}

/// A writer that counts the bytes and drops them.
#[derive(Debug, Default)]
pub struct CountingSink {
    /// Number of bytes written.
    pub bytes: u64,
}

impl io::Write for CountingSink {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.bytes += buf.len() as u64;
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

/// Implement the `AsyncWrite` trait of the selected runtime for `$ty` with
/// its `io::Write` implementation. Each call returns `Poll::Ready` and
/// never returns `Poll::Pending`.
macro_rules! async_write {
    ($ty:ty) => {
        #[cfg(feature = "smol")]
        impl futures::AsyncWrite for $ty {
            fn poll_write(
                mut self: Pin<&mut Self>,
                _: &mut Context<'_>,
                buf: &[u8],
            ) -> Poll<io::Result<usize>> {
                Poll::Ready(io::Write::write(&mut *self, buf))
            }

            fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
                Poll::Ready(Ok(()))
            }

            fn poll_close(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
                Poll::Ready(Ok(()))
            }
        }

        #[cfg(feature = "tokio")]
        impl tokio::io::AsyncWrite for $ty {
            fn poll_write(
                mut self: Pin<&mut Self>,
                _: &mut Context<'_>,
                buf: &[u8],
            ) -> Poll<io::Result<usize>> {
                Poll::Ready(io::Write::write(&mut *self, buf))
            }

            fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
                Poll::Ready(Ok(()))
            }

            fn poll_shutdown(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
                Poll::Ready(Ok(()))
            }
        }
    };
}

async_write!(CountingSink);

/// A writer that counts the bytes and drops them. It takes
/// [`SlowSink::NS_PER_KIB`] nanoseconds for each KiB, as a consumer that
/// uses the CPU for each byte. It busy-waits on the thread that writes.
#[derive(Debug, Default)]
pub struct SlowSink {
    /// Number of bytes written.
    pub bytes: u64,
}

impl SlowSink {
    /// Time in nanoseconds for each KiB: 100 ns, thus approximately 1.6 ms
    /// for 16 MiB.
    pub const NS_PER_KIB: u64 = 100;
}

impl io::Write for SlowSink {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let ns = Self::NS_PER_KIB * buf.len() as u64 / 1024;
        let end = Instant::now() + Duration::from_nanos(ns);
        while Instant::now() < end {
            std::hint::spin_loop();
        }
        self.bytes += buf.len() as u64;
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

async_write!(SlowSink);

/// The async runtime of the selected feature.
#[cfg(feature = "smol")]
pub struct Runtime;

#[cfg(feature = "smol")]
impl Runtime {
    /// Create a runtime.
    pub fn new() -> Self {
        Runtime
    }

    /// Run `fut` to completion on the current thread.
    ///
    /// The archives are in memory, so this uses the executor without the
    /// I/O reactor. `smol::block_on` can start the reactor thread, and its
    /// allocations then show in the measurements.
    pub fn block_on<F: Future>(&self, fut: F) -> F::Output {
        smol::future::block_on(fut)
    }
}

/// The async runtime of the selected feature.
#[cfg(feature = "tokio")]
pub struct Runtime(tokio::runtime::Runtime);

#[cfg(feature = "tokio")]
impl Runtime {
    /// Create a runtime.
    pub fn new() -> Self {
        Runtime(
            tokio::runtime::Builder::new_current_thread()
                .build()
                .unwrap(),
        )
    }

    /// Run `fut` to completion on the current thread.
    pub fn block_on<F: Future>(&self, fut: F) -> F::Output {
        self.0.block_on(fut)
    }
}

impl Default for Runtime {
    fn default() -> Self {
        Self::new()
    }
}

/// One module per library.
pub mod adapters {
    /// smol-tar with the selected runtime.
    pub mod smol_tar {
        #[cfg(feature = "smol")]
        use futures::{AsyncReadExt, AsyncWrite};
        #[cfg(feature = "tokio")]
        use tokio::io::{AsyncReadExt, AsyncWrite};
        use {
            crate::{body, mtime, Counts, Kind, Spec},
            ::smol_tar::{
                AttrList, TarDirectory, TarEntry, TarReader, TarRegularFile, TarSymlink, TarWriter,
            },
            futures::{future, StreamExt, TryStreamExt},
            smol_tar_fs::{FsSink, FsSource, FsStreamExt, ModePolicy},
            std::{fs, io::Result, path::Path},
        };

        /// Read every entry and every file body.
        pub async fn read_all(data: &[u8], buf: &mut [u8]) -> Result<Counts> {
            let mut tar = TarReader::new(data);
            let mut counts = Counts::default();
            while let Some(entry) = tar.next().await {
                counts.entries += 1;
                if let TarEntry::File(mut file) = entry? {
                    loop {
                        let n = file.read(buf).await?;
                        if n == 0 {
                            break;
                        }
                        counts.bytes += n as u64;
                    }
                }
            }
            Ok(counts)
        }

        /// Read every entry and drop every file body unread.
        pub async fn read_skip(data: &[u8]) -> Result<u64> {
            let mut tar = TarReader::new(data);
            let mut entries = 0;
            while let Some(entry) = tar.next().await {
                entry?;
                entries += 1;
            }
            Ok(entries)
        }

        /// Write `specs` into `out`.
        pub async fn write(specs: &[Spec], out: &mut Vec<u8>) -> Result<()> {
            let mut tar = TarWriter::new(out);
            for spec in specs {
                tar.write(entry(spec)).await?;
            }
            tar.finish().await
        }

        fn entry(spec: &Spec) -> TarEntry<'static, &'static [u8]> {
            match &spec.kind {
                Kind::Dir => TarDirectory::new(spec.path.as_str())
                    .with_mode(0o755)
                    .with_mtime(mtime())
                    .into(),
                Kind::File(size) => {
                    let file = TarRegularFile::new(spec.path.as_str(), *size as u64, body(*size))
                        .with_mode(0o644)
                        .with_mtime(mtime());
                    if spec.xattrs {
                        let mut attrs = AttrList::new();
                        attrs.push("user.a", &[b'a'; 16][..]);
                        attrs.push("user.b", &[b'b'; 32][..]);
                        file.with_attrs(attrs).into()
                    } else {
                        file.into()
                    }
                }
                Kind::Symlink(target) => TarSymlink::new(spec.path.as_str(), target.as_str())
                    .with_mtime(mtime())
                    .into(),
            }
        }

        /// Copy every second entry of `data` into `out`.
        pub async fn filter(data: &[u8], out: &mut Vec<u8>) -> Result<()> {
            let mut n = 0u64;
            let mut tar = TarWriter::new(out);
            TarReader::new(data)
                .try_filter(|_| {
                    n += 1;
                    future::ready(n % 2 == 0)
                })
                .forward(&mut tar)
                .await
        }

        /// Encode the tree at `root` into `out`. Each entry path starts
        /// with `prefix`.
        pub async fn encode_dir<W: AsyncWrite + Unpin>(
            root: &Path,
            prefix: &str,
            out: W,
        ) -> Result<()> {
            let mut tar = TarWriter::new(out);
            FsSource::new(root)
                .with_prefix(prefix)
                .into_tar()
                .forward(&mut tar)
                .await
        }

        /// Extract `data` into the new directory `dest`. With `minimal`,
        /// do not set the mode or the modification time.
        pub async fn extract(data: &[u8], dest: &Path, minimal: bool) -> Result<()> {
            // FsSink writes into an existing directory.
            fs::create_dir(dest)?;
            let sink = FsSink::new(dest);
            let sink = if minimal {
                sink.with_mode(ModePolicy::Skip).with_mtime(false)
            } else {
                sink
            };
            sink.extract(TarReader::new(data)).await
        }
    }

    /// The synchronous `tar` crate.
    pub mod tar {
        use {
            crate::{body, Counts, Kind, Spec, MTIME},
            ::tar::{Archive, Builder, EntryType, Header},
            std::{
                io::{Read, Result, Write},
                path::Path,
            },
        };

        /// Read every entry and every file body.
        pub fn read_all(data: &[u8], buf: &mut [u8]) -> Result<Counts> {
            let mut counts = Counts::default();
            for entry in Archive::new(data).entries()? {
                let mut entry = entry?;
                counts.entries += 1;
                if entry.header().entry_type().is_file() {
                    loop {
                        let n = entry.read(buf)?;
                        if n == 0 {
                            break;
                        }
                        counts.bytes += n as u64;
                    }
                }
            }
            Ok(counts)
        }

        /// Read every entry and drop every file body unread.
        pub fn read_skip(data: &[u8]) -> Result<u64> {
            let mut entries = 0;
            for entry in Archive::new(data).entries()? {
                entry?;
                entries += 1;
            }
            Ok(entries)
        }

        /// Write `specs` into `out` with ustar headers.
        pub fn write(specs: &[Spec], out: &mut Vec<u8>) -> Result<()> {
            write_with(specs, out, false)
        }

        pub(crate) fn write_with(specs: &[Spec], out: &mut Vec<u8>, gnu: bool) -> Result<()> {
            let mut tar = Builder::new(out);
            for spec in specs {
                let mut header = if gnu {
                    Header::new_gnu()
                } else {
                    Header::new_ustar()
                };
                header.set_mtime(MTIME);
                let data = match &spec.kind {
                    Kind::Dir => {
                        header.set_entry_type(EntryType::Directory);
                        header.set_mode(0o755);
                        header.set_size(0);
                        &[][..]
                    }
                    Kind::File(size) => {
                        header.set_entry_type(EntryType::Regular);
                        header.set_mode(0o644);
                        header.set_size(*size as u64);
                        body(*size)
                    }
                    Kind::Symlink(target) => {
                        header.set_entry_type(EntryType::Symlink);
                        header.set_mode(0o777);
                        header.set_size(0);
                        header.set_link_name(target)?;
                        &[][..]
                    }
                };
                tar.append_data(&mut header, &spec.path, data)?;
            }
            tar.finish()
        }

        /// Copy every second entry of `data` into `out`.
        pub fn filter(data: &[u8], out: &mut Vec<u8>) -> Result<()> {
            let mut tar = Builder::new(out);
            for (i, entry) in Archive::new(data).entries()?.enumerate() {
                let mut entry = entry?;
                if (i + 1) % 2 == 0 {
                    let header = entry.header().clone();
                    tar.append(&header, &mut entry)?;
                }
            }
            tar.finish()
        }

        /// Encode the tree at `root` into `out`. Each entry path starts
        /// with `prefix`.
        pub fn encode_dir<W: Write>(root: &Path, prefix: &str, out: W) -> Result<()> {
            let mut tar = Builder::new(out);
            tar.follow_symlinks(false);
            tar.append_dir_all(prefix, root)?;
            tar.finish()
        }

        /// Extract `data` into the new directory `dest`. With `minimal`,
        /// do not set the modification time.
        pub fn extract(data: &[u8], dest: &Path, minimal: bool) -> Result<()> {
            let mut archive = Archive::new(data);
            archive.set_preserve_mtime(!minimal);
            archive.unpack(dest)
        }
    }

    /// `async-tar` with the selected runtime.
    pub mod async_tar {
        #[cfg(feature = "smol")]
        use futures::{AsyncReadExt, AsyncWrite};
        #[cfg(feature = "tokio")]
        use tokio::io::{AsyncReadExt, AsyncWrite};
        use {
            crate::{body, Counts, Kind, Spec, MTIME},
            ::async_tar::{Archive, ArchiveBuilder, Builder, EntryType, Header},
            futures::StreamExt,
            std::{io::Result, path::Path},
        };

        /// Read every entry and every file body.
        pub async fn read_all(data: &[u8], buf: &mut [u8]) -> Result<Counts> {
            let mut entries = Archive::new(data).entries()?;
            let mut counts = Counts::default();
            while let Some(entry) = entries.next().await {
                let mut entry = entry?;
                counts.entries += 1;
                if entry.header().entry_type().is_file() {
                    loop {
                        let n = entry.read(buf).await?;
                        if n == 0 {
                            break;
                        }
                        counts.bytes += n as u64;
                    }
                }
            }
            Ok(counts)
        }

        /// Read every entry and drop every file body unread.
        pub async fn read_skip(data: &[u8]) -> Result<u64> {
            let mut entries = Archive::new(data).entries()?;
            let mut n = 0;
            while let Some(entry) = entries.next().await {
                entry?;
                n += 1;
            }
            Ok(n)
        }

        /// Write `specs` into `out` with ustar headers.
        pub async fn write(specs: &[Spec], out: &mut Vec<u8>) -> Result<()> {
            let mut tar = Builder::new(out);
            for spec in specs {
                let mut header = Header::new_ustar();
                header.set_mtime(MTIME);
                let data = match &spec.kind {
                    Kind::Dir => {
                        header.set_entry_type(EntryType::Directory);
                        header.set_mode(0o755);
                        header.set_size(0);
                        &[][..]
                    }
                    Kind::File(size) => {
                        header.set_entry_type(EntryType::Regular);
                        header.set_mode(0o644);
                        header.set_size(*size as u64);
                        body(*size)
                    }
                    Kind::Symlink(target) => {
                        header.set_entry_type(EntryType::Symlink);
                        header.set_mode(0o777);
                        header.set_size(0);
                        header.set_link_name(target.as_str())?;
                        &[][..]
                    }
                };
                tar.append_data(&mut header, spec.path.as_str(), data)
                    .await?;
            }
            tar.finish().await
        }

        /// Copy every second entry of `data` into `out`.
        pub async fn filter(data: &[u8], out: &mut Vec<u8>) -> Result<()> {
            let mut tar = Builder::new(out);
            let mut entries = Archive::new(data).entries()?;
            let mut i = 0u64;
            while let Some(entry) = entries.next().await {
                let mut entry = entry?;
                i += 1;
                if i % 2 == 0 {
                    let header = entry.header().clone();
                    tar.append(&header, &mut entry).await?;
                }
            }
            tar.finish().await
        }

        /// Encode the tree at `root` into `out`. Each entry path starts
        /// with `prefix`.
        pub async fn encode_dir<W: AsyncWrite + Unpin + Send + Sync>(
            root: &Path,
            prefix: &str,
            out: W,
        ) -> Result<()> {
            let mut tar = Builder::new(out);
            tar.follow_symlinks(false);
            tar.append_dir_all(prefix, root).await?;
            tar.finish().await
        }

        /// Extract `data` into the new directory `dest`. With `minimal`,
        /// do not set the modification time.
        pub async fn extract(data: &[u8], dest: &Path, minimal: bool) -> Result<()> {
            ArchiveBuilder::new(data)
                .set_preserve_mtime(!minimal)
                .build()
                .unpack(dest)
                .await
        }
    }

    /// `astral-tokio-tar`.
    #[cfg(feature = "tokio")]
    pub mod astral_tokio_tar {
        use {
            crate::{body, Counts, Kind, Spec, MTIME},
            ::tokio_tar::{Archive, ArchiveBuilder, Builder, EntryType, Header},
            futures::StreamExt,
            std::{io::Result, path::Path},
            tokio::io::{AsyncReadExt, AsyncWrite},
        };

        /// Read every entry and every file body.
        pub async fn read_all(data: &[u8], buf: &mut [u8]) -> Result<Counts> {
            let mut archive = Archive::new(data);
            let mut entries = archive.entries()?;
            let mut counts = Counts::default();
            while let Some(entry) = entries.next().await {
                let mut entry = entry?;
                counts.entries += 1;
                if entry.header().entry_type().is_file() {
                    loop {
                        let n = entry.read(buf).await?;
                        if n == 0 {
                            break;
                        }
                        counts.bytes += n as u64;
                    }
                }
            }
            Ok(counts)
        }

        /// Read every entry and drop every file body unread.
        pub async fn read_skip(data: &[u8]) -> Result<u64> {
            let mut archive = Archive::new(data);
            let mut entries = archive.entries()?;
            let mut n = 0;
            while let Some(entry) = entries.next().await {
                entry?;
                n += 1;
            }
            Ok(n)
        }

        /// Write `specs` into `out` with ustar headers.
        ///
        /// `Builder::new` spawns a task that writes the end-of-archive blocks
        /// on drop. This adapter uses `Builder::new_non_terminated` and
        /// `Builder::finish`, which write the same blocks without the task.
        pub async fn write(specs: &[Spec], out: &mut Vec<u8>) -> Result<()> {
            let mut tar = Builder::new_non_terminated(out);
            for spec in specs {
                let mut header = Header::new_ustar();
                header.set_mtime(MTIME);
                let data = match &spec.kind {
                    Kind::Dir => {
                        header.set_entry_type(EntryType::Directory);
                        header.set_mode(0o755);
                        header.set_size(0);
                        &[][..]
                    }
                    Kind::File(size) => {
                        header.set_entry_type(EntryType::Regular);
                        header.set_mode(0o644);
                        header.set_size(*size as u64);
                        body(*size)
                    }
                    Kind::Symlink(target) => {
                        header.set_entry_type(EntryType::Symlink);
                        header.set_mode(0o777);
                        header.set_size(0);
                        header.set_link_name(target.as_str())?;
                        &[][..]
                    }
                };
                tar.append_data(&mut header, spec.path.as_str(), data)
                    .await?;
            }
            tar.finish().await
        }

        /// Copy every second entry of `data` into `out`.
        pub async fn filter(data: &[u8], out: &mut Vec<u8>) -> Result<()> {
            let mut tar = Builder::new_non_terminated(out);
            let mut archive = Archive::new(data);
            let mut entries = archive.entries()?;
            let mut i = 0u64;
            while let Some(entry) = entries.next().await {
                let mut entry = entry?;
                i += 1;
                if i % 2 == 0 {
                    let header = entry.header().clone();
                    tar.append(&header, &mut entry).await?;
                }
            }
            tar.finish().await
        }

        /// Encode the tree at `root` into `out`. Each entry path starts
        /// with `prefix`.
        pub async fn encode_dir<W: AsyncWrite + Unpin + Send>(
            root: &Path,
            prefix: &str,
            out: W,
        ) -> Result<()> {
            let mut tar = Builder::new_non_terminated(out);
            tar.follow_symlinks(false);
            tar.append_dir_all(prefix, root).await?;
            tar.finish().await
        }

        /// Extract `data` into the new directory `dest`. With `minimal`,
        /// do not set the modification time.
        pub async fn extract(data: &[u8], dest: &Path, minimal: bool) -> Result<()> {
            ArchiveBuilder::new(data)
                .set_preserve_mtime(!minimal)
                .build()
                .unpack(dest)
                .await
        }
    }

    /// `tar-codec`.
    #[cfg(feature = "tokio")]
    pub mod tar_codec {
        use {
            crate::{body, Counts, Kind, Spec, BUF_SIZE},
            ::tar_codec::{
                builder::{BuilderPolicy, SymlinkPolicy},
                extract::ExtractPolicy,
                Archive, ArchiveBuilder, DecodePolicy, EntryMetadata, FilePayload, Member,
                MemberPayload, PaxDecodePolicy, PaxVendorExtensionPolicy, TarArchive, TarEncoder,
            },
            std::{
                io::{Error, ErrorKind, Result},
                path::Path,
            },
            tokio::io::AsyncWrite,
        };

        /// A reader of `data`. The default policy rejects unknown vendor
        /// PAX records. The pax corpus has `SCHILY.xattr` records, so the
        /// reader ignores the `SCHILY` records.
        fn archive(data: &[u8]) -> TarArchive<&[u8]> {
            TarArchive::new(data).with_policy(
                DecodePolicy::default().pax_policy(
                    PaxDecodePolicy::default()
                        .vendor_extension_policy(PaxVendorExtensionPolicy::ignore(["SCHILY"])),
                ),
            )
        }

        /// Read every entry and every file body. tar-codec puts each part
        /// of a body into a `Vec<u8>`.
        pub async fn read_all(data: &[u8], buf: &mut Vec<u8>) -> Result<Counts> {
            let mut tar = archive(data);
            let mut counts = Counts::default();
            while let Some(member) = tar.next_member().await.map_err(Error::other)? {
                counts.entries += 1;
                if let Member::File { mut payload, .. } = member {
                    while payload
                        .next_chunk(buf, BUF_SIZE)
                        .await
                        .map_err(Error::other)?
                    {
                        counts.bytes += buf.len() as u64;
                    }
                }
            }
            Ok(counts)
        }

        /// Read every entry and skip every file body.
        pub async fn read_skip(data: &[u8]) -> Result<u64> {
            let mut tar = archive(data);
            let mut entries = 0;
            while let Some(member) = tar.next_member().await.map_err(Error::other)? {
                if let Member::File { payload, .. } = member {
                    payload.skip().await.map_err(Error::other)?;
                }
                entries += 1;
            }
            Ok(entries)
        }

        /// Write `specs` into `out`. tar-codec writes a PAX header for each
        /// entry, and does not write the mode or the modification time. Its
        /// builder cannot write a symbolic link.
        pub async fn write(specs: &[Spec], out: &mut Vec<u8>) -> Result<()> {
            let mut tar = TarEncoder::new(out).builder();
            for spec in specs {
                match &spec.kind {
                    Kind::Dir => tar.add_directory(spec.path.trim_end_matches('/')).await,
                    Kind::File(size) => {
                        tar.add_file(
                            &spec.path,
                            FilePayload::new(*size as u64, body(*size)),
                            EntryMetadata::default(),
                        )
                        .await
                    }
                    Kind::Symlink(_) => {
                        return Err(Error::new(
                            ErrorKind::Unsupported,
                            "tar-codec cannot write a symbolic link",
                        ))
                    }
                }
                .map_err(Error::other)?;
            }
            tar.finish().await.map_err(Error::other)
        }

        /// Encode the tree at `root` into `out`. The default policy rejects
        /// a symbolic link, so the builder uses the policy that writes link
        /// entries.
        pub async fn encode_dir<W: AsyncWrite + Unpin>(root: &Path, out: W) -> Result<()> {
            let mut tar = TarEncoder::new(out)
                .builder()
                .with_policy(BuilderPolicy::default().symlink_policy(SymlinkPolicy::Preserve));
            tar.add_directory_all(root).await.map_err(Error::other)?;
            tar.finish().await.map_err(Error::other)
        }

        /// Extract `data` into the new directory `dest` with the default
        /// policy. tar-codec does not set the mode from the archive or the
        /// modification time, so it has no `minimal` option.
        pub async fn extract(data: &[u8], dest: &Path) -> Result<()> {
            TarArchive::new(data)
                .extract_in(dest, ExtractPolicy::default())
                .await
                .map_err(Error::other)
        }
    }
}
