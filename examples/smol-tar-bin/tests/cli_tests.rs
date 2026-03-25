use std::{
    env, fs,
    io::{self, Write},
    os::unix::fs::{symlink, MetadataExt},
    path::{Path, PathBuf},
    process::{Command, Output, Stdio},
    sync::atomic::{AtomicU64, Ordering},
    time::{SystemTime, UNIX_EPOCH},
};

use futures::SinkExt;
use smol_tar::{TarDirectory, TarEntry, TarLink, TarReader, TarRegularFile, TarWriter};

static NEXT_ID: AtomicU64 = AtomicU64::new(0);

#[cfg(all(feature = "smol", feature = "tokio"))]
compile_error!("enable only one runtime feature for CLI tests");
#[cfg(not(any(feature = "smol", feature = "tokio")))]
compile_error!("CLI tests require a runtime feature");

fn bin() -> &'static str {
    env!("CARGO_BIN_EXE_smol-tar")
}

struct TempDir {
    path: PathBuf,
}

impl TempDir {
    fn new() -> io::Result<Self> {
        let unique = format!(
            "smol-tar-cli-{}-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos(),
            NEXT_ID.fetch_add(1, Ordering::Relaxed)
        );
        let path = env::temp_dir().join(unique);
        fs::create_dir_all(&path)?;
        Ok(Self { path })
    }

    fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.path);
    }
}

fn run_command(current_dir: &Path, args: &[&str], stdin: Option<&[u8]>) -> io::Result<Output> {
    let mut command = Command::new(bin());
    command.current_dir(current_dir).args(args);
    if stdin.is_some() {
        command.stdin(Stdio::piped());
    }
    command.stdout(Stdio::piped()).stderr(Stdio::piped());

    let mut child = command.spawn()?;
    if let Some(input) = stdin {
        child.stdin.take().unwrap().write_all(input)?;
    }
    child.wait_with_output()
}

fn write_text(path: &Path, text: &str) -> io::Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    fs::write(path, text)
}

#[cfg(feature = "smol")]
type ArchiveFile = smol::fs::File;
#[cfg(feature = "tokio")]
type ArchiveFile = tokio::fs::File;

#[cfg(feature = "smol")]
type ArchiveCursor = futures_lite::io::Cursor<Vec<u8>>;
#[cfg(feature = "tokio")]
type ArchiveCursor = std::io::Cursor<Vec<u8>>;

#[cfg(feature = "smol")]
fn run_async<F>(future: F) -> io::Result<F::Output>
where
    F: std::future::Future,
{
    Ok(smol::block_on(future))
}

#[cfg(feature = "tokio")]
fn run_async<F>(future: F) -> io::Result<F::Output>
where
    F: std::future::Future,
{
    Ok(tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("build tokio runtime")
        .block_on(future))
}

#[cfg(feature = "smol")]
async fn open_archive_file(path: &Path) -> io::Result<ArchiveFile> {
    smol::fs::File::open(path).await
}

#[cfg(feature = "tokio")]
async fn open_archive_file(path: &Path) -> io::Result<ArchiveFile> {
    tokio::fs::File::open(path).await
}

#[cfg(feature = "smol")]
async fn create_archive_file(path: &Path) -> io::Result<ArchiveFile> {
    smol::fs::File::create(path).await
}

#[cfg(feature = "tokio")]
async fn create_archive_file(path: &Path) -> io::Result<ArchiveFile> {
    tokio::fs::File::create(path).await
}

#[cfg(feature = "smol")]
use futures_lite::StreamExt as _;
#[cfg(feature = "tokio")]
use tokio_stream::StreamExt as _;

fn read_archive_kinds(path: &Path) -> io::Result<Vec<(String, String, Option<String>)>> {
    run_async(async move {
        let file = open_archive_file(path).await?;
        let mut reader = TarReader::new(file);
        let mut entries = Vec::new();
        while let Some(entry) = reader.next().await {
            match entry? {
                TarEntry::Directory(dir) => {
                    entries.push(("dir".to_string(), dir.path().to_string(), None));
                }
                TarEntry::File(file) => {
                    entries.push(("file".to_string(), file.path().to_string(), None));
                }
                TarEntry::Symlink(link) => entries.push((
                    "symlink".to_string(),
                    link.path().to_string(),
                    Some(link.link().to_string()),
                )),
                TarEntry::Link(link) => entries.push((
                    "hardlink".to_string(),
                    link.path().to_string(),
                    Some(link.link().to_string()),
                )),
                TarEntry::Fifo(fifo) => {
                    entries.push(("fifo".to_string(), fifo.path().to_string(), None));
                }
                TarEntry::Device(device) => {
                    entries.push(("device".to_string(), device.path().to_string(), None));
                }
            }
        }
        Ok::<_, io::Error>(entries)
    })?
}

fn build_hardlink_first_archive(path: &Path) -> io::Result<()> {
    run_async(async move {
        let file = create_archive_file(path).await?;
        let mut writer = TarWriter::new(file);
        writer
            .send(TarEntry::from(TarDirectory::new("tree/")))
            .await?;
        writer
            .send(TarEntry::from(TarLink::new(
                "tree/link.txt",
                "tree/target.txt",
            )))
            .await?;
        writer
            .send(TarEntry::from(TarRegularFile::new(
                "tree/target.txt",
                5,
                ArchiveCursor::new(b"hello".to_vec()),
            )))
            .await?;
        writer.close().await?;
        Ok::<_, io::Error>(())
    })?
}

fn build_zero_length_then_file_archive(path: &Path) -> io::Result<()> {
    run_async(async move {
        let file = create_archive_file(path).await?;
        let mut writer = TarWriter::new(file);
        writer
            .send(TarEntry::from(TarDirectory::new("tree/")))
            .await?;
        writer
            .send(TarEntry::from(TarRegularFile::new(
                "tree/.lock",
                0,
                ArchiveCursor::new(Vec::new()),
            )))
            .await?;
        writer
            .send(TarEntry::from(TarRegularFile::new(
                "tree/after.txt",
                5,
                ArchiveCursor::new(b"after".to_vec()),
            )))
            .await?;
        writer.close().await?;
        Ok::<_, io::Error>(())
    })?
}

#[test]
fn invalid_invocation_reports_usage() -> io::Result<()> {
    let temp = TempDir::new()?;
    let output = run_command(temp.path(), &["-t", "-C", "out"], None)?;
    assert_eq!(output.status.code(), Some(2));
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(stderr.contains("option -f is required"));
    assert!(stderr.contains("usage:"));
    assert!(stderr.contains("smol-tar-bin"));
    Ok(())
}

#[test]
fn help_flag_prints_usage() -> io::Result<()> {
    let temp = TempDir::new()?;
    let output = run_command(temp.path(), &["--help"], None)?;
    assert_eq!(output.status.code(), Some(0));
    let stdout = String::from_utf8(output.stdout).unwrap();
    assert!(stdout.contains("usage:"));
    assert!(stdout.contains("smol-tar-bin -c -f ARCHIVE"));
    Ok(())
}

#[test]
fn create_list_and_extract_round_trip_with_hardlinks() -> io::Result<()> {
    let temp = TempDir::new()?;
    let src_root = temp.path().join("src");
    let tree = src_root.join("tree");
    fs::create_dir_all(tree.join("nested"))?;
    write_text(&tree.join("alpha.txt"), "alpha\n")?;
    write_text(&tree.join("nested/bravo.txt"), "bravo\n")?;
    fs::hard_link(tree.join("alpha.txt"), tree.join("alpha-link.txt"))?;
    symlink("alpha.txt", tree.join("sym.txt"))?;

    let archive = temp.path().join("archive.tar");
    let output = run_command(
        temp.path(),
        &[
            "-cf",
            archive.to_str().unwrap(),
            "-C",
            src_root.to_str().unwrap(),
            "tree",
        ],
        None,
    )?;
    assert!(output.status.success(), "{output:?}");

    let entries = read_archive_kinds(&archive)?;
    assert!(entries.contains(&("dir".to_string(), "tree/".to_string(), None)));
    let alpha_entries = entries
        .iter()
        .filter(|(_, path, _)| path == "tree/alpha.txt" || path == "tree/alpha-link.txt")
        .cloned()
        .collect::<Vec<_>>();
    assert_eq!(alpha_entries.len(), 2);
    assert_eq!(
        alpha_entries
            .iter()
            .filter(|(kind, _, _)| kind == "file")
            .count(),
        1
    );
    assert_eq!(
        alpha_entries
            .iter()
            .filter(|(kind, _, _)| kind == "hardlink")
            .count(),
        1
    );
    assert!(entries.contains(&(
        "symlink".to_string(),
        "tree/sym.txt".to_string(),
        Some("alpha.txt".to_string())
    )));

    let listing = run_command(temp.path(), &["-tf", archive.to_str().unwrap()], None)?;
    assert!(listing.status.success(), "{listing:?}");
    let stdout = String::from_utf8(listing.stdout).unwrap();
    assert!(stdout.contains("tree/alpha.txt\n"));
    assert!(stdout.contains("tree/alpha-link.txt\n"));
    assert!(stdout.contains("tree/nested/bravo.txt\n"));

    let dest = temp.path().join("dest");
    fs::create_dir_all(&dest)?;
    let extract = run_command(
        temp.path(),
        &[
            "-xf",
            archive.to_str().unwrap(),
            "-C",
            dest.to_str().unwrap(),
        ],
        None,
    )?;
    assert!(extract.status.success(), "{extract:?}");

    assert_eq!(fs::read_to_string(dest.join("tree/alpha.txt"))?, "alpha\n");
    assert_eq!(
        fs::read_to_string(dest.join("tree/nested/bravo.txt"))?,
        "bravo\n"
    );
    assert_eq!(
        fs::read_link(dest.join("tree/sym.txt"))?,
        PathBuf::from("alpha.txt")
    );
    let alpha = fs::metadata(dest.join("tree/alpha.txt"))?;
    let alpha_link = fs::metadata(dest.join("tree/alpha-link.txt"))?;
    assert_eq!(alpha.ino(), alpha_link.ino());

    Ok(())
}

#[test]
fn extract_defers_hardlinks_until_target_exists() -> io::Result<()> {
    let temp = TempDir::new()?;
    let archive = temp.path().join("deferred.tar");
    build_hardlink_first_archive(&archive)?;

    let dest = temp.path().join("out");
    fs::create_dir_all(&dest)?;
    let output = run_command(
        temp.path(),
        &[
            "-xf",
            archive.to_str().unwrap(),
            "-C",
            dest.to_str().unwrap(),
        ],
        None,
    )?;
    assert!(output.status.success(), "{output:?}");

    assert_eq!(fs::read_to_string(dest.join("tree/target.txt"))?, "hello");
    assert_eq!(fs::read_to_string(dest.join("tree/link.txt"))?, "hello");
    let target = fs::metadata(dest.join("tree/target.txt"))?;
    let link = fs::metadata(dest.join("tree/link.txt"))?;
    assert_eq!(target.ino(), link.ino());
    Ok(())
}

#[test]
fn extract_filter_limits_members() -> io::Result<()> {
    let temp = TempDir::new()?;
    let src_root = temp.path().join("src");
    let tree = src_root.join("tree");
    fs::create_dir_all(tree.join("nested"))?;
    write_text(&tree.join("alpha.txt"), "alpha\n")?;
    write_text(&tree.join("nested/bravo.txt"), "bravo\n")?;

    let archive = temp.path().join("filter.tar");
    let create = run_command(
        temp.path(),
        &[
            "-cf",
            archive.to_str().unwrap(),
            "-C",
            src_root.to_str().unwrap(),
            "tree",
        ],
        None,
    )?;
    assert!(create.status.success(), "{create:?}");

    let dest = temp.path().join("dest");
    fs::create_dir_all(&dest)?;
    let extract = run_command(
        temp.path(),
        &[
            "-xf",
            archive.to_str().unwrap(),
            "-C",
            dest.to_str().unwrap(),
            "tree/nested",
        ],
        None,
    )?;
    assert!(extract.status.success(), "{extract:?}");

    assert!(dest.join("tree/nested/bravo.txt").exists());
    assert!(!dest.join("tree/alpha.txt").exists());
    Ok(())
}

#[test]
fn stdio_extract_overwrites_existing_files() -> io::Result<()> {
    let temp = TempDir::new()?;
    let src_root = temp.path().join("src");
    let tree = src_root.join("tree");
    fs::create_dir_all(&tree)?;
    write_text(&tree.join("file.txt"), "new data\n")?;

    let create = run_command(
        temp.path(),
        &["-cf", "-", "-C", src_root.to_str().unwrap(), "tree"],
        None,
    )?;
    assert!(create.status.success(), "{create:?}");

    let dest = temp.path().join("dest");
    write_text(&dest.join("tree/file.txt"), "old data\n")?;
    let extract = run_command(
        temp.path(),
        &["-xf", "-", "-C", dest.to_str().unwrap()],
        Some(&create.stdout),
    )?;
    assert!(extract.status.success(), "{extract:?}");
    assert_eq!(
        fs::read_to_string(dest.join("tree/file.txt"))?,
        "new data\n"
    );
    Ok(())
}

#[test]
fn stdio_list_works_for_current_directory_archive() -> io::Result<()> {
    let temp = TempDir::new()?;
    let tree = temp.path().join("tree");
    fs::create_dir_all(tree.join("nested"))?;
    write_text(&tree.join("root.txt"), "root\n")?;
    write_text(&tree.join("nested/child.txt"), "child\n")?;

    let create = run_command(temp.path(), &["-cf-", "."], None)?;
    assert!(create.status.success(), "{create:?}");

    let list = run_command(temp.path(), &["-tf-"], Some(&create.stdout))?;
    assert!(list.status.success(), "{list:?}");
    let stdout = String::from_utf8(list.stdout).unwrap();
    assert!(stdout.contains("./\n"));
    assert!(stdout.contains("./tree/\n"));
    assert!(stdout.contains("./tree/root.txt\n"));
    assert!(stdout.contains("./tree/nested/child.txt\n"));
    Ok(())
}

#[test]
fn list_continues_after_zero_length_file() -> io::Result<()> {
    let temp = TempDir::new()?;
    let archive = temp.path().join("zero-then-file.tar");
    build_zero_length_then_file_archive(&archive)?;

    let output = run_command(temp.path(), &["-tf", archive.to_str().unwrap()], None)?;
    assert!(output.status.success(), "{output:?}");

    let stdout = String::from_utf8(output.stdout).unwrap();
    assert!(stdout.contains("tree/\n"));
    assert!(stdout.contains("tree/.lock\n"));
    assert!(stdout.contains("tree/after.txt\n"));
    Ok(())
}
