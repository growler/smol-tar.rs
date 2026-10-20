use std::{
    env, fs,
    io::{self, Write},
    os::unix::fs::{symlink, MetadataExt, PermissionsExt},
    path::{Path, PathBuf},
    process::{Command, Output, Stdio},
    sync::atomic::{AtomicU64, Ordering},
    time::{SystemTime, UNIX_EPOCH},
};

use futures::SinkExt;
use smol_tar::{
    AttrList, TarDevice, TarDirectory, TarEntry, TarLink, TarReader, TarRegularFile, TarSymlink,
    TarWriter,
};

static NEXT_ID: AtomicU64 = AtomicU64::new(0);

#[cfg(all(feature = "smol", feature = "tokio"))]
compile_error!("enable only one runtime feature for CLI tests");
#[cfg(not(any(feature = "smol", feature = "tokio")))]
compile_error!("CLI tests require a runtime feature");

fn bin() -> &'static str {
    env!("CARGO_BIN_EXE_picotar")
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

#[cfg(all(feature = "smol", not(feature = "tokio")))]
type ArchiveFile = smol::fs::File;
#[cfg(feature = "tokio")]
type ArchiveFile = tokio::fs::File;

#[cfg(all(feature = "smol", not(feature = "tokio")))]
type ArchiveCursor = futures_lite::io::Cursor<Vec<u8>>;
#[cfg(feature = "tokio")]
type ArchiveCursor = std::io::Cursor<Vec<u8>>;

#[cfg(all(feature = "smol", not(feature = "tokio")))]
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

#[cfg(all(feature = "smol", not(feature = "tokio")))]
async fn open_archive_file(path: &Path) -> io::Result<ArchiveFile> {
    smol::fs::File::open(path).await
}

#[cfg(feature = "tokio")]
async fn open_archive_file(path: &Path) -> io::Result<ArchiveFile> {
    tokio::fs::File::open(path).await
}

#[cfg(all(feature = "smol", not(feature = "tokio")))]
async fn create_archive_file(path: &Path) -> io::Result<ArchiveFile> {
    smol::fs::File::create(path).await
}

#[cfg(feature = "tokio")]
async fn create_archive_file(path: &Path) -> io::Result<ArchiveFile> {
    tokio::fs::File::create(path).await
}

#[cfg(all(feature = "smol", not(feature = "tokio")))]
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
    assert!(stderr.contains("picotar"));
    Ok(())
}

#[test]
fn help_flag_prints_usage() -> io::Result<()> {
    let temp = TempDir::new()?;
    let output = run_command(temp.path(), &["--help"], None)?;
    assert_eq!(output.status.code(), Some(0));
    let stdout = String::from_utf8(output.stdout).unwrap();
    assert!(stdout.contains("usage:"));
    assert!(stdout.contains("picotar -c -f ARCHIVE"));
    assert!(stdout.contains("  -f ARCHIVE "));
    assert!(stdout.contains("  -C DIR "));
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
fn extract_rejects_hardlink_before_target() -> io::Result<()> {
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
    assert!(
        !output.status.success(),
        "extraction should fail for hardlink before its target"
    );
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(
        stderr.contains("tree/link.txt: link target tree/target.txt is not in the archive"),
        "expected missing link target error, got: {stderr}"
    );
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

fn build_symlink_escape_archive(path: &Path) -> io::Result<()> {
    run_async(async move {
        let file = create_archive_file(path).await?;
        let mut writer: TarWriter<'_, '_, _, ArchiveCursor> = TarWriter::new(file);
        writer
            .send(TarEntry::from(TarDirectory::new("tree/")))
            .await?;
        writer
            .send(TarEntry::from(TarSymlink::new(
                "tree/escape",
                "../../../etc/passwd",
            )))
            .await?;
        writer.close().await?;
        Ok::<_, io::Error>(())
    })?
}

#[test]
fn extract_keeps_symlink_with_parent_traversal_target() -> io::Result<()> {
    let temp = TempDir::new()?;
    let archive = temp.path().join("symlink-escape.tar");
    build_symlink_escape_archive(&archive)?;

    let dest = temp.path().join("dest");
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
    assert_eq!(
        fs::read_link(dest.join("tree/escape"))?,
        PathBuf::from("../../../etc/passwd")
    );
    Ok(())
}

fn build_absolute_symlink_archive(path: &Path) -> io::Result<()> {
    run_async(async move {
        let file = create_archive_file(path).await?;
        let mut writer: TarWriter<'_, '_, _, ArchiveCursor> = TarWriter::new(file);
        writer
            .send(TarEntry::from(TarDirectory::new("tree/")))
            .await?;
        writer
            .send(TarEntry::from(TarSymlink::new("tree/abs", "/etc/passwd")))
            .await?;
        writer.close().await?;
        Ok::<_, io::Error>(())
    })?
}

#[test]
fn extract_keeps_symlink_with_absolute_target() -> io::Result<()> {
    let temp = TempDir::new()?;
    let archive = temp.path().join("symlink-abs.tar");
    build_absolute_symlink_archive(&archive)?;

    let dest = temp.path().join("dest");
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
    assert_eq!(
        fs::read_link(dest.join("tree/abs"))?,
        PathBuf::from("/etc/passwd")
    );
    Ok(())
}

fn run_ok(current_dir: &Path, args: &[&str], stdin: Option<&[u8]>) -> io::Result<Output> {
    let output = run_command(current_dir, args, stdin)?;
    assert!(output.status.success(), "{args:?}: {output:?}");
    Ok(output)
}

fn path_str(path: &Path) -> &str {
    path.to_str().unwrap()
}

fn mode_of(path: &Path) -> io::Result<u32> {
    Ok(fs::metadata(path)?.permissions().mode() & 0o7777)
}

/// Make `src/tree` with two files, a subdirectory and a hard link.
fn make_sample_tree(root: &Path) -> io::Result<PathBuf> {
    let src_root = root.join("src");
    let tree = src_root.join("tree");
    write_text(&tree.join("alpha.txt"), "alpha\n")?;
    write_text(&tree.join("nested/bravo.txt"), "bravo\n")?;
    fs::hard_link(tree.join("alpha.txt"), tree.join("alpha-link.txt"))?;
    Ok(src_root)
}

#[test]
fn compressed_round_trip() -> io::Result<()> {
    let cases: [(&str, &[u8]); 4] = [
        ("-z", &[0x1f, 0x8b]),
        ("-J", &[0xfd, b'7', b'z', b'X', b'Z', 0]),
        ("--xzip", &[0xfd, b'7', b'z', b'X', b'Z', 0]),
        ("--zstd", &[0x28, 0xb5, 0x2f, 0xfd]),
    ];
    for (flag, magic) in cases {
        let temp = TempDir::new()?;
        let src_root = make_sample_tree(temp.path())?;
        let archive = temp.path().join("archive");
        let archive = path_str(&archive);
        run_ok(
            temp.path(),
            &["-c", flag, "-f", archive, "-C", path_str(&src_root), "tree"],
            None,
        )?;
        assert!(fs::read(archive)?.starts_with(magic), "{flag}");

        let list = run_ok(temp.path(), &["-t", flag, "-f", archive], None)?;
        let stdout = String::from_utf8(list.stdout).unwrap();
        assert!(
            stdout.contains("tree/nested/bravo.txt\n"),
            "{flag}: {stdout}"
        );

        let dest = temp.path().join("dest");
        run_ok(
            temp.path(),
            &["-x", flag, "-f", archive, "-C", path_str(&dest)],
            None,
        )?;
        assert_eq!(
            fs::read_to_string(dest.join("tree/nested/bravo.txt"))?,
            "bravo\n"
        );
    }
    Ok(())
}

#[test]
fn compressed_stdio_round_trip() -> io::Result<()> {
    let temp = TempDir::new()?;
    let src_root = make_sample_tree(temp.path())?;
    let create = run_ok(
        temp.path(),
        &["-czf", "-", "-C", path_str(&src_root), "tree"],
        None,
    )?;
    assert!(create.stdout.starts_with(&[0x1f, 0x8b]));

    let dest = temp.path().join("dest");
    run_ok(
        temp.path(),
        &["-xzf", "-", "-C", path_str(&dest)],
        Some(&create.stdout),
    )?;
    assert_eq!(fs::read_to_string(dest.join("tree/alpha.txt"))?, "alpha\n");
    Ok(())
}

#[test]
fn create_excludes_paths() -> io::Result<()> {
    let temp = TempDir::new()?;
    let src_root = make_sample_tree(temp.path())?;
    let archive = temp.path().join("archive.tar");
    run_ok(
        &src_root,
        &["-cf", path_str(&archive), "--exclude", "tree/nested", "."],
        None,
    )?;

    let paths = read_archive_kinds(&archive)?
        .into_iter()
        .map(|(_, path, _)| path)
        .collect::<Vec<_>>();
    assert!(paths.contains(&"./tree/alpha.txt".to_string()), "{paths:?}");
    assert!(
        !paths.iter().any(|path| path.contains("nested")),
        "{paths:?}"
    );
    Ok(())
}

#[test]
fn extract_excludes_paths() -> io::Result<()> {
    let temp = TempDir::new()?;
    let src_root = make_sample_tree(temp.path())?;
    let archive = temp.path().join("archive.tar");
    run_ok(
        temp.path(),
        &["-cf", path_str(&archive), "-C", path_str(&src_root), "tree"],
        None,
    )?;

    let dest = temp.path().join("dest");
    run_ok(
        temp.path(),
        &[
            "-xf",
            path_str(&archive),
            "-C",
            path_str(&dest),
            "--exclude=tree/nested",
        ],
        None,
    )?;
    assert!(dest.join("tree/alpha.txt").exists());
    assert!(!dest.join("tree/nested").exists());
    Ok(())
}

#[test]
fn create_multiple_paths_shares_hardlinks() -> io::Result<()> {
    let temp = TempDir::new()?;
    let src_root = temp.path().join("src");
    write_text(&src_root.join("one/file.txt"), "shared\n")?;
    fs::create_dir_all(src_root.join("two"))?;
    fs::hard_link(src_root.join("one/file.txt"), src_root.join("two/link.txt"))?;

    let archive = temp.path().join("archive.tar");
    run_ok(
        temp.path(),
        &[
            "-cf",
            path_str(&archive),
            "-C",
            path_str(&src_root),
            "one",
            "two",
        ],
        None,
    )?;

    let entries = read_archive_kinds(&archive)?;
    assert!(entries.contains(&("file".to_string(), "one/file.txt".to_string(), None)));
    assert!(entries.contains(&(
        "hardlink".to_string(),
        "two/link.txt".to_string(),
        Some("one/file.txt".to_string())
    )));
    Ok(())
}

#[test]
fn verbose_writes_paths_to_stderr() -> io::Result<()> {
    let temp = TempDir::new()?;
    let src_root = make_sample_tree(temp.path())?;
    let archive = temp.path().join("archive.tar");
    let create = run_ok(
        temp.path(),
        &[
            "-cvf",
            path_str(&archive),
            "-C",
            path_str(&src_root),
            "tree",
        ],
        None,
    )?;
    let stderr = String::from_utf8(create.stderr).unwrap();
    assert!(stderr.contains("tree/nested/bravo.txt\n"), "{stderr}");

    let dest = temp.path().join("dest");
    let extract = run_ok(
        temp.path(),
        &["-xvf", path_str(&archive), "-C", path_str(&dest)],
        None,
    )?;
    let stderr = String::from_utf8(extract.stderr).unwrap();
    assert!(stderr.contains("tree/alpha.txt\n"), "{stderr}");
    assert!(extract.stdout.is_empty());
    Ok(())
}

#[test]
fn extract_creates_missing_destination() -> io::Result<()> {
    let temp = TempDir::new()?;
    let src_root = make_sample_tree(temp.path())?;
    let archive = temp.path().join("archive.tar");
    run_ok(
        temp.path(),
        &["-cf", path_str(&archive), "-C", path_str(&src_root), "tree"],
        None,
    )?;

    let dest = temp.path().join("missing/dest");
    run_ok(
        temp.path(),
        &["-xf", path_str(&archive), "-C", path_str(&dest)],
        None,
    )?;
    assert_eq!(fs::read_to_string(dest.join("tree/alpha.txt"))?, "alpha\n");
    Ok(())
}

#[test]
fn extract_does_not_create_destination_for_missing_archive() -> io::Result<()> {
    let temp = TempDir::new()?;
    let dest = temp.path().join("dest");
    let output = run_command(
        temp.path(),
        &["-xf", "missing.tar", "-C", path_str(&dest)],
        None,
    )?;
    assert_eq!(output.status.code(), Some(1));
    assert!(!dest.exists());
    Ok(())
}

#[test]
fn extract_strips_components() -> io::Result<()> {
    let temp = TempDir::new()?;
    let src_root = make_sample_tree(temp.path())?;
    let archive = temp.path().join("archive.tar");
    run_ok(&src_root, &["-cf", path_str(&archive), "."], None)?;

    let dest = temp.path().join("dest");
    run_ok(
        temp.path(),
        &[
            "-xf",
            path_str(&archive),
            "-C",
            path_str(&dest),
            "--strip-components=2",
        ],
        None,
    )?;
    assert_eq!(fs::read_to_string(dest.join("alpha.txt"))?, "alpha\n");
    assert_eq!(
        fs::read_to_string(dest.join("nested/bravo.txt"))?,
        "bravo\n"
    );
    assert!(!dest.join("tree").exists());
    let alpha = fs::metadata(dest.join("alpha.txt"))?;
    let alpha_link = fs::metadata(dest.join("alpha-link.txt"))?;
    assert_eq!(alpha.ino(), alpha_link.ino());
    Ok(())
}

#[test]
fn preserve_permissions_keeps_special_bits() -> io::Result<()> {
    let temp = TempDir::new()?;
    let src_root = make_sample_tree(temp.path())?;
    let script = src_root.join("tree/run.sh");
    write_text(&script, "#!/bin/sh\n")?;
    fs::set_permissions(&script, fs::Permissions::from_mode(0o4755))?;
    assert_eq!(mode_of(&script)?, 0o4755);

    let archive = temp.path().join("archive.tar");
    run_ok(
        temp.path(),
        &["-cf", path_str(&archive), "-C", path_str(&src_root), "tree"],
        None,
    )?;

    let plain = temp.path().join("plain");
    run_ok(
        temp.path(),
        &["-xf", path_str(&archive), "-C", path_str(&plain)],
        None,
    )?;
    assert_eq!(mode_of(&plain.join("tree/run.sh"))?, 0o755);

    let preserved = temp.path().join("preserved");
    run_ok(
        temp.path(),
        &["-xpf", path_str(&archive), "-C", path_str(&preserved)],
        None,
    )?;
    assert_eq!(mode_of(&preserved.join("tree/run.sh"))?, 0o4755);
    Ok(())
}

#[test]
fn same_owner_sets_owner_from_archive() -> io::Result<()> {
    let temp = TempDir::new()?;
    let src_root = make_sample_tree(temp.path())?;
    let archive = temp.path().join("archive.tar");
    run_ok(
        temp.path(),
        &["-cf", path_str(&archive), "-C", path_str(&src_root), "tree"],
        None,
    )?;

    // The files belong to the user of the test, so no privileges are necessary.
    let dest = temp.path().join("dest");
    run_ok(
        temp.path(),
        &[
            "-xf",
            path_str(&archive),
            "-C",
            path_str(&dest),
            "--same-owner",
        ],
        None,
    )?;
    let source = fs::metadata(src_root.join("tree/alpha.txt"))?;
    let extracted = fs::metadata(dest.join("tree/alpha.txt"))?;
    assert_eq!(
        (source.uid(), source.gid()),
        (extracted.uid(), extracted.gid())
    );
    Ok(())
}

fn build_xattr_archive(path: &Path) -> io::Result<()> {
    run_async(async move {
        let file = create_archive_file(path).await?;
        let mut writer = TarWriter::new(file);
        let attrs = AttrList::from(vec![(
            Box::<str>::from("user.picotar"),
            Box::<[u8]>::from(&b"value"[..]),
        )]);
        writer
            .send(TarEntry::from(TarDirectory::new("tree/")))
            .await?;
        writer
            .send(TarEntry::from(
                TarRegularFile::new("tree/file.txt", 5, ArchiveCursor::new(b"hello".to_vec()))
                    .with_attrs(attrs),
            ))
            .await?;
        writer.close().await?;
        Ok::<_, io::Error>(())
    })?
}

fn read_file_attrs(path: &Path, member: &str) -> io::Result<Vec<(String, Vec<u8>)>> {
    run_async(async move {
        let mut reader = TarReader::new(open_archive_file(path).await?);
        while let Some(entry) = reader.next().await {
            if let TarEntry::File(file) = entry? {
                if file.path() == member {
                    return Ok(file
                        .attrs()
                        .iter()
                        .map(|(name, value)| (name.to_string(), value.to_vec()))
                        .collect());
                }
            }
        }
        Err(io::Error::new(io::ErrorKind::NotFound, member.to_string()))
    })?
}

#[test]
fn xattrs_round_trip() -> io::Result<()> {
    let temp = TempDir::new()?;
    let archive = temp.path().join("xattr.tar");
    build_xattr_archive(&archive)?;

    let dest = temp.path().join("dest");
    run_ok(
        temp.path(),
        &["-xf", path_str(&archive), "-C", path_str(&dest), "--xattrs"],
        None,
    )?;

    let again = temp.path().join("again.tar");
    run_ok(
        temp.path(),
        &[
            "-cf",
            path_str(&again),
            "-C",
            path_str(&dest),
            "--xattrs",
            "tree",
        ],
        None,
    )?;
    assert_eq!(
        read_file_attrs(&again, "tree/file.txt")?,
        vec![("user.picotar".to_string(), b"value".to_vec())]
    );

    let plain = temp.path().join("plain.tar");
    run_ok(
        temp.path(),
        &["-cf", path_str(&plain), "-C", path_str(&dest), "tree"],
        None,
    )?;
    assert!(read_file_attrs(&plain, "tree/file.txt")?.is_empty());
    Ok(())
}

#[test]
fn skip_missing_links_continues_extraction() -> io::Result<()> {
    let temp = TempDir::new()?;
    let archive = temp.path().join("deferred.tar");
    build_hardlink_first_archive(&archive)?;

    let dest = temp.path().join("out");
    run_ok(
        temp.path(),
        &[
            "-xf",
            path_str(&archive),
            "-C",
            path_str(&dest),
            "--skip-missing-links",
        ],
        None,
    )?;
    assert_eq!(fs::read_to_string(dest.join("tree/target.txt"))?, "hello");
    assert!(!dest.join("tree/link.txt").exists());
    Ok(())
}

/// Make a regular file entry with the body `text`.
fn file_entry(path: &str, text: &str) -> TarEntry<'static, ArchiveCursor> {
    TarRegularFile::new(path, text.len() as u64, ArchiveCursor::new(text.into())).into()
}

/// Write `entries` to a new archive at `path`.
fn build_archive(path: &Path, entries: Vec<TarEntry<'static, ArchiveCursor>>) -> io::Result<()> {
    run_async(async move {
        let mut writer = TarWriter::new(create_archive_file(path).await?);
        for entry in entries {
            writer.send(entry).await?;
        }
        writer.close().await?;
        Ok::<_, io::Error>(())
    })?
}

/// Give the sorted names in the directory `path`. A missing directory has
/// no names.
fn dir_names(path: &Path) -> io::Result<Vec<String>> {
    let mut names = match fs::read_dir(path) {
        Ok(dir) => dir
            .map(|entry| Ok(entry?.file_name().to_string_lossy().into_owned()))
            .collect::<io::Result<Vec<_>>>()?,
        Err(err) if err.kind() == io::ErrorKind::NotFound => Vec::new(),
        Err(err) => return Err(err),
    };
    names.sort();
    Ok(names)
}

/// Set the mode of a path back when the test ends, so that the temporary
/// directory can be removed.
struct RestoreMode(PathBuf, u32);

impl Drop for RestoreMode {
    fn drop(&mut self) {
        let _ = fs::set_permissions(&self.0, fs::Permissions::from_mode(self.1));
    }
}

#[test]
fn create_skips_its_own_archive() -> io::Result<()> {
    let temp = TempDir::new()?;
    write_text(&temp.path().join("a.txt"), "a\n")?;
    run_ok(temp.path(), &["-cf", "self.tar", "."], None)?;

    let paths = read_archive_kinds(&temp.path().join("self.tar"))?
        .into_iter()
        .map(|(_, path, _)| path)
        .collect::<Vec<_>>();
    assert!(paths.contains(&"./a.txt".to_string()), "{paths:?}");
    assert!(
        !paths.iter().any(|path| path.ends_with("self.tar")),
        "{paths:?}"
    );
    Ok(())
}

#[test]
fn create_keeps_existing_archive_on_error() -> io::Result<()> {
    let temp = TempDir::new()?;
    let archive = temp.path().join("existing.tar");
    fs::write(&archive, b"old archive")?;

    let output = run_command(temp.path(), &["-cf", "existing.tar", "missing_dir"], None)?;
    assert!(!output.status.success(), "{output:?}");
    assert_eq!(fs::read(&archive)?, b"old archive");
    // No temporary file stays next to the archive.
    assert_eq!(dir_names(temp.path())?, vec!["existing.tar"]);
    Ok(())
}

#[test]
fn create_exclude_does_not_read_excluded_directory() -> io::Result<()> {
    let temp = TempDir::new()?;
    let src_root = make_sample_tree(temp.path())?;
    let locked = src_root.join("tree/locked");
    write_text(&locked.join("hidden.txt"), "hidden\n")?;
    fs::set_permissions(&locked, fs::Permissions::from_mode(0o000))?;
    let _restore = RestoreMode(locked.clone(), 0o755);
    if fs::read_dir(&locked).is_ok() {
        eprintln!("skip: the test user can read a directory with mode 000");
        return Ok(());
    }

    let archive = temp.path().join("archive.tar");
    run_ok(
        temp.path(),
        &[
            "-cf",
            path_str(&archive),
            "-C",
            path_str(&src_root),
            "--exclude",
            "tree/locked",
            "tree",
        ],
        None,
    )?;
    let entries = read_archive_kinds(&archive)?;
    assert!(
        !entries.iter().any(|(_, path, _)| path.contains("locked")),
        "{entries:?}"
    );
    Ok(())
}

#[test]
fn create_exclude_does_not_read_excluded_file_xattrs() -> io::Result<()> {
    let temp = TempDir::new()?;
    let src_root = make_sample_tree(temp.path())?;
    let secret = src_root.join("tree/secret");
    write_text(&secret, "secret\n")?;
    fs::set_permissions(&secret, fs::Permissions::from_mode(0o000))?;
    let _restore = RestoreMode(secret.clone(), 0o644);
    if fs::read(&secret).is_ok() {
        eprintln!("skip: the test user can read a file with mode 000");
        return Ok(());
    }

    let archive = temp.path().join("archive.tar");
    run_ok(
        temp.path(),
        &[
            "-cf",
            path_str(&archive),
            "-C",
            path_str(&src_root),
            "--xattrs",
            "--exclude",
            "tree/secret",
            "tree",
        ],
        None,
    )?;
    let entries = read_archive_kinds(&archive)?;
    assert!(
        !entries.iter().any(|(_, path, _)| path.contains("secret")),
        "{entries:?}"
    );
    Ok(())
}

/// Compress `data` with `encoder` and give the encoder back.
fn compress<E>(mut encoder: E, data: &[u8]) -> io::Result<E>
where
    E: futures::io::AsyncWrite + Unpin,
{
    use futures::io::AsyncWriteExt as _;
    futures_lite::future::block_on(async {
        encoder.write_all(data).await?;
        encoder.close().await?;
        Ok(encoder)
    })
}

#[test]
fn list_reads_multi_member_compressed_archives() -> io::Result<()> {
    use async_compression::futures::write::{GzipEncoder, XzEncoder, ZstdEncoder};

    let temp = TempDir::new()?;
    let src_root = make_sample_tree(temp.path())?;
    let plain = temp.path().join("plain.tar");
    run_ok(
        temp.path(),
        &["-cf", path_str(&plain), "-C", path_str(&src_root), "tree"],
        None,
    )?;
    let expected = run_ok(temp.path(), &["-tf", path_str(&plain)], None)?.stdout;

    // Split the archive at a block boundary between two entries, before
    // the end marker.
    let tar = fs::read(&plain)?;
    let middle = tar.len() / 2 / 512 * 512;
    assert!(middle >= 1024 && middle + 1024 <= tar.len());
    let (head, tail) = tar.split_at(middle);

    let mut gzip = compress(GzipEncoder::new(Vec::new()), head)?.into_inner();
    gzip.extend(compress(GzipEncoder::new(Vec::new()), tail)?.into_inner());
    let mut xz = compress(XzEncoder::new(Vec::new()), head)?.into_inner();
    xz.extend(compress(XzEncoder::new(Vec::new()), tail)?.into_inner());
    let mut zstd = compress(ZstdEncoder::new(Vec::new()), head)?.into_inner();
    zstd.extend(compress(ZstdEncoder::new(Vec::new()), tail)?.into_inner());

    for (flag, data) in [("-z", gzip), ("-J", xz), ("--zstd", zstd)] {
        let archive = temp.path().join("multi");
        fs::write(&archive, data)?;
        let output = run_command(temp.path(), &["-t", flag, "-f", path_str(&archive)], None)?;
        assert!(output.status.success(), "{flag}: {output:?}");
        assert_eq!(output.stdout, expected, "{flag}");
    }
    Ok(())
}

#[test]
fn strip_components_rejects_absolute_and_parent_paths() -> io::Result<()> {
    for member in ["/abs/dir/f", "../up/f"] {
        let temp = TempDir::new()?;
        let archive = temp.path().join("archive.tar");
        build_archive(&archive, vec![file_entry(member, "data")])?;

        let dest = temp.path().join("out");
        let output = run_command(
            temp.path(),
            &[
                "-xf",
                path_str(&archive),
                "--strip-components",
                "1",
                "-C",
                path_str(&dest),
            ],
            None,
        )?;
        assert!(!output.status.success(), "{member}: {output:?}");
        assert!(dir_names(&dest)?.is_empty(), "{member}");
    }
    Ok(())
}

#[test]
fn strip_components_reports_stripped_link_target() -> io::Result<()> {
    let temp = TempDir::new()?;
    let archive = temp.path().join("archive.tar");
    build_archive(
        &archive,
        vec![
            file_entry("x", "x"),
            TarLink::new("d/y", "x").into(),
            file_entry("d/z", "z"),
        ],
    )?;

    let dest = temp.path().join("out");
    let args = [
        "-xf",
        path_str(&archive),
        "--strip-components",
        "1",
        "-C",
        path_str(&dest),
    ];
    let output = run_command(temp.path(), &args, None)?;
    assert!(!output.status.success(), "{output:?}");
    let stderr = String::from_utf8(output.stderr).unwrap();
    // The message names the link or its target as a separate word.
    let mut words = stderr.split(|c: char| c.is_whitespace() || c == ':');
    assert!(words.any(|word| word == "d/y" || word == "x"), "{stderr}");

    let skip = temp.path().join("skip");
    run_ok(
        temp.path(),
        &[
            "-xf",
            path_str(&archive),
            "--strip-components",
            "1",
            "--skip-missing-links",
            "-C",
            path_str(&skip),
        ],
        None,
    )?;
    assert!(skip.join("z").exists());
    assert!(!skip.join("y").exists());
    Ok(())
}

#[test]
fn verbose_extract_writes_stripped_paths() -> io::Result<()> {
    let temp = TempDir::new()?;
    let archive = temp.path().join("archive.tar");
    build_archive(
        &archive,
        vec![
            TarDirectory::new("top/").into(),
            file_entry("top/a.txt", "a"),
        ],
    )?;

    let dest = temp.path().join("out");
    let output = run_ok(
        temp.path(),
        &[
            "-xvf",
            path_str(&archive),
            "--strip-components",
            "1",
            "-C",
            path_str(&dest),
        ],
        None,
    )?;
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert_eq!(stderr.lines().collect::<Vec<_>>(), vec!["a.txt"]);
    Ok(())
}

#[test]
fn verbose_extract_skips_device_without_devices_option() -> io::Result<()> {
    let temp = TempDir::new()?;
    let archive = temp.path().join("archive.tar");
    build_archive(
        &archive,
        vec![
            TarDevice::new_char("dev0", 1, 3).into(),
            file_entry("f", "f"),
        ],
    )?;

    let dest = temp.path().join("out");
    let output = run_ok(
        temp.path(),
        &["-xvf", path_str(&archive), "-C", path_str(&dest)],
        None,
    )?;
    let stderr = String::from_utf8(output.stderr).unwrap();
    let lines = stderr.lines().collect::<Vec<_>>();
    assert!(!lines.contains(&"dev0"), "{stderr}");
    assert!(lines.contains(&"f"), "{stderr}");
    Ok(())
}

#[test]
fn extract_exclude_ignores_current_directory_components() -> io::Result<()> {
    let temp = TempDir::new()?;
    let archive = temp.path().join("archive.tar");
    build_archive(
        &archive,
        vec![
            file_entry("tree/./secret", "secret"),
            file_entry("tree/keep", "keep"),
        ],
    )?;

    let dest = temp.path().join("out");
    run_ok(
        temp.path(),
        &[
            "-xf",
            path_str(&archive),
            "--exclude",
            "tree/secret",
            "-C",
            path_str(&dest),
        ],
        None,
    )?;
    assert!(!dest.join("tree/secret").exists());
    assert!(dest.join("tree/keep").exists());
    Ok(())
}
