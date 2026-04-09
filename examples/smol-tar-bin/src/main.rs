#[cfg(all(feature = "smol", feature = "tokio"))]
compile_error!("features `smol` and `tokio` are mutually exclusive");
#[cfg(not(any(feature = "smol", feature = "tokio")))]
compile_error!("either feature `smol` or `tokio` must be enabled");

#[cfg(feature = "smol")]
use ::smol::fs::{
    self,
    unix::{symlink, FileTypeExt, MetadataExt, PermissionsExt},
    File,
};
use ::smol_tar::{
    TarDirectory, TarEntry, TarLink, TarReader, TarRegularFile, TarSymlink, TarWriter,
};
#[cfg(feature = "tokio")]
use ::tokio::{
    fs::{self, File},
    io::{copy, AsyncRead, AsyncWrite, AsyncWriteExt},
    runtime::Builder,
};
#[cfg(feature = "tokio")]
use futures::SinkExt;
#[cfg(feature = "smol")]
use futures::{
    executor::block_on,
    io::{copy, AllowStdIo, AsyncRead, AsyncWrite, AsyncWriteExt},
    SinkExt, StreamExt,
};
use std::{
    collections::HashMap,
    ffi::OsString,
    io,
    path::{Component, Path, PathBuf},
    process::ExitCode,
    time::{Duration, SystemTime},
};
#[cfg(feature = "tokio")]
use std::{
    fs::Metadata,
    os::unix::fs::{FileTypeExt, MetadataExt, PermissionsExt},
};
#[cfg(feature = "tokio")]
use tokio_stream::StreamExt;

fn program_name() -> &'static str {
    env!("CARGO_PKG_NAME")
}

fn usage(program_name: &str) -> String {
    format!(
        "usage:\n  {program_name} -c -f ARCHIVE [-C DIR] PATH\n  {program_name} -x -f ARCHIVE [-C DIR] [PATH]\n  {program_name} -t -f ARCHIVE"
    )
}

fn main() -> ExitCode {
    let program_name = program_name();
    let args = std::env::args_os().skip(1).collect::<Vec<_>>();

    if args.len() == 1
        && args[0]
            .to_str()
            .is_some_and(|arg| matches!(arg, "-h" | "--help"))
    {
        println!("{}", usage(program_name));
        return ExitCode::SUCCESS;
    }

    let cli = match parse_cli(args) {
        Ok(cli) => cli,
        Err(message) => {
            eprintln!("{program_name}: {message}");
            eprintln!("{}", usage(program_name));
            return ExitCode::from(2);
        }
    };

    match run(cli) {
        Ok(()) => ExitCode::SUCCESS,
        Err(err) => {
            eprintln!("{program_name}: {err}");
            ExitCode::from(1)
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Mode {
    Create,
    Extract,
    List,
}

#[derive(Debug)]
pub struct Cli {
    pub mode: Mode,
    pub archive: OsString,
    pub chdir: Option<PathBuf>,
    pub path: Option<PathBuf>,
}

#[derive(Clone, Copy)]
pub(crate) struct UnixMeta {
    pub mode: u32,
    pub mtime: u32,
    pub uid: u32,
    pub gid: u32,
}

pub(crate) enum CreateNode {
    Directory {
        archive_path: String,
        meta: UnixMeta,
    },
    File {
        archive_path: String,
        fs_path: PathBuf,
        meta: UnixMeta,
        size: u64,
    },
    Symlink {
        archive_path: String,
        target: String,
        meta: UnixMeta,
    },
    Hardlink {
        archive_path: String,
        target: String,
    },
}

pub(crate) struct DeferredHardlink {
    pub path: PathBuf,
    pub target: String,
}

pub(crate) struct DeferredDirectory {
    pub path: PathBuf,
    pub mode: u32,
    pub mtime: std::time::SystemTime,
}

pub fn parse_cli<I>(args: I) -> Result<Cli, String>
where
    I: IntoIterator<Item = OsString>,
{
    let mut mode = None;
    let mut archive = None;
    let mut chdir = None;
    let mut positional = Vec::new();
    let mut end_of_options = false;
    let mut iter = args.into_iter();

    while let Some(arg) = iter.next() {
        if !end_of_options {
            if arg == "--" {
                end_of_options = true;
                continue;
            }

            if let Some(text) = arg.to_str() {
                if text.starts_with('-') && text.len() > 1 {
                    let mut chars = text[1..].char_indices().peekable();
                    while let Some((offset, flag)) = chars.next() {
                        match flag {
                            'c' => set_mode(&mut mode, Mode::Create)?,
                            'x' => set_mode(&mut mode, Mode::Extract)?,
                            't' => set_mode(&mut mode, Mode::List)?,
                            'f' | 'C' => {
                                let value_start = 1 + offset + flag.len_utf8();
                                let value = if value_start < text.len() {
                                    OsString::from(&text[value_start..])
                                } else {
                                    iter.next().ok_or_else(|| {
                                        format!("option -{flag} requires an argument")
                                    })?
                                };
                                if flag == 'f' {
                                    if archive.replace(value).is_some() {
                                        return Err(String::from(
                                            "option -f specified more than once",
                                        ));
                                    }
                                } else if chdir.replace(PathBuf::from(value)).is_some() {
                                    return Err(String::from("option -C specified more than once"));
                                }
                                break;
                            }
                            _ => return Err(format!("unknown option -{flag}")),
                        }
                        if chars.peek().is_none() {
                            break;
                        }
                    }
                    continue;
                }
            }
        }

        positional.push(PathBuf::from(arg));
    }

    let mode = mode.ok_or_else(|| String::from("one of -c, -x, or -t is required"))?;
    let archive = archive.ok_or_else(|| String::from("option -f is required"))?;

    if chdir.is_some() && mode == Mode::List {
        return Err(String::from("option -C is only valid with -c or -x"));
    }

    let path = match mode {
        Mode::Create => {
            if positional.len() != 1 {
                return Err(String::from("mode -c requires exactly one PATH argument"));
            }
            Some(positional.remove(0))
        }
        Mode::Extract => {
            if positional.len() > 1 {
                return Err(String::from("mode -x accepts at most one PATH argument"));
            }
            positional.pop()
        }
        Mode::List => {
            if !positional.is_empty() {
                return Err(String::from("mode -t does not accept PATH arguments"));
            }
            None
        }
    };

    Ok(Cli {
        mode,
        archive,
        chdir,
        path,
    })
}

fn set_mode(mode: &mut Option<Mode>, next: Mode) -> Result<(), String> {
    if let Some(current) = mode {
        if *current != next {
            return Err(String::from(
                "exactly one of -c, -x, or -t may be specified",
            ));
        }
    } else {
        *mode = Some(next);
    }
    Ok(())
}

pub(crate) fn resolve_source_path(base_dir: &Path, input: &Path) -> io::Result<PathBuf> {
    if input.is_absolute() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!(
                "absolute input paths are not supported: {}",
                input.display()
            ),
        ));
    }
    Ok(base_dir.join(input))
}

pub(crate) fn normalize_input_path(path: &Path) -> io::Result<String> {
    normalize_relative_path(path)
}

pub(crate) fn sanitize_archive_member(path: &str) -> io::Result<PathBuf> {
    Ok(PathBuf::from(normalize_relative_path(Path::new(path))?))
}

pub(crate) fn sanitize_symlink_target(target: &str) -> io::Result<&str> {
    for component in Path::new(target).components() {
        match component {
            Component::ParentDir => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!("symlink target escapes destination: {target}"),
                ));
            }
            Component::RootDir | Component::Prefix(_) => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!("absolute symlink target is not allowed: {target}"),
                ));
            }
            _ => {}
        }
    }
    Ok(target)
}

fn normalize_relative_path(path: &Path) -> io::Result<String> {
    let mut parts = Vec::new();
    let mut saw_cur_dir = false;

    for component in path.components() {
        match component {
            Component::Normal(part) => {
                let part = part.to_str().ok_or_else(|| {
                    io::Error::new(
                        io::ErrorKind::InvalidData,
                        "non-UTF-8 path is not supported",
                    )
                })?;
                parts.push(part.to_string());
            }
            Component::CurDir => {
                saw_cur_dir = true;
            }
            Component::RootDir | Component::Prefix(_) => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!("absolute path is not allowed: {}", path.display()),
                ));
            }
            Component::ParentDir => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!("path escapes destination: {}", path.display()),
                ));
            }
        }
    }

    if parts.is_empty() {
        if saw_cur_dir {
            return Ok(String::from("."));
        }
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("empty path is not allowed: {}", path.display()),
        ));
    }

    Ok(parts.join("/"))
}

pub(crate) fn ensure_directory_path(path: &str) -> String {
    if path.ends_with('/') {
        path.to_string()
    } else {
        format!("{path}/")
    }
}

pub(crate) fn join_archive_path(parent: &str, child: &str) -> String {
    if parent.is_empty() {
        child.to_string()
    } else {
        format!("{parent}/{child}")
    }
}

pub(crate) fn matches_filter(path: &str, filter: Option<&str>) -> bool {
    let Some(filter) = filter else {
        return true;
    };
    let path = path.trim_end_matches('/');
    let filter = filter.trim_end_matches('/');
    path == filter
        || path
            .strip_prefix(filter)
            .is_some_and(|rest| rest.starts_with('/'))
}

fn run(cli: Cli) -> io::Result<()> {
    #[cfg(feature = "smol")]
    {
        return block_on(run_async(cli));
    }

    #[cfg(feature = "tokio")]
    {
        Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("build tokio runtime")
            .block_on(run_async(cli))
    }
}

async fn run_async(cli: Cli) -> io::Result<()> {
    match cli.mode {
        Mode::Create => run_create(cli).await,
        Mode::Extract => run_extract(cli).await,
        Mode::List => run_list(cli).await,
    }
}

async fn run_create(cli: Cli) -> io::Result<()> {
    let source_arg = cli.path.expect("create mode requires a path");
    let base_dir = cli.chdir.unwrap_or_else(|| PathBuf::from("."));
    let source_path = resolve_source_path(&base_dir, &source_arg)?;
    let archive_root = normalize_input_path(&source_arg)?;
    let mut nodes = Vec::new();
    let mut hardlinks = HashMap::new();
    collect_create_nodes(&source_path, &archive_root, &mut nodes, &mut hardlinks).await?;

    let mut tar = TarWriter::new(open_archive_writer(&cli.archive).await?);

    for node in nodes {
        match node {
            CreateNode::Directory { archive_path, meta } => {
                tar.send(TarEntry::from(
                    TarDirectory::new(archive_path)
                        .with_uid(meta.uid)
                        .with_gid(meta.gid)
                        .with_mode(meta.mode)
                        .with_mtime(unix_time(meta.mtime)),
                ))
                .await?;
            }
            CreateNode::File {
                archive_path,
                fs_path,
                meta,
                size,
            } => {
                let reader: Box<dyn AsyncRead + Unpin + Send> =
                    Box::new(File::open(&fs_path).await?);
                tar.send(TarEntry::from(
                    TarRegularFile::new(archive_path, size, reader)
                        .with_uid(meta.uid)
                        .with_gid(meta.gid)
                        .with_mode(meta.mode)
                        .with_mtime(unix_time(meta.mtime)),
                ))
                .await?;
            }
            CreateNode::Symlink {
                archive_path,
                target,
                meta,
            } => {
                tar.send(TarEntry::from(
                    TarSymlink::new(archive_path, target)
                        .with_uid(meta.uid)
                        .with_gid(meta.gid)
                        .with_mode(meta.mode)
                        .with_mtime(unix_time(meta.mtime)),
                ))
                .await?;
            }
            CreateNode::Hardlink {
                archive_path,
                target,
            } => {
                tar.send(TarEntry::from(TarLink::new(archive_path, target)))
                    .await?;
            }
        }
    }

    tar.close().await
}

async fn run_list(cli: Cli) -> io::Result<()> {
    let mut tar = TarReader::new(open_archive_reader(&cli.archive).await?);
    let mut stdout = io::stdout().lock();

    while let Some(entry) = tar.next().await {
        use std::io::Write as _;

        match entry? {
            TarEntry::Directory(dir) => writeln!(stdout, "{}", dir.path())?,
            TarEntry::File(file) => writeln!(stdout, "{}", file.path())?,
            TarEntry::Symlink(link) => writeln!(stdout, "{}", link.path())?,
            TarEntry::Link(link) => writeln!(stdout, "{}", link.path())?,
            TarEntry::Fifo(fifo) => writeln!(stdout, "{}", fifo.path())?,
            TarEntry::Device(device) => writeln!(stdout, "{}", device.path())?,
        }
    }

    Ok(())
}

async fn run_extract(cli: Cli) -> io::Result<()> {
    let destination = cli.chdir.unwrap_or_else(|| PathBuf::from("."));
    let filter = match cli.path {
        Some(path) => Some(normalize_input_path(&path)?),
        None => None,
    };
    let mut tar = TarReader::new(open_archive_reader(&cli.archive).await?);
    let mut hardlinks = Vec::new();
    let mut directories = Vec::new();

    while let Some(entry) = tar.next().await {
        match entry? {
            TarEntry::Directory(dir) => {
                if !matches_filter(dir.path(), filter.as_deref()) {
                    continue;
                }
                let relative = sanitize_archive_member(dir.path())?;
                let full_path = destination.join(&relative);
                create_or_reuse_directory(&full_path).await?;
                directories.push(DeferredDirectory {
                    path: full_path,
                    mode: dir.mode(),
                    mtime: dir.mtime(),
                });
            }
            TarEntry::File(mut file) => {
                if !matches_filter(file.path(), filter.as_deref()) {
                    continue;
                }
                let relative = sanitize_archive_member(file.path())?;
                let full_path = destination.join(&relative);
                create_parent_directories(&full_path).await?;
                remove_existing_non_directory(&full_path).await?;
                let mut out = File::create(&full_path).await?;
                copy(&mut file, &mut out).await?;
                finalize_extracted_file(&mut out).await?;
                drop(out);
                set_mode_and_mtime(&full_path, file.mode(), file.mtime()).await?;
            }
            TarEntry::Symlink(link) => {
                if !matches_filter(link.path(), filter.as_deref()) {
                    continue;
                }
                let relative = sanitize_archive_member(link.path())?;
                let full_path = destination.join(&relative);
                create_parent_directories(&full_path).await?;
                remove_existing_non_directory(&full_path).await?;
                let target = sanitize_symlink_target(link.link())?;
                create_symlink(target, &full_path).await?;
            }
            TarEntry::Link(link) => {
                if !matches_filter(link.path(), filter.as_deref()) {
                    continue;
                }
                let relative = sanitize_archive_member(link.path())?;
                hardlinks.push(DeferredHardlink {
                    path: destination.join(relative),
                    target: link.link().to_string(),
                });
            }
            TarEntry::Fifo(fifo) => {
                return Err(io::Error::other(format!(
                    "unsupported fifo entry during extraction: {}",
                    fifo.path()
                )));
            }
            TarEntry::Device(device) => {
                return Err(io::Error::other(format!(
                    "unsupported device entry during extraction: {}",
                    device.path()
                )));
            }
        }
    }

    for hardlink in hardlinks {
        let target = destination.join(sanitize_archive_member(&hardlink.target)?);
        match fs::symlink_metadata(&target).await {
            Ok(_) => {}
            Err(err) if err.kind() == io::ErrorKind::NotFound => {
                return Err(io::Error::new(
                    io::ErrorKind::NotFound,
                    format!(
                        "hardlink target does not exist: {} -> {}",
                        hardlink.path.display(),
                        hardlink.target
                    ),
                ));
            }
            Err(err) => return Err(err),
        }
        create_parent_directories(&hardlink.path).await?;
        remove_existing_non_directory(&hardlink.path).await?;
        fs::hard_link(&target, &hardlink.path).await?;
    }

    for dir in directories.into_iter().rev() {
        set_mode_and_mtime(&dir.path, dir.mode, dir.mtime).await?;
    }

    Ok(())
}

async fn collect_create_nodes(
    fs_path: &Path,
    archive_path: &str,
    nodes: &mut Vec<CreateNode>,
    hardlinks: &mut HashMap<(u64, u64), String>,
) -> io::Result<()> {
    let mut stack = vec![(fs_path.to_path_buf(), archive_path.to_string())];

    while let Some((fs_path, archive_path)) = stack.pop() {
        let metadata = fs::symlink_metadata(&fs_path).await?;
        let meta = unix_meta(&metadata);
        let file_type = metadata.file_type();

        if file_type.is_dir() {
            nodes.push(CreateNode::Directory {
                archive_path: ensure_directory_path(&archive_path),
                meta,
            });

            let mut children = collect_directory_children(&fs_path).await?;
            children.sort_by(|left, right| left.0.cmp(&right.0));

            for (name, child_path) in children.into_iter().rev() {
                let child_archive_path = join_archive_path(&archive_path, &name);
                stack.push((child_path, child_archive_path));
            }

            continue;
        }

        if file_type.is_symlink() {
            let target = fs::read_link(&fs_path).await?;
            let target = target.to_str().ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    "non-UTF-8 symlink target is not supported",
                )
            })?;
            nodes.push(CreateNode::Symlink {
                archive_path: archive_path.to_string(),
                target: target.to_string(),
                meta,
            });
            continue;
        }

        if file_type.is_file() {
            if metadata.nlink() > 1 {
                let key = (metadata.dev(), metadata.ino());
                if let Some(first_path) = hardlinks.get(&key) {
                    nodes.push(CreateNode::Hardlink {
                        archive_path: archive_path.to_string(),
                        target: first_path.clone(),
                    });
                    continue;
                }
                hardlinks.insert(key, archive_path.to_string());
            }

            nodes.push(CreateNode::File {
                archive_path: archive_path.to_string(),
                fs_path,
                meta,
                size: metadata.len(),
            });
            continue;
        }

        let kind = if file_type.is_char_device() {
            "character device"
        } else if file_type.is_block_device() {
            "block device"
        } else if file_type.is_fifo() {
            "fifo"
        } else if file_type.is_socket() {
            "socket"
        } else {
            "unknown file type"
        };
        return Err(io::Error::other(format!(
            "unsupported filesystem entry for archiving: {} ({kind})",
            fs_path.display()
        )));
    }

    Ok(())
}

async fn open_archive_reader(archive: &OsString) -> io::Result<Box<dyn AsyncRead + Unpin + Send>> {
    if archive == "-" {
        return Ok(stdin_archive_reader());
    }
    Ok(Box::new(File::open(PathBuf::from(archive)).await?))
}

async fn open_archive_writer(archive: &OsString) -> io::Result<Box<dyn AsyncWrite + Unpin + Send>> {
    if archive == "-" {
        return Ok(stdout_archive_writer());
    }
    Ok(Box::new(File::create(PathBuf::from(archive)).await?))
}

#[cfg(feature = "smol")]
fn stdin_archive_reader() -> Box<dyn AsyncRead + Unpin + Send> {
    Box::new(AllowStdIo::new(io::stdin()))
}

#[cfg(feature = "smol")]
fn stdout_archive_writer() -> Box<dyn AsyncWrite + Unpin + Send> {
    Box::new(AllowStdIo::new(io::stdout()))
}

#[cfg(feature = "tokio")]
fn stdin_archive_reader() -> Box<dyn AsyncRead + Unpin + Send> {
    Box::new(::tokio::io::stdin())
}

#[cfg(feature = "tokio")]
fn stdout_archive_writer() -> Box<dyn AsyncWrite + Unpin + Send> {
    Box::new(::tokio::io::stdout())
}

#[cfg(feature = "smol")]
async fn collect_directory_children(fs_path: &Path) -> io::Result<Vec<(String, PathBuf)>> {
    let mut children = Vec::new();
    let mut read_dir = fs::read_dir(fs_path).await?;
    while let Some(child) = read_dir.next().await {
        let child = child?;
        let name = child.file_name();
        let name = name.to_str().ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "non-UTF-8 path is not supported",
            )
        })?;
        children.push((name.to_string(), child.path()));
    }
    Ok(children)
}

#[cfg(feature = "tokio")]
async fn collect_directory_children(fs_path: &Path) -> io::Result<Vec<(String, PathBuf)>> {
    let mut children = Vec::new();
    let mut read_dir = fs::read_dir(fs_path).await?;
    while let Some(child) = read_dir.next_entry().await? {
        let name = child.file_name();
        let name = name.to_str().ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "non-UTF-8 path is not supported",
            )
        })?;
        children.push((name.to_string(), child.path()));
    }
    Ok(children)
}

#[cfg(feature = "smol")]
fn unix_meta(metadata: &fs::Metadata) -> UnixMeta {
    let raw_mtime = metadata.mtime();
    let mtime = if raw_mtime < 0 {
        0
    } else {
        raw_mtime.min(i64::from(u32::MAX)) as u32
    };
    UnixMeta {
        mode: metadata.mode() & 0o7777,
        mtime,
        uid: metadata.uid(),
        gid: metadata.gid(),
    }
}

#[cfg(feature = "tokio")]
fn unix_meta(metadata: &Metadata) -> UnixMeta {
    let raw_mtime = metadata.mtime();
    let mtime = if raw_mtime < 0 {
        0
    } else {
        raw_mtime.min(i64::from(u32::MAX)) as u32
    };
    UnixMeta {
        mode: metadata.mode() & 0o7777,
        mtime,
        uid: metadata.uid(),
        gid: metadata.gid(),
    }
}

async fn create_parent_directories(path: &Path) -> io::Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).await?;
    }
    Ok(())
}

async fn create_or_reuse_directory(path: &Path) -> io::Result<()> {
    match fs::symlink_metadata(path).await {
        Ok(metadata) => {
            if metadata.file_type().is_dir() {
                Ok(())
            } else {
                Err(io::Error::new(
                    io::ErrorKind::AlreadyExists,
                    format!(
                        "cannot replace non-directory with directory: {}",
                        path.display()
                    ),
                ))
            }
        }
        Err(err) if err.kind() == io::ErrorKind::NotFound => {
            fs::create_dir_all(path).await?;
            Ok(())
        }
        Err(err) => Err(err),
    }
}

async fn remove_existing_non_directory(path: &Path) -> io::Result<()> {
    match fs::symlink_metadata(path).await {
        Ok(metadata) => {
            if metadata.file_type().is_dir() {
                Err(io::Error::new(
                    io::ErrorKind::AlreadyExists,
                    format!("cannot overwrite directory: {}", path.display()),
                ))
            } else {
                fs::remove_file(path).await
            }
        }
        Err(err) if err.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(err) => Err(err),
    }
}

#[cfg(feature = "smol")]
async fn create_symlink(target: &str, path: &Path) -> io::Result<()> {
    symlink(target, path).await
}

#[cfg(feature = "tokio")]
async fn create_symlink(target: &str, path: &Path) -> io::Result<()> {
    fs::symlink(target, path).await
}

#[cfg(feature = "smol")]
async fn finalize_extracted_file<W: AsyncWrite + Unpin>(writer: &mut W) -> io::Result<()> {
    writer.close().await
}

#[cfg(feature = "tokio")]
async fn finalize_extracted_file<W: AsyncWrite + Unpin>(writer: &mut W) -> io::Result<()> {
    writer.flush().await
}

async fn set_mode_and_mtime(path: &Path, mode: u32, mtime: SystemTime) -> io::Result<()> {
    fs::set_permissions(path, permissions_from_mode(mode)).await?;
    let times = std::fs::FileTimes::new().set_modified(mtime);
    std::fs::File::open(path)?.set_times(times)?;
    Ok(())
}

#[cfg(feature = "smol")]
fn permissions_from_mode(mode: u32) -> fs::Permissions {
    fs::Permissions::from_mode(mode)
}

#[cfg(feature = "tokio")]
fn permissions_from_mode(mode: u32) -> std::fs::Permissions {
    std::fs::Permissions::from_mode(mode)
}

fn unix_time(secs: u32) -> SystemTime {
    SystemTime::UNIX_EPOCH + Duration::from_secs(u64::from(secs))
}

#[cfg(test)]
mod tests {
    use super::{parse_cli, Cli, Mode};
    use std::ffi::OsString;
    use std::path::PathBuf;

    fn parse(args: &[&str]) -> Result<Cli, String> {
        parse_cli(args.iter().map(OsString::from))
    }

    #[test]
    fn parse_grouped_create_flags() {
        let cli = parse(&["-cf", "archive.tar", "-Csrc", "tree"]).unwrap();
        assert_eq!(cli.mode, Mode::Create);
        assert_eq!(cli.archive, "archive.tar");
        assert_eq!(cli.chdir, Some(PathBuf::from("src")));
        assert_eq!(cli.path, Some(PathBuf::from("tree")));
    }

    #[test]
    fn parse_rejects_c_for_list() {
        let err = parse(&["-t", "-f", "archive.tar", "-C", "out"]).unwrap_err();
        assert!(err.contains("option -C is only valid"));
    }

    #[test]
    fn parse_rejects_multiple_modes() {
        let err = parse(&["-cx", "-f", "archive.tar", "tree"]).unwrap_err();
        assert!(err.contains("exactly one of -c, -x, or -t"));
    }

    #[test]
    fn parse_accepts_current_directory_path() {
        let cli = parse(&["-cf-", "."]).unwrap();
        assert_eq!(cli.mode, Mode::Create);
        assert_eq!(cli.archive, "-");
        assert_eq!(cli.path, Some(PathBuf::from(".")));
    }
}
