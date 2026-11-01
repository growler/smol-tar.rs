#[cfg(all(feature = "smol", feature = "tokio"))]
compile_error!("features `smol` and `tokio` are mutually exclusive");
#[cfg(not(any(feature = "smol", feature = "tokio")))]
compile_error!("either feature `smol` or `tokio` must be enabled");

#[cfg(all(feature = "smol", not(feature = "tokio")))]
use ::smol::fs::{self, File};
use ::smol_tar::{
    TarDevice, TarDirectory, TarEntry, TarFifo, TarLink, TarReader, TarRegularFile, TarSymlink,
    TarWriter,
};
use ::smol_tar_fs::{
    DeviceNodes, FsEntry, FsKind, FsSink, FsSource, FsStreamExt, MissingLinkTarget, ModePolicy,
};
#[cfg(feature = "tokio")]
use ::tokio::{
    fs::{self, File},
    io::{AsyncRead, AsyncWrite, BufReader, BufWriter},
    runtime::Builder,
};
#[cfg(all(feature = "smol", not(feature = "tokio")))]
use async_compression::futures::{bufread as decode, write as encode};
#[cfg(feature = "tokio")]
use async_compression::tokio::{bufread as decode, write as encode};
#[cfg(all(feature = "smol", not(feature = "tokio")))]
use futures::{
    executor::block_on,
    io::{AllowStdIo, AsyncRead, AsyncWrite, BufReader, BufWriter},
};
use futures::{future, stream, StreamExt, TryStreamExt};
use std::{
    ffi::OsString,
    io, mem,
    os::unix::fs::MetadataExt,
    path::{Component, Path, PathBuf},
    process::ExitCode,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
};
#[cfg(feature = "tokio")]
use std::{
    pin::Pin,
    task::{ready, Context, Poll},
};

/// Size of the buffer around the archive file or stream.
const ARCHIVE_BUFFER_SIZE: usize = 256 * 1024;

fn program_name() -> &'static str {
    env!("CARGO_PKG_NAME")
}

fn usage(program_name: &str) -> String {
    format!(
        "usage:
  {program_name} -c -f ARCHIVE [-C DIR] [OPTIONS] PATH...
  {program_name} -x -f ARCHIVE [-C DIR] [OPTIONS] [PATH...]
  {program_name} -t -f ARCHIVE [OPTIONS]

options:
  -f ARCHIVE                  archive file, or - for stdin or stdout
  -C DIR                      source directory (-c) or destination (-x)
  -z, --gzip                  use gzip compression
  -J, --xz, --xzip            use xz compression
      --zstd                  use zstd compression
  -v, --verbose               write each entry path to stderr (-c, -x)
      --exclude PATH          skip the entries at or below PATH (-c, -x)
      --xattrs                read or write extended attributes (-c, -x)
  -p, --preserve-permissions  keep entry modes and setuid/setgid/sticky (-x)
      --same-permissions      same as -p; the default for root (-x)
      --no-same-permissions   apply the umask (-x); default if not root
  -m, --touch                 do not set the modification time (-x)
      --same-owner            set the archive owner (-x); default for root
      --no-same-owner         do not set the owner (-x); default if not root
      --devices               create device nodes (-x)
      --skip-missing-links    skip hard links without a target (-x)
      --strip-components N    remove N leading path components (-x)"
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

    let mut cli = match parse_cli(args) {
        Ok(cli) => cli,
        Err(message) => {
            eprintln!("{program_name}: {message}");
            eprintln!("{}", usage(program_name));
            return ExitCode::from(2);
        }
    };
    // The process has one thread here. Thus the short change of the umask
    // has no effect on other file creates.
    cli.options.umask = process_umask();
    // SAFETY: `geteuid` has no preconditions.
    cli.options.is_root = unsafe { libc::geteuid() } == 0;

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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Compression {
    Gzip,
    Xz,
    Zstd,
}

#[derive(Debug)]
pub struct Cli {
    pub mode: Mode,
    pub archive: OsString,
    pub chdir: Option<PathBuf>,
    pub paths: Vec<PathBuf>,
    pub options: Options,
}

#[derive(Debug, Default)]
pub struct Options {
    pub compression: Option<Compression>,
    pub verbose: bool,
    pub excludes: Vec<PathBuf>,
    pub xattrs: bool,
    /// -p or --no-same-permissions. None: no option given.
    pub preserve_permissions: Option<bool>,
    pub touch: bool,
    /// --same-owner or --no-same-owner. None: no option given.
    pub same_owner: Option<bool>,
    pub devices: bool,
    pub skip_missing_links: bool,
    pub strip_components: usize,
    /// Umask of the process. `main` sets it after the parse.
    pub umask: u32,
    /// The effective uid is 0. `main` sets it after the parse.
    pub is_root: bool,
}

/// Give the umask of the process. The process must have one thread.
// `mode_t` is `u32` on Linux and `u16` on Apple platforms.
#[allow(clippy::useless_conversion)]
fn process_umask() -> u32 {
    // SAFETY: `umask` has no preconditions. The second call restores the
    // umask of the first call.
    let umask = unsafe {
        let umask = libc::umask(0o022);
        libc::umask(umask);
        umask
    };
    umask.into()
}

/// Give the value of an extract option that is on for root by default:
/// `--same-permissions` or `--same-owner`. Without an option, the value is
/// true only for root.
fn root_default(flag: Option<bool>, is_root: bool) -> bool {
    flag.unwrap_or(is_root)
}

pub fn parse_cli<I>(args: I) -> Result<Cli, String>
where
    I: IntoIterator<Item = OsString>,
{
    let mut mode = None;
    let mut archive = None;
    let mut chdir = None;
    let mut options = Options::default();
    let mut paths = Vec::new();
    let mut end_of_options = false;
    let mut iter = args.into_iter();

    while let Some(arg) = iter.next() {
        if !end_of_options {
            if arg == "--" {
                end_of_options = true;
                continue;
            }

            if let Some(text) = arg.to_str() {
                if let Some(option) = text.strip_prefix("--") {
                    parse_long_option(option, &mut iter, &mut options)?;
                    continue;
                }

                if text.starts_with('-') && text.len() > 1 {
                    let mut chars = text[1..].char_indices().peekable();
                    while let Some((offset, flag)) = chars.next() {
                        match flag {
                            'c' => set_mode(&mut mode, Mode::Create)?,
                            'x' => set_mode(&mut mode, Mode::Extract)?,
                            't' => set_mode(&mut mode, Mode::List)?,
                            'z' => set_compression(&mut options.compression, Compression::Gzip)?,
                            'J' => set_compression(&mut options.compression, Compression::Xz)?,
                            'v' => options.verbose = true,
                            'p' => options.preserve_permissions = Some(true),
                            'm' => options.touch = true,
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

        paths.push(PathBuf::from(arg));
    }

    let mode = mode.ok_or_else(|| String::from("one of -c, -x, or -t is required"))?;
    let archive = archive.ok_or_else(|| String::from("option -f is required"))?;

    if mode == Mode::List {
        for (used, option) in [
            (chdir.is_some(), "-C"),
            (options.verbose, "-v"),
            (!options.excludes.is_empty(), "--exclude"),
            (options.xattrs, "--xattrs"),
        ] {
            if used {
                return Err(format!("option {option} is only valid with -c or -x"));
            }
        }
    }

    if mode != Mode::Extract {
        for (used, option) in [
            (options.preserve_permissions == Some(true), "-p"),
            (
                options.preserve_permissions == Some(false),
                "--no-same-permissions",
            ),
            (options.touch, "-m"),
            (options.same_owner == Some(true), "--same-owner"),
            (options.same_owner == Some(false), "--no-same-owner"),
            (options.devices, "--devices"),
            (options.skip_missing_links, "--skip-missing-links"),
            (options.strip_components > 0, "--strip-components"),
        ] {
            if used {
                return Err(format!("option {option} is only valid with -x"));
            }
        }
    }

    match mode {
        Mode::Create if paths.is_empty() => {
            return Err(String::from("mode -c requires at least one PATH argument"));
        }
        Mode::List if !paths.is_empty() => {
            return Err(String::from("mode -t does not accept PATH arguments"));
        }
        _ => {}
    }

    Ok(Cli {
        mode,
        archive,
        chdir,
        paths,
        options,
    })
}

/// Parse one long option. `option` is the argument without the leading `--`.
fn parse_long_option(
    option: &str,
    args: &mut impl Iterator<Item = OsString>,
    options: &mut Options,
) -> Result<(), String> {
    let (name, inline) = match option.split_once('=') {
        Some((name, value)) => (name, Some(value)),
        None => (option, None),
    };

    match name {
        "gzip" => set_compression(&mut options.compression, Compression::Gzip)?,
        "xz" | "xzip" => set_compression(&mut options.compression, Compression::Xz)?,
        "zstd" => set_compression(&mut options.compression, Compression::Zstd)?,
        "verbose" => options.verbose = true,
        "xattrs" => options.xattrs = true,
        "preserve-permissions" | "same-permissions" => options.preserve_permissions = Some(true),
        "no-same-permissions" => options.preserve_permissions = Some(false),
        "touch" => options.touch = true,
        "same-owner" => options.same_owner = Some(true),
        "no-same-owner" => options.same_owner = Some(false),
        "devices" => options.devices = true,
        "skip-missing-links" => options.skip_missing_links = true,
        "exclude" => {
            let value = option_value(name, inline, args)?;
            options.excludes.push(PathBuf::from(value));
            return Ok(());
        }
        "strip-components" => {
            let value = option_value(name, inline, args)?;
            options.strip_components = value
                .to_str()
                .and_then(|value| value.parse().ok())
                .ok_or_else(|| {
                    format!(
                        "option --strip-components requires a number: {}",
                        value.to_string_lossy()
                    )
                })?;
            return Ok(());
        }
        _ => return Err(format!("unknown option --{name}")),
    }

    if inline.is_some() {
        return Err(format!("option --{name} does not take an argument"));
    }
    Ok(())
}

/// Give the value of a long option: the text after `=`, or the next argument.
fn option_value(
    name: &str,
    inline: Option<&str>,
    args: &mut impl Iterator<Item = OsString>,
) -> Result<OsString, String> {
    match inline {
        Some(value) => Ok(OsString::from(value)),
        None => args
            .next()
            .ok_or_else(|| format!("option --{name} requires an argument")),
    }
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

fn set_compression(compression: &mut Option<Compression>, next: Compression) -> Result<(), String> {
    match compression {
        Some(current) if *current != next => Err(String::from(
            "only one of -z, -J, or --zstd may be specified",
        )),
        _ => {
            *compression = Some(next);
            Ok(())
        }
    }
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

fn normalize_input_paths(paths: &[PathBuf]) -> io::Result<Vec<String>> {
    paths
        .iter()
        .map(|path| normalize_input_path(path))
        .collect()
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

/// Give `true` if `path` is `prefix` or is below `prefix`. Empty and `.`
/// components do not count. The prefix `.` matches all paths.
pub(crate) fn is_under(path: &str, prefix: &str) -> bool {
    let mut path = path_components(path);
    path_components(prefix).all(|component| path.next() == Some(component))
}

/// Give the components of `path` without the empty and `.` components.
fn path_components(path: &str) -> impl Iterator<Item = &str> {
    path.split('/')
        .filter(|component| !component.is_empty() && *component != ".")
}

/// Give `true` if `path` is below one of `filters` and below none of
/// `excludes`. An empty `filters` selects all paths.
pub(crate) fn is_selected(path: &str, filters: &[String], excludes: &[String]) -> bool {
    (filters.is_empty() || filters.iter().any(|filter| is_under(path, filter)))
        && !excludes.iter().any(|exclude| is_under(path, exclude))
}

/// Remove the first `count` components of `path`. Empty components count
/// as nothing. Give `None` if no component stays. A path that starts with
/// `/` or has a `..` component does not change. Thus `FsSink` rejects it.
pub(crate) fn strip_path(path: &str, count: usize) -> Option<String> {
    if path.starts_with('/') || path.split('/').any(|component| component == "..") {
        return Some(path.to_owned());
    }
    let rest = path
        .split('/')
        .filter(|component| !component.is_empty())
        .skip(count)
        .collect::<Vec<_>>();
    if rest.is_empty() {
        return None;
    }
    let mut stripped = rest.join("/");
    if path.ends_with('/') {
        stripped.push('/');
    }
    Some(stripped)
}

/// Copy the metadata that `FsSink` uses from the entry `$old` to `$new`.
/// The macro reads the metadata before `$new`, thus `$new` can move `$old`.
macro_rules! with_metadata {
    ($new:expr, $old:ident) => {{
        let (mode, uid, gid, mtime) = ($old.mode(), $old.uid(), $old.gid(), $old.mtime());
        let attrs = mem::take($old.attrs_mut());
        $new.with_mode(mode)
            .with_uid(uid)
            .with_gid(gid)
            .with_mtime(mtime)
            .with_attrs(attrs)
    }};
}

/// Remove the first `count` components from the path of `entry`, and from
/// the target of a hard link. Give `None` if no component stays. For a hard
/// link whose target has no component that stays, give `None` if
/// `skip_missing_links` is `true`, and an error if it is `false`.
fn strip_entry<'a, R: AsyncRead + Unpin + 'a>(
    entry: TarEntry<'a, R>,
    count: usize,
    skip_missing_links: bool,
) -> io::Result<Option<TarEntry<'a, TarRegularFile<'a, R>>>> {
    let Some(path) = strip_path(entry.path(), count) else {
        return Ok(None);
    };
    Ok(Some(match entry {
        // The old entry is the body of the new entry.
        TarEntry::File(mut file) => {
            with_metadata!(TarRegularFile::new(path, file.size(), file), file).into()
        }
        TarEntry::Directory(mut dir) => with_metadata!(TarDirectory::new(path), dir).into(),
        TarEntry::Symlink(mut link) => {
            with_metadata!(TarSymlink::new(path, link.link()), link).into()
        }
        TarEntry::Fifo(mut fifo) => with_metadata!(TarFifo::new(path), fifo).into(),
        TarEntry::Device(mut device) => {
            let stripped = if device.is_char() {
                TarDevice::new_char(path, device.major(), device.minor())
            } else {
                TarDevice::new_block(path, device.major(), device.minor())
            };
            with_metadata!(stripped, device).into()
        }
        TarEntry::Link(link) => match strip_path(link.link(), count) {
            Some(target) => TarLink::new(path, target).into(),
            None if skip_missing_links => return Ok(None),
            None => {
                return Err(io::Error::new(
                    io::ErrorKind::NotFound,
                    format!(
                        "{}: link target {} has no component after --strip-components",
                        link.path(),
                        link.link()
                    ),
                ))
            }
        },
    }))
}

fn run(cli: Cli) -> io::Result<()> {
    #[cfg(all(feature = "smol", not(feature = "tokio")))]
    {
        block_on(run_async(cli))
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
    let base_dir = cli.chdir.unwrap_or_else(|| PathBuf::from("."));
    let options = cli.options;
    let excludes = normalize_input_paths(&options.excludes)?;
    let mut sources = Vec::with_capacity(cli.paths.len());
    for path in &cli.paths {
        let excludes = excludes.clone();
        sources.push(
            FsSource::new(resolve_source_path(&base_dir, path)?)
                .with_prefix(normalize_input_path(path)?)
                .with_xattrs(options.xattrs)
                // The source does not read or walk an excluded node.
                .with_filter(move |path| is_selected(path, &[], &excludes)),
        );
    }
    let target = open_create_target(&cli.archive).await?;
    let mut tar = TarWriter::new(archive_writer(target.writer, options.compression));

    // One `into_tar` for all sources also finds the hard links between them.
    let result = stream::iter(sources)
        .flatten()
        .try_filter(|entry| future::ready(!is_archive_file(entry, &target.skip)))
        .into_tar()
        .inspect_ok(|entry| {
            if options.verbose {
                eprintln!("{}", entry.path());
            }
        })
        .forward(&mut tar)
        .await;

    let Some((temp, path)) = target.rename else {
        return result;
    };
    let result = match result {
        Ok(()) => fs::rename(&temp, &path).await,
        Err(err) => Err(err),
    };
    if result.is_err() {
        // The old archive, if any, does not change.
        let _ = fs::remove_file(&temp).await;
    }
    result
}

/// Archive file of create mode.
struct CreateTarget {
    writer: Box<dyn AsyncWrite + Unpin + Send>,
    /// Temporary file, and the path that it replaces after a successful run.
    rename: Option<(PathBuf, PathBuf)>,
    /// Name, device and inode of each file that the archive must not hold.
    skip: Vec<(OsString, u64, u64)>,
}

/// Open the archive file of create mode. For `-`, write to stdout. For a
/// regular file or a missing file, write to a temporary file in the same
/// directory. For another kind of file, for example a FIFO, write to the
/// file.
async fn open_create_target(archive: &OsString) -> io::Result<CreateTarget> {
    let mut target = CreateTarget {
        writer: stdout_archive_writer(),
        rename: None,
        skip: Vec::new(),
    };
    if archive == "-" {
        return Ok(target);
    }
    let archive = PathBuf::from(archive);
    let (path, permissions) = match fs::metadata(&archive).await {
        Ok(metadata) if !metadata.is_file() => {
            target.writer = Box::new(File::create(&archive).await?);
            return Ok(target);
        }
        // Replace the file, not a symlink to it.
        Ok(metadata) => {
            let path = fs::canonicalize(&archive).await?;
            target.skip.push(file_key(&path, &metadata));
            (path, Some(metadata.permissions()))
        }
        // A symlink to a missing file. Create the file through the symlink.
        Err(err)
            if err.kind() == io::ErrorKind::NotFound
                && fs::symlink_metadata(&archive).await.is_ok() =>
        {
            let file = File::create(&archive).await?;
            let path = fs::canonicalize(&archive).await?;
            target.skip.push(file_key(&path, &file.metadata().await?));
            target.writer = Box::new(file);
            return Ok(target);
        }
        Err(err) if err.kind() == io::ErrorKind::NotFound => (archive, None),
        Err(err) => return Err(err),
    };
    let name = path.file_name().ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("archive path has no file name: {}", path.display()),
        )
    })?;
    let mut temp_name = OsString::from(".");
    temp_name.push(name);
    temp_name.push(format!(".{}.tmp", std::process::id()));
    let temp = path.with_file_name(temp_name);
    let file = fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&temp)
        .await?;
    // The new archive keeps the permissions of the old archive.
    if let Some(permissions) = permissions {
        if let Err(err) = fs::set_permissions(&temp, permissions).await {
            let _ = fs::remove_file(&temp).await;
            return Err(err);
        }
    }
    target.skip.push(file_key(&temp, &file.metadata().await?));
    target.writer = Box::new(file);
    target.rename = Some((temp, path));
    Ok(target)
}

/// Give the name, the device and the inode of the file at `path`.
fn file_key(path: &Path, metadata: &std::fs::Metadata) -> (OsString, u64, u64) {
    let name = path.file_name().unwrap_or_default().to_owned();
    (name, metadata.dev(), metadata.ino())
}

/// Give `true` if `entry` is one of the files in `skip`.
fn is_archive_file(entry: &FsEntry, skip: &[(OsString, u64, u64)]) -> bool {
    entry.kind() == FsKind::File
        && skip.iter().any(|(name, dev, ino)| {
            // The blocking call runs only for a file with the name of an
            // archive file.
            entry.fs_path().file_name() == Some(name.as_os_str())
                && std::fs::symlink_metadata(entry.fs_path())
                    .is_ok_and(|metadata| metadata.dev() == *dev && metadata.ino() == *ino)
        })
}

async fn run_list(cli: Cli) -> io::Result<()> {
    let mut tar = TarReader::new(open_archive_reader(&cli.archive, cli.options.compression).await?);
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
    let options = cli.options;
    let filters = normalize_input_paths(&cli.paths)?;
    let excludes = normalize_input_paths(&options.excludes)?;
    let reader = open_archive_reader(&cli.archive, options.compression).await?;
    fs::create_dir_all(&destination).await?;

    let entries = TarReader::new(reader)
        .try_filter(|entry| future::ready(is_selected(entry.path(), &filters, &excludes)));

    let owner_failed = Arc::new(AtomicBool::new(false));
    let count = options.strip_components;
    if count == 0 {
        fs_sink(&destination, &options, Arc::clone(&owner_failed))
            .extract(entries.inspect_ok(|entry| print_entry(entry, &options)))
            .await?;
    } else {
        fs_sink(&destination, &options, Arc::clone(&owner_failed))
            .extract(
                entries
                    .try_filter_map(|entry| {
                        future::ready(strip_entry(entry, count, options.skip_missing_links))
                    })
                    .inspect_ok(|entry| print_entry(entry, &options)),
            )
            .await?;
    }
    if owner_failed.load(Ordering::Relaxed) {
        return Err(io::Error::other(
            "exiting with failure status due to previous errors",
        ));
    }
    Ok(())
}

/// With `-v`, write the path of `entry` to stderr. Do not write the path of
/// a device entry that `FsSink` skips.
fn print_entry<R: AsyncRead + Unpin>(entry: &TarEntry<'_, R>, options: &Options) {
    if options.verbose && (options.devices || !matches!(entry, TarEntry::Device(_))) {
        eprintln!("{}", entry.path());
    }
}

/// Make the sink for the extraction. If the owner change of a node fails,
/// write a warning and set `owner_failed`.
fn fs_sink<'a, R: AsyncRead + Unpin + Send + 'a>(
    destination: &Path,
    options: &Options,
    owner_failed: Arc<AtomicBool>,
) -> FsSink<'a, R> {
    let keep = root_default(options.preserve_permissions, options.is_root);
    FsSink::new(destination)
        .with_mode(if keep {
            ModePolicy::Apply
        } else {
            ModePolicy::Mask(options.umask)
        })
        .with_special_bits(keep)
        .with_mtime(!options.touch)
        .with_owner(root_default(options.same_owner, options.is_root))
        .with_owner_error(move |path, err| {
            use std::io::Write as _;

            let line = format!("{}: {path}: {err}\n", program_name());
            let _ = io::stderr().write_all(line.as_bytes());
            owner_failed.store(true, Ordering::Relaxed);
            Ok(())
        })
        .with_xattrs(options.xattrs)
        .with_device_nodes(if options.devices {
            DeviceNodes::Create
        } else {
            DeviceNodes::Skip
        })
        .with_missing_link_target(if options.skip_missing_links {
            MissingLinkTarget::Skip
        } else {
            MissingLinkTarget::Error
        })
}

async fn open_archive_reader(
    archive: &OsString,
    compression: Option<Compression>,
) -> io::Result<Box<dyn AsyncRead + Unpin + Send>> {
    let source: Box<dyn AsyncRead + Unpin + Send> = if archive == "-" {
        stdin_archive_reader()
    } else {
        Box::new(File::open(PathBuf::from(archive)).await?)
    };
    let source = BufReader::with_capacity(ARCHIVE_BUFFER_SIZE, source);
    // A compressed file can hold more than one member or frame, for example
    // the output of pigz or of `cat a.gz b.gz`.
    macro_rules! decoder {
        ($decoder:ident) => {{
            let mut decoder = decode::$decoder::new(source);
            decoder.multiple_members(true);
            Box::new(decoder)
        }};
    }
    let reader: Box<dyn AsyncRead + Unpin + Send> = match compression {
        None => Box::new(source),
        Some(Compression::Gzip) => decoder!(GzipDecoder),
        Some(Compression::Xz) => decoder!(XzDecoder),
        Some(Compression::Zstd) => decoder!(ZstdDecoder),
    };
    Ok(reader)
}

fn archive_writer(
    target: Box<dyn AsyncWrite + Unpin + Send>,
    compression: Option<Compression>,
) -> Box<dyn AsyncWrite + Unpin + Send> {
    #[cfg(feature = "tokio")]
    let target: Box<dyn AsyncWrite + Unpin + Send> = Box::new(FlushOnShutdown(target));
    let target: Box<dyn AsyncWrite + Unpin + Send> = match compression {
        None => target,
        Some(Compression::Gzip) => Box::new(encode::GzipEncoder::new(target)),
        Some(Compression::Xz) => Box::new(encode::XzEncoder::new(target)),
        Some(Compression::Zstd) => Box::new(encode::ZstdEncoder::new(target)),
    };
    // TarWriter writes each header and each part of a body separately. The
    // close of the TarWriter flushes the buffer and ends the compression.
    Box::new(BufWriter::with_capacity(ARCHIVE_BUFFER_SIZE, target))
}

#[cfg(all(feature = "smol", not(feature = "tokio")))]
fn stdin_archive_reader() -> Box<dyn AsyncRead + Unpin + Send> {
    Box::new(AllowStdIo::new(io::stdin()))
}

#[cfg(all(feature = "smol", not(feature = "tokio")))]
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

/// Writer that flushes the inner writer before the shutdown of it.
///
/// The shutdown of the tokio stdout does not wait for the write that is in
/// progress. Without a flush, the archive end can get lost when the runtime
/// stops.
#[cfg(feature = "tokio")]
struct FlushOnShutdown<W>(W);

#[cfg(feature = "tokio")]
impl<W: AsyncWrite + Unpin> AsyncWrite for FlushOnShutdown<W> {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.0).poll_write(cx, buf)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.0).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        ready!(Pin::new(&mut self.0).poll_flush(cx))?;
        Pin::new(&mut self.0).poll_shutdown(cx)
    }
}

#[cfg(test)]
mod tests {
    use super::{is_selected, parse_cli, root_default, strip_path, Cli, Compression, Mode};
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
        assert_eq!(cli.paths, vec![PathBuf::from("tree")]);
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
        assert_eq!(cli.paths, vec![PathBuf::from(".")]);
    }

    #[test]
    fn parse_create_options() {
        let cli = parse(&[
            "-czvf",
            "archive.tar.gz",
            "--xattrs",
            "--exclude",
            "tree/a",
            "--exclude=tree/b",
            "tree",
            "other",
        ])
        .unwrap();
        assert_eq!(cli.options.compression, Some(Compression::Gzip));
        assert!(cli.options.verbose);
        assert!(cli.options.xattrs);
        assert_eq!(
            cli.options.excludes,
            vec![PathBuf::from("tree/a"), PathBuf::from("tree/b")]
        );
        assert_eq!(
            cli.paths,
            vec![PathBuf::from("tree"), PathBuf::from("other")]
        );
    }

    #[test]
    fn parse_extract_options() {
        let cli = parse(&[
            "-xpmJf",
            "archive.tar.xz",
            "--same-owner",
            "--devices",
            "--skip-missing-links",
            "--strip-components=2",
        ])
        .unwrap();
        assert_eq!(cli.options.compression, Some(Compression::Xz));
        assert_eq!(cli.options.preserve_permissions, Some(true));
        assert!(cli.options.touch);
        assert_eq!(cli.options.same_owner, Some(true));
        assert!(cli.options.devices);
        assert!(cli.options.skip_missing_links);
        assert_eq!(cli.options.strip_components, 2);
        assert!(cli.paths.is_empty());

        let cli = parse(&["-x", "--xzip", "--strip-components", "1", "-f", "a"]).unwrap();
        assert_eq!(cli.options.compression, Some(Compression::Xz));
        assert_eq!(cli.options.strip_components, 1);
        assert!(!cli.options.touch);
        assert_eq!(cli.options.preserve_permissions, None);

        let cli = parse(&["-xf", "a", "--touch"]).unwrap();
        assert!(cli.options.touch);
    }

    #[test]
    fn parse_rejects_extract_options_for_create() {
        let err = parse(&["-cpf", "archive.tar", "tree"]).unwrap_err();
        assert_eq!(err, "option -p is only valid with -x");
        let err = parse(&["-cmf", "archive.tar", "tree"]).unwrap_err();
        assert_eq!(err, "option -m is only valid with -x");
        let err = parse(&["-tf", "archive.tar", "--strip-components=1"]).unwrap_err();
        assert_eq!(err, "option --strip-components is only valid with -x");
    }

    #[test]
    fn parse_same_permissions_options() {
        let cli = parse(&["-xf", "a", "--same-permissions"]).unwrap();
        assert_eq!(cli.options.preserve_permissions, Some(true));
        let cli = parse(&["-xf", "a", "--no-same-permissions"]).unwrap();
        assert_eq!(cli.options.preserve_permissions, Some(false));
        let cli = parse(&["-xf", "a"]).unwrap();
        assert_eq!(cli.options.preserve_permissions, None);
        let cli = parse(&["-xpf", "a", "--no-same-permissions"]).unwrap();
        assert_eq!(cli.options.preserve_permissions, Some(false));
    }

    #[test]
    fn parse_rejects_same_permissions_options_outside_extract() {
        let err = parse(&["-cf", "a", "--no-same-permissions", "tree"]).unwrap_err();
        assert_eq!(err, "option --no-same-permissions is only valid with -x");
        let err = parse(&["-tf", "a", "--no-same-permissions"]).unwrap_err();
        assert_eq!(err, "option --no-same-permissions is only valid with -x");
        let err = parse(&["-cf", "a", "--same-permissions", "tree"]).unwrap_err();
        assert_eq!(err, "option -p is only valid with -x");
        let err = parse(&["-tf", "a", "--same-permissions"]).unwrap_err();
        assert_eq!(err, "option -p is only valid with -x");
        let err = parse(&["-tpf", "a"]).unwrap_err();
        assert_eq!(err, "option -p is only valid with -x");
    }

    #[test]
    fn parse_same_owner_options() {
        let cli = parse(&["-xf", "a", "--same-owner"]).unwrap();
        assert_eq!(cli.options.same_owner, Some(true));
        let cli = parse(&["-xf", "a", "--no-same-owner"]).unwrap();
        assert_eq!(cli.options.same_owner, Some(false));
        let cli = parse(&["-xf", "a"]).unwrap();
        assert_eq!(cli.options.same_owner, None);
        let cli = parse(&["-xf", "a", "--same-owner", "--no-same-owner"]).unwrap();
        assert_eq!(cli.options.same_owner, Some(false));
        let cli = parse(&["-xf", "a", "--no-same-owner", "--same-owner"]).unwrap();
        assert_eq!(cli.options.same_owner, Some(true));
    }

    #[test]
    fn parse_rejects_same_owner_options_outside_extract() {
        let err = parse(&["-cf", "a", "--same-owner", "tree"]).unwrap_err();
        assert_eq!(err, "option --same-owner is only valid with -x");
        let err = parse(&["-tf", "a", "--same-owner"]).unwrap_err();
        assert_eq!(err, "option --same-owner is only valid with -x");
        let err = parse(&["-cf", "a", "--no-same-owner", "tree"]).unwrap_err();
        assert_eq!(err, "option --no-same-owner is only valid with -x");
        let err = parse(&["-tf", "a", "--no-same-owner"]).unwrap_err();
        assert_eq!(err, "option --no-same-owner is only valid with -x");
    }

    #[test]
    fn root_default_depends_on_root() {
        assert!(!root_default(None, false));
        assert!(root_default(None, true));
    }

    #[test]
    fn root_default_option_overrides_root() {
        assert!(root_default(Some(true), false));
        assert!(!root_default(Some(false), true));
    }

    #[test]
    fn parse_rejects_create_options_for_list() {
        let err = parse(&["-tvf", "archive.tar"]).unwrap_err();
        assert_eq!(err, "option -v is only valid with -c or -x");
        let err = parse(&["-tf", "archive.tar", "--exclude=a"]).unwrap_err();
        assert_eq!(err, "option --exclude is only valid with -c or -x");
    }

    #[test]
    fn parse_rejects_two_compressions() {
        let err = parse(&["-tzf", "archive.tar", "--zstd"]).unwrap_err();
        assert!(err.contains("only one of -z, -J, or --zstd"));
    }

    #[test]
    fn parse_rejects_bad_long_options() {
        let err = parse(&["-tf", "a", "--gzip=1"]).unwrap_err();
        assert_eq!(err, "option --gzip does not take an argument");
        let err = parse(&["-tf", "a", "--bogus"]).unwrap_err();
        assert_eq!(err, "unknown option --bogus");
        let err = parse(&["-xf", "a", "--strip-components=x"]).unwrap_err();
        assert!(err.contains("requires a number"));
        let err = parse(&["-xf", "a", "--exclude"]).unwrap_err();
        assert_eq!(err, "option --exclude requires an argument");
    }

    #[test]
    fn parse_requires_create_path() {
        let err = parse(&["-cf", "archive.tar"]).unwrap_err();
        assert_eq!(err, "mode -c requires at least one PATH argument");
    }

    #[test]
    fn selection_ignores_leading_current_directory() {
        let filters = [String::from("tree/nested")];
        let excludes = [String::from("tree/nested/skip")];
        assert!(is_selected("./tree/nested/a.txt", &filters, &excludes));
        assert!(is_selected("tree/nested/", &filters, &excludes));
        assert!(!is_selected("tree/nested-other", &filters, &excludes));
        assert!(!is_selected("./tree/nested/skip/b", &filters, &excludes));
        assert!(is_selected("./", &[String::from(".")], &[]));
        assert!(!is_selected("./tree", &[], &[String::from(".")]));
    }

    #[test]
    fn selection_ignores_all_current_directory_components() {
        let secret = [String::from("tree/secret")];
        assert!(!is_selected("././tree/secret", &[], &secret));
        assert!(!is_selected("tree/./secret", &[], &secret));
        assert!(is_selected("tree/./secret/x", &secret, &[]));
    }

    #[test]
    fn strip_path_removes_leading_components() {
        assert_eq!(strip_path("a/b/c", 1).as_deref(), Some("b/c"));
        assert_eq!(strip_path("./a/b/", 2).as_deref(), Some("b/"));
        assert_eq!(strip_path("a//b", 1).as_deref(), Some("b"));
        assert_eq!(strip_path("a/", 1), None);
        assert_eq!(strip_path("a/b", 2), None);
    }
}

#[cfg(all(test, feature = "tokio"))]
mod tokio_tests {
    use super::{archive_writer, decode, Compression};
    use ::smol_tar::{TarDirectory, TarWriter};
    use ::tokio::io::{AsyncReadExt, AsyncWrite};
    use std::io;
    use std::pin::Pin;
    use std::sync::{Arc, Mutex};
    use std::task::{Context, Poll};

    /// Model of `tokio::io::Stdout`: the flush makes the written data visible,
    /// and the shutdown does nothing.
    #[derive(Clone, Default)]
    struct LazyStdout {
        pending: Arc<Mutex<Vec<u8>>>,
        written: Arc<Mutex<Vec<u8>>>,
    }

    impl AsyncWrite for LazyStdout {
        fn poll_write(
            self: Pin<&mut Self>,
            _: &mut Context<'_>,
            buf: &[u8],
        ) -> Poll<io::Result<usize>> {
            self.pending.lock().unwrap().extend_from_slice(buf);
            Poll::Ready(Ok(buf.len()))
        }

        fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
            let mut pending = self.pending.lock().unwrap();
            self.written.lock().unwrap().append(&mut pending);
            Poll::Ready(Ok(()))
        }

        fn poll_shutdown(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }
    }

    async fn write_archive(compression: Option<Compression>) -> Vec<u8> {
        let target = LazyStdout::default();
        let mut tar =
            TarWriter::<_, &[u8]>::new(archive_writer(Box::new(target.clone()), compression));
        tar.write(TarDirectory::new("dir/").into()).await.unwrap();
        tar.finish().await.unwrap();
        let written = target.written.lock().unwrap().clone();
        written
    }

    fn assert_complete(tar: &[u8]) {
        assert_eq!(tar.len() % 512, 0);
        assert!(tar.len() > 1024);
        assert!(tar.ends_with(&[0; 1024]));
    }

    #[test]
    fn close_flushes_archive_target() {
        let rt = ::tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();
        rt.block_on(async {
            assert_complete(&write_archive(None).await);

            let written = write_archive(Some(Compression::Gzip)).await;
            let mut tar = Vec::new();
            decode::GzipDecoder::new(&written[..])
                .read_to_end(&mut tar)
                .await
                .unwrap();
            assert_complete(&tar);
        });
    }
}
