#![cfg(unix)]

mod fs_common;

use {
    fs_common::{block_on, collect, make_fifo, TempDir},
    futures::{future, Stream, StreamExt, TryStreamExt},
    smol_tar::{TarEntry, TarReader, TarWriter},
    smol_tar_fs::{FsEntry, FsKind, FsSource, FsStreamExt},
    std::{
        fs,
        io::{self, ErrorKind},
        os::unix::fs::{symlink, MetadataExt, PermissionsExt},
        path::Path,
        pin::pin,
        time::{Duration, UNIX_EPOCH},
    },
};

#[cfg(all(feature = "smol", not(feature = "tokio")))]
use smol::io::{AsyncRead, AsyncReadExt};
#[cfg(feature = "tokio")]
use tokio::io::{AsyncRead, AsyncReadExt};

/// Nodes of the test tree. A name that ends with `/` is a directory.
const TREE: [&str; 9] = [
    "a.txt", "b", "a/", "a/x", "c/", "c/y", "c/d/", "c/d/z", "e/",
];

/// Create the nodes `names` in `root`, in the order of the iterator.
fn make_tree<'a>(root: &Path, names: impl IntoIterator<Item = &'a str>) {
    for name in names {
        let path = root.join(name);
        if name.ends_with('/') {
            fs::create_dir_all(path).unwrap();
        } else {
            fs::create_dir_all(path.parent().unwrap()).unwrap();
            fs::write(path, name).unwrap();
        }
    }
}

/// Give all entries of a stream. Each item must be `Ok`.
fn entries(source: impl Stream<Item = io::Result<FsEntry>>) -> Vec<FsEntry> {
    collect(source).into_iter().map(Result::unwrap).collect()
}

/// Give the archive paths of all entries of a stream.
fn paths(source: impl Stream<Item = io::Result<FsEntry>>) -> Vec<String> {
    entries(source)
        .iter()
        .map(|entry| entry.path().to_owned())
        .collect()
}

#[test]
fn order_files_first() {
    let tmp = TempDir::new();
    make_tree(tmp.path(), TREE);
    assert_eq!(
        paths(FsSource::new(tmp.path()).with_prefix("pkg")),
        [
            "pkg/",
            "pkg/a.txt",
            "pkg/b",
            "pkg/a/",
            "pkg/a/x",
            "pkg/c/",
            "pkg/c/y",
            "pkg/c/d/",
            "pkg/c/d/z",
            "pkg/e/",
        ]
    );
}

#[test]
fn order_does_not_depend_on_creation_order() {
    let forward = TempDir::new();
    let reverse = TempDir::new();
    make_tree(forward.path(), TREE);
    make_tree(reverse.path(), TREE.into_iter().rev());
    assert_eq!(
        paths(FsSource::new(forward.path()).with_prefix("pkg")),
        paths(FsSource::new(reverse.path()).with_prefix("pkg"))
    );
}

#[test]
fn empty_prefix_has_no_root_entry() {
    let tmp = TempDir::new();
    make_tree(tmp.path(), TREE);
    let paths = paths(FsSource::new(tmp.path()));
    assert_eq!(paths.len(), TREE.len());
    assert_eq!(paths[0], "a.txt");
}

#[test]
fn single_file_root() {
    let tmp = TempDir::new();
    let path = tmp.path().join("file");
    fs::write(&path, b"data").unwrap();
    let entries = entries(FsSource::new(&path).with_prefix("f"));
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].path(), "f");
    assert_eq!(entries[0].kind(), FsKind::File);
    assert_eq!(entries[0].size(), 4);
    assert_eq!(entries[0].fs_path(), path);
}

#[test]
fn single_file_root_without_prefix_fails() {
    let tmp = TempDir::new();
    let path = tmp.path().join("file");
    fs::write(&path, b"data").unwrap();
    let items = collect(FsSource::new(&path));
    assert_eq!(items.len(), 1);
    let Err(err) = &items[0] else {
        panic!("root that is not a directory gives an entry without a prefix");
    };
    assert_eq!(err.kind(), ErrorKind::InvalidInput);
}

#[test]
fn metadata() {
    let tmp = TempDir::new();
    let path = tmp.path().join("f");
    fs::write(&path, b"hello").unwrap();
    fs::set_permissions(&path, fs::Permissions::from_mode(0o640)).unwrap();
    let mtime = UNIX_EPOCH + Duration::new(1_700_000_000, 123_456_789);
    fs::File::options()
        .write(true)
        .open(&path)
        .unwrap()
        .set_modified(mtime)
        .unwrap();
    let entries = entries(FsSource::new(tmp.path()));
    assert_eq!(entries.len(), 1);
    let entry = &entries[0];
    let md = fs::symlink_metadata(&path).unwrap();
    assert_eq!(entry.path(), "f");
    assert_eq!(entry.kind(), FsKind::File);
    assert_eq!(entry.mode(), md.mode() & 0o7777);
    assert_eq!(entry.mode(), 0o640);
    assert_eq!(entry.uid(), md.uid());
    assert_eq!(entry.gid(), md.gid());
    assert_eq!(entry.size(), md.len());
    assert_eq!(entry.mtime(), md.modified().unwrap());
    assert_eq!(entry.link(), None);
}

#[test]
fn symlink_is_not_followed() {
    let tmp = TempDir::new();
    make_tree(tmp.path(), ["d/x"]);
    std::os::unix::fs::symlink("d", tmp.path().join("l")).unwrap();
    let entries = entries(FsSource::new(tmp.path()));
    let paths: Vec<&str> = entries.iter().map(FsEntry::path).collect();
    assert_eq!(paths, ["l", "d/", "d/x"]);
    assert_eq!(entries[0].kind(), FsKind::Symlink);
    assert_eq!(entries[0].link(), Some("d"));
    assert_eq!(entries[0].size(), 0);
}

#[test]
fn socket_is_skipped() {
    let tmp = TempDir::new();
    fs::write(tmp.path().join("f"), b"").unwrap();
    let _sock = std::os::unix::net::UnixListener::bind(tmp.path().join("s")).unwrap();
    assert_eq!(paths(FsSource::new(tmp.path())), ["f"]);
}

#[test]
fn fs_path() {
    let tmp = TempDir::new();
    make_tree(tmp.path(), TREE);
    let entries = entries(FsSource::new(tmp.path()).with_prefix("pkg"));
    assert_eq!(entries.len(), TREE.len() + 1);
    for entry in &entries {
        let relative = entry.path().strip_prefix("pkg/").unwrap();
        let relative = relative.strip_suffix('/').unwrap_or(relative);
        assert_eq!(
            entry.fs_path(),
            tmp.path().join(relative),
            "{}",
            entry.path()
        );
    }
}

#[test]
fn try_filter_drops_entries() {
    let tmp = TempDir::new();
    make_tree(tmp.path(), TREE);
    let source = FsSource::new(tmp.path())
        .with_prefix("pkg")
        .try_filter(|entry| future::ready(!entry.path().starts_with("pkg/c/")));
    assert_eq!(
        paths(source),
        ["pkg/", "pkg/a.txt", "pkg/b", "pkg/a/", "pkg/a/x", "pkg/e/"]
    );
}

/// Tar entry in a form that tests can compare.
#[derive(Debug, PartialEq)]
enum Item {
    /// Path and body.
    File(String, Vec<u8>),
    /// Path and target.
    Link(String, String),
    /// Path and target.
    Symlink(String, String),
    /// Path.
    Dir(String),
    /// Path.
    Fifo(String),
    /// Path, `true` for a character device, major and minor numbers.
    Device(String, bool, u32, u32),
}

/// Give the comparable form of `entry`. Read the body of a regular file.
async fn item<R: AsyncRead>(entry: TarEntry<'_, R>) -> Item {
    match entry {
        TarEntry::File(file) => {
            let path = file.path().to_owned();
            let mut file = pin!(file);
            let mut body = Vec::new();
            file.read_to_end(&mut body).await.unwrap();
            Item::File(path, body)
        }
        TarEntry::Link(link) => Item::Link(link.path().into(), link.link().into()),
        TarEntry::Symlink(link) => Item::Symlink(link.path().into(), link.link().into()),
        TarEntry::Directory(dir) => Item::Dir(dir.path().into()),
        TarEntry::Fifo(fifo) => Item::Fifo(fifo.path().into()),
        TarEntry::Device(dev) => {
            Item::Device(dev.path().into(), dev.is_char(), dev.major(), dev.minor())
        }
    }
}

/// Give the tar entries of a stream in the comparable form.
fn tar_items(source: impl Stream<Item = io::Result<FsEntry>>) -> Vec<Item> {
    block_on(async {
        let entries: Vec<_> = source.into_tar().try_collect().await.unwrap();
        let mut items = Vec::new();
        for entry in entries {
            items.push(item(entry).await);
        }
        items
    })
}

/// Write the tar entries of a stream to an archive in memory.
fn archive(source: impl Stream<Item = io::Result<FsEntry>>) -> Vec<u8> {
    let mut data = Vec::new();
    block_on(source.into_tar().forward(TarWriter::new(&mut data))).unwrap();
    data
}

/// Read the entries of an archive in the comparable form.
fn read_archive(data: &[u8]) -> Vec<Item> {
    block_on(async {
        let mut reader = pin!(TarReader::new(data));
        let mut items = Vec::new();
        while let Some(entry) = reader.next().await {
            items.push(item(entry.unwrap()).await);
        }
        items
    })
}

/// Body of the regular file in the hard link tests.
const DATA: &[u8] = b"data";

/// Create the regular file `a` in `root`, and the hard links `links` to it.
fn hard_links(root: &Path, links: &[&str]) {
    fs::write(root.join("a"), DATA).unwrap();
    for link in links {
        fs::hard_link(root.join("a"), root.join(link)).unwrap();
    }
}

/// Give the source of `root` without the entries with the paths `skip`.
fn without<'a>(root: &Path, skip: &'a [&'a str]) -> impl Stream<Item = io::Result<FsEntry>> + 'a {
    FsSource::new(root).try_filter(move |entry| future::ready(!skip.contains(&entry.path())))
}

/// Regular file `path` with the body `DATA`.
fn file(path: &str) -> Item {
    Item::File(path.into(), DATA.into())
}

/// Hard link `path` to `target`.
fn link(path: &str, target: &str) -> Item {
    Item::Link(path.into(), target.into())
}

#[test]
fn hardlink_second_path_is_link() {
    let tmp = TempDir::new();
    hard_links(tmp.path(), &["b"]);
    assert_eq!(
        tar_items(FsSource::new(tmp.path())),
        [file("a"), link("b", "a")]
    );
}

#[test]
fn hardlink_first_path_filtered() {
    let tmp = TempDir::new();
    hard_links(tmp.path(), &["b"]);
    assert_eq!(tar_items(without(tmp.path(), &["a"])), [file("b")]);
}

#[test]
fn hardlink_link_path_filtered() {
    let tmp = TempDir::new();
    hard_links(tmp.path(), &["b"]);
    assert_eq!(tar_items(without(tmp.path(), &["b"])), [file("a")]);
}

#[test]
fn hardlink_three_paths_first_filtered() {
    let tmp = TempDir::new();
    hard_links(tmp.path(), &["b", "c"]);
    assert_eq!(
        tar_items(without(tmp.path(), &["a"])),
        [file("b"), link("c", "b")]
    );
}

#[test]
fn hardlink_outside_tree() {
    let tmp = TempDir::new();
    fs::create_dir(tmp.path().join("d")).unwrap();
    hard_links(tmp.path(), &["d/b"]);
    assert_eq!(tar_items(FsSource::new(tmp.path().join("d"))), [file("b")]);
}

#[test]
fn hardlink_symlink() {
    let tmp = TempDir::new();
    fs::write(tmp.path().join("t"), DATA).unwrap();
    symlink("t", tmp.path().join("a")).unwrap();
    fs::hard_link(tmp.path().join("a"), tmp.path().join("b")).unwrap();
    if !fs::symlink_metadata(tmp.path().join("b"))
        .unwrap()
        .file_type()
        .is_symlink()
    {
        eprintln!("skip: a hard link to a symlink follows the symlink");
        return;
    }
    assert_eq!(
        tar_items(FsSource::new(tmp.path())),
        [
            Item::Symlink("a".into(), "t".into()),
            link("b", "a"),
            file("t"),
        ]
    );
}

#[cfg(any(target_os = "linux", target_vendor = "apple"))]
#[test]
fn char_device() {
    #[cfg(target_os = "linux")]
    let (major, minor) = (1, 3);
    #[cfg(target_vendor = "apple")]
    let (major, minor) = (3, 2);
    assert_eq!(
        tar_items(FsSource::new("/dev/null").with_prefix("null")),
        [Item::Device("null".into(), true, major, minor)]
    );
}

#[test]
fn fifo() {
    let tmp = TempDir::new();
    let path = tmp.path().join("p");
    make_fifo(&path, 0o644);
    assert_eq!(
        tar_items(FsSource::new(tmp.path())),
        [Item::Fifo("p".into())]
    );
}

#[test]
fn forward_to_writer() {
    let tmp = TempDir::new();
    make_tree(tmp.path(), ["f", "skip", "d/", "d/g"]);
    fs::hard_link(tmp.path().join("f"), tmp.path().join("h")).unwrap();
    symlink("f", tmp.path().join("s")).unwrap();
    let source = FsSource::new(tmp.path())
        .with_prefix("pkg")
        .try_filter(|entry| future::ready(entry.path() != "pkg/skip"));
    assert_eq!(
        read_archive(&archive(source)),
        [
            Item::Dir("pkg/".into()),
            Item::File("pkg/f".into(), b"f".into()),
            link("pkg/h", "pkg/f"),
            Item::Symlink("pkg/s".into(), "f".into()),
            Item::Dir("pkg/d/".into()),
            Item::File("pkg/d/g".into(), b"d/g".into()),
        ]
    );
}

#[cfg(any(target_os = "linux", target_os = "android", target_vendor = "apple"))]
mod xattr {
    use {
        super::*,
        rustix::{
            fs::{setxattr, XattrFlags},
            io::Errno,
        },
        smol_tar::AttrList,
    };

    /// Set an xattr. Give `false` if the filesystem does not support
    /// `user.*` xattrs.
    fn set_xattr(path: &Path, name: &str, value: &[u8]) -> bool {
        match setxattr(path, name, value, XattrFlags::empty()) {
            Ok(()) => true,
            Err(Errno::NOTSUP) => {
                eprintln!("skip: no user xattrs in {}", path.display());
                false
            }
            Err(err) => panic!("setxattr {name}: {err}"),
        }
    }

    /// Give the `user.*` attributes.
    fn user_attrs(list: &AttrList) -> Vec<(&str, &[u8])> {
        list.iter()
            .filter(|(name, _)| name.starts_with("user."))
            .collect()
    }

    /// Give the entry with the archive path `path`.
    fn find<'a>(entries: &'a [FsEntry], path: &str) -> &'a FsEntry {
        entries
            .iter()
            .find(|entry| entry.path() == path)
            .unwrap_or_else(|| panic!("no entry {path}"))
    }

    #[test]
    fn xattrs_off_by_default() {
        let tmp = TempDir::new();
        let path = tmp.path().join("f");
        fs::write(&path, b"").unwrap();
        if !set_xattr(&path, "user.a", b"1") {
            return;
        }
        let entries = entries(FsSource::new(tmp.path()));
        assert!(find(&entries, "f").xattrs().is_empty());
    }

    #[test]
    fn xattrs_read() {
        let tmp = TempDir::new();
        make_tree(tmp.path(), ["f", "d/"]);
        if !set_xattr(tmp.path(), "user.root", b"r")
            || !set_xattr(&tmp.path().join("f"), "user.file", b"f")
            || !set_xattr(&tmp.path().join("d"), "user.dir", b"d")
        {
            return;
        }
        let entries = entries(
            FsSource::new(tmp.path())
                .with_prefix("pkg")
                .with_xattrs(true),
        );
        assert_eq!(
            user_attrs(find(&entries, "pkg/").xattrs()),
            [("user.root", &b"r"[..])]
        );
        assert_eq!(
            user_attrs(find(&entries, "pkg/f").xattrs()),
            [("user.file", &b"f"[..])]
        );
        assert_eq!(
            user_attrs(find(&entries, "pkg/d/").xattrs()),
            [("user.dir", &b"d"[..])]
        );
    }

    /// Give the path and the extended attributes of each regular file and
    /// each directory in an archive.
    fn archive_attrs(data: &[u8]) -> Vec<(String, AttrList)> {
        block_on(async {
            let mut reader = pin!(TarReader::new(data));
            let mut attrs = Vec::new();
            while let Some(entry) = reader.next().await {
                match entry.unwrap() {
                    TarEntry::File(file) => attrs.push((file.path().into(), file.attrs().clone())),
                    TarEntry::Directory(dir) => {
                        attrs.push((dir.path().into(), dir.attrs().clone()))
                    }
                    _ => {}
                }
            }
            attrs
        })
    }

    #[test]
    fn xattrs_in_archive() {
        let tmp = TempDir::new();
        make_tree(tmp.path(), ["f", "d/"]);
        if !set_xattr(&tmp.path().join("f"), "user.file", b"f")
            || !set_xattr(&tmp.path().join("f"), "user.more", &[0, 0xff])
            || !set_xattr(&tmp.path().join("d"), "user.dir", b"d")
        {
            return;
        }
        let archived = archive_attrs(&archive(FsSource::new(tmp.path()).with_xattrs(true)));
        let paths: Vec<&str> = archived.iter().map(|(path, _)| path.as_str()).collect();
        assert_eq!(paths, ["f", "d/"]);
        assert_eq!(
            user_attrs(&archived[0].1),
            [("user.file", &b"f"[..]), ("user.more", &[0, 0xff][..])]
        );
        assert_eq!(user_attrs(&archived[1].1), [("user.dir", &b"d"[..])]);
    }

    #[test]
    fn hardlink_has_no_xattrs() {
        let tmp = TempDir::new();
        fs::write(tmp.path().join("a"), DATA).unwrap();
        if !set_xattr(&tmp.path().join("a"), "user.a", b"1") {
            return;
        }
        fs::hard_link(tmp.path().join("a"), tmp.path().join("b")).unwrap();
        let source = || FsSource::new(tmp.path()).with_xattrs(true);
        let entries: Vec<_> = block_on(source().into_tar().try_collect()).unwrap();
        let [TarEntry::File(file), TarEntry::Link(link)] = &entries[..] else {
            panic!("the entries are not a file and a link");
        };
        assert_eq!(file.path(), "a");
        assert_eq!(user_attrs(file.attrs()), [("user.a", &b"1"[..])]);
        assert_eq!((link.path(), link.link()), ("b", "a"));
        let data = archive(source());
        let record = b"SCHILY.xattr.user.a=";
        let count = data
            .windows(record.len())
            .filter(|window| window == record)
            .count();
        assert_eq!(count, 1);
    }
}
