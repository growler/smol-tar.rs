#![cfg(all(unix, feature = "fs"))]

mod fs_common;

use {
    fs_common::{collect, TempDir},
    futures::{future, Stream, TryStreamExt},
    smol_tar::fs::{FsEntry, FsKind, FsSource},
    std::{
        fs,
        io::{self, ErrorKind},
        os::unix::fs::{MetadataExt, PermissionsExt},
        path::Path,
        time::{Duration, UNIX_EPOCH},
    },
};

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
}
