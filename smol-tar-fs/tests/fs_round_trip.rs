#![cfg(unix)]

mod fs_common;

use {
    fs_common::{block_on, make_fifo, TempDir},
    futures::StreamExt,
    rustix::fs::{utimensat, AtFlags, Timespec, Timestamps, CWD, UTIME_OMIT},
    smol_tar::{TarReader, TarWriter},
    smol_tar_fs::{FsSink, FsSource, FsStreamExt},
    std::{
        fs::{self, FileType, Permissions},
        os::unix::fs::{symlink, FileTypeExt, MetadataExt, PermissionsExt},
        path::{Path, PathBuf},
    },
};

/// Prefix of the archive paths.
const PREFIX: &str = "pkg";

/// Size of the large file, in bytes.
const BIG_SIZE: usize = 200 * 1024;

/// Modification time of the root, in seconds. Each other node has a
/// different time.
const MTIME: i64 = 1_500_000_000;

/// Nodes of the test tree with their modes, parents before children. The
/// root has the empty path. A symlink has no mode. The list does not have
/// `a/data.link`, because it is the same inode as `data`.
const NODES: &[(&str, Option<u32>)] = &[
    ("", Some(0o755)),
    ("a", Some(0o755)),
    ("a/b", Some(0o750)),
    ("a/b/c", Some(0o755)),
    ("a/b/c/leaf", Some(0o644)),
    ("abs", None),
    ("big", Some(0o644)),
    ("data", Some(0o644)),
    ("empty", Some(0o644)),
    ("fifo", Some(0o640)),
    ("rel", None),
    ("ro", Some(0o555)),
    ("ro/child", Some(0o644)),
    ("secret", Some(0o600)),
];

/// Names and values of the `user.*` xattrs of one node.
type Attrs = &'static [(&'static str, &'static [u8])];

/// `user.*` xattrs of the test tree, for each node that has them.
const XATTRS: &[(&str, Attrs)] = &[
    ("", &[("user.root", b"r")]),
    ("a", &[("user.dir", b"d")]),
    ("big", &[("user.a", b"1"), ("user.bin", b"\x00\xff\x80\n")]),
];

/// Node of a tree.
struct Node {
    /// Path relative to the root of the tree.
    path: PathBuf,
    meta: Meta,
    /// Body of a regular file.
    body: Option<Vec<u8>>,
}

/// Metadata of a node.
#[derive(Debug, PartialEq)]
struct Meta {
    kind: &'static str,
    /// Permission bits. A symlink has no mode.
    mode: Option<u32>,
    /// Modification time: seconds and nanoseconds.
    mtime: (i64, i64),
    /// Target of a symlink.
    target: Option<PathBuf>,
    /// `user.*` xattrs, sorted by name.
    xattrs: Vec<(String, Vec<u8>)>,
}

/// Give the body of the large file.
fn big_body() -> Vec<u8> {
    (0..BIG_SIZE).map(|i| (i % 251) as u8).collect()
}

/// Build the test tree in the empty directory `root`. Set the modes and the
/// modification times of all nodes.
fn make_tree(root: &Path) {
    let path = |name: &str| root.join(name);
    fs::create_dir_all(path("a/b/c")).unwrap();
    fs::create_dir(path("ro")).unwrap();
    for (name, body) in [
        ("empty", &b""[..]),
        ("secret", b"secret"),
        ("data", b"data"),
        ("a/b/c/leaf", b"leaf"),
        ("ro/child", b"child"),
    ] {
        fs::write(path(name), body).unwrap();
    }
    fs::write(path("big"), big_body()).unwrap();
    fs::hard_link(path("data"), path("a/data.link")).unwrap();
    symlink("a/b/c/leaf", path("rel")).unwrap();
    symlink("/etc/passwd", path("abs")).unwrap();
    make_fifo(&path("fifo"), 0o640);
    // Children come before their parents, and the root comes last.
    for (i, &(name, mode)) in NODES.iter().enumerate().rev() {
        if let Some(mode) = mode {
            fs::set_permissions(path(name), Permissions::from_mode(mode)).unwrap();
        }
        set_mtime(&path(name), MTIME + i as i64);
    }
}

/// Set the modification time of `path` to `sec` seconds. Do not change the
/// access time. Do not follow a symlink.
fn set_mtime(path: &Path, sec: i64) {
    let times = Timestamps {
        last_access: Timespec {
            tv_sec: 0,
            tv_nsec: UTIME_OMIT,
        },
        last_modification: Timespec {
            tv_sec: sec,
            tv_nsec: 0,
        },
    };
    utimensat(CWD, path, &times, AtFlags::SYMLINK_NOFOLLOW).unwrap();
}

/// Give a name for the kind of a node.
fn kind(file_type: FileType) -> &'static str {
    if file_type.is_dir() {
        "dir"
    } else if file_type.is_file() {
        "file"
    } else if file_type.is_symlink() {
        "symlink"
    } else if file_type.is_fifo() {
        "fifo"
    } else {
        "other"
    }
}

/// Give the nodes of the tree at `root`, sorted by path. The root is the
/// node with the empty path. Do not follow symlinks.
fn snapshot(root: &Path) -> Vec<Node> {
    let mut nodes = Vec::new();
    let mut pending = vec![PathBuf::new()];
    while let Some(rel) = pending.pop() {
        let path = root.join(&rel);
        let md = fs::symlink_metadata(&path).unwrap();
        if md.is_dir() {
            for entry in fs::read_dir(&path).unwrap() {
                pending.push(rel.join(entry.unwrap().file_name()));
            }
        }
        let symlink = md.file_type().is_symlink();
        nodes.push(Node {
            meta: Meta {
                kind: kind(md.file_type()),
                mode: (!symlink).then(|| md.mode() & 0o7777),
                mtime: (md.mtime(), md.mtime_nsec()),
                target: symlink.then(|| fs::read_link(&path).unwrap()),
                xattrs: user_attrs(&path),
            },
            body: md.is_file().then(|| fs::read(&path).unwrap()),
            path: rel,
        });
    }
    nodes.sort_by(|a, b| a.path.cmp(&b.path));
    nodes
}

/// Make sure that the snapshots `left` and `right` have the same nodes. On
/// failure, do not show the file bodies.
fn assert_same(left: &[Node], right: &[Node]) {
    let paths = |nodes: &[Node]| nodes.iter().map(|n| n.path.clone()).collect::<Vec<_>>();
    assert_eq!(paths(left), paths(right));
    for (l, r) in left.iter().zip(right) {
        assert_eq!(l.meta, r.meta, "metadata of {:?}", l.path);
        assert!(l.body == r.body, "body of {:?} differs", l.path);
    }
}

/// Write the tree at `root` to an archive in memory, with the xattrs and
/// with the prefix `PREFIX`.
fn archive(root: &Path) -> Vec<u8> {
    let mut data = Vec::new();
    let source = FsSource::new(root).with_prefix(PREFIX).with_xattrs(true);
    block_on(source.into_tar().forward(TarWriter::new(&mut data))).unwrap();
    data
}

/// Set the xattrs `XATTRS` in the tree at `root`. Give `false` and print a
/// message if the filesystem does not support `user.*` xattrs.
#[cfg(any(target_os = "linux", target_os = "android", target_vendor = "apple"))]
fn add_xattrs(root: &Path) -> bool {
    use rustix::{
        fs::{lsetxattr, XattrFlags},
        io::Errno,
    };

    for &(name, attrs) in XATTRS {
        for &(attr, value) in attrs {
            match lsetxattr(root.join(name), attr, value, XattrFlags::empty()) {
                Ok(()) => {}
                Err(Errno::NOTSUP) => {
                    eprintln!("no user xattrs in {}: no xattr checks", root.display());
                    return false;
                }
                Err(err) => panic!("setxattr {attr}: {err}"),
            }
        }
    }
    true
}

/// Give `false`: the platform has no `user.*` xattrs.
#[cfg(not(any(target_os = "linux", target_os = "android", target_vendor = "apple")))]
fn add_xattrs(_root: &Path) -> bool {
    false
}

/// Give the `user.*` xattrs of `path`, sorted by name. Do not follow a
/// symlink. A filesystem without xattr support gives an empty list.
#[cfg(any(target_os = "linux", target_os = "android", target_vendor = "apple"))]
fn user_attrs(path: &Path) -> Vec<(String, Vec<u8>)> {
    use rustix::{
        fs::{lgetxattr, llistxattr},
        io::Errno,
    };

    let mut names = [0; 1024];
    let len = match llistxattr(path, &mut names[..]) {
        Ok(len) => len,
        Err(Errno::NOTSUP) => return Vec::new(),
        Err(err) => panic!("listxattr {}: {err}", path.display()),
    };
    let mut attrs: Vec<_> = names[..len]
        .split(|&byte| byte == 0)
        .filter(|name| name.starts_with(b"user."))
        .map(|name| {
            let name = std::str::from_utf8(name).unwrap();
            let mut value = [0; 256];
            let len = lgetxattr(path, name, &mut value[..]).unwrap();
            (name.to_owned(), value[..len].to_vec())
        })
        .collect();
    attrs.sort();
    attrs
}

/// Give no xattrs: the platform has no `user.*` xattrs.
#[cfg(not(any(target_os = "linux", target_os = "android", target_vendor = "apple")))]
fn user_attrs(_path: &Path) -> Vec<(String, Vec<u8>)> {
    Vec::new()
}

#[test]
fn round_trip() {
    let tmp = TempDir::new();
    let src = tmp.path().join("src");
    let dest = tmp.path().join("dest");
    fs::create_dir(&src).unwrap();
    fs::create_dir(&dest).unwrap();
    make_tree(&src);
    let xattrs = add_xattrs(&src);

    let first = archive(&src);
    let sink = FsSink::new(&dest).with_xattrs(true);
    block_on(TarReader::new(&first[..]).forward(sink)).unwrap();

    let names: Vec<_> = fs::read_dir(&dest)
        .unwrap()
        .map(|entry| entry.unwrap().file_name())
        .collect();
    assert_eq!(names, [PREFIX], "entries of {}", dest.display());
    let out = dest.join(PREFIX);
    let nodes = snapshot(&out);
    assert_same(&snapshot(&src), &nodes);
    if xattrs {
        for &(name, attrs) in XATTRS {
            let expected: Vec<_> = attrs
                .iter()
                .map(|&(attr, value)| (attr.to_owned(), value.to_vec()))
                .collect();
            let node = nodes.iter().find(|n| n.path == Path::new(name)).unwrap();
            assert_eq!(node.meta.xattrs, expected, "xattrs of {name:?}");
        }
    }
    let ino = |name: &str| fs::symlink_metadata(out.join(name)).unwrap().ino();
    assert_eq!(ino("data"), ino("a/data.link"));

    let second = archive(&src);
    assert!(first == second, "the second archive differs from the first");
}
