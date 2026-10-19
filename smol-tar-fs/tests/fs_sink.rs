#![cfg(unix)]

mod fs_common;

use {
    fs_common::{block_on, TempDir},
    futures::{future, StreamExt, TryStreamExt},
    smol_tar::{
        TarDirectory, TarEntry, TarLink, TarReader, TarRegularFile, TarRegularFileReader,
        TarSymlink, TarWriter,
    },
    smol_tar_fs::{FsSink, MissingLinkTarget},
    std::{
        fs::{self, Permissions},
        io::{self, ErrorKind},
        os::unix::fs::{symlink, MetadataExt, PermissionsExt},
        path::{Path, PathBuf},
        time::{Duration, SystemTime, UNIX_EPOCH},
    },
};

/// Entry of a test archive.
type Entry = TarEntry<'static, &'static [u8]>;

/// Sink for the entries of a `TarReader` on an archive in memory.
type Sink<'a> = FsSink<'a, TarRegularFileReader<'a, &'a [u8]>>;

/// Modification time of the test entries, in seconds.
const MTIME: u64 = 1_500_000_000;

/// Give the modification time `MTIME`.
fn mtime() -> SystemTime {
    UNIX_EPOCH + Duration::from_secs(MTIME)
}

/// Write `entries` to an archive in memory.
fn archive(entries: Vec<Entry>) -> Vec<u8> {
    let mut data = Vec::new();
    block_on(async {
        let mut writer = TarWriter::new(&mut data);
        for entry in entries {
            writer.write(entry).await.unwrap();
        }
        writer.finish().await.unwrap();
    });
    data
}

/// Forward the entries of the archive `data` into `sink`.
fn extract_into<'a>(data: &'a [u8], sink: Sink<'a>) -> io::Result<()> {
    block_on(TarReader::new(data).forward(sink))
}

/// Forward the entries of the archive `data` into a sink for `dest` with
/// the default options.
fn extract(data: &[u8], dest: &Path) -> io::Result<()> {
    extract_into(data, FsSink::new(dest))
}

/// Forward the entries of the archive `data`, except the entry with the
/// path `skip`, into `sink`.
fn extract_without<'a>(data: &'a [u8], skip: &'a str, sink: Sink<'a>) -> io::Result<()> {
    block_on(
        TarReader::new(data)
            .try_filter(move |entry| future::ready(entry.path() != skip))
            .forward(sink),
    )
}

/// Regular file `path` with the body `body`.
fn file(path: &str, body: &'static [u8]) -> Entry {
    TarRegularFile::new(path, body.len() as u64, body).into()
}

/// Directory `path`.
fn dir(path: &str) -> Entry {
    TarDirectory::new(path).into()
}

/// Hard link `path` to `target`.
fn link(path: &str, target: &str) -> Entry {
    TarLink::new(path, target).into()
}

/// Create the empty directory `name` in `tmp`. Give its path.
fn subdir(tmp: &TempDir, name: &str) -> PathBuf {
    let path = tmp.path().join(name);
    fs::create_dir(&path).unwrap();
    path
}

/// Give the number of nodes in the directory `dir`.
fn count(dir: &Path) -> usize {
    fs::read_dir(dir).unwrap().count()
}

/// Give the permission and special bits of `path`. Do not follow a symlink.
fn mode(path: &Path) -> u32 {
    fs::symlink_metadata(path).unwrap().mode() & 0o7777
}

/// Give the uid and gid of a new file in `dir`.
fn owner_in(dir: &Path) -> (u32, u32) {
    let probe = dir.join("probe");
    fs::write(&probe, b"").unwrap();
    let meta = fs::metadata(&probe).unwrap();
    fs::remove_file(&probe).unwrap();
    (meta.uid(), meta.gid())
}

#[test]
fn rejects_parent_component() {
    let tmp = TempDir::new();
    let dest = subdir(&tmp, "dest");
    let err = extract(&archive(vec![file("../x", b"x")]), &dest).unwrap_err();
    assert_eq!(err.kind(), ErrorKind::InvalidData);
    assert!(err.to_string().starts_with("../x: "), "{err}");
    assert!(!tmp.path().join("x").exists());
    assert_eq!(count(&dest), 0);
}

#[test]
fn rejects_absolute_path() {
    let tmp = TempDir::new();
    let err = extract(&archive(vec![file("/x", b"x")]), tmp.path()).unwrap_err();
    assert_eq!(err.kind(), ErrorKind::InvalidData);
    assert_eq!(count(tmp.path()), 0);
}

#[test]
fn rejects_symlink_parent() {
    let tmp = TempDir::new();
    let dest = subdir(&tmp, "dest");
    let outside = subdir(&tmp, "outside");
    let data = archive(vec![
        TarSymlink::new("a", outside.to_str().unwrap()).into(),
        file("a/x", b"x"),
    ]);
    let err = extract(&data, &dest).unwrap_err();
    assert!(err.to_string().starts_with("a/x: "), "{err}");
    assert_eq!(count(&outside), 0);
}

#[test]
fn replaces_symlink_at_final_component() {
    let tmp = TempDir::new();
    let dest = subdir(&tmp, "dest");
    let outside = subdir(&tmp, "outside");
    fs::write(outside.join("f"), b"old").unwrap();
    symlink(outside.join("f"), dest.join("x")).unwrap();
    extract(&archive(vec![file("x", b"new")]), &dest).unwrap();
    assert!(fs::symlink_metadata(dest.join("x")).unwrap().is_file());
    assert_eq!(fs::read(dest.join("x")).unwrap(), b"new");
    assert_eq!(fs::read(outside.join("f")).unwrap(), b"old");
}

#[test]
fn replaces_hardlinked_file() {
    let tmp = TempDir::new();
    let dest = subdir(&tmp, "dest");
    let outside = subdir(&tmp, "outside");
    fs::write(outside.join("f"), b"old").unwrap();
    fs::hard_link(outside.join("f"), dest.join("x")).unwrap();
    extract(&archive(vec![file("x", b"new")]), &dest).unwrap();
    assert_eq!(fs::read(outside.join("f")).unwrap(), b"old");
    assert_eq!(fs::read(dest.join("x")).unwrap(), b"new");
    assert_ne!(
        fs::metadata(outside.join("f")).unwrap().ino(),
        fs::metadata(dest.join("x")).unwrap().ino()
    );
}

#[test]
fn link_target_outside_archive() {
    let tmp = TempDir::new();
    let err = extract(&archive(vec![link("x", "../../etc/passwd")]), tmp.path()).unwrap_err();
    assert_eq!(err.kind(), ErrorKind::NotFound);
    assert!(fs::symlink_metadata(tmp.path().join("x")).is_err());
}

#[test]
fn symlink_target_is_stored() {
    let tmp = TempDir::new();
    let data = archive(vec![
        TarSymlink::new("s", "/etc/passwd").into(),
        TarSymlink::new("t", "../../x").into(),
    ]);
    extract(&data, tmp.path()).unwrap();
    assert_eq!(
        fs::read_link(tmp.path().join("s")).unwrap(),
        Path::new("/etc/passwd")
    );
    assert_eq!(
        fs::read_link(tmp.path().join("t")).unwrap(),
        Path::new("../../x")
    );
}

#[test]
fn link_after_target() {
    let tmp = TempDir::new();
    extract(&archive(vec![file("a", b"a"), link("b", "a")]), tmp.path()).unwrap();
    assert_eq!(
        fs::metadata(tmp.path().join("a")).unwrap().ino(),
        fs::metadata(tmp.path().join("b")).unwrap().ino()
    );
}

#[test]
fn link_before_target() {
    let tmp = TempDir::new();
    let err = extract(&archive(vec![link("b", "a"), file("a", b"a")]), tmp.path()).unwrap_err();
    assert_eq!(err.kind(), ErrorKind::NotFound);
    assert!(err.to_string().contains("is not in the archive"), "{err}");
}

#[test]
fn link_target_not_from_archive() {
    let tmp = TempDir::new();
    fs::write(tmp.path().join("a"), b"a").unwrap();
    let err = extract(&archive(vec![link("b", "a")]), tmp.path()).unwrap_err();
    assert_eq!(err.kind(), ErrorKind::NotFound);
    assert!(fs::symlink_metadata(tmp.path().join("b")).is_err());
}

#[test]
fn link_target_filtered_error() {
    let tmp = TempDir::new();
    let data = archive(vec![file("a", b"a"), link("b", "a")]);
    let err = extract_without(&data, "a", FsSink::new(tmp.path())).unwrap_err();
    assert_eq!(err.kind(), ErrorKind::NotFound);
    assert!(err.to_string().contains("is not in the archive"), "{err}");
}

#[test]
fn link_target_filtered_skip() {
    let tmp = TempDir::new();
    let data = archive(vec![file("a", b"a"), link("b", "a")]);
    let sink = FsSink::new(tmp.path()).with_missing_link_target(MissingLinkTarget::Skip);
    extract_without(&data, "a", sink).unwrap();
    assert!(!tmp.path().join("a").exists());
    assert!(!tmp.path().join("b").exists());
}

#[test]
fn link_to_directory() {
    let tmp = TempDir::new();
    let err = extract(&archive(vec![dir("d/"), link("l", "d")]), tmp.path()).unwrap_err();
    assert_eq!(err.kind(), ErrorKind::NotFound);
}

#[test]
fn link_to_replaced_path() {
    let tmp = TempDir::new();
    let data = archive(vec![file("a", b"a"), dir("a/"), link("b", "a")]);
    let err = extract(&data, tmp.path()).unwrap_err();
    assert_eq!(err.kind(), ErrorKind::NotFound);
}

#[test]
fn file_mode_and_mtime() {
    let tmp = TempDir::new();
    let entry = TarRegularFile::new("f", 1, &b"f"[..])
        .with_mode(0o640)
        .with_mtime(mtime());
    extract(&archive(vec![entry.into()]), tmp.path()).unwrap();
    let path = tmp.path().join("f");
    assert_eq!(mode(&path), 0o640);
    assert_eq!(fs::metadata(&path).unwrap().mtime(), MTIME as i64);
}

#[test]
fn special_bits_removed() {
    let tmp = TempDir::new();
    let entry = TarRegularFile::new("f", 1, &b"f"[..]).with_mode(0o4755);
    extract(&archive(vec![entry.into()]), tmp.path()).unwrap();
    assert_eq!(mode(&tmp.path().join("f")), 0o755);
}

#[test]
fn special_bits_kept() {
    let tmp = TempDir::new();
    let entry = TarRegularFile::new("f", 1, &b"f"[..]).with_mode(0o4755);
    let data = archive(vec![entry.into()]);
    extract_into(&data, FsSink::new(tmp.path()).with_special_bits(true)).unwrap();
    assert_eq!(mode(&tmp.path().join("f")), 0o4755);
}

#[test]
fn read_only_directory() {
    let tmp = TempDir::new();
    let data = archive(vec![
        TarDirectory::new("d/").with_mode(0o555).into(),
        file("d/x", b"x"),
    ]);
    extract(&data, tmp.path()).unwrap();
    assert_eq!(fs::read(tmp.path().join("d/x")).unwrap(), b"x");
    assert_eq!(mode(&tmp.path().join("d")), 0o555);
}

#[test]
fn directory_mtime_after_children() {
    let tmp = TempDir::new();
    let data = archive(vec![
        TarDirectory::new("d/").with_mtime(mtime()).into(),
        file("d/x", b"x"),
        dir("d/e/"),
        file("d/e/y", b"y"),
    ]);
    extract(&data, tmp.path()).unwrap();
    let meta = fs::metadata(tmp.path().join("d")).unwrap();
    assert_eq!(meta.mtime(), MTIME as i64);
}

#[test]
fn directory_metadata_deepest_first() {
    let tmp = TempDir::new();
    if owner_in(tmp.path()).0 == 0 {
        eprintln!("skip: the process runs as root");
        return;
    }
    let data = archive(vec![
        TarDirectory::new("d/").with_mode(0o600).into(),
        dir("d/e/"),
    ]);
    extract(&data, tmp.path()).unwrap();
    assert_eq!(mode(&tmp.path().join("d")), 0o600);
}

#[test]
fn implicit_parent() {
    let tmp = TempDir::new();
    extract(&archive(vec![file("a/b/c", b"c")]), tmp.path()).unwrap();
    for path in ["a", "a/b"] {
        let path = tmp.path().join(path);
        assert!(fs::symlink_metadata(&path).unwrap().is_dir());
        assert_eq!(mode(&path) & 0o777, 0o755);
    }
}

#[test]
fn owner_kept() {
    let tmp = TempDir::new();
    let (uid, gid) = owner_in(tmp.path());
    let data = archive(vec![
        TarRegularFile::new("f", 1, &b"f"[..])
            .with_uid(uid)
            .with_gid(gid)
            .into(),
        TarDirectory::new("d/").with_uid(uid).with_gid(gid).into(),
        TarSymlink::new("s", "f").with_uid(uid).with_gid(gid).into(),
    ]);
    extract_into(&data, FsSink::new(tmp.path()).with_owner(true)).unwrap();
    for path in ["f", "d", "s"] {
        let meta = fs::symlink_metadata(tmp.path().join(path)).unwrap();
        assert_eq!((meta.uid(), meta.gid()), (uid, gid), "{path}");
    }
    if uid == 0 {
        return;
    }
    let entry = TarRegularFile::new("g", 1, &b"g"[..])
        .with_uid(uid + 1)
        .with_gid(gid);
    let data = archive(vec![entry.into()]);
    let err = extract_into(&data, FsSink::new(tmp.path()).with_owner(true)).unwrap_err();
    assert_eq!(err.kind(), ErrorKind::PermissionDenied);
}

#[test]
fn symlink_mtime() {
    let tmp = TempDir::new();
    let entry = TarSymlink::new("s", "missing").with_mtime(mtime());
    extract(&archive(vec![entry.into()]), tmp.path()).unwrap();
    let meta = fs::symlink_metadata(tmp.path().join("s")).unwrap();
    assert!(meta.file_type().is_symlink());
    assert_eq!(meta.mtime(), MTIME as i64);
}

#[cfg(any(target_os = "linux", target_os = "android", target_vendor = "apple"))]
mod xattr {
    use {
        super::*,
        rustix::{
            fs::{lgetxattr, llistxattr, setxattr, XattrFlags},
            io::Errno,
        },
        smol_tar::AttrList,
    };

    /// Give `true` if the filesystem of `dir` supports `user.*` xattrs.
    /// Print a skip message if not. Set `user.probe` on `dir`.
    fn user_xattrs_supported(dir: &Path) -> bool {
        match setxattr(dir, "user.probe", b"1", XattrFlags::empty()) {
            Ok(()) => true,
            Err(Errno::NOTSUP) => {
                eprintln!("skip: no user xattrs in {}", dir.display());
                false
            }
            Err(err) => panic!("setxattr user.probe: {err}"),
        }
    }

    /// Give `true` if the process gets `EPERM` when it sets `trusted.x` on
    /// `dir`. Print a skip message if not.
    #[cfg(target_os = "linux")]
    fn trusted_xattrs_denied(dir: &Path) -> bool {
        match setxattr(dir, "trusted.x", b"1", XattrFlags::empty()) {
            Err(Errno::PERM) => true,
            Ok(()) => {
                eprintln!("skip: the process can set trusted xattrs");
                false
            }
            Err(err) => {
                eprintln!("skip: setxattr trusted.x: {err}");
                false
            }
        }
    }

    /// Give the `user.*` xattrs of `path`, sorted by name. Do not follow a
    /// symlink.
    fn user_attrs(path: &Path) -> Vec<(String, Vec<u8>)> {
        let mut names = [0; 1024];
        let len = llistxattr(path, &mut names[..]).unwrap();
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

    /// Give `attrs` in the form of `user_attrs`.
    fn owned(attrs: &[(&str, &[u8])]) -> Vec<(String, Vec<u8>)> {
        attrs
            .iter()
            .map(|(name, value)| (name.to_string(), value.to_vec()))
            .collect()
    }

    /// Regular file `path` with an empty body and the xattrs `attrs`.
    fn file_with(path: &str, attrs: &[(&str, &[u8])]) -> Entry {
        let list = attrs.iter().fold(AttrList::new(), |list, &(name, value)| {
            list.with(name, value)
        });
        TarRegularFile::new(path, 0, &b""[..])
            .with_attrs(list)
            .into()
    }

    #[test]
    fn xattrs_off_by_default() {
        let tmp = TempDir::new();
        if !user_xattrs_supported(tmp.path()) {
            return;
        }
        extract(
            &archive(vec![file_with("f", &[("user.a", b"1")])]),
            tmp.path(),
        )
        .unwrap();
        assert!(user_attrs(&tmp.path().join("f")).is_empty());
    }

    #[test]
    fn xattrs_restored() {
        let tmp = TempDir::new();
        if !user_xattrs_supported(tmp.path()) {
            return;
        }
        let file_attrs: &[(&str, &[u8])] = &[("user.a", b"1"), ("user.bin", b"\x00\xff\x80\n")];
        let data = archive(vec![
            file_with("f", file_attrs),
            TarDirectory::new("d/")
                .with_attrs(AttrList::new().with("user.d", &b"d"[..]))
                .into(),
            TarDirectory::new("ro/")
                .with_mode(0o555)
                .with_attrs(AttrList::new().with("user.ro", &b"r"[..]))
                .into(),
        ]);
        extract_into(&data, FsSink::new(tmp.path()).with_xattrs(true)).unwrap();
        assert_eq!(user_attrs(&tmp.path().join("f")), owned(file_attrs));
        assert_eq!(
            user_attrs(&tmp.path().join("d")),
            owned(&[("user.d", b"d")])
        );
        assert_eq!(
            user_attrs(&tmp.path().join("ro")),
            owned(&[("user.ro", b"r")])
        );
        assert_eq!(mode(&tmp.path().join("ro")), 0o555);
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn xattr_policy_error() {
        let tmp = TempDir::new();
        if !user_xattrs_supported(tmp.path()) {
            return;
        }
        if !trusted_xattrs_denied(tmp.path()) {
            return;
        }
        let data = archive(vec![file_with("f", &[("trusted.x", b"1")])]);
        let err = extract_into(&data, FsSink::new(tmp.path()).with_xattrs(true)).unwrap_err();
        assert_eq!(err.kind(), ErrorKind::PermissionDenied);
        assert!(err.to_string().starts_with("f: xattr trusted.x: "), "{err}");
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn xattr_policy_skip() {
        use smol_tar_fs::UnsupportedXattr;

        let tmp = TempDir::new();
        if !user_xattrs_supported(tmp.path()) {
            return;
        }
        if !trusted_xattrs_denied(tmp.path()) {
            return;
        }
        let data = archive(vec![file_with(
            "f",
            &[("trusted.x", b"1"), ("user.a", b"1")],
        )]);
        let sink = FsSink::new(tmp.path())
            .with_xattrs(true)
            .with_unsupported_xattr(UnsupportedXattr::Skip);
        extract_into(&data, sink).unwrap();
        assert_eq!(
            user_attrs(&tmp.path().join("f")),
            owned(&[("user.a", b"1")])
        );
    }

    #[test]
    fn xattrs_on_link_ignored() {
        let tmp = TempDir::new();
        if !user_xattrs_supported(tmp.path()) {
            return;
        }
        let data = archive(vec![file_with("a", &[("user.a", b"1")]), link("b", "a")]);
        extract_into(&data, FsSink::new(tmp.path()).with_xattrs(true)).unwrap();
        assert_eq!(
            user_attrs(&tmp.path().join("b")),
            owned(&[("user.a", b"1")])
        );
    }
}

#[test]
fn fifo_created() {
    use {smol_tar::TarFifo, std::os::unix::fs::FileTypeExt};

    let tmp = TempDir::new();
    let entry = TarFifo::new("p").with_mode(0o640);
    extract(&archive(vec![entry.into()]), tmp.path()).unwrap();
    let path = tmp.path().join("p");
    assert!(fs::symlink_metadata(&path).unwrap().file_type().is_fifo());
    assert_eq!(mode(&path), 0o640);
}

#[test]
fn device_needs_privilege() {
    use smol_tar::TarDevice;

    let tmp = TempDir::new();
    if owner_in(tmp.path()).0 == 0 {
        eprintln!("skip: the process runs as root");
        return;
    }
    let entry = TarDevice::new_char("null", 1, 3);
    let err = extract(&archive(vec![entry.into()]), tmp.path()).unwrap_err();
    assert_eq!(err.kind(), ErrorKind::PermissionDenied);
    assert!(err.to_string().starts_with("null: "), "{err}");
}

#[test]
fn existing_directory_for_file() {
    let tmp = TempDir::new();
    let path = subdir(&tmp, "x");
    let err = extract(&archive(vec![file("x", b"x")]), tmp.path()).unwrap_err();
    assert_eq!(err.kind(), ErrorKind::AlreadyExists);
    assert!(fs::symlink_metadata(&path).unwrap().is_dir());
}

#[test]
fn finish_is_idempotent() {
    let tmp = TempDir::new();
    let path = tmp.path().join("d");
    block_on(async {
        let mut sink = FsSink::new(tmp.path());
        let entry: Entry = TarDirectory::new("d/").with_mode(0o750).into();
        sink.write(entry).await.unwrap();
        sink.finish().await.unwrap();
        assert_eq!(mode(&path), 0o750);
        fs::set_permissions(&path, Permissions::from_mode(0o700)).unwrap();
        sink.finish().await.unwrap();
    });
    assert_eq!(mode(&path), 0o700);
}

#[test]
fn write_after_finish_fails() {
    let tmp = TempDir::new();
    let err = block_on(async {
        let mut sink = FsSink::new(tmp.path());
        sink.finish().await.unwrap();
        sink.write(file("f", b"f")).await.unwrap_err()
    });
    assert_eq!(err.to_string(), "the sink is finished");
    assert!(!tmp.path().join("f").exists());
}

#[test]
fn error_poisons_sink() {
    let tmp = TempDir::new();
    block_on(async {
        let mut sink = FsSink::new(tmp.path());
        let err = sink.write(file("../x", b"x")).await.unwrap_err();
        assert_eq!(err.kind(), ErrorKind::InvalidData);
        let err = sink.write(file("y", b"y")).await.unwrap_err();
        assert_eq!(err.to_string(), "the sink stopped after an error");
        assert!(sink.finish().await.is_err());
    });
    assert!(!tmp.path().join("y").exists());
}

#[test]
fn error_has_path() {
    let tmp = TempDir::new();
    fs::create_dir_all(tmp.path().join("d/x")).unwrap();
    block_on(async {
        let err = FsSink::new(tmp.path())
            .write(file("./d//x", b"x"))
            .await
            .unwrap_err();
        assert!(err.to_string().starts_with("d/x: "), "{err}");
        let err = FsSink::new(tmp.path())
            .write(file("a/../b", b"b"))
            .await
            .unwrap_err();
        assert!(err.to_string().starts_with("a/../b: "), "{err}");
    });
}
