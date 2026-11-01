#![cfg(unix)]

mod fs_common;

use {
    fs_common::{block_on, data, kept_setgid, make_fifo, setgid_dir, TempDir},
    futures::{future, StreamExt, TryStreamExt},
    smol_tar::{
        TarDevice, TarDirectory, TarEntry, TarFifo, TarLink, TarReader, TarRegularFile,
        TarRegularFileReader, TarSymlink, TarWriter,
    },
    smol_tar_fs::{DeviceNodes, FsSink, MissingLinkTarget, ModePolicy},
    std::{
        fs::{self, Permissions},
        io::{self, ErrorKind},
        os::unix::fs::{symlink, MetadataExt, PermissionsExt},
        path::{Path, PathBuf},
        sync::{Arc, Mutex},
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
fn archive<'b>(entries: Vec<TarEntry<'b, &'b [u8]>>) -> Vec<u8> {
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
fn file<'b>(path: &str, body: &'b [u8]) -> TarEntry<'b, &'b [u8]> {
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

/// Append a ustar entry `name` with the body `body` to `out`. Pad the
/// body to a full block.
fn push_raw(out: &mut Vec<u8>, name: &str, typeflag: u8, linkname: &str, body: &[u8]) {
    let mut block = [0; 512];
    block[..name.len()].copy_from_slice(name.as_bytes());
    block[100..108].copy_from_slice(b"0000644\0");
    block[108..116].copy_from_slice(b"0000000\0");
    block[116..124].copy_from_slice(b"0000000\0");
    block[124..136].copy_from_slice(format!("{:011o}\0", body.len()).as_bytes());
    block[136..148].copy_from_slice(b"00000000000\0");
    block[148..156].fill(b' ');
    block[156] = typeflag;
    block[157..157 + linkname.len()].copy_from_slice(linkname.as_bytes());
    block[257..263].copy_from_slice(b"ustar\0");
    block[263..265].copy_from_slice(b"00");
    let sum: u32 = block.iter().map(|&byte| u32::from(byte)).sum();
    block[148..156].copy_from_slice(format!("{sum:06o}\0 ").as_bytes());
    out.extend_from_slice(&block);
    out.extend_from_slice(body);
    out.resize(out.len().next_multiple_of(512), 0);
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
    assert_eq!(err.kind(), ErrorKind::PermissionDenied);
    assert!(err.to_string().starts_with("a/x: "), "{err}");
    assert_eq!(count(&outside), 0);
}

/// A parent symlink from the archive with a target inside the destination.
#[test]
fn follows_symlink_parent_from_archive() {
    let tmp = TempDir::new();
    let data = archive(vec![
        dir("b/"),
        TarSymlink::new("a", "b").into(),
        file("a/x", b"x"),
    ]);
    extract(&data, tmp.path()).unwrap();
    assert_eq!(fs::read(tmp.path().join("b/x")).unwrap(), b"x");
    assert_eq!(fs::read_link(tmp.path().join("a")).unwrap(), Path::new("b"));
    assert_eq!(count(tmp.path()), 2);
}

/// A parent symlink that is in the destination before the extraction, with
/// a target inside the destination.
#[test]
fn follows_existing_symlink_parent() {
    let tmp = TempDir::new();
    fs::create_dir_all(tmp.path().join("usr/lib")).unwrap();
    symlink("usr/lib", tmp.path().join("lib")).unwrap();
    extract(&archive(vec![file("lib/f", b"f")]), tmp.path()).unwrap();
    assert_eq!(fs::read(tmp.path().join("usr/lib/f")).unwrap(), b"f");
    assert_eq!(
        fs::read_link(tmp.path().join("lib")).unwrap(),
        Path::new("usr/lib")
    );
}

/// An absolute parent symlink gives an error, also when its target is in
/// the destination. The sink creates no node and no missing parent
/// directory.
#[test]
fn rejects_absolute_symlink_parent_inside_dest() {
    let tmp = TempDir::new();
    let dest = subdir(&tmp, "dest");
    let target = subdir(&tmp, "dest/d");
    symlink(&target, dest.join("s")).unwrap();
    for path in ["s/x", "s/e/x"] {
        let err = extract(&archive(vec![file(path, b"x")]), &dest).unwrap_err();
        assert_eq!(err.kind(), ErrorKind::PermissionDenied);
        assert!(err.to_string().starts_with(&format!("{path}: ")), "{err}");
        let msg = "a path led outside of the filesystem";
        assert!(err.to_string().contains(msg), "{err}");
        assert_eq!(count(&target), 0);
        assert_eq!(count(&dest), 2);
        assert_eq!(fs::read_link(dest.join("s")).unwrap(), target);
        assert_eq!(count(tmp.path()), 1);
    }
}

/// A hard link target below a parent symlink inside the destination. The
/// link goes to the node behind the symlink.
#[test]
fn link_target_below_symlink_parent() {
    let tmp = TempDir::new();
    let data = archive(vec![
        dir("b/"),
        TarSymlink::new("a", "b").into(),
        file("a/x", b"x"),
        link("h", "a/x"),
    ]);
    extract(&data, tmp.path()).unwrap();
    assert_eq!(
        fs::metadata(tmp.path().join("b/x")).unwrap().ino(),
        fs::symlink_metadata(tmp.path().join("h")).unwrap().ino()
    );
}

/// A hard link with a path that leads through a parent symlink to its own
/// target. The sink keeps the target.
#[test]
fn link_to_itself_through_symlink_parent() {
    let tmp = TempDir::new();
    let data = archive(vec![
        dir("d/"),
        file("d/f", b"f"),
        TarSymlink::new("l", "d").into(),
        link("l/f", "d/f"),
    ]);
    extract(&data, tmp.path()).unwrap();
    assert_eq!(fs::read(tmp.path().join("d/f")).unwrap(), b"f");
    assert_eq!(fs::metadata(tmp.path().join("d/f")).unwrap().nlink(), 1);
    assert_eq!(fs::read_link(tmp.path().join("l")).unwrap(), Path::new("d"));
}

/// A hard link with a target path that leads through a parent symlink to
/// the path of the link. The sink keeps the target.
#[test]
fn link_to_itself_through_symlink_target_parent() {
    let tmp = TempDir::new();
    let data = archive(vec![
        dir("d/"),
        TarSymlink::new("l", "d").into(),
        file("l/f", b"f"),
        link("d/f", "l/f"),
    ]);
    extract(&data, tmp.path()).unwrap();
    assert_eq!(fs::read(tmp.path().join("d/f")).unwrap(), b"f");
    assert_eq!(fs::metadata(tmp.path().join("d/f")).unwrap().nlink(), 1);
    assert_eq!(fs::read_link(tmp.path().join("l")).unwrap(), Path::new("d"));
}

/// A file in a batch replaces a parent symlink. A later file of the batch
/// below that path does not go to the old target of the symlink.
#[test]
fn file_in_batch_replaces_parent_symlink() {
    let tmp = TempDir::new();
    let data = archive(vec![
        dir("a/"),
        TarSymlink::new("l", "a").into(),
        file("l/f1", b"1"),
        file("l", b"l"),
        file("l/f2", b"2"),
    ]);
    let err = block_on(FsSink::new(tmp.path()).extract(TarReader::new(&data[..]))).unwrap_err();
    assert_eq!(err.kind(), ErrorKind::NotADirectory);
    assert!(err.to_string().starts_with("l/f2: "), "{err}");
    assert_eq!(fs::read(tmp.path().join("a/f1")).unwrap(), b"1");
    assert_eq!(fs::read(tmp.path().join("l")).unwrap(), b"l");
    assert!(!tmp.path().join("a/f2").exists());
}

/// A symlink entry replaces a parent symlink. A later entry below that path
/// goes to the new target.
#[test]
fn symlink_replaces_parent_symlink() {
    let tmp = TempDir::new();
    let data = archive(vec![
        dir("x/"),
        dir("y/"),
        TarSymlink::new("l", "x").into(),
        TarSymlink::new("l/s1", "t").into(),
        TarSymlink::new("l", "y").into(),
        TarSymlink::new("l/s2", "t").into(),
        file("l/f", b"f"),
    ]);
    block_on(FsSink::new(tmp.path()).extract(TarReader::new(&data[..]))).unwrap();
    assert!(fs::symlink_metadata(tmp.path().join("x/s1")).is_ok());
    assert!(fs::symlink_metadata(tmp.path().join("y/s2")).is_ok());
    assert_eq!(fs::read(tmp.path().join("y/f")).unwrap(), b"f");
    assert_eq!(count(&tmp.path().join("x")), 1);
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

/// The archive changes the parent symlink of a link target. The target path
/// then leads to a node that the sink did not create.
#[test]
fn link_target_behind_changed_symlink() {
    let data = archive(vec![
        TarSymlink::new("a", "b").into(),
        dir("b/"),
        file("a/x", b"x"),
        TarSymlink::new("a", "c").into(),
        link("h", "a/x"),
    ]);
    let tmp = TempDir::new();
    fs::write(subdir(&tmp, "c").join("x"), b"old").unwrap();
    let err = extract(&data, tmp.path()).unwrap_err();
    assert_eq!(err.kind(), ErrorKind::NotFound);
    assert!(err.to_string().starts_with("h: "), "{err}");
    assert!(err.to_string().contains("is not in the archive"), "{err}");
    assert!(fs::symlink_metadata(tmp.path().join("h")).is_err());
    assert_eq!(fs::metadata(tmp.path().join("c/x")).unwrap().nlink(), 1);
    let tmp = TempDir::new();
    fs::write(subdir(&tmp, "c").join("x"), b"old").unwrap();
    let sink = FsSink::new(tmp.path()).with_missing_link_target(MissingLinkTarget::Skip);
    extract_into(&data, sink).unwrap();
    assert!(fs::symlink_metadata(tmp.path().join("h")).is_err());
    assert_eq!(fs::metadata(tmp.path().join("c/x")).unwrap().nlink(), 1);
    assert_eq!(fs::read(tmp.path().join("b/x")).unwrap(), b"x");
}

/// Extract `data` with the default policy and with
/// `MissingLinkTarget::Skip`. The hard link `h` has a target path that
/// leads to no node.
fn check_link_target_leads_to_no_node(data: &[u8]) {
    let tmp = TempDir::new();
    let err = extract(data, tmp.path()).unwrap_err();
    assert_eq!(err.kind(), ErrorKind::NotFound);
    let msg = "h: link target a/x is not in the archive";
    assert_eq!(err.to_string(), msg);
    assert!(fs::symlink_metadata(tmp.path().join("h")).is_err());
    let tmp = TempDir::new();
    let sink = FsSink::new(tmp.path()).with_missing_link_target(MissingLinkTarget::Skip);
    extract_into(data, sink).unwrap();
    assert!(fs::symlink_metadata(tmp.path().join("h")).is_err());
    assert_eq!(fs::read(tmp.path().join("b/x")).unwrap(), b"x");
}

/// The archive changes the parent symlink of a link target to a directory
/// without the target name.
#[test]
fn link_target_behind_changed_symlink_is_missing() {
    check_link_target_leads_to_no_node(&archive(vec![
        TarSymlink::new("a", "b").into(),
        dir("b/"),
        file("a/x", b"x"),
        dir("c/"),
        TarSymlink::new("a", "c").into(),
        link("h", "a/x"),
    ]));
}

/// The archive changes the parent symlink of a link target to a dangling
/// symlink.
#[test]
fn link_target_behind_dangling_symlink() {
    check_link_target_leads_to_no_node(&archive(vec![
        TarSymlink::new("a", "b").into(),
        dir("b/"),
        file("a/x", b"x"),
        TarSymlink::new("a", "e").into(),
        link("h", "a/x"),
    ]));
}

/// A later file entry replaces the parent symlink of a link target.
#[test]
fn link_target_behind_symlink_replaced_by_file() {
    check_link_target_leads_to_no_node(&archive(vec![
        TarSymlink::new("a", "b").into(),
        dir("b/"),
        file("a/x", b"x"),
        file("a", b"a"),
        link("h", "a/x"),
    ]));
}

/// The archive changes the parent symlink of a link target to a symlink to
/// a regular file.
#[test]
fn link_target_behind_symlink_to_file() {
    check_link_target_leads_to_no_node(&archive(vec![
        TarSymlink::new("a", "b").into(),
        dir("b/"),
        file("a/x", b"x"),
        file("r", b"r"),
        TarSymlink::new("a", "r").into(),
        link("h", "a/x"),
    ]));
}

/// The archive changes the parent symlink of a link target to a symlink to
/// itself.
#[test]
fn link_target_behind_symlink_loop() {
    check_link_target_leads_to_no_node(&archive(vec![
        TarSymlink::new("a", "b").into(),
        dir("b/"),
        file("a/x", b"x"),
        TarSymlink::new("a", "a").into(),
        link("h", "a/x"),
    ]));
}

/// A relative parent symlink in the destination that leads outside the
/// destination gives an error. The sink writes nothing outside.
#[test]
fn rejects_relative_symlink_parent_outside_dest() {
    let tmp = TempDir::new();
    let dest = subdir(&tmp, "dest");
    let out = subdir(&tmp, "out");
    symlink("../out", dest.join("s")).unwrap();
    let err = extract(&archive(vec![file("s/x", b"x")]), &dest).unwrap_err();
    assert_eq!(err.kind(), ErrorKind::PermissionDenied);
    assert!(err.to_string().starts_with("s/x: "), "{err}");
    assert_eq!(count(&out), 0);
    assert_eq!(count(&dest), 1);
}

/// A parent symlink below an open directory of the sink leads outside that
/// directory. A target inside the destination gives the parent directory. A
/// target outside the destination gives an error, and the sink writes
/// nothing outside.
#[test]
fn symlink_parent_below_open_directory() {
    let tmp = TempDir::new();
    let dest = subdir(&tmp, "dest");
    let out = subdir(&tmp, "out");
    let data = archive(vec![
        dir("a/"),
        dir("b/"),
        TarSymlink::new("a/l", "../b").into(),
        file("a/l/f", b"f"),
        TarSymlink::new("a/m", "../../out").into(),
        file("a/m/g", b"g"),
    ]);
    let err = extract(&data, &dest).unwrap_err();
    assert_eq!(err.kind(), ErrorKind::PermissionDenied);
    assert!(err.to_string().starts_with("a/m/g: "), "{err}");
    assert_eq!(fs::read(dest.join("b/f")).unwrap(), b"f");
    assert_eq!(count(&dest.join("b")), 1);
    assert_eq!(count(&out), 0);
}

#[test]
fn link_with_data() {
    let tmp = TempDir::new();
    let mut hidden = Vec::new();
    push_raw(&mut hidden, "smuggled.txt", b'0', "", b"pwned\n");
    let mut data = Vec::new();
    push_raw(&mut data, "PaxHeader", b'x', "", b"11 mtime=1\n");
    push_raw(&mut data, "data.bin", b'0', "", &hidden);
    push_raw(&mut data, "PaxHeader", b'x', "", b"11 mtime=1\n");
    push_raw(&mut data, "hl", b'1', "data.bin", &hidden);
    push_raw(&mut data, "visible1.txt", b'0', "", b"second\n");
    data.extend_from_slice(&[0; 1024]);
    extract(&data, tmp.path()).unwrap();
    assert!(!tmp.path().join("smuggled.txt").exists());
    let visible = fs::read(tmp.path().join("visible1.txt")).unwrap();
    assert_eq!(visible, b"second\n");
    let meta = fs::metadata(tmp.path().join("data.bin")).unwrap();
    assert_eq!(meta.len(), 1024);
    let link = fs::metadata(tmp.path().join("hl")).unwrap();
    assert_eq!(meta.ino(), link.ino());
    assert_eq!(count(tmp.path()), 3);
}

#[test]
fn symlink_with_data() {
    let tmp = TempDir::new();
    let mut hidden = Vec::new();
    push_raw(&mut hidden, "smuggled.txt", b'0', "", b"pwned\n");
    let mut data = Vec::new();
    push_raw(&mut data, "visible0.txt", b'0', "", b"first\n");
    push_raw(&mut data, "sl", b'2', "visible0.txt", &hidden);
    push_raw(&mut data, "visible1.txt", b'0', "", b"second\n");
    data.extend_from_slice(&[0; 1024]);
    let err = extract(&data, tmp.path()).unwrap_err();
    assert_eq!(err.kind(), ErrorKind::InvalidData);
    let msg = err.to_string();
    assert!(msg.contains("symlink entry has a non-zero size"), "{msg}");
    assert!(!tmp.path().join("smuggled.txt").exists());
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

/// A file gets a mode without write permission. The sink can still write the
/// body.
#[test]
fn file_mode_without_write_permission() {
    let tmp = TempDir::new();
    let data = archive(vec![
        TarRegularFile::new("r", 1, &b"r"[..])
            .with_mode(0o444)
            .into(),
        TarRegularFile::new("n", 1, &b"n"[..])
            .with_mode(0o000)
            .into(),
    ]);
    block_on(FsSink::new(tmp.path()).extract(TarReader::new(&data[..]))).unwrap();
    assert_eq!(mode(&tmp.path().join("r")), 0o444);
    assert_eq!(mode(&tmp.path().join("n")), 0o000);
    assert_eq!(fs::metadata(tmp.path().join("r")).unwrap().len(), 1);
    assert_eq!(fs::metadata(tmp.path().join("n")).unwrap().len(), 1);
    if let Ok(body) = fs::read(tmp.path().join("r")) {
        assert_eq!(body, b"r");
    }
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
    let setgid = kept_setgid(tmp.path());
    assert_eq!(mode(&tmp.path().join("d")), 0o555 | setgid);
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
    let setgid = kept_setgid(tmp.path());
    assert_eq!(mode(&tmp.path().join("d")), 0o600 | setgid);
}

/// Two entries reach one directory, one of them through a parent symlink.
/// The last entry sets the metadata.
#[test]
fn last_entry_through_symlink_sets_directory_metadata() {
    let tmp = TempDir::new();
    let data = archive(vec![
        dir("b/"),
        TarSymlink::new("a", "b").into(),
        TarDirectory::new("b/d/").with_mode(0o700).into(),
        TarDirectory::new("a/d/").with_mode(0o750).into(),
    ]);
    extract(&data, tmp.path()).unwrap();
    let setgid = kept_setgid(tmp.path());
    assert_eq!(mode(&tmp.path().join("b/d")), 0o750 | setgid);
}

/// An entry reaches a directory through a parent symlink. The entry path has
/// the same number of components as the path of the parent directory on
/// disk. The finish sets the mode of the parent after the child.
#[test]
fn directory_through_symlink_before_its_parent() {
    // SAFETY: `geteuid` has no preconditions.
    if unsafe { libc::geteuid() } == 0 {
        eprintln!("skip: the process runs as root");
        return;
    }
    let tmp = TempDir::new();
    let data = archive(vec![
        TarDirectory::new("x/").with_mode(0o755).into(),
        TarDirectory::new("x/y/").with_mode(0o000).into(),
        TarSymlink::new("zz", "x/y").into(),
        TarDirectory::new("zz/z/").with_mode(0o755).into(),
    ]);
    let result = block_on(FsSink::new(tmp.path()).extract(TarReader::new(&data[..])));
    let y = tmp.path().join("x/y");
    let y_mode = mode(&y);
    fs::set_permissions(&y, Permissions::from_mode(0o755)).unwrap();
    result.unwrap();
    assert!(fs::symlink_metadata(y.join("z")).unwrap().is_dir());
    assert_eq!(y_mode, kept_setgid(tmp.path()));
}

/// The destination has the directory `x` with search permission and no read
/// permission. A directory entry below `x` gets its metadata. The sink finds
/// the depth of the new directory on disk with search permission only.
#[cfg(any(target_os = "linux", target_os = "android", target_os = "freebsd"))]
#[test]
fn directory_below_unreadable_ancestor() {
    // SAFETY: `geteuid` has no preconditions.
    if unsafe { libc::geteuid() } == 0 {
        eprintln!("skip: the process runs as root");
        return;
    }
    let tmp = TempDir::new();
    let x = subdir(&tmp, "x");
    fs::create_dir(x.join("z")).unwrap();
    fs::set_permissions(&x, Permissions::from_mode(0o311)).unwrap();
    let data = archive(vec![
        file("x/z/f", b"f"),
        TarDirectory::new("x/z/y/")
            .with_mode(0o750)
            .with_mtime(mtime())
            .into(),
    ]);
    let result = extract(&data, tmp.path());
    let x_mode = mode(&x);
    fs::set_permissions(&x, Permissions::from_mode(0o755)).unwrap();
    result.unwrap();
    assert_eq!(x_mode, 0o311);
    assert_eq!(fs::read(x.join("z/f")).unwrap(), b"f");
    let y = x.join("z/y");
    assert_eq!(mode(&y) & 0o777, 0o750);
    assert_eq!(fs::metadata(&y).unwrap().mtime(), MTIME as i64);
}

#[test]
fn implicit_parent() {
    let tmp = TempDir::new();
    // A directory that the process creates with the mode 0o777 and its umask.
    let ref_mode = mode(&subdir(&tmp, "ref")) & 0o777;
    extract(&archive(vec![file("a/b/c", b"c")]), tmp.path()).unwrap();
    for path in ["a", "a/b"] {
        let path = tmp.path().join(path);
        assert!(fs::symlink_metadata(&path).unwrap().is_dir());
        assert_eq!(mode(&path) & 0o777, ref_mode);
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
    let message = format!("g: cannot set the owner to uid {}, gid {gid}: ", uid + 1);
    assert!(err.to_string().starts_with(&message), "{err}");
}

/// A handler that accepts the owner errors lets the sink continue. The nodes
/// keep the user of the process. A regular file and a FIFO do not get the
/// setuid and setgid bits of the entry. A directory gets them.
#[test]
fn owner_error_handler_continues() {
    let tmp = TempDir::new();
    let (uid, gid) = owner_in(tmp.path());
    if uid == 0 {
        eprintln!("skip: the process runs as root");
        return;
    }
    // A user that is not a member of the group of a directory cannot set the
    // setgid bit on it.
    let probe = subdir(&tmp, "probe");
    fs::set_permissions(&probe, Permissions::from_mode(0o2755)).unwrap();
    let setgid = mode(&probe) & 0o2000;
    let dest = subdir(&tmp, "dest");
    let big = data(BIG, 0);
    let data = archive(vec![
        TarRegularFile::new("a", 1, &b"a"[..])
            .with_mode(0o4755)
            .with_uid(uid + 1)
            .with_gid(gid)
            .into(),
        TarRegularFile::new("b", big.len() as u64, &big[..])
            .with_mode(0o6755)
            .with_uid(uid + 1)
            .with_gid(gid)
            .into(),
        TarSymlink::new("s", "a")
            .with_uid(uid + 1)
            .with_gid(gid)
            .into(),
        TarFifo::new("p")
            .with_mode(0o2644)
            .with_uid(uid + 1)
            .with_gid(gid)
            .into(),
        TarDirectory::new("d/")
            .with_mode(0o2755)
            .with_uid(uid + 1)
            .with_gid(gid)
            .into(),
        TarRegularFile::new("c", 1, &b"c"[..])
            .with_mode(0o5755)
            .with_uid(uid + 1)
            .with_gid(gid)
            .into(),
        TarFifo::new("q")
            .with_mode(0o7644)
            .with_uid(uid + 1)
            .with_gid(gid)
            .into(),
    ]);
    let errors = Arc::new(Mutex::new(Vec::new()));
    let sink = FsSink::new(&dest)
        .with_owner(true)
        .with_special_bits(true)
        .with_owner_error({
            let errors = Arc::clone(&errors);
            move |path, err| {
                errors.lock().unwrap().push((path.to_owned(), err.kind()));
                Ok(())
            }
        });
    extract_into(&data, sink).unwrap();
    let mut errors = errors.lock().unwrap().clone();
    errors.sort();
    let denied = |path: &str| (path.to_owned(), ErrorKind::PermissionDenied);
    assert_eq!(errors, ["a", "b", "c", "d", "p", "q", "s"].map(denied));
    for path in ["a", "b", "c", "s", "p", "q", "d"] {
        let meta = fs::symlink_metadata(dest.join(path)).unwrap();
        assert_eq!(meta.uid(), uid, "{path}");
    }
    assert_eq!(mode(&dest.join("a")), 0o755);
    assert_eq!(mode(&dest.join("b")), 0o755);
    assert_eq!(mode(&dest.join("c")), 0o1755);
    assert_eq!(mode(&dest.join("p")), 0o644);
    assert_eq!(mode(&dest.join("q")), 0o1644);
    assert_eq!(mode(&dest.join("d")), 0o755 | setgid);
    assert_eq!(fs::read(dest.join("a")).unwrap(), b"a");
    assert_eq!(fs::read(dest.join("b")).unwrap(), big);
}

/// Without a mode and a time to set, the sink sets the owner of a directory
/// when it creates the directory. The handler lets the sink continue after
/// an owner change that the system refuses.
#[test]
fn owner_error_handler_continues_without_mode() {
    let tmp = TempDir::new();
    let (uid, gid) = owner_in(tmp.path());
    if uid == 0 {
        eprintln!("skip: the process runs as root");
        return;
    }
    let dest = subdir(&tmp, "dest");
    let data = archive(vec![
        TarDirectory::new("d/")
            .with_uid(uid + 1)
            .with_gid(gid)
            .into(),
        TarRegularFile::new("d/f", 1, &b"f"[..])
            .with_uid(uid + 1)
            .with_gid(gid)
            .into(),
    ]);
    let errors = Arc::new(Mutex::new(Vec::new()));
    let sink = FsSink::new(&dest)
        .with_owner(true)
        .with_mode(ModePolicy::Skip)
        .with_mtime(false)
        .with_owner_error({
            let errors = Arc::clone(&errors);
            move |path, err| {
                errors.lock().unwrap().push((path.to_owned(), err.kind()));
                Ok(())
            }
        });
    extract_into(&data, sink).unwrap();
    let mut errors = errors.lock().unwrap().clone();
    errors.sort();
    let denied = |path: &str| (path.to_owned(), ErrorKind::PermissionDenied);
    assert_eq!(errors, ["d", "d/f"].map(denied));
    for path in ["d", "d/f"] {
        let meta = fs::symlink_metadata(dest.join(path)).unwrap();
        assert_eq!(meta.uid(), uid, "{path}");
    }
}

/// With the owner option, a regular file gets the mode of the entry after
/// the owner change. This applies to a file in a batch and to a larger file.
/// The modes 0o640 and 0o750 need a mode change after the owner change. The
/// mode 0o644 does not.
#[test]
fn owner_kept_with_file_mode() {
    let tmp = TempDir::new();
    let (uid, gid) = owner_in(tmp.path());
    let big = data(BIG, 0);
    let tar = archive(vec![
        TarRegularFile::new("small", 1, &b"s"[..])
            .with_mode(0o640)
            .with_uid(uid)
            .with_gid(gid)
            .into(),
        TarRegularFile::new("big", big.len() as u64, &big[..])
            .with_mode(0o750)
            .with_uid(uid)
            .with_gid(gid)
            .into(),
        TarRegularFile::new("plain", 1, &b"p"[..])
            .with_mode(0o644)
            .with_uid(uid)
            .with_gid(gid)
            .into(),
    ]);
    extract_into(&tar, FsSink::new(tmp.path()).with_owner(true)).unwrap();
    assert_eq!(mode(&tmp.path().join("small")), 0o640);
    assert_eq!(mode(&tmp.path().join("big")), 0o750);
    assert_eq!(mode(&tmp.path().join("plain")), 0o644);
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

/// Give entries of each kind with the mode `mode` and the time `MTIME`: the
/// file `f`, the file `big` with the body `big`, the directory `d` with the
/// file `d/x`, the FIFO `p` and the symlink `s`.
fn kinds(mode: u32, big: &[u8]) -> Vec<TarEntry<'_, &[u8]>> {
    vec![
        TarRegularFile::new("f", 1, &b"f"[..])
            .with_mode(mode)
            .with_mtime(mtime())
            .into(),
        TarRegularFile::new("big", big.len() as u64, big)
            .with_mode(mode)
            .with_mtime(mtime())
            .into(),
        TarDirectory::new("d/")
            .with_mode(mode)
            .with_mtime(mtime())
            .into(),
        TarRegularFile::new("d/x", 1, &b"x"[..])
            .with_mode(mode)
            .with_mtime(mtime())
            .into(),
        TarFifo::new("p").with_mode(mode).with_mtime(mtime()).into(),
        TarSymlink::new("s", "f").with_mtime(mtime()).into(),
    ]
}

/// Size of the body of `big` in `kinds`. The sink does not write it in a
/// batch.
const BIG: usize = 300 * 1024;

#[test]
fn mtime_skipped() {
    let tmp = TempDir::new();
    let big = data(BIG, 0);
    let tar = archive(kinds(0o755, &big));
    extract_into(&tar, FsSink::new(tmp.path()).with_mtime(false)).unwrap();
    for path in ["f", "big", "d", "d/x", "p", "s"] {
        let meta = fs::symlink_metadata(tmp.path().join(path)).unwrap();
        assert_ne!(meta.mtime(), MTIME as i64, "{path}");
    }
    assert_eq!(fs::read(tmp.path().join("big")).unwrap(), big);
}

#[test]
fn mode_masked() {
    let tmp = TempDir::new();
    let big = data(BIG, 0);
    let tar = archive(kinds(0o4777, &big));
    let sink = FsSink::new(tmp.path()).with_mode(ModePolicy::Mask(0o027));
    extract_into(&tar, sink).unwrap();
    let setgid = kept_setgid(tmp.path());
    for (path, expect) in [
        ("f", 0o750),
        ("big", 0o750),
        ("d", 0o750 | setgid),
        ("d/x", 0o750),
        ("p", 0o750),
    ] {
        assert_eq!(mode(&tmp.path().join(path)), expect, "{path}");
    }
}

#[test]
fn mode_masked_keeps_special_bits() {
    let tmp = TempDir::new();
    let entry = TarRegularFile::new("f", 1, &b"f"[..]).with_mode(0o4777);
    let tar = archive(vec![entry.into()]);
    // The sink ignores the bits of the mask outside 0o777.
    let sink = FsSink::new(tmp.path())
        .with_mode(ModePolicy::Mask(0o7022))
        .with_special_bits(true);
    extract_into(&tar, sink).unwrap();
    assert_eq!(mode(&tmp.path().join("f")), 0o4755);
}

/// Without `with_special_bits`, a directory keeps the setgid bit that it
/// gets from its parent.
#[test]
fn directory_keeps_parent_setgid() {
    let tmp = TempDir::new();
    let Some(dest) = setgid_dir(tmp.path(), "dest") else {
        return;
    };
    let tar = archive(vec![
        TarDirectory::new("d/").with_mode(0o750).into(),
        TarDirectory::new("d/e/").with_mode(0o700).into(),
        file("d/e/f", b"f"),
    ]);
    extract(&tar, &dest).unwrap();
    assert_eq!(mode(&dest.join("d")), 0o2750);
    assert_eq!(mode(&dest.join("d/e")), 0o2700);
    assert_eq!(mode(&dest.join("d/e/f")), 0o644);
}

/// With `with_special_bits`, a directory gets the exact mode of the entry.
#[test]
fn special_bits_directory_loses_parent_setgid() {
    let tmp = TempDir::new();
    let Some(dest) = setgid_dir(tmp.path(), "dest") else {
        return;
    };
    let tar = archive(vec![
        TarDirectory::new("d/").with_mode(0o755).into(),
        TarDirectory::new("s/").with_mode(0o2750).into(),
    ]);
    extract_into(&tar, FsSink::new(&dest).with_special_bits(true)).unwrap();
    assert_eq!(mode(&dest.join("d")), 0o755);
    assert_eq!(mode(&dest.join("s")), 0o2750);
}

/// Without `with_special_bits`, the sink ignores the special bits of a
/// directory entry. An existing directory keeps its special bits.
#[test]
fn mode_masked_ignores_directory_special_bits() {
    let tmp = TempDir::new();
    let dest = subdir(&tmp, "dest");
    fs::set_permissions(&dest, Permissions::from_mode(0o755)).unwrap();
    let ex = dest.join("ex");
    fs::create_dir(&ex).unwrap();
    fs::set_permissions(&ex, Permissions::from_mode(0o2700)).unwrap();
    let ex_setgid = mode(&ex) == 0o2700;
    let tar = archive(vec![
        TarDirectory::new("sg/").with_mode(0o2755).into(),
        TarDirectory::new("sk/").with_mode(0o1755).into(),
        TarDirectory::new("ex/").with_mode(0o755).into(),
    ]);
    let sink = FsSink::new(&dest).with_mode(ModePolicy::Mask(0o022));
    extract_into(&tar, sink).unwrap();
    assert_eq!(mode(&dest.join("sg")), 0o755);
    assert_eq!(mode(&dest.join("sk")), 0o755);
    if !ex_setgid {
        eprintln!("skip: the process cannot set the setgid bit");
        return;
    }
    assert_eq!(mode(&ex), 0o2755);
}

#[test]
fn mode_masked_read_only_directory() {
    let tmp = TempDir::new();
    let tar = archive(vec![
        TarDirectory::new("d/").with_mode(0o755).into(),
        file("d/x", b"x"),
    ]);
    let sink = FsSink::new(tmp.path()).with_mode(ModePolicy::Mask(0o222));
    extract_into(&tar, sink).unwrap();
    assert_eq!(fs::read(tmp.path().join("d/x")).unwrap(), b"x");
    let setgid = kept_setgid(tmp.path());
    assert_eq!(mode(&tmp.path().join("d")), 0o555 | setgid);
}

#[test]
fn mode_masked_implicit_parent() {
    let tmp = TempDir::new();
    // A directory that the process creates with the mode 0o777 and its umask.
    let ref_mode = mode(&subdir(&tmp, "ref"));
    let tar = archive(vec![file("secret/sub/file", b"f")]);
    let sink = FsSink::new(tmp.path()).with_mode(ModePolicy::Mask(0o077));
    extract_into(&tar, sink).unwrap();
    for path in ["secret", "secret/sub"] {
        assert_eq!(mode(&tmp.path().join(path)), ref_mode & !0o077, "{path}");
    }
}

#[test]
fn mode_skipped() {
    let tmp = TempDir::new();
    // Nodes that the process creates with the default modes and its umask.
    fs::write(tmp.path().join("ref-file"), b"").unwrap();
    let ref_dir = subdir(&tmp, "ref-dir");
    make_fifo(&tmp.path().join("ref-fifo"), 0o666);
    let file_mode = mode(&tmp.path().join("ref-file"));
    let dir_mode = mode(&ref_dir);
    let fifo_mode = mode(&tmp.path().join("ref-fifo"));
    let big = data(BIG, 0);
    let tar = archive(kinds(0o4500, &big));
    for mtime in [true, false] {
        let dest = subdir(&tmp, &format!("dest-{mtime}"));
        let sink = FsSink::new(&dest)
            .with_mode(ModePolicy::Skip)
            .with_special_bits(true)
            .with_mtime(mtime);
        extract_into(&tar, sink).unwrap();
        for (path, expect) in [
            ("f", file_mode),
            ("big", file_mode),
            ("d", dir_mode),
            ("d/x", file_mode),
            ("p", fifo_mode),
        ] {
            assert_eq!(mode(&dest.join(path)), expect, "{path} mtime={mtime}");
        }
        let meta = fs::metadata(dest.join("d")).unwrap();
        assert_eq!(meta.mtime() == MTIME as i64, mtime);
    }
}

#[test]
fn mode_skipped_keeps_existing_directory() {
    let tmp = TempDir::new();
    let dir = subdir(&tmp, "d");
    fs::set_permissions(&dir, Permissions::from_mode(0o751)).unwrap();
    let tar = archive(vec![TarDirectory::new("d/").with_mode(0o700).into()]);
    extract_into(&tar, FsSink::new(tmp.path()).with_mode(ModePolicy::Skip)).unwrap();
    assert_eq!(mode(&dir), 0o751);
}

#[cfg(any(target_os = "linux", target_os = "android", target_vendor = "apple"))]
mod xattr {
    #[cfg(target_os = "linux")]
    use smol_tar_fs::UnsupportedXattr;
    use {
        super::*,
        rustix::{
            fs::{lgetxattr, llistxattr, setxattr, XattrFlags},
            io::Errno,
        },
        smol_tar::AttrList,
        std::sync::{Arc, Mutex},
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

    /// Give `true` if the process can set `trusted.x` on `dir`. Print a skip
    /// message if not.
    #[cfg(target_os = "linux")]
    fn trusted_xattrs_allowed(dir: &Path) -> bool {
        if setxattr(dir, "trusted.x", b"1", XattrFlags::empty()).is_ok() {
            return true;
        }
        eprintln!("skip: the process cannot set trusted xattrs");
        false
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
        let setgid = kept_setgid(tmp.path());
        assert_eq!(mode(&tmp.path().join("ro")), 0o555 | setgid);
    }

    #[test]
    fn directory_xattrs_with_mode_and_mtime_skipped() {
        let tmp = TempDir::new();
        if !user_xattrs_supported(tmp.path()) {
            return;
        }
        let data = archive(vec![
            TarDirectory::new("d/")
                .with_attrs(AttrList::new().with("user.d", &b"d"[..]))
                .into(),
            file("d/x", b"x"),
        ]);
        let sink = FsSink::new(tmp.path())
            .with_xattrs(true)
            .with_mode(ModePolicy::Skip)
            .with_mtime(false);
        extract_into(&data, sink).unwrap();
        assert_eq!(
            user_attrs(&tmp.path().join("d")),
            owned(&[("user.d", b"d")])
        );
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
        let sink = FsSink::new(tmp.path())
            .with_xattrs(true)
            .with_xattr_filter(|_| true);
        let err = extract_into(&data, sink).unwrap_err();
        assert_eq!(err.kind(), ErrorKind::PermissionDenied);
        assert!(err.to_string().starts_with("f: xattr trusted.x: "), "{err}");
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn xattr_policy_skip() {
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
            .with_xattr_filter(|_| true)
            .with_unsupported_xattr(UnsupportedXattr::Skip);
        extract_into(&data, sink).unwrap();
        assert_eq!(
            user_attrs(&tmp.path().join("f")),
            owned(&[("user.a", b"1")])
        );
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn xattr_default_filter() {
        let tmp = TempDir::new();
        if !user_xattrs_supported(tmp.path()) {
            return;
        }
        let trusted = || AttrList::new().with("trusted.b", &b"2"[..]);
        let data = archive(vec![
            file_with(
                "f",
                &[
                    ("user.a", b"1"),
                    ("trusted.b", b"2"),
                    ("security.capability", b"invalid"),
                ],
            ),
            TarDirectory::new("d/").with_attrs(trusted()).into(),
            TarSymlink::new("s", "f").with_attrs(trusted()).into(),
        ]);
        let sink = FsSink::new(tmp.path())
            .with_xattrs(true)
            .with_unsupported_xattr(UnsupportedXattr::Error);
        extract_into(&data, sink).unwrap();
        let path = tmp.path().join("f");
        assert_eq!(user_attrs(&path), owned(&[("user.a", b"1")]));
        // Without CAP_SYS_ADMIN, a `trusted.*` read gives ENODATA: `trusted.b` checks need root.
        for name in ["trusted.b", "security.capability"] {
            let err = lgetxattr(&path, name, &mut [0; 256][..]).unwrap_err();
            assert_eq!(err, Errno::NODATA, "{name}");
        }
        let dir = tmp.path().join("d");
        let err = lgetxattr(&dir, "trusted.b", &mut [0; 256][..]).unwrap_err();
        assert_eq!(err, Errno::NODATA);
    }

    #[test]
    fn xattr_filter_rejects_all() {
        let tmp = TempDir::new();
        if !user_xattrs_supported(tmp.path()) {
            return;
        }
        let mut attrs: Vec<(&str, &[u8])> = vec![("user.a", b"1")];
        if cfg!(target_os = "linux") {
            attrs.push(("trusted.b", b"2"));
        }
        let data = archive(vec![file_with("f", &attrs)]);
        let seen = Arc::new(Mutex::new(Vec::new()));
        let filter = {
            let seen = Arc::clone(&seen);
            move |name: &str| {
                seen.lock().unwrap().push(name.to_owned());
                false
            }
        };
        let sink = FsSink::new(tmp.path())
            .with_xattrs(true)
            .with_xattr_filter(filter.clone());
        extract_into(&data, sink).unwrap();
        assert!(user_attrs(&tmp.path().join("f")).is_empty());
        // The reader gives the attributes sorted by name.
        let mut names: Vec<_> = attrs.iter().map(|(name, _)| name.to_string()).collect();
        names.sort();
        assert_eq!(*seen.lock().unwrap(), names);

        let tmp = TempDir::new();
        seen.lock().unwrap().clear();
        extract_into(&data, FsSink::new(tmp.path()).with_xattr_filter(filter)).unwrap();
        assert!(seen.lock().unwrap().is_empty());
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn xattr_filter_allows_trusted() {
        let tmp = TempDir::new();
        if !user_xattrs_supported(tmp.path()) {
            return;
        }
        if !trusted_xattrs_allowed(tmp.path()) {
            return;
        }
        let data = archive(vec![file_with(
            "f",
            &[("user.a", b"1"), ("trusted.b", b"2")],
        )]);
        let sink = FsSink::new(tmp.path())
            .with_xattrs(true)
            .with_xattr_filter(|name| name.starts_with("user.") || name.starts_with("trusted."));
        extract_into(&data, sink).unwrap();
        let path = tmp.path().join("f");
        assert_eq!(user_attrs(&path), owned(&[("user.a", b"1")]));
        let mut value = [0; 256];
        let len = lgetxattr(&path, "trusted.b", &mut value[..]).unwrap();
        assert_eq!(&value[..len], b"2");
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
    use std::os::unix::fs::FileTypeExt;

    let tmp = TempDir::new();
    let entry = TarFifo::new("p").with_mode(0o640);
    extract(&archive(vec![entry.into()]), tmp.path()).unwrap();
    let path = tmp.path().join("p");
    assert!(fs::symlink_metadata(&path).unwrap().file_type().is_fifo());
    assert_eq!(mode(&path), 0o640);
}

#[test]
fn device_skipped_by_default() {
    let data = archive(vec![
        TarDevice::new_char("dev/null", 1, 3).into(),
        TarDevice::new_block("sda", 8, 0).into(),
        file("f", b"f"),
    ]);
    for policy in [None, Some(DeviceNodes::Skip)] {
        let tmp = TempDir::new();
        let mut sink = FsSink::new(tmp.path());
        if let Some(policy) = policy {
            sink = sink.with_device_nodes(policy);
        }
        extract_into(&data, sink).unwrap();
        assert!(!tmp.path().join("dev").exists());
        assert!(!tmp.path().join("sda").exists());
        assert_eq!(fs::read(tmp.path().join("f")).unwrap(), b"f");
        assert_eq!(count(tmp.path()), 1, "{policy:?}");
    }
}

#[test]
fn device_policy_error() {
    let tmp = TempDir::new();
    let data = archive(vec![
        file("a", b"a"),
        TarDevice::new_char("./d//null", 1, 3).into(),
        file("b", b"b"),
    ]);
    let sink = FsSink::new(tmp.path()).with_device_nodes(DeviceNodes::Error);
    let err = extract_into(&data, sink).unwrap_err();
    assert_eq!(err.kind(), ErrorKind::PermissionDenied);
    assert!(err.to_string().starts_with("d/null: "), "{err}");
    assert!(!tmp.path().join("d").exists());
    assert!(!tmp.path().join("b").exists());
    assert_eq!(fs::read(tmp.path().join("a")).unwrap(), b"a");
}

#[test]
fn device_policy_create() {
    use {
        rustix::fs::{major, minor},
        std::os::unix::fs::FileTypeExt,
    };

    let tmp = TempDir::new();
    let entry = TarDevice::new_char("null", 1, 3)
        .with_mode(0o640)
        .with_mtime(mtime());
    let data = archive(vec![entry.into()]);
    let sink = FsSink::new(tmp.path()).with_device_nodes(DeviceNodes::Create);
    let result = extract_into(&data, sink);
    if owner_in(tmp.path()).0 != 0 {
        let err = result.unwrap_err();
        assert_eq!(err.kind(), ErrorKind::PermissionDenied);
        assert!(err.to_string().starts_with("null: "), "{err}");
        assert!(!err.to_string().contains("not allowed"), "{err}");
        return;
    }
    if let Err(err) = &result {
        if err.kind() == ErrorKind::PermissionDenied && !err.to_string().contains("not allowed") {
            eprintln!("skip: no CAP_MKNOD");
            return;
        }
    }
    result.unwrap();
    let meta = fs::symlink_metadata(tmp.path().join("null")).unwrap();
    assert!(meta.file_type().is_char_device());
    assert_eq!((major(meta.rdev() as _), minor(meta.rdev() as _)), (1, 3));
    assert_eq!(meta.mode() & 0o7777, 0o640);
    assert_eq!(meta.mtime(), MTIME as i64);
}

#[test]
fn device_path_checked_before_policy() {
    let data = archive(vec![TarDevice::new_char("../x", 1, 3).into()]);
    for policy in [DeviceNodes::Create, DeviceNodes::Skip, DeviceNodes::Error] {
        let tmp = TempDir::new();
        let dest = subdir(&tmp, "dest");
        let sink = FsSink::new(&dest).with_device_nodes(policy);
        let err = extract_into(&data, sink).unwrap_err();
        assert_eq!(err.kind(), ErrorKind::InvalidData, "{policy:?}: {err}");
        assert!(err.to_string().starts_with("../x: "), "{err}");
        assert!(!tmp.path().join("x").exists());
        assert_eq!(count(&dest), 0);
    }
}

#[test]
fn link_to_skipped_device() {
    let data = archive(vec![
        TarDevice::new_char("null", 1, 3).into(),
        link("l", "null"),
    ]);
    let tmp = TempDir::new();
    let err = extract(&data, tmp.path()).unwrap_err();
    assert_eq!(err.kind(), ErrorKind::NotFound);
    let tmp = TempDir::new();
    let sink = FsSink::new(tmp.path()).with_missing_link_target(MissingLinkTarget::Skip);
    extract_into(&data, sink).unwrap();
    assert_eq!(count(tmp.path()), 0);
}

#[test]
fn skipped_device_is_not_link_target() {
    let tmp = TempDir::new();
    let data = archive(vec![
        file("x", b"x"),
        TarDevice::new_char("x", 1, 3).into(),
        link("y", "x"),
    ]);
    let err = extract(&data, tmp.path()).unwrap_err();
    assert_eq!(err.kind(), ErrorKind::NotFound);
    assert_eq!(fs::read(tmp.path().join("x")).unwrap(), b"x");
    assert!(!tmp.path().join("y").exists());
}

/// A skipped device is not a link target when the file is in a batch.
#[test]
fn skipped_device_is_not_link_target_in_batch() {
    let tmp = TempDir::new();
    let data = archive(vec![
        file("x", b"x"),
        TarDevice::new_char("x", 1, 3).into(),
        link("y", "x"),
    ]);
    let err = block_on(FsSink::new(tmp.path()).extract(TarReader::new(&data[..]))).unwrap_err();
    assert_eq!(err.kind(), ErrorKind::NotFound);
    assert_eq!(fs::read(tmp.path().join("x")).unwrap(), b"x");
    assert!(!tmp.path().join("y").exists());
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
        assert_eq!(mode(&path), 0o750 | kept_setgid(tmp.path()));
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
        // The sink writes a small file at the next flush.
        let mut sink = FsSink::new(tmp.path());
        sink.write(file("./d//x", b"x")).await.unwrap();
        let err = sink.flush().await.unwrap_err();
        assert!(err.to_string().starts_with("d/x: "), "{err}");
        let err = FsSink::new(tmp.path())
            .write(file("a/../b", b"b"))
            .await
            .unwrap_err();
        assert!(err.to_string().starts_with("a/../b: "), "{err}");
    });
}

#[test]
fn batches_keep_order_and_data() {
    let tmp = TempDir::new();
    // More files than one batch holds, in directories that alternate. Then
    // more bytes than one batch holds.
    let mut expected = Vec::new();
    for i in 0..150 {
        expected.push((format!("d{}/s/f{i}", i % 3), data(1 + i % 7, i)));
    }
    for i in 0..6 {
        expected.push((format!("big{i}"), data(200 * 1024, i)));
    }
    let entries = expected
        .iter()
        .map(|(path, data)| file(path, data))
        .collect();
    extract(&archive(entries), tmp.path()).unwrap();
    for (path, data) in &expected {
        assert_eq!(fs::read(tmp.path().join(path)).unwrap(), *data, "{path}");
    }
}

#[test]
fn same_path_twice_in_batch() {
    let tmp = TempDir::new();
    let data = archive(vec![file("x", b"one"), file("x", b"two")]);
    extract(&data, tmp.path()).unwrap();
    assert_eq!(fs::read(tmp.path().join("x")).unwrap(), b"two");
}

#[test]
fn directory_after_batched_file_with_same_name() {
    let tmp = TempDir::new();
    let data = archive(vec![
        file("x", b"x"),
        TarDirectory::new("x/").with_mtime(mtime()).into(),
    ]);
    extract(&data, tmp.path()).unwrap();
    assert!(tmp.path().join("x").is_dir());
}

#[test]
fn write_keeps_small_file_until_flush() {
    let tmp = TempDir::new();
    block_on(async {
        let mut sink = FsSink::new(tmp.path());
        sink.write(file("a", b"a")).await.unwrap();
        assert!(!tmp.path().join("a").exists());
        sink.flush().await.unwrap();
        assert_eq!(fs::read(tmp.path().join("a")).unwrap(), b"a");
        sink.write(file("b", b"b")).await.unwrap();
        sink.finish().await.unwrap();
    });
    assert_eq!(fs::read(tmp.path().join("b")).unwrap(), b"b");
}

/// A directory entry does not write the batch.
#[test]
fn directory_keeps_batch() {
    let tmp = TempDir::new();
    block_on(async {
        let mut sink = FsSink::new(tmp.path());
        sink.write(file("a/f", b"f")).await.unwrap();
        sink.write(dir("b/")).await.unwrap();
        assert!(!tmp.path().join("a/f").exists());
        sink.finish().await.unwrap();
    });
    assert_eq!(fs::read(tmp.path().join("a/f")).unwrap(), b"f");
    assert!(fs::symlink_metadata(tmp.path().join("b")).unwrap().is_dir());
}

/// A directory entry below a parent symlink sees the batched file that
/// creates the target of the symlink.
#[test]
fn directory_through_symlink_after_batched_file() {
    let tmp = TempDir::new();
    let data = archive(vec![
        TarSymlink::new("s", "x/y").into(),
        file("x/y/f", b"f"),
        dir("s/d/"),
    ]);
    block_on(FsSink::new(tmp.path()).extract(TarReader::new(&data[..]))).unwrap();
    assert!(fs::symlink_metadata(tmp.path().join("x/y/d"))
        .unwrap()
        .is_dir());
    assert_eq!(fs::read(tmp.path().join("x/y/f")).unwrap(), b"f");
}

/// A directory entry replaces a batched file with the same name. `extract`
/// does not flush between the entries.
#[test]
fn extract_directory_after_batched_file_with_same_name() {
    let tmp = TempDir::new();
    let data = archive(vec![file("a", b"a"), dir("a/")]);
    block_on(FsSink::new(tmp.path()).extract(TarReader::new(&data[..]))).unwrap();
    assert!(fs::symlink_metadata(tmp.path().join("a")).unwrap().is_dir());
}

/// A drop of the `extract` future while the stream is not ready discards
/// the batch.
#[test]
fn cancelled_extract_discards_batch() {
    let tmp = TempDir::new();
    block_on(async {
        let entries = futures::stream::iter([Ok::<_, io::Error>(file("a", b"x"))])
            .chain(futures::stream::pending());
        let mut fut = Box::pin(FsSink::new(tmp.path()).extract(entries));
        // The first poll puts "a" in the batch. Then the stream is not ready.
        assert!(futures::poll!(&mut fut).is_pending());
        drop(fut);
    });
    assert!(!tmp.path().join("a").exists());
}

/// A drop of the sink after `write` discards the batch.
#[test]
fn drop_discards_batch() {
    let tmp = TempDir::new();
    block_on(async {
        let mut sink = FsSink::new(tmp.path());
        sink.write(file("a", b"x")).await.unwrap();
        drop(sink);
    });
    assert!(!tmp.path().join("a").exists());
}

#[test]
fn drop_after_flush_does_not_panic() {
    let tmp = TempDir::new();
    block_on(async {
        let mut sink = FsSink::new(tmp.path());
        sink.write(file("f", b"f")).await.unwrap();
        sink.flush().await.unwrap();
        drop(sink);
    });
    assert_eq!(fs::read(tmp.path().join("f")).unwrap(), b"f");
}

#[test]
fn drop_after_finish_does_not_panic() {
    let tmp = TempDir::new();
    block_on(async {
        let mut sink = FsSink::new(tmp.path());
        sink.write(file("f", b"f")).await.unwrap();
        sink.finish().await.unwrap();
        drop(sink);
    });
    assert_eq!(fs::read(tmp.path().join("f")).unwrap(), b"f");
}

#[test]
fn batch_error_keeps_earlier_files() {
    let tmp = TempDir::new();
    fs::write(tmp.path().join("d"), b"").unwrap();
    let data = archive(vec![
        file("ok", b"ok"),
        file("d/x", b"x"),
        file("after", b"after"),
    ]);
    let err = extract(&data, tmp.path()).unwrap_err();
    assert!(err.to_string().starts_with("d/x: "), "{err}");
    assert_eq!(fs::read(tmp.path().join("ok")).unwrap(), b"ok");
    assert!(!tmp.path().join("after").exists());
}

#[test]
fn short_body_writes_earlier_files() {
    let tmp = TempDir::new();
    let body = data(1000, 0);
    let mut data = archive(vec![file("ok", b"ok"), file("bad", &body)]);
    // Keep the header of "bad" and a part of its body.
    data.truncate(512 * 4 + 100);
    let err = extract(&data, tmp.path()).unwrap_err();
    assert_eq!(err.kind(), ErrorKind::UnexpectedEof);
    assert!(err.to_string().starts_with("bad: "), "{err}");
    assert_eq!(fs::read(tmp.path().join("ok")).unwrap(), b"ok");
}

#[test]
fn extract_writes_all_entries() {
    let tmp = TempDir::new();
    let mut expected = Vec::new();
    for i in 0..150 {
        expected.push((format!("d{}/s/f{i}", i % 3), data(1 + i % 7, i)));
    }
    expected.push(("huge".to_owned(), data(300 * 1024, 0)));
    let mut entries: Vec<_> = expected
        .iter()
        .map(|(path, data)| file(path, data))
        .collect();
    entries.push(TarLink::new("link", "d0/s/f0").into());
    let data = archive(entries);
    block_on(FsSink::new(tmp.path()).extract(TarReader::new(&data[..]))).unwrap();
    for (path, data) in &expected {
        assert_eq!(fs::read(tmp.path().join(path)).unwrap(), *data, "{path}");
    }
    let meta = |path: &str| fs::metadata(tmp.path().join(path)).unwrap();
    assert_eq!(meta("link").ino(), meta("d0/s/f0").ino());
}

#[test]
fn extract_writes_files_in_many_directories() {
    // The files go to 200 directories in turn. This is more directories
    // than the sink keeps open.
    let tmp = TempDir::new();
    let expected: Vec<_> = (0..600)
        .map(|i| (format!("d{}/s{}/f{i}", i % 100, i % 2), data(1 + i % 7, i)))
        .collect();
    let entries = expected
        .iter()
        .map(|(path, data)| file(path, data))
        .collect();
    let data = archive(entries);
    block_on(FsSink::new(tmp.path()).extract(TarReader::new(&data[..]))).unwrap();
    for (path, data) in &expected {
        assert_eq!(fs::read(tmp.path().join(path)).unwrap(), *data, "{path}");
    }
    assert_eq!(count(tmp.path()), 100);
}

#[test]
fn extract_stream_error_writes_batch() {
    let tmp = TempDir::new();
    let mut data = archive(vec![file("ok", b"ok"), file("next", b"next")]);
    // Keep the entry "ok" and a part of the next header.
    data.truncate(512 * 2 + 100);
    block_on(FsSink::new(tmp.path()).extract(TarReader::new(&data[..]))).unwrap_err();
    assert_eq!(fs::read(tmp.path().join("ok")).unwrap(), b"ok");
}

/// A nested tree and a deep chain of directories with modes and times. The
/// entries go back from the deepest level of the chain to the first levels.
#[test]
fn nested_directories_round_trip() {
    let tmp = TempDir::new();
    let dir_mode = |depth: usize| 0o700 | ((depth as u32 % 3) << 3);
    // Paths in archive order. A path that ends with `/` is a directory.
    let mut paths = Vec::new();
    for a in 1..=3 {
        paths.push(format!("a{a}/"));
        for b in 1..=3 {
            paths.push(format!("a{a}/b{b}/"));
            for f in 1..=3 {
                paths.push(format!("a{a}/b{b}/f{f}"));
            }
        }
        paths.push(format!("a{a}/f"));
    }
    let chain: Vec<String> = (1..=20).map(|depth| vec!["c"; depth].join("/")).collect();
    paths.extend(chain.iter().map(|dir| format!("{dir}/")));
    paths.extend(chain.iter().rev().map(|dir| format!("{dir}/f")));
    paths.push("a1/b1/g".to_string());
    let entries = paths
        .iter()
        .map(|path| match path.strip_suffix('/') {
            Some(dir) => {
                let depth = dir.split('/').count();
                TarDirectory::new(path.as_str())
                    .with_mode(dir_mode(depth))
                    .with_mtime(UNIX_EPOCH + Duration::from_secs(MTIME + depth as u64))
                    .into()
            }
            None => file(path, path.as_bytes()),
        })
        .collect();
    let data = archive(entries);
    block_on(FsSink::new(tmp.path()).extract(TarReader::new(&data[..]))).unwrap();
    let setgid = kept_setgid(tmp.path());
    for path in &paths {
        match path.strip_suffix('/') {
            Some(dir) => {
                let depth = dir.split('/').count();
                let meta = fs::symlink_metadata(tmp.path().join(dir)).unwrap();
                assert_eq!(meta.mode() & 0o7777, dir_mode(depth) | setgid, "{dir}");
                assert_eq!(meta.mtime(), (MTIME + depth as u64) as i64, "{dir}");
            }
            None => {
                let body = fs::read(tmp.path().join(path)).unwrap();
                assert_eq!(body, path.as_bytes(), "{path}");
            }
        }
    }
}

/// A symlink entry replaces the symlink of a grandparent. A later entry
/// below that path goes to the new target.
#[test]
fn symlink_replaces_grandparent_symlink() {
    let tmp = TempDir::new();
    let data = archive(vec![
        dir("d/"),
        dir("d/y/"),
        dir("e/"),
        dir("e/y/"),
        TarSymlink::new("x", "d").into(),
        file("x/y/f1", b"1"),
        TarSymlink::new("x", "e").into(),
        file("x/y/f2", b"2"),
    ]);
    block_on(FsSink::new(tmp.path()).extract(TarReader::new(&data[..]))).unwrap();
    assert_eq!(fs::read(tmp.path().join("d/y/f1")).unwrap(), b"1");
    assert_eq!(fs::read(tmp.path().join("e/y/f2")).unwrap(), b"2");
    assert_eq!(count(&tmp.path().join("d/y")), 1);
    assert_eq!(count(&tmp.path().join("e/y")), 1);
}

/// A file entry replaces a symlink below a directory. A later entry below
/// the path of the symlink gives an error.
#[test]
fn file_replaces_nested_symlink_parent() {
    let tmp = TempDir::new();
    let data = archive(vec![
        dir("b/"),
        dir("a/"),
        TarSymlink::new("a/l", "../b").into(),
        file("a/l/f1", b"1"),
        file("a/l", b"l"),
        file("a/l/f2", b"2"),
    ]);
    let err = block_on(FsSink::new(tmp.path()).extract(TarReader::new(&data[..]))).unwrap_err();
    assert_eq!(err.kind(), ErrorKind::NotADirectory);
    assert!(err.to_string().starts_with("a/l/f2: "), "{err}");
    assert_eq!(fs::read(tmp.path().join("b/f1")).unwrap(), b"1");
    assert_eq!(fs::read(tmp.path().join("a/l")).unwrap(), b"l");
    assert_eq!(count(&tmp.path().join("b")), 1);
}

/// A file entry replaces a symlink that the parent path passes through with
/// `..`. A later entry with that parent path gives an error.
#[test]
fn file_replaces_dotdot_symlink() {
    let tmp = TempDir::new();
    let data = archive(vec![
        dir("q/"),
        TarSymlink::new("n", "q").into(),
        TarSymlink::new("s", "n/..").into(),
        file("s/n", b"n"),
        file("s/z", b"z"),
    ]);
    let err = block_on(FsSink::new(tmp.path()).extract(TarReader::new(&data[..]))).unwrap_err();
    assert_eq!(err.kind(), ErrorKind::NotADirectory, "{err}");
    assert!(err.to_string().starts_with("s/z: "), "{err}");
    assert!(!tmp.path().join("z").exists());
}

/// A directory entry replaces a parent symlink. A later entry below that
/// path goes to the new directory.
#[test]
fn directory_replaces_parent_symlink() {
    let tmp = TempDir::new();
    let data = archive(vec![
        dir("d/"),
        TarSymlink::new("s", "d").into(),
        file("s/f", b"f"),
        dir("s/"),
        file("s/g", b"g"),
    ]);
    block_on(FsSink::new(tmp.path()).extract(TarReader::new(&data[..]))).unwrap();
    assert!(fs::symlink_metadata(tmp.path().join("s")).unwrap().is_dir());
    assert_eq!(fs::read(tmp.path().join("s/g")).unwrap(), b"g");
    assert_eq!(fs::read(tmp.path().join("d/f")).unwrap(), b"f");
    assert_eq!(count(&tmp.path().join("d")), 1);
    assert_eq!(count(&tmp.path().join("s")), 1);
}

/// A directory entry replaces a symlink that the parent path passes through
/// with `..`. Then the path of the entry leads to another directory. A later
/// entry below that path goes to the directory that the path leads to now.
#[test]
fn directory_replaces_dotdot_symlink() {
    let tmp = TempDir::new();
    let data = archive(vec![
        dir("r/"),
        dir("w/"),
        dir("w/v/"),
        TarSymlink::new("r/n", "../w/v").into(),
        TarSymlink::new("s", "r/n/..").into(),
        TarSymlink::new("w/x", "../r").into(),
        dir("s/x/n/"),
        file("s/x/n/z", b"z"),
    ]);
    // The finish sees that `s/x/n` leads to another directory now.
    let err = block_on(FsSink::new(tmp.path()).extract(TarReader::new(&data[..]))).unwrap_err();
    assert_eq!(err.kind(), ErrorKind::InvalidData, "{err}");
    assert_eq!(fs::read(tmp.path().join("r/x/n/z")).unwrap(), b"z");
    assert_eq!(count(&tmp.path().join("r/n")), 0);
}

/// Another process moves an open directory of the sink and puts a symlink
/// that leads outside the destination at its path. An entry with that
/// parent path goes to the moved directory. After the sink closes the
/// directory, an entry with that parent path gives an error.
#[test]
fn moved_open_directory() {
    let tmp = TempDir::new();
    let dest = subdir(&tmp, "dest");
    let outside = subdir(&tmp, "outside");
    block_on(async {
        let mut sink = FsSink::new(&dest);
        sink.write(dir("a/")).await.unwrap();
        sink.write(dir("a/b/")).await.unwrap();
        sink.flush().await.unwrap();
        fs::rename(dest.join("a/b"), dest.join("moved")).unwrap();
        symlink("../../outside", dest.join("a/b")).unwrap();
        sink.write(file("a/b/f", b"f")).await.unwrap();
        sink.flush().await.unwrap();
        // The parent of this entry is `a`. Then the sink closes `a/b`.
        sink.write(file("a/x", b"x")).await.unwrap();
        sink.write(file("a/b/g", b"g")).await.unwrap();
        let err = sink.flush().await.unwrap_err();
        assert_eq!(err.kind(), ErrorKind::PermissionDenied);
        assert!(err.to_string().starts_with("a/b/g: "), "{err}");
    });
    assert_eq!(fs::read(dest.join("moved/f")).unwrap(), b"f");
    assert_eq!(fs::read(dest.join("a/x")).unwrap(), b"x");
    assert_eq!(count(&outside), 0);
}

/// Another process moves an open directory of the sink. An entry with a
/// parent path below the path of that directory goes to a new directory in
/// the moved directory.
#[test]
fn moved_open_directory_gets_entries_below_it() {
    let tmp = TempDir::new();
    let dest = subdir(&tmp, "dest");
    block_on(async {
        let mut sink = FsSink::new(&dest);
        sink.write(dir("a/")).await.unwrap();
        sink.flush().await.unwrap();
        fs::rename(dest.join("a"), dest.join("moved")).unwrap();
        sink.write(file("a/c/f", b"f")).await.unwrap();
        sink.flush().await.unwrap();
    });
    assert_eq!(fs::read(dest.join("moved/c/f")).unwrap(), b"f");
    assert!(fs::symlink_metadata(dest.join("a")).is_err());
}
