#![cfg(unix)]

// The umask applies to the whole process. The tests in this file set the
// umask. Thus they are in a separate test binary, and the tests of the other
// files do not see this umask. A lock makes the tests of this file run one
// at a time.

mod fs_common;

use {
    fs_common::{block_on, kept_setgid, setgid_dir, TempDir},
    futures::StreamExt,
    smol_tar::{
        TarDevice, TarDirectory, TarEntry, TarFifo, TarReader, TarRegularFile,
        TarRegularFileReader, TarSymlink, TarWriter,
    },
    smol_tar_fs::{DeviceNodes, FsSink, ModePolicy},
    std::{
        fs::{self, Permissions},
        io::{self, ErrorKind},
        os::unix::fs::{FileTypeExt, MetadataExt, PermissionsExt},
        path::{Path, PathBuf},
        sync::{Mutex, MutexGuard, PoisonError},
    },
};

/// Sink for the entries of a `TarReader` on an archive in memory.
type Sink<'a> = FsSink<'a, TarRegularFileReader<'a, &'a [u8]>>;

/// Lock for the umask of the process.
static UMASK_LOCK: Mutex<()> = Mutex::new(());

/// Umask of the process for one test. Drop sets the old umask and then
/// releases the lock.
struct Umask {
    old: libc::mode_t,
    _lock: MutexGuard<'static, ()>,
}

impl Drop for Umask {
    fn drop(&mut self) {
        // SAFETY: `umask` has no preconditions.
        unsafe { libc::umask(self.old) };
    }
}

/// Lock the umask and set it to `mask`.
fn set_umask(mask: libc::mode_t) -> Umask {
    let lock = UMASK_LOCK.lock().unwrap_or_else(PoisonError::into_inner);
    // SAFETY: `umask` has no preconditions.
    let old = unsafe { libc::umask(mask) };
    Umask { old, _lock: lock }
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

/// Regular file `path` with the body `body`.
fn file<'b>(path: &str, body: &'b [u8]) -> TarEntry<'b, &'b [u8]> {
    TarRegularFile::new(path, body.len() as u64, body).into()
}

/// Create the empty directory `name` in `tmp`. Give its path.
fn subdir(tmp: &TempDir, name: &str) -> PathBuf {
    let path = tmp.path().join(name);
    fs::create_dir(&path).unwrap();
    path
}

/// Give the permission and special bits of `path`. Do not follow a symlink.
fn mode(path: &Path) -> u32 {
    fs::symlink_metadata(path).unwrap().mode() & 0o7777
}

#[test]
fn mode_skipped_implicit_parent_uses_umask() {
    let _umask = set_umask(0o077);
    let tmp = TempDir::new();
    let dest = subdir(&tmp, "dest");
    let tar = archive(vec![file("a/b/f", b"f"), TarDirectory::new("d/").into()]);
    extract_into(&tar, FsSink::new(&dest).with_mode(ModePolicy::Skip)).unwrap();
    let setgid = mode(&dest) & 0o2000;
    for path in ["a", "a/b", "d"] {
        assert_eq!(mode(&dest.join(path)), 0o700 | setgid, "{path}");
    }
}

/// The umask removes bits from the mode of a new file. The sink sets the
/// mode of the entry after the write. This applies to a file in a batch and
/// to a larger file.
#[test]
fn file_mode_ignores_umask() {
    let _umask = set_umask(0o077);
    let tmp = TempDir::new();
    let dest = subdir(&tmp, "dest");
    let big = vec![b'b'; 512 * 1024];
    let tar = archive(vec![
        TarRegularFile::new("small", 1, &b"s"[..])
            .with_mode(0o644)
            .into(),
        TarRegularFile::new("big", big.len() as u64, &big[..])
            .with_mode(0o755)
            .into(),
    ]);
    block_on(FsSink::new(&dest).extract(TarReader::new(&tar[..]))).unwrap();
    assert_eq!(mode(&dest.join("small")), 0o644);
    assert_eq!(mode(&dest.join("big")), 0o755);
}

/// A missing parent gets 0o777 less the umask, and with a mask also without
/// the bits of the mask. This applies to the parents of the files in a
/// batch, of a larger file and of a symlink.
#[test]
fn implicit_parent_uses_umask() {
    // One byte more than the largest file in a batch (256 KiB).
    let big = vec![b'b'; 256 * 1024 + 1];
    let tar = archive(vec![
        TarRegularFile::new("tree/sub/run.sh", 1, &b"r"[..])
            .with_mode(0o755)
            .into(),
        file("tree/f", b"f"),
        TarRegularFile::new("big/sub/f", big.len() as u64, &big[..]).into(),
        TarSymlink::new("link/sub/l", "t").into(),
    ]);
    for (umask, policy, expect) in [
        (0o077, ModePolicy::Apply, 0o700),
        (0o002, ModePolicy::Apply, 0o775),
        (0o077, ModePolicy::Mask(0o077), 0o700),
        (0o002, ModePolicy::Mask(0o002), 0o775),
        (0o002, ModePolicy::Mask(0o027), 0o750),
    ] {
        let _umask = set_umask(umask);
        let tmp = TempDir::new();
        let dest = subdir(&tmp, "dest");
        extract_into(&tar, FsSink::new(&dest).with_mode(policy)).unwrap();
        let setgid = mode(&dest) & 0o2000;
        for path in ["tree", "tree/sub", "big", "big/sub", "link", "link/sub"] {
            assert_eq!(
                mode(&dest.join(path)),
                expect | setgid,
                "{path} umask {umask:o} {policy:?}"
            );
        }
    }
}

/// A later directory entry sets the mode of a missing parent at the finish.
#[test]
fn implicit_parent_later_entry_sets_mode() {
    let _umask = set_umask(0o077);
    let tmp = TempDir::new();
    let dest = subdir(&tmp, "dest");
    let tar = archive(vec![
        file("t/s/f", b"f"),
        TarDirectory::new("t/").with_mode(0o751).into(),
    ]);
    extract_into(&tar, FsSink::new(&dest)).unwrap();
    // Without `with_special_bits`, both directories keep the setgid bit that
    // they get from a setgid `dest`. For `t`, the bit must also stay after
    // the mode change at the finish.
    assert_eq!(mode(&dest.join("t")), 0o751 | kept_setgid(&dest));
    assert_eq!(mode(&dest.join("t/s")), 0o700 | (mode(&dest) & 0o2000));
}

/// The umask 0o277 removes the write bit of the owner from a new
/// directory. A new directory of a directory entry has mode 0o700 until the
/// finish, thus the sink can create a file in it. The finish sets the mode
/// of the entry. A process without privileges needs the mode 0o700 for the
/// file create.
#[test]
fn entry_dir_ignores_umask() {
    // SAFETY: `geteuid` has no preconditions.
    if unsafe { libc::geteuid() } == 0 {
        eprintln!("skip: the process runs as root");
        return;
    }
    let _umask = set_umask(0o277);
    let tmp = TempDir::new();
    // The umask also applies to the directories of the test.
    fs::set_permissions(tmp.path(), Permissions::from_mode(0o700)).unwrap();
    let tar = archive(vec![
        TarDirectory::new("d/").with_mode(0o755).into(),
        TarRegularFile::new("d/f", 1, &b"f"[..])
            .with_mode(0o644)
            .into(),
    ]);
    for (name, policy) in [
        ("apply", ModePolicy::Apply),
        ("mask", ModePolicy::Mask(0o022)),
    ] {
        let dest = subdir(&tmp, name);
        fs::set_permissions(&dest, Permissions::from_mode(0o700)).unwrap();
        extract_into(&tar, FsSink::new(&dest).with_mode(policy)).unwrap();
        assert_eq!(mode(&dest.join("d")), 0o755, "{policy:?}");
        assert_eq!(mode(&dest.join("d/f")), 0o644, "{policy:?}");
        assert_eq!(fs::read(dest.join("d/f")).unwrap(), b"f", "{policy:?}");
    }
}

/// Without `with_special_bits`, a directory keeps the special bits that it
/// has on disk, and the sink ignores the special bits of the entry. A new
/// directory gets the setgid bit from its parent. A file gets no special
/// bits.
#[test]
fn mask_keeps_setgid_from_parent() {
    let _umask = set_umask(0o022);
    let tmp = TempDir::new();
    let Some(dest) = setgid_dir(tmp.path(), "dest") else {
        return;
    };
    // `set_permissions` removes the setgid bit that the directories get from
    // `dest`.
    let ex = dest.join("ex");
    fs::create_dir(&ex).unwrap();
    fs::set_permissions(&ex, Permissions::from_mode(0o700)).unwrap();
    assert_eq!(mode(&ex), 0o700);
    let st = dest.join("st");
    fs::create_dir(&st).unwrap();
    fs::set_permissions(&st, Permissions::from_mode(0o1700)).unwrap();
    assert_eq!(mode(&st), 0o1700);
    let tar = archive(vec![
        TarDirectory::new("tree/").with_mode(0o755).into(),
        TarDirectory::new("tree/sub/").with_mode(0o750).into(),
        TarRegularFile::new("tree/sub/f", 1, &b"f"[..])
            .with_mode(0o644)
            .into(),
        TarDirectory::new("m/b/").with_mode(0o755).into(),
        TarDirectory::new("sg/").with_mode(0o2755).into(),
        TarDirectory::new("ex/").with_mode(0o755).into(),
        TarDirectory::new("st/").with_mode(0o755).into(),
    ]);
    let sink = FsSink::new(&dest).with_mode(ModePolicy::Mask(0o022));
    extract_into(&tar, sink).unwrap();
    for (path, expect) in [
        ("tree", 0o2755),
        ("tree/sub", 0o2750),
        ("tree/sub/f", 0o644),
        ("m", 0o2755),
        ("m/b", 0o2755),
        ("sg", 0o2755),
        ("ex", 0o755),
        ("st", 0o1755),
    ] {
        assert_eq!(mode(&dest.join(path)), expect, "{path}");
    }
}

/// With the umask 0, a new FIFO gets the mode 0o666 and a new device node
/// gets the mode 0o600. Linux 5.8 and later let a process without
/// CAP_MKNOD create the character device 0:0, so the test needs no root.
#[test]
fn mode_skipped_device_is_owner_only() {
    let _umask = set_umask(0);
    let tmp = TempDir::new();
    fn sink<'a>(dest: &Path) -> Sink<'a> {
        FsSink::new(dest)
            .with_mode(ModePolicy::Skip)
            .with_device_nodes(DeviceNodes::Create)
    }
    let dest = subdir(&tmp, "fifo");
    let tar = archive(vec![TarFifo::new("p").with_mode(0o600).into()]);
    extract_into(&tar, sink(&dest)).unwrap();
    assert_eq!(mode(&dest.join("p")), 0o666);

    // Give the metadata of the node `n` from the device entry `dev` in the
    // new directory `dir`, or `None` if the system refuses the node.
    let device = |dir, dev: TarDevice| {
        let dest = subdir(&tmp, dir);
        let tar = archive(vec![dev.with_mode(0o644).into()]);
        match extract_into(&tar, sink(&dest)) {
            Err(e) if e.kind() == ErrorKind::PermissionDenied => {
                eprintln!("skip: cannot create the {dir} device: {e}");
                None
            }
            result => {
                result.unwrap();
                Some(fs::symlink_metadata(dest.join("n")).unwrap())
            }
        }
    };
    if let Some(meta) = device("char", TarDevice::new_char("n", 0, 0)) {
        assert!(meta.file_type().is_char_device());
        assert_eq!(meta.mode() & 0o7777, 0o600);
    }

    // SAFETY: `geteuid` has no preconditions.
    if unsafe { libc::geteuid() } != 0 {
        return;
    }
    if let Some(meta) = device("block", TarDevice::new_block("n", 7, 0)) {
        assert!(meta.file_type().is_block_device());
        assert_eq!(meta.mode() & 0o7777, 0o600);
    }
}
