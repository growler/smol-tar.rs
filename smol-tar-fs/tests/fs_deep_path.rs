#![cfg(unix)]

// The descriptor limit applies to the whole process. The test in this file
// sets a low limit. Thus it is in a separate test binary, and the tests of
// the other files do not see this limit.

mod fs_common;

use {
    fs_common::{block_on, TempDir},
    rustix::{
        fd::OwnedFd,
        fs::{fstat, openat, Mode, OFlags, CWD},
    },
    smol_tar::{TarDirectory, TarEntry, TarReader, TarRegularFile, TarSymlink, TarWriter},
    smol_tar_fs::FsSink,
    std::{fs, io::Read, path::Path},
};

/// Number of directory levels above the file.
const DEPTH: usize = 300;

/// Soft limit of open descriptors during the extraction.
const NOFILE: libc::rlim_t = 64;

/// Flags to open a directory for the check of the tree.
const DIR_FLAGS: OFlags = OFlags::RDONLY
    .union(OFlags::DIRECTORY)
    .union(OFlags::NOFOLLOW)
    .union(OFlags::CLOEXEC);

/// Descriptor limit of the process for one test. Drop sets the old hard
/// limit and a soft limit of at least 1024, if the hard limit allows it.
/// `TempDir` removes the deep tree with one open descriptor for each level.
struct NoFile {
    old: libc::rlimit,
}

impl Drop for NoFile {
    fn drop(&mut self) {
        // `RLIM_INFINITY` is the largest value of `rlim_t`. Thus `min` keeps
        // a soft limit that is not larger than the hard limit.
        let limit = libc::rlimit {
            rlim_cur: self.old.rlim_cur.max(1024).min(self.old.rlim_max),
            rlim_max: self.old.rlim_max,
        };
        // SAFETY: `limit` is a valid `rlimit`.
        unsafe { libc::setrlimit(libc::RLIMIT_NOFILE, &limit) };
    }
}

/// Set the soft limit of open descriptors to `max`, if the limit is
/// larger.
fn limit_nofile(max: libc::rlim_t) -> NoFile {
    let mut old = libc::rlimit {
        rlim_cur: 0,
        rlim_max: 0,
    };
    // SAFETY: `old` is a valid `rlimit` to write.
    assert_eq!(unsafe { libc::getrlimit(libc::RLIMIT_NOFILE, &mut old) }, 0);
    let limit = libc::rlimit {
        rlim_cur: max.min(old.rlim_cur),
        rlim_max: old.rlim_max,
    };
    // SAFETY: `limit` is a valid `rlimit`.
    assert_eq!(unsafe { libc::setrlimit(libc::RLIMIT_NOFILE, &limit) }, 0);
    NoFile { old }
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

/// Open each directory of `names` in `dest`, one level at a time. Keep one
/// open directory at a time. Give the last directory. `std::fs` cannot open
/// a path longer than `PATH_MAX`.
fn open_deep(dest: &Path, names: &[String]) -> OwnedFd {
    let mut dir = openat(CWD, dest, DIR_FLAGS, Mode::empty()).unwrap();
    for name in names {
        dir = openat(&dir, name.as_str(), DIR_FLAGS, Mode::empty()).unwrap();
    }
    dir
}

/// The sink extracts a file below 300 directory levels with a limit of 64
/// open descriptors. The path is longer than `PATH_MAX`, thus on Linux
/// `openat2` gives `ENAMETOOLONG` and the userspace resolver resolves the
/// path. The "implicit" archive has no directory entry. The sink creates each
/// parent. The "explicit" archive has an entry for each directory. At the
/// finish, the sink opens each directory from the destination to set its
/// mode. Each archive also has the symlink `up` beside the file, with a
/// target of 300 `..` components, and the file `g` below `up`. To open the
/// parent of `g`, the userspace resolver goes down 300 levels and then up
/// 300 levels. Thus `g` goes to the destination.
#[test]
fn deep_path_with_low_descriptor_limit() {
    let names: Vec<String> = (0..DEPTH).map(|i| format!("d{i:019}")).collect();
    let path = names.join("/");
    assert!(path.len() > 4096);
    let file = format!("{path}/f");
    let up = format!("{path}/up");
    let target = vec![".."; DEPTH].join("/");
    let g = format!("{up}/g");
    let files = || {
        [
            TarEntry::from(TarRegularFile::new(file.as_str(), 1, &b"f"[..])),
            TarSymlink::new(up.as_str(), target.as_str()).into(),
            TarRegularFile::new(g.as_str(), 1, &b"g"[..]).into(),
        ]
    };
    let mut explicit: Vec<TarEntry<'_, &[u8]>> = (1..=DEPTH)
        .map(|k| {
            TarDirectory::new(names[..k].join("/") + "/")
                .with_mode(0o750)
                .into()
        })
        .collect();
    explicit.extend(files());
    let implicit = archive(Vec::from(files()));
    let explicit = archive(explicit);
    let layouts = [("implicit", implicit), ("explicit", explicit)];

    let tmp = TempDir::new();
    {
        let _limit = limit_nofile(NOFILE);
        for (name, tar) in &layouts {
            let dest = tmp.path().join(name);
            fs::create_dir(&dest).unwrap();
            if let Err(err) = block_on(FsSink::new(&dest).extract(TarReader::new(&tar[..]))) {
                panic!("{name}: {err}");
            }
        }
    }

    for (name, _) in &layouts {
        let dir = open_deep(&tmp.path().join(name), &names);
        let f = openat(&dir, "f", OFlags::RDONLY | OFlags::CLOEXEC, Mode::empty()).unwrap();
        let mut body = Vec::new();
        fs::File::from(f).read_to_end(&mut body).unwrap();
        assert_eq!(body, b"f", "{name}");
        assert_eq!(
            fs::read(tmp.path().join(name).join("g")).unwrap(),
            b"g",
            "{name}"
        );
        if *name == "explicit" {
            assert_eq!(fstat(&dir).unwrap().st_mode & 0o777, 0o750, "{name}");
        }
    }
}
