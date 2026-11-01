//! Path rules of the sink: normalization of entry paths and the open of
//! parent directories.

use {crate::sys, std::io};

/// Split an entry path into its components. Remove empty components and `.`
/// components. A path that starts with `/`, a `..` component, or a path with
/// no components gives `InvalidData`.
pub(crate) fn normalize(path: &str) -> io::Result<Vec<&str>> {
    let invalid = |msg| io::Error::new(io::ErrorKind::InvalidData, msg);
    if path.starts_with('/') {
        return Err(invalid("absolute path"));
    }
    let mut parts = Vec::new();
    for part in path.split('/') {
        match part {
            "" | "." => {}
            ".." => return Err(invalid("path has a `..` component")),
            part => parts.push(part),
        }
    }
    if parts.is_empty() {
        return Err(invalid("empty path"));
    }
    Ok(parts)
}

/// Open the directory at the normalized path `path` in `root`. `path` has
/// one or more components. `root` can be the destination or another open
/// directory.
///
/// The open follows a symlink in each component, also in the last
/// component, if the target stays inside `root`. A path or a symlink that
/// leads outside `root` gives `PermissionDenied`, also if the target is
/// inside the destination. An absolute symlink also gives
/// `PermissionDenied`.
///
/// Without `create`, open the path with `sys::open_beneath`. With `create`
/// set, open the path with `sys::create_dirs_beneath`. It creates each
/// missing directory of `path` with the mode `mode` less the umask. With
/// `mode` set to `None`, use the mode 0o777 less the umask. A new directory
/// keeps the setgid bit that it gets from its parent. A missing component in
/// a symlink target gives `NotFound`, and the function creates no directory
/// through this symlink.
///
/// The error has no path.
pub(crate) fn open_dir(
    root: &sys::Dir,
    path: &str,
    create: bool,
    mode: Option<u32>,
) -> io::Result<sys::Dir> {
    if create {
        sys::create_dirs_beneath(root, path, mode.unwrap_or(0o777))
    } else {
        sys::open_beneath(root, path)
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::testutil::TempDir,
        std::{
            fs::{self, Permissions},
            io::ErrorKind,
            os::unix::fs::{symlink, MetadataExt, PermissionsExt},
            path::Path,
        },
    };

    #[test]
    fn normalize_removes_empty_and_dot() {
        assert_eq!(normalize("a/./b//c/").unwrap(), ["a", "b", "c"]);
    }

    #[test]
    fn normalize_rejects_invalid() {
        for path in ["/a", "a/../b", "..", "", "./"] {
            let err = normalize(path).unwrap_err();
            assert_eq!(err.kind(), ErrorKind::InvalidData, "{path:?}");
        }
    }

    fn mode_of(path: &Path) -> u32 {
        fs::symlink_metadata(path).unwrap().mode() & 0o7777
    }

    #[test]
    fn open_dir_creates_with_mode_less_umask() {
        let tmp = TempDir::new();
        // A directory that the process creates with the mode 0o777 and its
        // umask.
        fs::create_dir(tmp.path().join("ref")).unwrap();
        let ref_mode = mode_of(&tmp.path().join("ref"));
        let root = sys::open_dest(tmp.path()).unwrap();
        open_dir(&root, "a/b", true, Some(0o750)).unwrap();
        // A setgid temporary directory also gives the setgid bit.
        let expect = (0o2000 | 0o750) & ref_mode;
        assert_eq!(mode_of(&tmp.path().join("a")), expect);
        assert_eq!(mode_of(&tmp.path().join("a/b")), expect);
    }

    #[test]
    fn open_dir_create_keeps_setgid() {
        let tmp = TempDir::new();
        let parent = tmp.path().join("p");
        fs::create_dir(&parent).unwrap();
        fs::set_permissions(&parent, Permissions::from_mode(0o2775)).unwrap();
        if mode_of(&parent) != 0o2775 {
            eprintln!("skip: the process cannot set the setgid bit");
            return;
        }
        fs::create_dir(parent.join("probe")).unwrap();
        if mode_of(&parent.join("probe")) & 0o2000 == 0 {
            eprintln!("skip: a new directory does not get the setgid bit");
            return;
        }
        let expect = 0o2000 | (0o755 & mode_of(&parent.join("probe")));
        let root = sys::open_dest(&parent).unwrap();
        open_dir(&root, "a/b", true, Some(0o755)).unwrap();
        assert_eq!(mode_of(&parent.join("a")), expect);
        assert_eq!(mode_of(&parent.join("a/b")), expect);
    }

    #[test]
    fn open_dir_missing_gives_not_found() {
        let tmp = TempDir::new();
        let root = sys::open_dest(tmp.path()).unwrap();
        let err = open_dir(&root, "a/b", false, None).unwrap_err();
        assert_eq!(err.kind(), ErrorKind::NotFound);
        assert!(fs::symlink_metadata(tmp.path().join("a")).is_err());
    }

    #[test]
    fn open_dir_file_in_path_gives_not_a_directory() {
        let tmp = TempDir::new();
        fs::write(tmp.path().join("f"), b"").unwrap();
        let root = sys::open_dest(tmp.path()).unwrap();
        for path in ["f", "f/x"] {
            let err = open_dir(&root, path, true, Some(0o755)).unwrap_err();
            assert_eq!(err.kind(), ErrorKind::NotADirectory, "{path}");
        }
        assert!(tmp.path().join("f").is_file());
    }

    #[test]
    fn open_dir_follows_symlink_inside_root() {
        let tmp = TempDir::new();
        fs::create_dir(tmp.path().join("d")).unwrap();
        symlink("d", tmp.path().join("l")).unwrap();
        let root = sys::open_dest(tmp.path()).unwrap();
        open_dir(&root, "l/x", true, Some(0o755)).unwrap();
        assert!(tmp.path().join("d/x").is_dir());
        // The last component also follows the symlink.
        let dir = open_dir(&root, "l", false, None).unwrap();
        sys::open_child_dir(&dir, "x").unwrap();
    }

    #[test]
    fn open_dir_create_opens_existing_path() {
        let tmp = TempDir::new();
        fs::create_dir_all(tmp.path().join("d/x")).unwrap();
        symlink("d", tmp.path().join("l")).unwrap();
        let root = sys::open_dest(tmp.path()).unwrap();
        for path in ["d", "l"] {
            let dir = open_dir(&root, path, true, Some(0o755)).unwrap();
            sys::open_child_dir(&dir, "x").unwrap();
        }
        assert_eq!(fs::read_dir(tmp.path().join("d")).unwrap().count(), 1);
    }

    #[test]
    fn open_dir_create_through_dangling_symlink_gives_not_found() {
        let tmp = TempDir::new();
        fs::create_dir(tmp.path().join("d")).unwrap();
        symlink("new", tmp.path().join("n")).unwrap();
        symlink("d/missing", tmp.path().join("ld")).unwrap();
        let root = sys::open_dest(tmp.path()).unwrap();
        for path in ["n", "n/x", "ld", "ld/x"] {
            let err = open_dir(&root, path, true, Some(0o755)).unwrap_err();
            assert_eq!(err.kind(), ErrorKind::NotFound, "{path}");
        }
        assert!(fs::symlink_metadata(tmp.path().join("new")).is_err());
        assert!(fs::symlink_metadata(tmp.path().join("d/missing")).is_err());
    }

    #[test]
    fn open_dir_symlink_outside_root_gives_permission_denied() {
        let tmp = TempDir::new();
        let dest = tmp.path().join("dest");
        fs::create_dir(&dest).unwrap();
        fs::create_dir(tmp.path().join("out")).unwrap();
        symlink("../out", dest.join("l")).unwrap();
        let root = sys::open_dest(&dest).unwrap();
        for path in ["l", "l/x"] {
            let err = open_dir(&root, path, true, Some(0o755)).unwrap_err();
            assert_eq!(err.kind(), ErrorKind::PermissionDenied, "{path}");
        }
        assert_eq!(fs::read_dir(tmp.path().join("out")).unwrap().count(), 0);
    }

    #[test]
    fn open_dir_absolute_symlink_gives_permission_denied() {
        let tmp = TempDir::new();
        let target = tmp.path().join("d");
        fs::create_dir(&target).unwrap();
        symlink(&target, tmp.path().join("l")).unwrap();
        let root = sys::open_dest(tmp.path()).unwrap();
        for path in ["l", "l/x"] {
            let err = open_dir(&root, path, true, Some(0o755)).unwrap_err();
            assert_eq!(err.kind(), ErrorKind::PermissionDenied, "{path}");
        }
        assert_eq!(fs::read_dir(&target).unwrap().count(), 0);
    }
}
