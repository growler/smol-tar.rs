//! Path rules of the sink: normalization of entry paths and the open of
//! parent directories.

use {
    crate::{sys, with_path},
    std::{io, ops::Deref},
};

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

/// Directory that holds the last component of an entry path.
#[derive(Debug)]
pub(crate) enum Parent<'a> {
    /// The destination root.
    Root(&'a sys::Dir),
    /// A directory below the destination root.
    Owned(sys::Dir),
}

impl Deref for Parent<'_> {
    type Target = sys::Dir;

    fn deref(&self) -> &sys::Dir {
        match self {
            Parent::Root(dir) => dir,
            Parent::Owned(dir) => dir,
        }
    }
}

/// Open the directory in `root` that holds the last component of `parts`.
/// Open each component except the last without following symlinks. With
/// `create` set, create a missing directory. An error has the path of
/// `parts`.
pub(crate) fn open_parent<'a>(
    root: &'a sys::Dir,
    parts: &[&str],
    create: bool,
) -> io::Result<Parent<'a>> {
    let dirs = parts.split_last().map_or(&[][..], |(_, dirs)| dirs);
    let mut parent = Parent::Root(root);
    for part in dirs {
        let dir = sys::open_child_dir(&parent, part, create)
            .map_err(|err| with_path(err, &parts.join("/")))?;
        parent = Parent::Owned(dir);
    }
    Ok(parent)
}

#[cfg(test)]
mod tests {
    use {super::*, crate::testutil::TempDir, std::io::ErrorKind};

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

    #[test]
    fn open_parent_one_component_is_root() {
        let tmp = TempDir::new();
        let root = sys::open_dest(tmp.path()).unwrap();
        let parent = open_parent(&root, &["f"], true).unwrap();
        assert!(matches!(parent, Parent::Root(_)));
    }

    #[test]
    fn open_parent_does_not_follow_symlink() {
        let tmp = TempDir::new();
        let target = tmp.path().join("d");
        std::fs::create_dir(&target).unwrap();
        std::os::unix::fs::symlink("d", tmp.path().join("l")).unwrap();
        let root = sys::open_dest(tmp.path()).unwrap();
        let err = open_parent(&root, &["l", "x", "f"], true).unwrap_err();
        assert!(err.to_string().starts_with("l/x/f: "), "{err}");
        assert_eq!(std::fs::read_dir(&target).unwrap().count(), 0);
    }
}
