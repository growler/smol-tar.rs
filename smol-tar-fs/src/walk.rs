//! Read one directory through the backend, then sort and split the children.

use {
    super::{sys, Child, FsKind},
    std::{collections::VecDeque, io, path::Path},
};

/// Read the children of `dir` for which `keep` gives `true`. Give the
/// non-directories and the directories in two lists. Each list is sorted by
/// name, in the order of the UTF-8 bytes.
pub(crate) fn read_sorted(
    dir: &sys::Dir,
    fs_path: &Path,
    xattrs: bool,
    keep: &dyn Fn(&str, FsKind) -> bool,
) -> io::Result<(VecDeque<Child>, VecDeque<Child>)> {
    let mut all = sys::read_dir(dir, fs_path, xattrs, keep)?;
    let is_dir = |child: &Child| child.meta.kind == FsKind::Directory;
    all.sort_unstable_by(|a, b| (is_dir(a), &a.name).cmp(&(is_dir(b), &b.name)));
    let dirs = all.split_off(all.partition_point(|child| !is_dir(child)));
    Ok((all.into(), dirs.into()))
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::{testutil::TempDir, Root},
    };

    fn names(list: &VecDeque<Child>) -> Vec<&str> {
        list.iter().map(|child| &*child.name).collect()
    }

    #[test]
    fn read_sorted_splits_and_sorts() {
        let tmp = TempDir::new();
        let path = tmp.path();
        std::fs::write(path.join("c"), b"").unwrap();
        std::fs::write(path.join("a"), b"").unwrap();
        std::fs::create_dir(path.join("b")).unwrap();
        if path.join("B").exists() {
            eprintln!("skip: the filesystem ignores the case of names");
            return;
        }
        std::fs::write(path.join("a.txt"), b"").unwrap();
        std::fs::write(path.join("B"), b"").unwrap();
        let Root::Dir { dir, .. } = sys::open_root(path, false).unwrap() else {
            panic!("root is not a directory");
        };
        let (files, dirs) = read_sorted(&dir, path, false, &|_, _| true).unwrap();
        assert_eq!(names(&files), ["B", "a", "a.txt", "c"]);
        assert_eq!(names(&dirs), ["b"]);
    }
}
