//! Every library encodes and extracts the disk trees with the same result.

use {
    smol_tar_bench::{adapters, Kind, Runtime, Tree, BUF_SIZE},
    std::{fs, io, path::Path},
};

/// Node counts of a directory tree.
#[derive(Debug, Default, PartialEq, Eq)]
struct Nodes {
    dirs: u64,
    files: u64,
    symlinks: u64,
    bytes: u64,
}

fn walk(dir: &Path, nodes: &mut Nodes) {
    for entry in fs::read_dir(dir).unwrap() {
        let path = entry.unwrap().path();
        let meta = fs::symlink_metadata(&path).unwrap();
        if meta.is_dir() {
            nodes.dirs += 1;
            walk(&path, nodes);
        } else if meta.is_symlink() {
            nodes.symlinks += 1;
        } else {
            nodes.files += 1;
            nodes.bytes += meta.len();
        }
    }
}

fn nodes(dir: &Path) -> Nodes {
    let mut nodes = Nodes::default();
    walk(dir, &mut nodes);
    nodes
}

#[test]
fn encoders_agree() {
    let rt = Runtime::new();
    let mut buf = vec![0; BUF_SIZE];
    for tree in Tree::ALL {
        let fixture = tree.fixture();
        let (root, prefix) = (&fixture.root, tree.root());

        let mut out = Vec::new();
        adapters::tar::encode_dir(root, prefix, &mut out).unwrap();
        let want = adapters::tar::read_all(&out, &mut buf).unwrap();
        assert_eq!(want.bytes, fixture.body_bytes, "{tree}: tar");

        let mut out = Vec::new();
        rt.block_on(adapters::smol_tar::encode_dir(root, prefix, &mut out))
            .unwrap();
        let got = adapters::tar::read_all(&out, &mut buf).unwrap();
        assert_eq!(got, want, "{tree}: smol-tar");

        let mut out = Vec::new();
        rt.block_on(adapters::async_tar::encode_dir(root, prefix, &mut out))
            .unwrap();
        let got = adapters::tar::read_all(&out, &mut buf).unwrap();
        assert_eq!(got, want, "{tree}: async-tar");

        #[cfg(feature = "tokio")]
        {
            let mut out = Vec::new();
            rt.block_on(adapters::astral_tokio_tar::encode_dir(
                root, prefix, &mut out,
            ))
            .unwrap();
            let got = adapters::tar::read_all(&out, &mut buf).unwrap();
            assert_eq!(got, want, "{tree}: astral-tokio-tar");

            let mut out = Vec::new();
            rt.block_on(adapters::tar_codec::encode_dir(root, &mut out))
                .unwrap();
            let got = adapters::tar::read_all(&out, &mut buf).unwrap();
            assert_eq!(got, want, "{tree}: tar-codec");
        }
    }
}

#[test]
fn extractors_agree() {
    let rt = Runtime::new();
    for tree in Tree::ALL {
        let fixture = tree.fixture();
        let data = &fixture.archive[..];
        let count = |f: fn(&Kind) -> bool| fixture.specs.iter().filter(|s| f(&s.kind)).count();
        let files = count(|k| matches!(k, Kind::File(_))) as u64;
        let symlinks = count(|k| matches!(k, Kind::Symlink(_))) as u64;

        for minimal in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let check = |name: &str, result: io::Result<()>| -> Nodes {
                result.unwrap_or_else(|e| panic!("{tree} {minimal} {name}: {e}"));
                nodes(&dir.path().join(name))
            };

            let want = check(
                "tar",
                adapters::tar::extract(data, &dir.path().join("tar"), minimal),
            );
            assert_eq!(want.files, files, "{tree}: tar");
            assert_eq!(want.symlinks, symlinks, "{tree}: tar");
            assert_eq!(want.bytes, fixture.body_bytes, "{tree}: tar");

            let dest = dir.path().join("smol-tar");
            let got = check(
                "smol-tar",
                rt.block_on(adapters::smol_tar::extract(data, &dest, minimal)),
            );
            assert_eq!(got, want, "{tree} {minimal}: smol-tar");

            let dest = dir.path().join("async-tar");
            let got = check(
                "async-tar",
                rt.block_on(adapters::async_tar::extract(data, &dest, minimal)),
            );
            assert_eq!(got, want, "{tree} {minimal}: async-tar");

            #[cfg(feature = "tokio")]
            {
                let dest = dir.path().join("astral-tokio-tar");
                let got = check(
                    "astral-tokio-tar",
                    rt.block_on(adapters::astral_tokio_tar::extract(data, &dest, minimal)),
                );
                assert_eq!(got, want, "{tree} {minimal}: astral-tokio-tar");

                let dest = dir.path().join("tar-codec");
                let got = check(
                    "tar-codec",
                    rt.block_on(adapters::tar_codec::extract(data, &dest)),
                );
                assert_eq!(got, want, "{tree} {minimal}: tar-codec");
            }
        }
    }
}
