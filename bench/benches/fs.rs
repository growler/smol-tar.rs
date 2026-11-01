//! Wall time of directory encoding and archive extraction on disk.
//!
//! The benchmarks follow the comparison benchmark of tar-codec: the encoder
//! reads a tree on disk and writes into a sink that counts the bytes, and
//! the extractor reads an archive in memory and writes a new directory.

use {
    divan::{
        counter::{BytesCount, ItemsCount},
        Bencher,
    },
    smol_tar_bench::{adapters, CountingSink, Fixture, Runtime, SlowSink, Tree},
    std::{io, path::Path},
};

#[global_allocator]
static ALLOC: divan::AllocProfiler = divan::AllocProfiler::system();

fn main() {
    divan::main();
}

/// Add the body size and the entry count of `fixture` as counters.
fn counted<'a, 'b>(bencher: Bencher<'a, 'b>, fixture: &Fixture) -> Bencher<'a, 'b> {
    bencher
        .counter(BytesCount::new(fixture.body_bytes))
        .counter(ItemsCount::new(fixture.specs.len()))
}

/// Measure `extract` with a new destination for each sample. The benchmark
/// makes and removes the temporary directory outside the measurement.
fn extract_with(bencher: Bencher, fixture: &Fixture, extract: impl Fn(&Path) -> io::Result<()>) {
    counted(bencher, fixture)
        .with_inputs(|| tempfile::tempdir().unwrap())
        .bench_local_refs(|dir| extract(&dir.path().join("out")).unwrap());
}

/// Encode the tree into a sink that counts the bytes.
mod encode_dir {
    use super::*;

    #[divan::bench(args = Tree::ALL)]
    fn smol_tar(bencher: Bencher, tree: Tree) {
        let (rt, fixture) = (Runtime::new(), tree.fixture());
        counted(bencher, &fixture).bench_local(|| {
            let mut sink = CountingSink::default();
            rt.block_on(adapters::smol_tar::encode_dir(
                &fixture.root,
                tree.root(),
                &mut sink,
            ))
            .unwrap();
            sink.bytes
        });
    }

    /// smol-tar with a sink that takes a fixed time for each byte. The
    /// reads of the file data run while the sink takes the data.
    #[divan::bench(args = Tree::ALL)]
    fn smol_tar_slow_sink(bencher: Bencher, tree: Tree) {
        let (rt, fixture) = (Runtime::new(), tree.fixture());
        counted(bencher, &fixture).bench_local(|| {
            let mut sink = SlowSink::default();
            rt.block_on(adapters::smol_tar::encode_dir(
                &fixture.root,
                tree.root(),
                &mut sink,
            ))
            .unwrap();
            sink.bytes
        });
    }

    #[divan::bench(args = Tree::ALL)]
    fn async_tar(bencher: Bencher, tree: Tree) {
        let (rt, fixture) = (Runtime::new(), tree.fixture());
        counted(bencher, &fixture).bench_local(|| {
            let mut sink = CountingSink::default();
            rt.block_on(adapters::async_tar::encode_dir(
                &fixture.root,
                tree.root(),
                &mut sink,
            ))
            .unwrap();
            sink.bytes
        });
    }

    #[cfg(feature = "tokio")]
    #[divan::bench(args = Tree::ALL)]
    fn astral_tokio_tar(bencher: Bencher, tree: Tree) {
        let (rt, fixture) = (Runtime::new(), tree.fixture());
        counted(bencher, &fixture).bench_local(|| {
            let mut sink = CountingSink::default();
            rt.block_on(adapters::astral_tokio_tar::encode_dir(
                &fixture.root,
                tree.root(),
                &mut sink,
            ))
            .unwrap();
            sink.bytes
        });
    }

    #[cfg(feature = "tokio")]
    #[divan::bench(args = Tree::ALL)]
    fn tar_codec(bencher: Bencher, tree: Tree) {
        let (rt, fixture) = (Runtime::new(), tree.fixture());
        counted(bencher, &fixture).bench_local(|| {
            let mut sink = CountingSink::default();
            rt.block_on(adapters::tar_codec::encode_dir(&fixture.root, &mut sink))
                .unwrap();
            sink.bytes
        });
    }

    #[divan::bench(args = Tree::ALL)]
    fn tar(bencher: Bencher, tree: Tree) {
        let fixture = tree.fixture();
        counted(bencher, &fixture).bench_local(|| {
            let mut sink = CountingSink::default();
            adapters::tar::encode_dir(&fixture.root, tree.root(), &mut sink).unwrap();
            sink.bytes
        });
    }
}

/// Extract the archive with the default settings of each library.
mod extract {
    use super::*;

    #[divan::bench(args = Tree::ALL, sample_size = 1)]
    fn smol_tar(bencher: Bencher, tree: Tree) {
        let (rt, fixture) = (Runtime::new(), tree.fixture());
        extract_with(bencher, &fixture, |dest| {
            rt.block_on(adapters::smol_tar::extract(&fixture.archive, dest, false))
        });
    }

    #[divan::bench(args = Tree::ALL, sample_size = 1)]
    fn async_tar(bencher: Bencher, tree: Tree) {
        let (rt, fixture) = (Runtime::new(), tree.fixture());
        extract_with(bencher, &fixture, |dest| {
            rt.block_on(adapters::async_tar::extract(&fixture.archive, dest, false))
        });
    }

    #[cfg(feature = "tokio")]
    #[divan::bench(args = Tree::ALL, sample_size = 1)]
    fn astral_tokio_tar(bencher: Bencher, tree: Tree) {
        let (rt, fixture) = (Runtime::new(), tree.fixture());
        extract_with(bencher, &fixture, |dest| {
            rt.block_on(adapters::astral_tokio_tar::extract(
                &fixture.archive,
                dest,
                false,
            ))
        });
    }

    #[cfg(feature = "tokio")]
    #[divan::bench(args = Tree::ALL, sample_size = 1)]
    fn tar_codec(bencher: Bencher, tree: Tree) {
        let (rt, fixture) = (Runtime::new(), tree.fixture());
        extract_with(bencher, &fixture, |dest| {
            rt.block_on(adapters::tar_codec::extract(&fixture.archive, dest))
        });
    }

    #[divan::bench(args = Tree::ALL, sample_size = 1)]
    fn tar(bencher: Bencher, tree: Tree) {
        let fixture = tree.fixture();
        extract_with(bencher, &fixture, |dest| {
            adapters::tar::extract(&fixture.archive, dest, false)
        });
    }
}

/// Extract the archive without the mode and the modification time, where the
/// library has an option for them. tar-codec does not set them at all.
mod extract_minimal {
    use super::*;

    #[divan::bench(args = Tree::ALL, sample_size = 1)]
    fn smol_tar(bencher: Bencher, tree: Tree) {
        let (rt, fixture) = (Runtime::new(), tree.fixture());
        extract_with(bencher, &fixture, |dest| {
            rt.block_on(adapters::smol_tar::extract(&fixture.archive, dest, true))
        });
    }

    #[divan::bench(args = Tree::ALL, sample_size = 1)]
    fn async_tar(bencher: Bencher, tree: Tree) {
        let (rt, fixture) = (Runtime::new(), tree.fixture());
        extract_with(bencher, &fixture, |dest| {
            rt.block_on(adapters::async_tar::extract(&fixture.archive, dest, true))
        });
    }

    #[cfg(feature = "tokio")]
    #[divan::bench(args = Tree::ALL, sample_size = 1)]
    fn astral_tokio_tar(bencher: Bencher, tree: Tree) {
        let (rt, fixture) = (Runtime::new(), tree.fixture());
        extract_with(bencher, &fixture, |dest| {
            rt.block_on(adapters::astral_tokio_tar::extract(
                &fixture.archive,
                dest,
                true,
            ))
        });
    }

    #[cfg(feature = "tokio")]
    #[divan::bench(args = Tree::ALL, sample_size = 1)]
    fn tar_codec(bencher: Bencher, tree: Tree) {
        let (rt, fixture) = (Runtime::new(), tree.fixture());
        extract_with(bencher, &fixture, |dest| {
            rt.block_on(adapters::tar_codec::extract(&fixture.archive, dest))
        });
    }

    #[divan::bench(args = Tree::ALL, sample_size = 1)]
    fn tar(bencher: Bencher, tree: Tree) {
        let fixture = tree.fixture();
        extract_with(bencher, &fixture, |dest| {
            adapters::tar::extract(&fixture.archive, dest, true)
        });
    }
}

/// Extract the archive with no metadata updates: smol-tar does not set the
/// mode or the modification time, and tar-codec does not set them at all.
/// tar-codec supports only tokio.
#[cfg(feature = "tokio")]
mod extract_bare {
    use super::*;

    #[divan::bench(args = Tree::ALL, sample_size = 1)]
    fn smol_tar(bencher: Bencher, tree: Tree) {
        let (rt, fixture) = (Runtime::new(), tree.fixture());
        extract_with(bencher, &fixture, |dest| {
            rt.block_on(adapters::smol_tar::extract(&fixture.archive, dest, true))
        });
    }

    #[divan::bench(args = Tree::ALL, sample_size = 1)]
    fn tar_codec(bencher: Bencher, tree: Tree) {
        let (rt, fixture) = (Runtime::new(), tree.fixture());
        extract_with(bencher, &fixture, |dest| {
            rt.block_on(adapters::tar_codec::extract(&fixture.archive, dest))
        });
    }
}
