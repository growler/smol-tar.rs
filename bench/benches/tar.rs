//! CPU time and allocation benchmarks for smol-tar and other tar libraries.
//!
//! Every archive is in memory, so the results do not include disk I/O.

use {
    divan::{
        counter::{BytesCount, ItemsCount},
        Bencher,
    },
    smol_tar_bench::{adapters, Corpus, Runtime, BUF_SIZE},
};

#[global_allocator]
static ALLOC: divan::AllocProfiler = divan::AllocProfiler::system();

fn main() {
    divan::main();
}

/// Add the archive size and the entry count of `corpus` as counters.
fn counted<'a, 'b>(bencher: Bencher<'a, 'b>, corpus: Corpus) -> Bencher<'a, 'b> {
    let data = corpus.data();
    bencher
        .counter(BytesCount::new(data.archive.len()))
        .counter(ItemsCount::new(data.entries))
}

/// Read every entry and every file body into a 64 KiB buffer.
mod read_all {
    use super::*;

    #[divan::bench(args = Corpus::ALL)]
    fn smol_tar(bencher: Bencher, corpus: Corpus) {
        let (rt, data, mut buf) = (
            Runtime::new(),
            &corpus.data().archive[..],
            vec![0; BUF_SIZE],
        );
        counted(bencher, corpus).bench_local(|| {
            rt.block_on(adapters::smol_tar::read_all(data, &mut buf))
                .unwrap()
        });
    }

    #[divan::bench(args = Corpus::ALL)]
    fn async_tar(bencher: Bencher, corpus: Corpus) {
        let (rt, data, mut buf) = (
            Runtime::new(),
            &corpus.data().archive[..],
            vec![0; BUF_SIZE],
        );
        counted(bencher, corpus).bench_local(|| {
            rt.block_on(adapters::async_tar::read_all(data, &mut buf))
                .unwrap()
        });
    }

    #[cfg(feature = "tokio")]
    #[divan::bench(args = Corpus::ALL)]
    fn astral_tokio_tar(bencher: Bencher, corpus: Corpus) {
        let (rt, data, mut buf) = (
            Runtime::new(),
            &corpus.data().archive[..],
            vec![0; BUF_SIZE],
        );
        counted(bencher, corpus).bench_local(|| {
            rt.block_on(adapters::astral_tokio_tar::read_all(data, &mut buf))
                .unwrap()
        });
    }

    #[divan::bench(args = Corpus::ALL)]
    fn tar(bencher: Bencher, corpus: Corpus) {
        let (data, mut buf) = (&corpus.data().archive[..], vec![0; BUF_SIZE]);
        counted(bencher, corpus).bench_local(|| adapters::tar::read_all(data, &mut buf).unwrap());
    }
}

/// Read every entry and skip every file body.
mod read_skip {
    use super::*;

    #[divan::bench(args = Corpus::ALL)]
    fn smol_tar(bencher: Bencher, corpus: Corpus) {
        let (rt, data) = (Runtime::new(), &corpus.data().archive[..]);
        counted(bencher, corpus)
            .bench_local(|| rt.block_on(adapters::smol_tar::read_skip(data)).unwrap());
    }

    #[divan::bench(args = Corpus::ALL)]
    fn async_tar(bencher: Bencher, corpus: Corpus) {
        let (rt, data) = (Runtime::new(), &corpus.data().archive[..]);
        counted(bencher, corpus)
            .bench_local(|| rt.block_on(adapters::async_tar::read_skip(data)).unwrap());
    }

    #[cfg(feature = "tokio")]
    #[divan::bench(args = Corpus::ALL)]
    fn astral_tokio_tar(bencher: Bencher, corpus: Corpus) {
        let (rt, data) = (Runtime::new(), &corpus.data().archive[..]);
        counted(bencher, corpus).bench_local(|| {
            rt.block_on(adapters::astral_tokio_tar::read_skip(data))
                .unwrap()
        });
    }

    #[divan::bench(args = Corpus::ALL)]
    fn tar(bencher: Bencher, corpus: Corpus) {
        let data = &corpus.data().archive[..];
        counted(bencher, corpus).bench_local(|| adapters::tar::read_skip(data).unwrap());
    }
}

/// Write the corpus entries into a reused `Vec<u8>`.
mod write {
    use super::*;

    #[divan::bench(args = Corpus::USTAR)]
    fn smol_tar(bencher: Bencher, corpus: Corpus) {
        let (rt, specs) = (Runtime::new(), &corpus.data().specs[..]);
        let mut out = Vec::with_capacity(corpus.data().archive.len());
        counted(bencher, corpus).bench_local(|| {
            out.clear();
            rt.block_on(adapters::smol_tar::write(specs, &mut out))
                .unwrap();
            out.len()
        });
    }

    #[divan::bench(args = Corpus::USTAR)]
    fn async_tar(bencher: Bencher, corpus: Corpus) {
        let (rt, specs) = (Runtime::new(), &corpus.data().specs[..]);
        let mut out = Vec::with_capacity(corpus.data().archive.len());
        counted(bencher, corpus).bench_local(|| {
            out.clear();
            rt.block_on(adapters::async_tar::write(specs, &mut out))
                .unwrap();
            out.len()
        });
    }

    #[cfg(feature = "tokio")]
    #[divan::bench(args = Corpus::USTAR)]
    fn astral_tokio_tar(bencher: Bencher, corpus: Corpus) {
        let (rt, specs) = (Runtime::new(), &corpus.data().specs[..]);
        let mut out = Vec::with_capacity(corpus.data().archive.len());
        counted(bencher, corpus).bench_local(|| {
            out.clear();
            rt.block_on(adapters::astral_tokio_tar::write(specs, &mut out))
                .unwrap();
            out.len()
        });
    }

    #[divan::bench(args = Corpus::USTAR)]
    fn tar(bencher: Bencher, corpus: Corpus) {
        let specs = &corpus.data().specs[..];
        let mut out = Vec::with_capacity(corpus.data().archive.len());
        counted(bencher, corpus).bench_local(|| {
            out.clear();
            adapters::tar::write(specs, &mut out).unwrap();
            out.len()
        });
    }
}

/// Copy every second entry of the corpus into a reused `Vec<u8>`.
mod filter {
    use super::*;

    #[divan::bench(args = Corpus::FILTER)]
    fn smol_tar(bencher: Bencher, corpus: Corpus) {
        let (rt, data) = (Runtime::new(), &corpus.data().archive[..]);
        let mut out = Vec::with_capacity(data.len());
        counted(bencher, corpus).bench_local(|| {
            out.clear();
            rt.block_on(adapters::smol_tar::filter(data, &mut out))
                .unwrap();
            out.len()
        });
    }

    #[divan::bench(args = Corpus::FILTER)]
    fn async_tar(bencher: Bencher, corpus: Corpus) {
        let (rt, data) = (Runtime::new(), &corpus.data().archive[..]);
        let mut out = Vec::with_capacity(data.len());
        counted(bencher, corpus).bench_local(|| {
            out.clear();
            rt.block_on(adapters::async_tar::filter(data, &mut out))
                .unwrap();
            out.len()
        });
    }

    #[cfg(feature = "tokio")]
    #[divan::bench(args = Corpus::FILTER)]
    fn astral_tokio_tar(bencher: Bencher, corpus: Corpus) {
        let (rt, data) = (Runtime::new(), &corpus.data().archive[..]);
        let mut out = Vec::with_capacity(data.len());
        counted(bencher, corpus).bench_local(|| {
            out.clear();
            rt.block_on(adapters::astral_tokio_tar::filter(data, &mut out))
                .unwrap();
            out.len()
        });
    }

    #[divan::bench(args = Corpus::FILTER)]
    fn tar(bencher: Bencher, corpus: Corpus) {
        let data = &corpus.data().archive[..];
        let mut out = Vec::with_capacity(data.len());
        counted(bencher, corpus).bench_local(|| {
            out.clear();
            adapters::tar::filter(data, &mut out).unwrap();
            out.len()
        });
    }
}
