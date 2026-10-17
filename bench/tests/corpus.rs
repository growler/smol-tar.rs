//! Every library reads, writes and filters the corpora with the same result.

use smol_tar_bench::{adapters, Corpus, Counts, Runtime, BUF_SIZE};

#[test]
fn readers_agree() {
    let rt = Runtime::new();
    let mut buf = vec![0; BUF_SIZE];
    for corpus in Corpus::ALL {
        let data = corpus.data();
        let archive = &data.archive[..];
        let want = Counts {
            entries: data.entries,
            bytes: data.body_bytes,
        };

        let got = adapters::tar::read_all(archive, &mut buf).unwrap();
        assert_eq!(got, want, "{corpus}: tar");
        let got = rt
            .block_on(adapters::smol_tar::read_all(archive, &mut buf))
            .unwrap();
        assert_eq!(got, want, "{corpus}: smol-tar");
        let got = rt
            .block_on(adapters::async_tar::read_all(archive, &mut buf))
            .unwrap();
        assert_eq!(got, want, "{corpus}: async-tar");
        #[cfg(feature = "tokio")]
        {
            let got = rt
                .block_on(adapters::astral_tokio_tar::read_all(archive, &mut buf))
                .unwrap();
            assert_eq!(got, want, "{corpus}: astral-tokio-tar");
        }

        assert_eq!(
            adapters::tar::read_skip(archive).unwrap(),
            want.entries,
            "{corpus}: tar"
        );
        let got = rt.block_on(adapters::smol_tar::read_skip(archive)).unwrap();
        assert_eq!(got, want.entries, "{corpus}: smol-tar");
        let got = rt
            .block_on(adapters::async_tar::read_skip(archive))
            .unwrap();
        assert_eq!(got, want.entries, "{corpus}: async-tar");
        #[cfg(feature = "tokio")]
        {
            let got = rt
                .block_on(adapters::astral_tokio_tar::read_skip(archive))
                .unwrap();
            assert_eq!(got, want.entries, "{corpus}: astral-tokio-tar");
        }
    }
}

#[test]
fn writers_agree() {
    let rt = Runtime::new();
    let mut buf = vec![0; BUF_SIZE];
    for corpus in Corpus::USTAR {
        let data = corpus.data();
        let want = Counts {
            entries: data.entries,
            bytes: data.body_bytes,
        };
        let mut out = Vec::new();

        adapters::tar::write(&data.specs, &mut out).unwrap();
        assert_eq!(
            adapters::tar::read_all(&out, &mut buf).unwrap(),
            want,
            "{corpus}: tar"
        );
        out.clear();
        rt.block_on(adapters::smol_tar::write(&data.specs, &mut out))
            .unwrap();
        assert_eq!(
            adapters::tar::read_all(&out, &mut buf).unwrap(),
            want,
            "{corpus}: smol-tar"
        );
        out.clear();
        rt.block_on(adapters::async_tar::write(&data.specs, &mut out))
            .unwrap();
        assert_eq!(
            adapters::tar::read_all(&out, &mut buf).unwrap(),
            want,
            "{corpus}: async-tar"
        );
        #[cfg(feature = "tokio")]
        {
            out.clear();
            rt.block_on(adapters::astral_tokio_tar::write(&data.specs, &mut out))
                .unwrap();
            let got = adapters::tar::read_all(&out, &mut buf).unwrap();
            assert_eq!(got, want, "{corpus}: astral-tokio-tar");
        }
    }
}

#[test]
fn filters_agree() {
    let rt = Runtime::new();
    let mut buf = vec![0; BUF_SIZE];
    for corpus in Corpus::FILTER {
        let archive = &corpus.data().archive[..];
        let mut out = Vec::new();

        adapters::tar::filter(archive, &mut out).unwrap();
        let want = adapters::tar::read_all(&out, &mut buf).unwrap();
        assert_eq!(want.entries, corpus.data().entries / 2, "{corpus}: tar");
        out.clear();
        rt.block_on(adapters::smol_tar::filter(archive, &mut out))
            .unwrap();
        assert_eq!(
            adapters::tar::read_all(&out, &mut buf).unwrap(),
            want,
            "{corpus}: smol-tar"
        );
        out.clear();
        rt.block_on(adapters::async_tar::filter(archive, &mut out))
            .unwrap();
        assert_eq!(
            adapters::tar::read_all(&out, &mut buf).unwrap(),
            want,
            "{corpus}: async-tar"
        );
        #[cfg(feature = "tokio")]
        {
            out.clear();
            rt.block_on(adapters::astral_tokio_tar::filter(archive, &mut out))
                .unwrap();
            let got = adapters::tar::read_all(&out, &mut buf).unwrap();
            assert_eq!(got, want, "{corpus}: astral-tokio-tar");
        }
    }
}
