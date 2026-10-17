# smol-tar
[![Tests](https://github.com/growler/smol-tar.rs/actions/workflows/test.yml/badge.svg)](https://github.com/growler/smol-tar.rs/actions/workflows/test.yml) ![Coverage](https://img.shields.io/endpoint?url=https://gist.githubusercontent.com/growler/afe6b562550658e432029a3999ff9a8d/raw/smol-tar.rs-coverage.json) [![Docs](https://docs.rs/smol-tar/badge.svg)](https://docs.rs/smol-tar) [![Crates.io](https://img.shields.io/crates/v/smol-tar.svg?maxAge=2592000)](https://crates.io/crates/smol-tar)

A minimal async streaming tar reader and writer. It does not access the
filesystem. It reads archives from async readers and writes archives to async
writers.

## Supported formats

Reads:

- Old tar
- GNU tar
- POSIX ustar/pax
- GNU long names and long links
- PAX path metadata, timestamps, numeric ids, sizes, and extended attributes

Writes:

- POSIX ustar/pax
- PAX records for long paths, long link targets, timestamps, numeric ids,
  symbolic names, sizes, and extended attributes

## Not supported

- Sparse files and multi-volume archives (the reader returns an error)

## Installation

Runtime selection:

```toml
[dependencies]
smol-tar = "0.1"
```

```toml
[dependencies]
smol-tar = { version = "0.1", default-features = false, features = ["tokio"] }
```

## Feature flags

- `smol` (default): use the `futures-io` traits `AsyncRead` and `AsyncWrite`.
  `smol` and other `futures`-based runtimes use these traits.
- `tokio`: use the `tokio::io` traits `AsyncRead` and `AsyncWrite`.
- `futures`: add the `futures` crate as a dependency. The library code does
  not use it.

Enable `smol` or `tokio`. If you enable neither feature, the build fails. If
you enable both features, the crate uses the `tokio` traits.

## Minimum supported Rust version

Rust 1.85.

## Reading archives

```rust
use smol::{
    io::{copy, sink, Cursor},
    stream::StreamExt,
};
use smol_tar::{TarEntry, TarReader};

let data = Cursor::new(vec![0; 1024]);
let mut tar = TarReader::new(data);

while let Some(entry) = tar.next().await {
    match entry? {
        TarEntry::File(mut file) => {
            println!("file: {} ({} bytes)", file.path(), file.size());
            copy(&mut file, &mut sink()).await?;
        }
        TarEntry::Directory(dir) => {
            println!("dir: {}", dir.path());
        }
        other => {
            println!("other: {}", other.path());
        }
    }
}
```

Regular file bodies are streamed. To move on to the next entry, either read the
file body to the end or drop the file reader.

## Writing archives

```rust
use smol::io::Cursor;
use smol_tar::{
    TarDirectory, TarEntry, TarRegularFile, TarWriter
};

let sink = Cursor::new(Vec::<u8>::new());
let mut tar = TarWriter::new(sink);

tar.write(TarDirectory::new("bin/").into()).await?;

let body = Cursor::new(b"hello\n");
tar.write(
    TarRegularFile::new(
        "bin/hello.txt", 6, body,
    ).into()
).await?;

tar.finish().await?;
```

## Filtering one archive into another sink

The writer also implements the `futures_sink::Sink` trait. This example
filters the entries of one archive into a second archive:

```rust
use futures::{future, SinkExt, StreamExt, TryStreamExt};
use smol::io::Cursor;
use smol_tar::*;

let mut input = Cursor::new(Vec::<u8>::new());
let mut source = TarWriter::new(&mut input);
source.send(
    TarDirectory::new("bin/").into()
).await?;
source.send(TarRegularFile::new(
        "bin/keep.txt", 5, Cursor::new(b"keep\n".as_ref())
    ).into(),
).await?;
source.send(
    TarRegularFile::new(
        "share/skip.txt", 5, Cursor::new(b"skip\n".as_ref())
    ).into(),
).await?;
source.send(
    TarRegularFile::new(
        "bin/run.sh", 8, Cursor::new(b"echo hi\n".as_ref())
    ).with_mode(0o755).into(),
).await?;
source.close().await?;
input.set_position(0);

let mut output = Cursor::new(Vec::<u8>::new());
let mut filtered = TarWriter::new(&mut output);

TarReader::new(&mut input)
    .try_filter(|entry| {
        future::ready(entry.path().starts_with("bin/"))
    })
    .forward(&mut filtered)
    .await?;

filtered.close().await?;
output.set_position(0);

let paths: Vec<String> = TarReader::new(output)
    .map_ok(|entry| entry.path().to_string())
    .try_collect()
    .await?;

assert_eq!(paths, vec!["bin/", "bin/keep.txt", "bin/run.sh"]);
```

## Example CLI

The repository also includes a feature-gated example CLI in
[`examples/smol-tar-bin`](examples/smol-tar-bin/README.md).

## Benchmarks

The [`bench`](bench/README.md) crate compares smol-tar with async-tar,
astral-tokio-tar, and the synchronous `tar` crate. All archives are in
memory, so the results do not include disk I/O. Refer to
[`bench/README.md`](bench/README.md) for the corpora, the benchmarks, and the
commands.

In short, the differences between these libraries are a few milliseconds
for archives that hold thousands of entries and live entirely in memory. The
first real disk or network read will hide them. Pick the library whose API
you find the most pleasing to look at. Performance will not make the
decision for you.

Test environment: Intel Core i9-14900KF, Linux 6.18 x86_64, Rust 1.89.0.

The numbers in the lists below come from the smol build.

Results:

- With the large corpus (4 files of 64 MiB), all libraries have the same
  time within 5%. The memory copy sets the time.
- In `read_all` and `read_skip`, smol-tar has the lowest time for the pax,
  gnu-long, and mixed corpora in both builds. For pax, smol-tar reads the
  archive in 9.9 ms and `tar` reads it in 21.0 ms.
- In `read_all` with the small corpus, `tar` has the lowest time: 3.9 ms.
  smol-tar takes 4.5 ms.
- In `write` with the small corpus, smol-tar takes 1.9 ms, `tar` takes
  3.2 ms, and async-tar takes 4.2 ms.
- In `filter` with the small corpus, `tar` has the lowest time: 4.6 ms.
  smol-tar takes 5.1 ms. With the mixed corpus, smol-tar and `tar` both take
  10.9 ms.

Allocations:

- `read_all`: smol-tar makes 1 allocation for each ustar entry, 2 for each
  GNU long-name entry, and 8 for each PAX entry. The other libraries make 2
  for each ustar entry, 5 for each GNU long-name entry, and 5 or 6 for each
  PAX entry.
- `write` with the small corpus: smol-tar makes 1 allocation for each entry,
  `tar` makes 5, and async-tar and astral-tokio-tar make 6.
- `filter` with the small corpus: smol-tar allocates 206 KB in each
  iteration, `tar` allocates 400 KB, and async-tar and astral-tokio-tar
  allocate 41 MB.
- The peak live heap of smol-tar is less than 1.5 KB in `read_all` and
  approximately 66 KB in `read_skip` and `filter`. The 66 KB include the
  64 KiB skip buffer.

The tables show the median time in milliseconds for 100 samples.

smol build (`--features smol`):

| Benchmark | Corpus   | smol-tar | async-tar | tar   |
| --------- | -------- | -------: | --------: | ----: |
| read_all  | small    |     4.46 |      4.89 |  3.91 |
| read_all  | large    |    13.82 |     13.78 | 13.80 |
| read_all  | pax      |     9.90 |     14.74 | 20.99 |
| read_all  | gnu-long |     7.44 |     10.84 |  7.81 |
| read_all  | mixed    |     7.52 |     10.55 |  9.36 |
| read_skip | small    |     3.61 |      4.56 |  3.73 |
| read_skip | large    |    13.73 |     13.77 | 13.77 |
| read_skip | pax      |     9.03 |     14.11 | 20.86 |
| read_skip | gnu-long |     6.64 |     10.15 |  7.67 |
| read_skip | mixed    |     6.79 |      8.21 |  7.41 |
| write     | small    |     1.85 |      4.22 |  3.19 |
| write     | large    |    25.78 |     25.17 | 25.47 |
| write     | mixed    |     6.55 |      9.17 |  8.01 |
| filter    | small    |     5.13 |      6.95 |  4.60 |
| filter    | mixed    |    10.86 |     12.65 | 10.93 |

tokio build (`--no-default-features --features tokio`):

| Benchmark | Corpus   | smol-tar | async-tar | astral-tokio-tar | tar   |
| --------- | -------- | -------: | --------: | ---------------: | ----: |
| read_all  | small    |     4.91 |      4.72 |             5.75 |  4.15 |
| read_all  | large    |    14.09 |     13.89 |            13.94 | 13.88 |
| read_all  | pax      |    10.17 |     15.12 |            14.65 | 19.64 |
| read_all  | gnu-long |     7.76 |     10.42 |            12.52 |  8.09 |
| read_all  | mixed    |     8.14 |     10.65 |            11.57 |  9.54 |
| read_skip | small    |     3.73 |      4.69 |             5.27 |  3.80 |
| read_skip | large    |    13.87 |     13.93 |            13.95 | 13.97 |
| read_skip | pax      |     9.04 |     14.62 |            14.33 | 19.22 |
| read_skip | gnu-long |     6.55 |     10.08 |            12.05 |  7.69 |
| read_skip | mixed    |     6.86 |      8.58 |             9.04 |  7.49 |
| write     | small    |     1.91 |      4.21 |             4.18 |  3.37 |
| write     | large    |    25.32 |     26.31 |            25.44 | 25.12 |
| write     | mixed    |     6.59 |      9.25 |             9.47 |  8.12 |
| filter    | small    |     5.99 |      6.91 |             8.06 |  4.62 |
| filter    | mixed    |    11.73 |     12.76 |            13.76 | 10.92 |

# Rationale

For one of my projects, I needed an asynchronous streaming library for reading
and writing tar archives. The project was based on `async-std` (now
[`smol`](https://crates.io/crates/smol)), so
[`tokio-tar`](https://crates.io/crates/tokio-tar) was not a sensible fit and I
started with [`async-tar`](https://crates.io/crates/async-tar). Later on I
needed a bit more functionality, but the depth and number of abstractions in
`async-tar` became rather a lot to hold in my head, so I put together a small,
manageable state machine instead.

The library is properly fuzzed and well covered by tests. It has also seen
heavy use in a real internal CI/CD pipeline for some time, and now seems ready
for release.

# Caveats

- `TarReader` and the `TarRegularFileReader` values that it yields are
  `Send + Sync`, but they share one archive stream. Use them in sequence, not
  at the same time. Read each file body to the end or drop it before you get
  the next entry.
- Library paths and example-CLI paths must be valid UTF-8.
- The reader rejects a GNU long name or long link that is larger than 4096
  bytes, a PAX extension that is larger than 1 MiB, and more than 256 global
  PAX extensions.

# Special thanks

To authors of [`async-tar`](https://crates.io/crates/async-tar) for the inspiration.

# LICENSE

MIT
