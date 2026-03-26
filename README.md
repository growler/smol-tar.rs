# smol-tar
[![Tests](https://github.com/growler/smol-tar.rs/actions/workflows/test.yml/badge.svg)](https://github.com/growler/smol-tar.rs/actions/workflows/test.yml) ![Coverage](https://img.shields.io/endpoint?url=https://gist.githubusercontent.com/growler/afe6b562550658e432029a3999ff9a8d/raw/smol-tar.rs-coverage.json) [![Docs](https://docs.rs/smol-tar/badge.svg)](https://docs.rs/smol-tar) [![Crates.io](https://img.shields.io/crates/v/smol-tar.svg?maxAge=2592000)](https://crates.io/crates/smol-tar)

A minimal async streaming tar reader/writer. It does not perform filesystem 
operations itself; it works with async readers, writers, and streams.

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
use smol_tar::{TarDirectory, TarEntry, TarRegularFile, TarWriter};

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

Alongside the direct API, the writer also implements the composable
`futures_sink::Sink` interface:

## Filtering one archive into another sink

```rust
use futures::{future, SinkExt, StreamExt, TryStreamExt};
use smol::io::Cursor;
use smol_tar::{TarDirectory, TarEntry, TarReader, TarRegularFile, TarWriter};

let mut input = Cursor::new(Vec::<u8>::new());
let mut source = TarWriter::new(&mut input);
source
    .send(TarDirectory::new("bin/").into())
    .await?;
source
    .send(
        TarRegularFile::new("bin/keep.txt", 5, Cursor::new(b"keep\n".as_ref()))
            .into(),
    )
    .await?;
source
    .send(
        TarRegularFile::new("share/skip.txt", 5, Cursor::new(b"skip\n".as_ref()))
            .into(),
    )
    .await?;
source
    .send(
        TarRegularFile::new("bin/run.sh", 8, Cursor::new(b"echo hi\n".as_ref()))
            .with_mode(0o755)
            .into(),
    )
    .await?;
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

## Caveats

- Regular file bodies are streamed. To advance to the next entry, either read
  the file body to the end or drop the file reader.
- Library paths and example-CLI paths must be valid UTF-8.

# LICENSE

MIT
