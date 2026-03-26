# smol-tar
[![Tests](https://github.com/growler/smol-tar.rs/actions/workflows/test.yml/badge.svg)](https://github.com/growler/smol-tar.rs/actions/workflows/test.yml) ![Coverage](https://img.shields.io/endpoint?url=https://gist.githubusercontent.com/growler/afe6b562550658e432029a3999ff9a8d/raw/smol-tar.rs-coverage.json) [![Docs](https://docs.rs/smol-tar/badge.svg)](https://docs.rs/smol-tar) [![Crates.io](https://img.shields.io/crates/v/smol-tar.svg?maxAge=2592000)](https://crates.io/crates/smol-tar)

A minimal async streaming tar reader/writer for smol or tokio I/O.

Reading uses a streaming state machine, so the archive never needs to be fully
buffered, while writing uses a compact staging buffer with backpressure via
`Sink`.

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

## Installation

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
use futures_lite::{io::Cursor, StreamExt};
use smol_tar::{TarEntry, TarReader};

let data = Cursor::new(vec![0; 1024]);
let mut tar = TarReader::new(data);

while let Some(entry) = tar.next().await {
    match entry? {
        TarEntry::File(file) => {
            println!("file: {} ({} bytes)", file.path(), file.size());
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

Regular file bodies are streamed. To continue to the next entry, either consume
the file body fully or drop the file reader.

## Writing archives

```rust
use futures::io::Cursor;
use smol_tar::{TarDirectory, TarEntry, TarRegularFile, TarWriter};

let sink = Cursor::new(Vec::<u8>::new());
let mut tar = TarWriter::new(sink);

tar.write(TarEntry::from(TarDirectory::new("bin/"))).await?;

let body = Cursor::new(b"hello\n".to_vec());
tar.write(TarEntry::from(TarRegularFile::new(
    "bin/hello.txt",
    6,
    body,
)))
.await?;

tar.finish().await?;
```

Finishing the writer emits the two zero blocks required to terminate a tar
archive. The `Sink` implementation remains available when you want to compose a
writer with `Stream`/`TryStream` adapters.

`TarWriter::new(...)` usually infers its file-reader type from the file entries
you send later. If you only send metadata-only entries such as directories, you
may need an explicit type annotation; the rustdoc for `TarWriter` shows that
form.

## Filtering one archive into another sink

```rust
use futures::{future, SinkExt, StreamExt, TryStreamExt};
use futures::io::Cursor;
use smol_tar::{TarDirectory, TarEntry, TarReader, TarRegularFile, TarWriter};

let mut input = Cursor::new(Vec::<u8>::new());
let mut source = TarWriter::new(&mut input);
source
    .send(TarDirectory::new("bin/").into())
    .await?;
source
    .send(TarRegularFile::new("bin/keep.txt", 5, Cursor::new(b"keep\n".to_vec()))
            .into())
    .await?;
source
    .send(TarRegularFile::new("share/skip.txt", 5, Cursor::new(b"skip\n".to_vec()))
            .into())
    .await?;
source
    .send(TarRegularFile::new("bin/run.sh", 8, Cursor::new(b"echo hi\n".to_vec()))
            .with_mode(0o755).into())
    .await?;
source.close().await?;
input.set_position(0);

let mut output = Cursor::new(Vec::<u8>::new());
let mut filtered = TarWriter::new(&mut output);

TarReader::new(&mut input)
    .try_filter(|entry| std::future::ready(entry.path().starts_with("bin/")))
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

The repository also ships a feature-gated example CLI in
[`examples/smol-tar-bin`](examples/smol-tar-bin/README.md).

# Rationale

For one of my projects, I needed an asynchronous streaming library for reading
and writing tar archives. Since the project was based on `async-std` (now
[`smol`](https://crates.io/crates/smol)),
[`tokio-tar`](https://crates.io/crates/tokio-tar) was out of scope, so I
started using [`async-tar`](https://crates.io/crates/async-tar). Later, I
needed to add some functionality, but the depth and number of abstractions in
`async-tar` exceeded my cognitive capacity, so I quickly put together a simple,
manageable state machine.

The library is properly fuzzed and covered by tests. It has also seen heavy use
in a real internal CI/CD pipeline for a while, and now seems ready to be
released.

## Caveats

- Regular file bodies are streamed. To advance to the next entry, fully consume
  the file body or drop the file reader.
- Library paths and example-CLI paths are UTF-8 only.

# LICENSE

MIT
