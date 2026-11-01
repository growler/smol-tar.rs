# smol-tar
[![Tests](https://github.com/growler/smol-tar.rs/actions/workflows/test.yml/badge.svg)](https://github.com/growler/smol-tar.rs/actions/workflows/test.yml) ![Coverage](https://img.shields.io/endpoint?url=https://gist.githubusercontent.com/growler/afe6b562550658e432029a3999ff9a8d/raw/smol-tar.rs-coverage.json) [![Docs](https://docs.rs/smol-tar/badge.svg)](https://docs.rs/smol-tar) [![Crates.io](https://img.shields.io/crates/v/smol-tar.svg?maxAge=2592000)](https://crates.io/crates/smol-tar)

smol-tar is an async streaming tar reader and writer for Rust. `TarReader` is
a `Stream` of tar entries from an `AsyncRead` source. `TarWriter` is a `Sink`
of tar entries to an `AsyncWrite` destination. The crate works with smol and
other `futures-io` runtimes, and with tokio. It does not access the
filesystem.

- Reads old tar, GNU tar, and POSIX ustar/pax archives.
- Writes POSIX ustar archives with PAX extension records.
- The [`smol-tar-fs`](https://crates.io/crates/smol-tar-fs) crate gives a
  `Stream` of tar entries from a directory tree and a `Sink` that writes tar
  entries to a directory, on Unix.
- Has a fuzz target for the reader. The tests run on Rust 1.85, stable, and
  nightly, with the smol traits and with the tokio traits.


Use smol-tar to read or write tar data as a stream in async code. The
source or destination can be a file, a socket, an HTTP body, a
decompressor, or memory. 

Use smol-tar with [`smol-tar-fs`](https://crates.io/crates/smol-tar-fs)
to pack a directory tree into an archive or to unpack an archive into a
directory.

## Installation

For smol and other `futures-io` runtimes:

```toml
[dependencies]
smol-tar = "0.2"
```

For tokio:

```toml
[dependencies]
smol-tar = { version = "0.2", default-features = false, features = ["tokio"] }
```

The minimum supported Rust version is 1.85.

## Feature flags

- `smol` (default): use the `futures-io` traits `AsyncRead`, `AsyncBufRead`
  and `AsyncWrite`. `smol` and other `futures`-based runtimes use these
  traits.
- `tokio`: use the `tokio::io` traits `AsyncRead`, `AsyncBufRead` and
  `AsyncWrite`.
- `futures`: add the `futures` crate as a dependency. The library code does
  not use it.

Enable `smol` or `tokio`. If you enable neither feature, the build fails. If
you enable both features, the crate uses the `tokio` traits.

## Usage rules

1. Use the entries in sequence. `TarReader` and the file entries that it
   yields share one archive stream. Read each file body to the end or drop
   it before you get the next entry. A pending request for the next entry
   wakes when the current body ends or its file reader is dropped.
   If the source is `Send`, both the archive and its file readers are
   `Send` and `Sync`.
2. Wrap a file or a socket in a `BufReader` before you give it to
   `TarReader`. The reader does not buffer the source.
3. Give `TarWriter` file bodies that implement `AsyncBufRead`. The writer
   writes the data from the buffer of the body reader directly to the
   destination, so the buffer size sets the size of each body write. Wrap a
   body that implements only `AsyncRead`, for example a file, in a
   `BufReader`. To write the entries of a `TarReader`, give the `TarReader`
   a source that implements `AsyncBufRead`.
4. `TarWriter` writes the header, each part of the body, and the padding of
   each entry as separate writes. To decrease the number of writes to a file
   or a socket, wrap it in a `BufWriter`.
5. Give `TarRegularFile::new` the exact size of the body. If the body is
   shorter, the write fails with `ErrorKind::UnexpectedEof`. If the body is
   longer, the writer does not read the remaining bytes.
6. `TarWriter::write` and `SinkExt::send` are not cancel safe. If the call
   fails or you drop its future, the entry can be in the archive or not. If
   you write the entry again, the archive can contain it two times. To know
   if the writer accepted the entry, use `SinkExt::feed`, or `poll_ready` and
   then `start_send`. An `Ok` from `start_send` means that the writer
   accepted the entry. In that case, call `TarWriter::flush` after each
   accepted entry, before you read the next entry from a `TarReader`.
7. Call `TarWriter::finish` or `SinkExt::close` after the last entry. These
   methods write the two zero blocks that end a tar archive and close the
   destination. `StreamExt::forward` closes the writer at the end of the
   stream. After that, `write` and `SinkExt::send` return an error. After
   these methods close the destination, a second call to `TarWriter::finish`
   or `SinkExt::close` does nothing and returns `Ok`.
8. All file entries of one `TarWriter` use the same body type `R`. To write
   bodies of different types, box them as
   `Box<dyn AsyncBufRead + Unpin + Send>`. If you write only entries without a
   body, such as directories, set `R` explicitly, for example with the
   `Empty` type of `smol::io` or `tokio::io`:
   `TarWriter::<_, Empty>::new(destination)`.
9. Paths, link targets, and user and group names are UTF-8 strings. Extended
   attribute names are UTF-8 strings. Extended attribute values are bytes.

## Errors

All functions return `std::io::Error`. The reader uses these error kinds:

- `ErrorKind::InvalidData`: a header is malformed, has a bad checksum, has an
  unsupported entry type (for example, a sparse file), has a name that is not
  valid UTF-8, or is larger than a [limit](#limits). A symbolic link entry
  with a size that is not 0 also gives this error.
- `ErrorKind::UnexpectedEof`: the source ends before the end of the archive
  or before the end of a file body.
- Errors from the source reader pass through without change.

An `InvalidData` or `UnexpectedEof` error that the reader makes ends the
stream. The next poll gives `None`. An error that the source reader returns
passes through without change and does not end the stream. The reader keeps
its position, and the next poll reads from the source again.

## Examples

Each example is a program in the [`examples`](examples) directory. The test
suite makes sure that the code below is equal to these programs. These
commands compile the programs:

- `cargo test`: `read`, `read_gz`, `write`, and `filter`.
- `cargo test --no-default-features --features tokio`: `read_tokio`.

With `--features tokio`, the default `smol` feature stays on. Then the smol
programs compile to a `main` that writes a message to stderr and exits with
status 1.

### Read an archive with smol

Dependencies: `smol-tar`, `smol`.

<!-- example: examples/read.rs -->
```rust
//! List the entries of a tar archive with smol.
//!
//! Run: cargo run --example read -- ARCHIVE.tar

// With the tokio feature, smol-tar uses the tokio traits, and the smol main
// below cannot compile. In that case, a short main writes a message to
// stderr and exits with status 1.
#![cfg_attr(feature = "tokio", allow(unused_imports))]

use smol::{
    fs::File,
    io::{copy, sink, BufReader},
    stream::StreamExt,
};
use smol_tar::{TarEntry, TarReader};

#[cfg(not(feature = "tokio"))]
fn main() -> std::io::Result<()> {
    let path = std::env::args().nth(1).expect("usage: read ARCHIVE.tar");
    smol::block_on(async {
        // TarReader does not buffer the source. Wrap a file in a BufReader.
        let mut tar = TarReader::new(BufReader::new(File::open(path).await?));
        while let Some(entry) = tar.next().await {
            match entry? {
                TarEntry::File(mut file) => {
                    // Read the body to the end or drop it before the next entry.
                    let size = copy(&mut file, &mut sink()).await?;
                    println!("file {} ({size} bytes)", file.path());
                }
                TarEntry::Directory(dir) => println!("dir {}", dir.path()),
                TarEntry::Symlink(link) => println!("symlink {} -> {}", link.path(), link.link()),
                other => println!("other {}", other.path()),
            }
        }
        Ok(())
    })
}

#[cfg(feature = "tokio")]
fn main() {
    eprintln!("this example needs the smol feature without the tokio feature");
    std::process::exit(1);
}
```

### Read an archive with tokio

Dependencies: `smol-tar` with the `tokio` feature, `tokio` with the `fs`,
`io-util`, `macros`, and `rt-multi-thread` features, `tokio-stream`.

<!-- example: examples/read_tokio.rs -->
```rust
//! List the entries of a tar archive with tokio.
//!
//! Run: cargo run --example read_tokio --no-default-features --features tokio -- ARCHIVE.tar

use smol_tar::{TarEntry, TarReader};
use tokio::{
    fs::File,
    io::{copy, sink, BufReader},
};
use tokio_stream::StreamExt;

#[tokio::main]
async fn main() -> std::io::Result<()> {
    let path = std::env::args()
        .nth(1)
        .expect("usage: read_tokio ARCHIVE.tar");
    // TarReader does not buffer the source. Wrap a file in a BufReader.
    let mut tar = TarReader::new(BufReader::new(File::open(path).await?));
    while let Some(entry) = tar.next().await {
        match entry? {
            TarEntry::File(mut file) => {
                // Read the body to the end or drop it before the next entry.
                let size = copy(&mut file, &mut sink()).await?;
                println!("file {} ({size} bytes)", file.path());
            }
            TarEntry::Directory(dir) => println!("dir {}", dir.path()),
            other => println!("other {}", other.path()),
        }
    }
    Ok(())
}
```

### Read a .tar.gz archive

Dependencies: `smol-tar`, `smol`, `async-compression` with the `futures-io`
and `gzip` features. With tokio, use the `tokio` feature of
`async-compression` and `async_compression::tokio::bufread::GzipDecoder`.

<!-- example: examples/read_gz.rs -->
```rust
//! List the files of a gzip-compressed tar archive with smol.
//!
//! Run: cargo run --example read_gz -- ARCHIVE.tar.gz

// With the tokio feature, smol-tar uses the tokio traits, and the smol main
// below cannot compile. In that case, a short main writes a message to
// stderr and exits with status 1.
#![cfg_attr(feature = "tokio", allow(unused_imports))]

use async_compression::futures::bufread::GzipDecoder;
use smol::{
    fs::File,
    io::{copy, sink, BufReader},
    stream::StreamExt,
};
use smol_tar::{TarEntry, TarReader};

#[cfg(not(feature = "tokio"))]
fn main() -> std::io::Result<()> {
    let path = std::env::args()
        .nth(1)
        .expect("usage: read_gz ARCHIVE.tar.gz");
    smol::block_on(async {
        let gz = GzipDecoder::new(BufReader::new(File::open(path).await?));
        let mut tar = TarReader::new(gz);
        while let Some(entry) = tar.next().await {
            if let TarEntry::File(mut file) = entry? {
                let size = copy(&mut file, &mut sink()).await?;
                println!("{} ({size} bytes)", file.path());
            }
        }
        Ok(())
    })
}

#[cfg(feature = "tokio")]
fn main() {
    eprintln!("this example needs the smol feature without the tokio feature");
    std::process::exit(1);
}
```

### Write an archive

Dependencies: `smol-tar`, `smol`.

<!-- example: examples/write.rs -->
```rust
//! Write files from disk into a new tar archive with smol.
//!
//! Run: cargo run --example write -- OUT.tar FILE...

// With the tokio feature, smol-tar uses the tokio traits, and the smol main
// below cannot compile. In that case, a short main writes a message to
// stderr and exits with status 1.
#![cfg_attr(feature = "tokio", allow(unused_imports))]

use smol::{fs::File, io::BufReader};
use smol_tar::{TarDirectory, TarRegularFile, TarWriter};

#[cfg(not(feature = "tokio"))]
fn main() -> std::io::Result<()> {
    let mut args = std::env::args().skip(1);
    let out = args.next().expect("usage: write OUT.tar FILE...");
    smol::block_on(async {
        // TarWriter writes each header and each part of a body separately.
        // To decrease the number of writes, wrap the file in a BufWriter.
        let mut tar = TarWriter::new(File::create(out).await?);
        tar.write(TarDirectory::new("files/").with_mode(0o755).into())
            .await?;
        for path in args {
            let file = File::open(&path).await?;
            let meta = file.metadata().await?;
            let name = format!("files/{}", path.trim_start_matches('/'));
            // TarWriter writes the body from the buffer of the body reader.
            // The buffer size sets the size of each body write.
            let body = BufReader::with_capacity(64 * 1024, file);
            // The size must be equal to the number of bytes in the body.
            let entry = TarRegularFile::new(name, meta.len(), body)
                .with_mode(0o644)
                .with_mtime(meta.modified()?);
            tar.write(entry.into()).await?;
        }
        // Write the two zero blocks that end the archive and close the file.
        tar.finish().await
    })
}

#[cfg(feature = "tokio")]
fn main() {
    eprintln!("this example needs the smol feature without the tokio feature");
    std::process::exit(1);
}
```

### Filter one archive into another

`TarReader` is a `Stream` and `TarWriter` is a `Sink` of the same entry type.
Thus the stream combinators of the `futures` crate connect them.

Dependencies: `smol-tar`, `smol`, `futures`.

<!-- example: examples/filter.rs -->
```rust
//! Copy the entries whose path starts with PREFIX into a new archive.
//!
//! Run: cargo run --example filter -- IN.tar OUT.tar PREFIX

// With the tokio feature, smol-tar uses the tokio traits, and the smol main
// below cannot compile. In that case, a short main writes a message to
// stderr and exits with status 1.
#![cfg_attr(feature = "tokio", allow(unused_imports))]

use futures::{future, StreamExt, TryStreamExt};
use smol::{fs::File, io::BufReader};
use smol_tar::{TarReader, TarWriter};

#[cfg(not(feature = "tokio"))]
fn main() -> std::io::Result<()> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let [input, output, prefix] = args.as_slice() else {
        panic!("usage: filter IN.tar OUT.tar PREFIX");
    };
    smol::block_on(async {
        let mut out = TarWriter::new(File::create(output).await?);
        // TarReader is a Stream and TarWriter is a Sink of the same entry type.
        // forward() closes the writer at the end of the stream.
        TarReader::new(BufReader::new(File::open(input).await?))
            .try_filter(|entry| future::ready(entry.path().starts_with(prefix.as_str())))
            .forward(&mut out)
            .await
    })
}

#[cfg(feature = "tokio")]
fn main() {
    eprintln!("this example needs the smol feature without the tokio feature");
    std::process::exit(1);
}
```

## API summary

Refer to [docs.rs](https://docs.rs/smol-tar) for the full API.

`TarReader::new(source)`
: A `Stream` of `std::io::Result<TarEntry>`.

`TarWriter::new(destination)`
: A `Sink` of `TarEntry`. It also has the methods `write(entry)`,
  `flush()` and `finish()`.

`TarEntry`
: An enum with the variants `File`, `Link`, `Symlink`, `Directory`,
  `Device`, and `Fifo`. `TarEntry::path()` returns the path of each variant.
  Each entry type converts into `TarEntry` with `.into()`.

`TarRegularFile::new(path, size, body)`
: A regular file. It implements `AsyncRead` for the body. `size()` returns
  the body size.

`TarDirectory::new(path)`
: A directory.

`TarSymlink::new(path, target)` and `TarLink::new(path, target)`
: A symbolic link and a hard link. `link()` returns the target.

`TarDevice::new_char(path, major, minor)` and `TarDevice::new_block(path, major, minor)`
: A character device and a block device.

`TarFifo::new(path)`
: A FIFO.

Metadata
: All entry types except `TarLink` have the getters `mode()`, `mtime()`,
  `atime()`, `ctime()`, `uid()`, `gid()`, `uname()`, `gname()`, and
  `attrs()`. They also have the builder methods `with_mode`, `with_mtime`,
  `with_atime`, `with_ctime`, `with_uid`, `with_gid`, `with_uname`,
  `with_gname`, and `with_attrs`.

`AttrList`
: The extended attributes of an entry. The writer stores them in pax
  records.

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

Not supported:

- Sparse files and multi-volume archives. The reader returns an error for
  these entries.

## Entry sizes

The reader uses the size of an entry to find the next header. The size is
the PAX `size` record of the entry. If the entry has no `size` record, the
size is the size field of the header.

- Regular file: the size is the length of the file body after the header.
  `TarRegularFile` gives the body.
- Hard link: data can follow the header. The reader skips the data and its
  padding. `TarLink` has no body.
- Symbolic link: the size must be 0. Another size gives an
  `ErrorKind::InvalidData` error.
- Directory, character device, block device and FIFO: no data follows the
  header. The next header starts in the block after the header.
  `TarDirectory::size()` gives the size of a directory entry.
- In an old tar header, a regular file entry with a path that ends with `/`
  is a directory entry.

Other tar parsers can read data after a directory, device or FIFO header.
Thus they can find different entries in the same archive. Use the same
parser to validate and to extract an archive.

## Global PAX headers

The records of a global PAX header apply to all the entries after it. A PAX
record of the entry takes precedence over a global record with the same
key. A later global record replaces an earlier one.

- The reader ignores the `path`, `linkpath` and `size` records of a global
  header.
- A global record with an empty value deletes the global record of its key.
  For a `SCHILY.xattr.*` record, an empty value is the value of the
  attribute.
- The reader parses each global header one time. All entries share the
  global records.
- An entry from the reader gives its extended attributes sorted by name,
  with one attribute for each name.

## Limits

The reader returns an `ErrorKind::InvalidData` error for these archive
contents:

- a GNU long name or long link that is larger than 4096 bytes
- a PAX extension that is larger than 1 MiB
- more than 256 global PAX extensions
- global PAX records that are larger than 1 MiB together

The writer returns an `ErrorKind::Other` error for an entry whose headers,
with the PAX records, are larger than 128 KiB.

## Example CLI

The repository also includes the example CLI
[`picotar`](https://github.com/growler/smol-tar.rs/tree/main/picotar). It
creates, extracts, and lists archives on disk.

## Benchmarks

The [`bench`](https://github.com/growler/smol-tar.rs/tree/main/bench) crate 
compares smol-tar with async-tar, astral-tokio-tar, tar-codec and the synchronous
`tar` crate. All archives are in memory, so the results do not include disk I/O.

- For archives that hold thousands of entries, the times of all libraries
  are within a few milliseconds of each other. The first real disk or
  network read will hide the differences. Pick the library whose API you
  find the most pleasing to look at. Performance will not make the decision
  for you.
- smol-tar has the lowest time to read the pax and GNU long-name corpora.
  For pax, smol-tar reads the archive in 9.9 ms and `tar` reads it in
  21.0 ms.
- smol-tar makes fewer allocations for each ustar and GNU long-name entry
  than the other libraries. Its peak live heap is less than 1.5 KB when it
  reads all entries.

Refer to
[`bench/README.md`](https://github.com/growler/smol-tar.rs/blob/main/bench/README.md#results)
for the result tables, the allocation counts, and the test environment.

## Rationale

For one of my projects, I needed an asynchronous streaming library for reading
and writing tar archives. The project was based on `async-std` (now
[`smol`](https://crates.io/crates/smol)), so
[`tokio-tar`](https://crates.io/crates/tokio-tar) was not a sensible fit and I
started with [`async-tar`](https://crates.io/crates/async-tar). Later on I
needed a bit more functionality, but the depth and number of abstractions in
`async-tar` became rather a lot to hold in my head, so I put together a small,
manageable state machine instead.

The reader has a cargo-fuzz target in the `fuzz` directory. The coverage
badge above shows the test coverage. The library has also seen heavy use in a
real internal CI/CD pipeline for some time.

## Special thanks

To authors of [`async-tar`](https://crates.io/crates/async-tar) for the inspiration.

## License

MIT
