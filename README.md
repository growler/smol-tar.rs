# smol-tar
[![Tests](https://github.com/growler/smol-tar.rs/actions/workflows/test.yml/badge.svg)](https://github.com/growler/smol-tar.rs/actions/workflows/test.yml) ![Coverage](https://img.shields.io/endpoint?url=https://gist.githubusercontent.com/growler/afe6b562550658e432029a3999ff9a8d/raw/smol-tar.rs-coverage.json) [![Docs](https://docs.rs/smol-tar/badge.svg)](https://docs.rs/smol-tar) [![Crates.io](https://img.shields.io/crates/v/smol-tar.svg?maxAge=2592000)](https://crates.io/crates/smol-tar)

smol-tar is an async streaming tar reader and writer for Rust. `TarReader` is
a `Stream` of tar entries from an `AsyncRead` source. `TarWriter` is a `Sink`
of tar entries to an `AsyncWrite` destination. The crate works with smol and
other `futures-io` runtimes, and with tokio. Without the `fs` feature, it
does not access the filesystem.

- Reads old tar, GNU tar, and POSIX ustar/pax archives.
- Writes POSIX ustar archives with pax extension records.
- Streams file bodies. The reader does not keep file bodies in memory.
- Makes 1 heap allocation for each ustar entry when it reads or writes.
- With the `fs` feature, packs a directory tree into an archive and unpacks
  an archive into a directory, on Unix.
- Has a fuzz target for the reader. The tests run on Rust 1.85, stable, and
  nightly, with the smol traits and with the tokio traits.

## Choose a crate

- Use smol-tar to read or write tar data as a stream in async code. The
  source or destination can be a file, a socket, an HTTP body, a
  decompressor, or memory. Your code decides what goes to disk.
- Use smol-tar with the `fs` feature to pack a directory tree into an
  archive or to unpack an archive into a directory, on Unix.
- Use [`tar`](https://crates.io/crates/tar) for synchronous code.
- On platforms other than Unix, use
  [`async-tar`](https://crates.io/crates/async-tar) or
  [`astral-tokio-tar`](https://crates.io/crates/astral-tokio-tar) to unpack
  an archive into a directory with one call (`unpack`), or to add a
  directory tree from disk with one call (`append_dir_all`).

## Installation

For smol and other `futures-io` runtimes:

```toml
[dependencies]
smol-tar = "0.1"
```

For tokio:

```toml
[dependencies]
smol-tar = { version = "0.1", default-features = false, features = ["tokio"] }
```

The minimum supported Rust version is 1.85.

## Feature flags

- `smol` (default): use the `futures-io` traits `AsyncRead` and `AsyncWrite`.
  `smol` and other `futures`-based runtimes use these traits.
- `tokio`: use the `tokio::io` traits `AsyncRead` and `AsyncWrite`.
- `futures`: add the `futures` crate as a dependency. The library code does
  not use it.
- `fs`: add the `smol_tar::fs` module. It packs a directory tree into an
  archive and unpacks an archive into a directory. The module is available
  only on Unix. On other platforms, the crate has no `fs` module. On Apple
  platforms, the `fs` feature needs macOS 13, iOS 16, tvOS 16, or watchOS 9
  or later.

Enable `smol` or `tokio`. If you enable neither feature, the build fails. If
you enable both features, the crate uses the `tokio` traits.

## Usage rules

1. Use the entries in sequence. `TarReader` and the file entries that it
   yields share one archive stream. Read each file body to the end or drop
   it before you get the next entry. The types are `Send` and `Sync`, but do
   not read two of them at the same time.
2. Wrap a file or a socket in a `BufReader` before you give it to
   `TarReader`. The reader does not buffer the source. `TarWriter` writes
   the header, each part of the body (up to 16 KiB), and the padding of each
   entry as separate writes. To decrease the number of writes to a file or a
   socket, wrap it in a `BufWriter`.
3. Give `TarRegularFile::new` the exact size of the body. If the body is
   shorter, the write fails with `ErrorKind::UnexpectedEof`. If the body is
   longer, the writer does not read the remaining bytes.
4. Call `TarWriter::finish` or `SinkExt::close` after the last entry. These
   methods write the two zero blocks that end a tar archive and close the
   destination. `StreamExt::forward` closes the writer at the end of the
   stream.
5. All file entries of one `TarWriter` use the same body type `R`. To write
   bodies of different types, box them as
   `Box<dyn AsyncRead + Unpin + Send>`. If you write only entries without a
   body, such as directories, set `R` explicitly, for example with the
   `Empty` type of `smol::io` or `tokio::io`:
   `TarWriter::<_, Empty>::new(destination)`.
6. Paths, link targets, and user and group names are UTF-8 strings. Extended
   attribute names are UTF-8 strings. Extended attribute values are bytes.
7. Call `FsSink::finish` or `SinkExt::close` after the last entry. These
   methods set the mode and the modification time of the directories of the
   directory entries. With `with_owner` and `with_xattrs`, they also set the
   owner and the extended attributes of these directories.
   `StreamExt::forward` closes the sink at the end of the stream. If you drop
   the sink before, the directories that the sink created for directory
   entries keep mode 0o700.
8. In an archive for `FsSink`, put the target of each hard link before the
   link. `FsSink` creates a hard link only to a path that it created before
   from the same archive.
9. Filter the entries of `FsSource` before `FsStreamExt::into_tar`. Then each
   hard link in the archive points to an entry in the archive.
10. `FsSource` and `FsSink` do not read or write extended attributes by
    default. To keep the extended attributes, call `with_xattrs(true)` on
    both of them.
11. With the `tokio` feature, use `FsSource`, `FsFile`, `FsSink`, and the
    stream of `into_tar` inside a Tokio runtime context. Outside of a Tokio
    runtime context, they panic.

## Errors

All functions return `std::io::Error`. The reader uses these error kinds:

- `ErrorKind::InvalidData`: a header is malformed, has a bad checksum, has an
  unsupported entry type (for example, a sparse file), has a name that is not
  valid UTF-8, or is larger than a [limit](#limits).
- `ErrorKind::UnexpectedEof`: the source ends before the end of the archive
  or before the end of a file body.
- Errors from the source reader pass through without change.

`FsSource` and `FsFile` use these error kinds:

- `ErrorKind::InvalidInput`: the root has a kind that the source does not
  support, for example a socket. This error also occurs when the root is not
  a directory and the prefix is empty.
- `ErrorKind::InvalidData`: one of these conditions occurs:
  - A name, a symlink target, or an extended attribute name is not valid
    UTF-8.
  - The modification time of a node is out of range.
  - A node changes after the source read its metadata. For example, another
    directory replaces a directory, or a node of a different kind replaces
    a symlink. With `with_xattrs(true)`, a different node at the name of a
    node also gives this error. The message is "file changed".
  - When `FsFile` opens the file, the identity or the size of the file is
    different from the values that the source read. The message is "file
    changed". A file of up to 256 KiB with less data than that size gives
    the same error.
- Other errors from the system pass through. Each error has the path of the
  node. After an error, the stream ends.

`FsFile` compares the identity and the size only when it opens the file. If
a file larger than 256 KiB gets smaller after the open, the body ends early.
Then the reader of the body, for example `TarWriter`, gives an
`ErrorKind::UnexpectedEof` error.

`FsSink` uses these error kinds:

- `ErrorKind::InvalidData`: the entry path starts with `/`, has a `..`
  component, or has no components.
- `ErrorKind::AlreadyExists`: a directory has the name of an entry that is
  not a directory.
- `ErrorKind::NotFound`: the destination does not exist, or the target of a
  hard link is not available and the policy is `MissingLinkTarget::Error`.
- `ErrorKind::PermissionDenied`: the sink cannot set the owner, create a
  device, or write an extended attribute.
- `ErrorKind::Unsupported`: the platform cannot create a FIFO or a device.
  This error also occurs when the filesystem does not support extended
  attributes, or when the platform cannot write extended attributes on the
  kind of the node.
- `ErrorKind::UnexpectedEof`: the file body ends before the size of the
  entry.
- Other errors from the system or from the body reader pass through.

Each error message starts with the entry path, without its empty and `.`
components. An error of the entry path itself shows the path from the
archive. An error from the open of the destination starts with the
destination path. After an error, the sink stops. Each later call gives the
error "the sink stopped after an error". After `finish`, a write gives the
error "the sink is finished". These two errors have no path.

## Examples

Each example is a program in the [`examples`](examples) directory. The test
suite makes sure that the code below is equal to these programs. These
commands compile the programs:

- `cargo test`: `read`, `read_gz`, `write`, and `filter`.
- `cargo test --features fs`: the same programs, and `pack` and `unpack`.
- `cargo test --no-default-features --features tokio`: `read_tokio`.

### Read an archive with smol

Dependencies: `smol-tar`, `smol`.

<!-- example: examples/read.rs -->
```rust
//! List the entries of a tar archive with smol.
//!
//! Run: cargo run --example read -- ARCHIVE.tar

use smol::{
    fs::File,
    io::{copy, sink, BufReader},
    stream::StreamExt,
};
use smol_tar::{TarEntry, TarReader};

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

use async_compression::futures::bufread::GzipDecoder;
use smol::{
    fs::File,
    io::{copy, sink, BufReader},
    stream::StreamExt,
};
use smol_tar::{TarEntry, TarReader};

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
```

### Write an archive

Dependencies: `smol-tar`, `smol`.

<!-- example: examples/write.rs -->
```rust
//! Write files from disk into a new tar archive with smol.
//!
//! Run: cargo run --example write -- OUT.tar FILE...

use smol::fs::File;
use smol_tar::{TarDirectory, TarRegularFile, TarWriter};

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
            // The size must be equal to the number of bytes in the body.
            let entry = TarRegularFile::new(name, meta.len(), file)
                .with_mode(0o644)
                .with_mtime(meta.modified()?);
            tar.write(entry.into()).await?;
        }
        // Write the two zero blocks that end the archive and close the file.
        tar.finish().await
    })
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

use futures::{future, StreamExt, TryStreamExt};
use smol::{fs::File, io::BufReader};
use smol_tar::{TarReader, TarWriter};

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
```

### Pack a directory

`FsSource` is a `Stream` of the nodes in a directory tree. `into_tar`
converts the nodes to tar entries.

Dependencies: `smol-tar` with the `fs` feature, `smol`, `futures`.

<!-- example: examples/pack.rs -->
```rust
//! Pack a directory tree into a new tar archive with smol.
//!
//! Run: cargo run --example pack --features fs -- OUT.tar DIR

use futures::{future, StreamExt, TryStreamExt};
use smol::{fs::File, io::BufWriter};
use smol_tar::{
    fs::{FsSource, FsStreamExt},
    TarWriter,
};

fn main() -> std::io::Result<()> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let [output, dir] = args.as_slice() else {
        panic!("usage: pack OUT.tar DIR");
    };
    smol::block_on(async {
        // TarWriter writes each header and each part of a body separately.
        // A BufWriter decreases the number of writes to the file.
        let file = BufWriter::with_capacity(256 * 1024, File::create(output).await?);
        let mut out = TarWriter::new(file);
        // Filter the entries before into_tar(). Then each hard link in the
        // archive points to an entry that is in the archive.
        // forward() closes the writer at the end of the stream. The close
        // also flushes the BufWriter.
        FsSource::new(dir)
            .try_filter(|entry| future::ready(!entry.path().starts_with(".git/")))
            .into_tar()
            .forward(&mut out)
            .await
    })
}
```

### Unpack an archive into a directory

`FsSink` is a `Sink` of tar entries. It writes each entry to a directory.

Dependencies: `smol-tar` with the `fs` feature, `smol`, `futures`.

<!-- example: examples/unpack.rs -->
```rust
//! Unpack a tar archive into a directory with smol.
//!
//! Run: cargo run --example unpack --features fs -- IN.tar DIR

use futures::StreamExt;
use smol::{fs::File, io::BufReader};
use smol_tar::{fs::FsSink, TarReader};

fn main() -> std::io::Result<()> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let [input, dir] = args.as_slice() else {
        panic!("usage: unpack IN.tar DIR");
    };
    smol::block_on(async {
        // DIR must exist. If it does not exist, the sink gives a NotFound
        // error at the first entry. forward() calls finish() at the end of
        // the stream. finish() sets the mode and the modification time of
        // the directories.
        TarReader::new(BufReader::new(File::open(input).await?))
            .forward(FsSink::new(dir))
            .await
    })
}
```

## API summary

Refer to [docs.rs](https://docs.rs/smol-tar) for the full API.

`TarReader::new(source)`
: A `Stream` of `std::io::Result<TarEntry>`.

`TarWriter::new(destination)`
: A `Sink` of `TarEntry`. It also has the methods `write(entry)` and
  `finish()`.

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

`smol_tar::fs` (`fs` feature, Unix only)
: `FsSource::new(dir)` is a `Stream` of the nodes in a directory tree. Its
  options are `with_prefix` and `with_xattrs`. `FsStreamExt::into_tar()`
  converts the nodes to `TarEntry` values. The body of a regular file is an
  `FsFile`. `FsSink::new(dir)` is a `Sink` of `TarEntry` that writes the
  entries to a directory. It also has the methods `write(entry)` and
  `finish()`. Its options are `with_missing_link_target`, `with_owner`,
  `with_special_bits`, `with_xattrs`, and `with_unsupported_xattr`.

## Migrate from the tar crate

These are the smol-tar equivalents of the [`tar`](https://docs.rs/tar) and
[`async-tar`](https://docs.rs/async-tar) API:

- `Archive::new(r).entries()?` -- `TarReader::new(r)`, which is a `Stream`.
- `entry.path()?` -- `entry.path()`, which returns `&str`.
- `entry.header().entry_type()` -- a `match` on the `TarEntry` variant.
- `entry.header().size()?` -- `file.size()` on `TarEntry::File(file)`.
- `Read` or `AsyncRead` on an entry -- `AsyncRead` on `TarRegularFile`.
- `Builder::new(w)` -- `TarWriter::new(w)`.
- `builder.append_data(&mut header, path, body)` --
  `writer.write(TarRegularFile::new(path, size, body).into())`.
- `header.set_mode(m)`, `header.set_mtime(t)` -- `.with_mode(m)`,
  `.with_mtime(t)` on the entry.
- `builder.append_dir(path, src)` --
  `writer.write(TarDirectory::new(path).into())`.
- `builder.finish()` or `builder.into_inner()` -- `writer.finish()`.
- `archive.unpack(dir)` -- `TarReader::new(r).forward(FsSink::new(dir))`
  with the `fs` feature. `FsSink` needs an existing directory. The
  `unpack` method of the `tar` crate creates the directory.
- `builder.append_dir_all(path, dir)` --
  `writer.send_all(&mut FsSource::new(dir).with_prefix(path).into_tar())`
  with the `fs` feature and `SinkExt`. `FsSource` stores symlinks as
  symlinks and does not follow them, the same as
  `builder.follow_symlinks(false)`.
- `builder.append_path_with_name(path, name)` for a file --
  `writer.send_all(&mut FsSource::new(path).with_prefix(name).into_tar())`
  with the `fs` feature and `SinkExt`. For `builder.append_path(path)`, use
  `path` as the name. For a directory, smol-tar has no equivalent, because
  `FsSource` gives the directory and all of its contents.

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

## Limits

The reader returns an `ErrorKind::InvalidData` error for these archive
contents:

- a GNU long name or long link that is larger than 4096 bytes
- a PAX extension that is larger than 1 MiB
- more than 256 global PAX extensions

## Example CLI

The repository also includes a feature-gated example CLI in
[`examples/smol-tar-bin`](examples/smol-tar-bin/README.md). It creates,
extracts, and lists archives on disk.

## Benchmarks

The [`bench`](bench/README.md) crate compares smol-tar with async-tar,
astral-tokio-tar, and the synchronous `tar` crate. All archives are in
memory, so the results do not include disk I/O.

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

Refer to [`bench/README.md`](bench/README.md#results) for the result tables,
the allocation counts, and the test environment.

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
