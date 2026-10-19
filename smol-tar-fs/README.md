# smol-tar-fs
[![Docs](https://docs.rs/smol-tar-fs/badge.svg)](https://docs.rs/smol-tar-fs) [![Crates.io](https://img.shields.io/crates/v/smol-tar-fs.svg?maxAge=2592000)](https://crates.io/crates/smol-tar-fs)

smol-tar-fs gives a `Stream` of tar entries from a directory tree and a
`Sink` that writes a stream of tar entries to a directory. Use it with the
[`smol-tar`](https://crates.io/crates/smol-tar) reader and writer. The crate
works with smol and other `futures-io` runtimes, and with tokio.

- `FsSource` is a `Stream` of the nodes in a directory tree.
  `FsStreamExt::into_tar` converts the nodes to `TarEntry` values for
  `TarWriter`.
- `FsSink` is a `Sink` of `TarEntry` values, for example from `TarReader`.
  It writes each entry to a directory.

The source and the sink run the filesystem calls on the blocking thread pool
of the runtime.

## Platforms

The crate is available only on Unix. On other platforms, the crate is
empty. On Apple platforms, the crate needs macOS 13, iOS 16, tvOS 16, or
watchOS 9 or later.

## Installation

For smol and other `futures-io` runtimes:

```toml
[dependencies]
smol-tar = "0.1"
smol-tar-fs = "0.1"
```

For tokio:

```toml
[dependencies]
smol-tar = { version = "0.1", default-features = false, features = ["tokio"] }
smol-tar-fs = { version = "0.1", default-features = false, features = ["tokio"] }
```

The minimum supported Rust version is 1.85.

## Feature flags

- `smol` (default): use the `futures-io` traits `AsyncRead` and
  `AsyncWrite`, and the thread pool of the `blocking` crate. This feature
  enables the `smol` feature of `smol-tar`.
- `tokio`: use the `tokio::io` traits `AsyncRead` and `AsyncWrite`, and the
  blocking thread pool of Tokio. This feature enables the `tokio` feature of
  `smol-tar`.

Enable `smol` or `tokio`. If you enable neither feature, the build fails. If
you enable both features, the crate uses `tokio`. Enable the same runtime
feature for `smol-tar` and `smol-tar-fs`. If `smol-tar` uses `tokio` and
`smol-tar-fs` uses `smol`, the build fails.

## Usage rules

1. Call `FsSink::finish` or `SinkExt::close` after the last entry. These
   methods set the mode and the modification time of the directories of the
   directory entries. With `with_owner` and `with_xattrs`, they also set the
   owner and the extended attributes of these directories.
   `StreamExt::forward` closes the sink at the end of the stream. If you drop
   the sink before, the directories that the sink created for directory
   entries keep mode 0o700.
2. In an archive for `FsSink`, put the target of each hard link before the
   link. `FsSink` creates a hard link only to a path that it created before
   from the same archive.
3. Filter the entries of `FsSource` before `FsStreamExt::into_tar`. Then each
   hard link in the archive points to an entry in the archive.
4. `FsSource` and `FsSink` do not read or write extended attributes by
   default. To keep the extended attributes, call `with_xattrs(true)` on
   both of them.
5. With the `tokio` feature, use `FsSource`, `FsFile`, `FsSink`, and the
   stream of `into_tar` inside a Tokio runtime context. Outside of a Tokio
   runtime context, they panic.

## Errors

All functions return `std::io::Error`.

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
- `ErrorKind::InvalidInput`: the entry path or the link target has a NUL
  byte, or the platform cannot encode the device numbers or the
  modification time.
- `ErrorKind::AlreadyExists`: a directory has the name of an entry that is
  not a directory.
- `ErrorKind::NotFound`: the destination does not exist, or the target of a
  hard link is not available and the policy is `MissingLinkTarget::Error`.
- `ErrorKind::PermissionDenied`: the sink cannot set the owner, create a
  device, or write an extended attribute.
- `ErrorKind::Unsupported`: the filesystem does not support extended
  attributes, or the platform cannot write extended attributes on the kind
  of the node.
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
suite makes sure that the code below is equal to these programs. `cargo test`
compiles the programs.

### Pack a directory

`FsSource` is a `Stream` of the nodes in a directory tree. `into_tar`
converts the nodes to tar entries.

Dependencies: `smol-tar`, `smol-tar-fs`, `smol`, `futures`.

<!-- example: examples/pack.rs -->
```rust
//! Pack a directory tree into a new tar archive with smol.
//!
//! Run: cargo run --example pack -- OUT.tar DIR

use futures::{future, StreamExt, TryStreamExt};
use smol::{fs::File, io::BufWriter};
use smol_tar::TarWriter;
use smol_tar_fs::{FsSource, FsStreamExt};

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

Dependencies: `smol-tar`, `smol-tar-fs`, `smol`, `futures`.

<!-- example: examples/unpack.rs -->
```rust
//! Unpack a tar archive into a directory with smol.
//!
//! Run: cargo run --example unpack -- IN.tar DIR

use futures::StreamExt;
use smol::{fs::File, io::BufReader};
use smol_tar::TarReader;
use smol_tar_fs::FsSink;

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

Refer to [docs.rs](https://docs.rs/smol-tar-fs) for the full API.

`FsSource::new(dir)`
: A `Stream` of the nodes in a directory tree. Its options are
  `with_prefix` and `with_xattrs`.

`FsStreamExt::into_tar()`
: Converts the nodes to `TarEntry` values. The body of a regular file is an
  `FsFile`.

`FsSink::new(dir)`
: A `Sink` of `TarEntry` that writes the entries to a directory. It also
  has the methods `write(entry)` and `finish()`. Its options are
  `with_missing_link_target`, `with_owner`, `with_special_bits`,
  `with_xattrs`, and `with_unsupported_xattr`.

## License

MIT
