# smol-tar-fs
[![Tests](https://github.com/growler/smol-tar.rs/actions/workflows/test.yml/badge.svg)](https://github.com/growler/smol-tar.rs/actions/workflows/test.yml) ![Coverage](https://img.shields.io/endpoint?url=https://gist.githubusercontent.com/growler/afe6b562550658e432029a3999ff9a8d/raw/smol-tar.rs-coverage.json) [![Docs](https://docs.rs/smol-tar-fs/badge.svg)](https://docs.rs/smol-tar-fs) [![Crates.io](https://img.shields.io/crates/v/smol-tar-fs.svg?maxAge=2592000)](https://crates.io/crates/smol-tar-fs)

smol-tar-fs gives a `Stream` of tar entries from a directory tree and a
`Sink` that writes a stream of tar entries to a directory. Use it with the
[`smol-tar`](https://crates.io/crates/smol-tar) reader and writer. The crate
works with smol and other `futures-io` runtimes, and with tokio.

- `FsSource` is a `Stream` of the nodes in a directory tree.
  `FsStreamExt::into_tar` converts the nodes to `TarEntry` values for
  `TarWriter`.
- `FsSink` is a `Sink` of `TarEntry` values, for example from `TarReader`.
  It writes each entry to a directory. `FsSink::extract` writes the entries
  of a stream.

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
smol-tar = "0.2"
smol-tar-fs = "0.2"
```

For tokio:

```toml
[dependencies]
smol-tar = { version = "0.2", default-features = false, features = ["tokio"] }
smol-tar-fs = { version = "0.2", default-features = false, features = ["tokio"] }
```

The minimum supported Rust version is 1.85.

## Feature flags

- `smol` (default): use the `futures-io` traits `AsyncRead`, `AsyncBufRead`
  and `AsyncWrite`, and the thread pool of the `blocking` crate. This feature
  enables the `smol` feature of `smol-tar`.
- `tokio`: use the `tokio::io` traits `AsyncRead`, `AsyncBufRead` and
  `AsyncWrite`, and the blocking thread pool of Tokio. This feature enables
  the `tokio` feature of `smol-tar`.

Enable `smol` or `tokio`. If you enable neither feature, the build fails. If
you enable both features, the crate uses `tokio`. Enable the same runtime
feature for `smol-tar` and `smol-tar-fs`. If `smol-tar` uses `tokio` and
`smol-tar-fs` uses `smol`, the build fails.

## Usage rules

1. Call `FsSink::finish` or `SinkExt::close` after the last entry. These
   methods write the entries of the batch and set the mode and the
   modification time of the directories of the directory entries. With
   `with_owner` and `with_xattrs`, they also set the owner and the extended
   attributes of these directories.
   `FsSink::extract` and `StreamExt::forward` close the sink at the end of
   the stream. A drop of the `FsSink::extract` future, for example at a
   timeout, also drops the sink. If you drop the sink before the end of the
   finish, these rules apply. If the sink has no job on the blocking thread
   pool, the directories that the sink created for directory entries keep
   mode 0o700. The entries of the batch are not written, and no error comes
   back. A job that runs on the blocking thread pool continues after the
   drop. Thus a job that writes the batch or runs the finish can write the
   entries of the batch and set the metadata of the directories after the
   drop returns. With the `smol` feature, the drop cancels a job that waits
   for a thread. With the `tokio` feature, a job that waits for a thread
   runs after the drop. With `with_mode(ModePolicy::Skip)` and
   `with_mtime(false)`, the sink sets the owner and the extended attributes
   of a directory when it creates the directory.
2. In an archive for `FsSink`, put the target of each hard link before the
   link. `FsSink` creates a hard link only to a node that it created before
   from the same archive. The node at the target path must have the
   identity that `FsSink` stored when it created the node.
3. Filter the entries of `FsSource` before `FsStreamExt::into_tar`. Then each
   hard link in the archive points to an entry in the archive.
4. `FsSource` and `FsSink` do not read or write extended attributes by
   default. To keep the extended attributes, call `with_xattrs(true)` on
   both of them. On Apple platforms, the default filter of `FsSink` accepts
   all names. On Linux and Android, it accepts only the names that start
   with `user.`. To write other names, call `with_xattr_filter`. On other
   Unix platforms, `FsSink` cannot write extended attributes.
   `with_unsupported_xattr` sets the action for them.
5. By default, `FsSink` skips character and block device entries and
   creates no device nodes. To create device nodes, call
   `with_device_nodes(DeviceNodes::Create)`. To stop at a device entry, use
   `DeviceNodes::Error`. A device entry with a path that is not valid gives
   an `ErrorKind::InvalidData` error with each policy.
6. By default, `FsSink` sets the permission bits and the modification time of
   each node from the entry. The umask of the process does not change the
   mode of an entry. A missing parent directory gets mode 0o777 less the
   umask with each mode policy. `with_mode(ModePolicy::Mask(mask))` removes
   the bits of `mask` from the mode of each entry and from the mode of a
   missing parent directory.
   `with_mode(ModePolicy::Skip)` does not set the
   mode: a new node gets the default mode less the umask, for example 0o644
   for a file with the umask 0o022. A device node gets 0o600 less the umask.
   `with_mtime(false)` does not set the modification time. With
   `ModePolicy::Skip` and `with_mtime(false)`, `FsSink` makes fewer system
   calls for each node.
7. With the `tokio` feature, use `FsSource`, `FsFile`, `FsSink`, and the
   stream of `into_tar` inside a Tokio runtime context. Outside of a Tokio
   runtime context, they panic.
8. Use `FsSink::extract` to write a stream of entries. The sink puts regular
   files of up to 256 KiB and directory entries in batches. A batch has up
   to 64 entries and up to 1 MiB of data. One job on the blocking thread
   pool writes each batch. `forward` flushes the sink each time that the
   stream is not ready. With a `TarReader` stream, `forward` flushes the
   sink after each regular file. Thus each batch has at most one file.
   `FsSink::write` can return before the entry is on disk. `start_send` and
   `feed` return before the sink writes the entry. With a `TarReader`
   stream, a `next` after the `feed` of a regular file does not return until
   the sink runs the job. The sink keeps the body, and the body keeps the
   source of the reader. For full batches, use `FsSink::write` or
   `FsSink::extract`. `FsSink::write` does not flush the batch. Thus the
   batches fill up to the limits. You can also use `send` or `forward`, or
   call `flush` after `feed`. A flush after each entry writes each entry in
   its own job. Call `FsSink::finish` or `SinkExt::close` one time, after
   the last entry. `finish` and `close` also write the batch. A `flush`
   before them is not necessary. If you stop before the end of the archive,
   call `FsSink::flush` to write the entries of the batch. `finish` also
   sets the metadata of the directories. After an error from the stream,
   `forward` does not flush or close the sink. To write the last entry and
   the batch after such an error, give `&mut sink` to `forward`, then call
   `flush` or `close`. If you drop the sink while the entries wait in the
   batch, these entries are not written, and no error comes back. A job that
   writes the batch continues after the drop. See rule 1. An error of an
   entry in the batch comes from a later call, and the error has the path of
   that entry.
9. `FsSink` follows a symlink in a parent component of an entry path if
   the symlink target stays inside the destination. For example, with
   `lib -> usr/lib` in the destination, the entry `lib/f` goes to
   `usr/lib/f`. A parent symlink that leads outside the destination gives
   an `ErrorKind::PermissionDenied` error. An absolute parent symlink also
   gives this error when its target is in the destination. These rules
   apply to the symlinks of the archive and to the symlinks that are in the
   destination before the extraction.

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
    changed".
- Other errors from the system pass through. Each error has the path of the
  node. After an error, the stream ends.

`FsFile` compares the identity and the size only when it opens the file. If
a file gets smaller after the open, the read of the body gives an
`ErrorKind::InvalidData` error "file changed". Errors from the reads after
the open do not have the path of the node.

`FsSink` uses these error kinds:

- `ErrorKind::InvalidData`: the entry path starts with `/`, has a `..`
  component, or has no components. At `finish`, the path of a directory
  entry leads to another directory. The message is "PATH: file changed".
- `ErrorKind::InvalidInput`: the entry path or the link target has a NUL
  byte, or the platform cannot encode the device numbers or the
  modification time.
- `ErrorKind::AlreadyExists`: a directory has the name of an entry that is
  not a directory.
- `ErrorKind::NotFound`: the destination does not exist, or the target of a
  hard link is not available and the policy is `MissingLinkTarget::Error`.
  For example, the target is not in the archive, the target path leads to
  another node or to no node, or the target path has a symlink loop. The
  message is "PATH: link target TARGET is not in the archive".
- `ErrorKind::PermissionDenied`: a parent symlink of the entry path or of
  a hard link target leads outside the destination or is absolute. The
  sink also gives this error if it cannot create a device or write an
  extended attribute. A device entry with the policy
  `DeviceNodes::Error` also gives this error. The message is "PATH: device
  entries are not allowed".
- `ErrorKind::Unsupported`: the filesystem does not support extended
  attributes, the platform cannot write extended attributes, or the
  platform cannot write extended attributes on the kind of the node.
- `ErrorKind::UnexpectedEof`: the file body ends before the size of the
  entry.
- The sink cannot set the owner. The kind comes from the system error.
  Examples are `ErrorKind::PermissionDenied` for a user without privileges
  and, on Linux, `ErrorKind::InvalidInput` for an id that the user
  namespace does not map. The message is "PATH: cannot set the owner to
  uid UID, gid GID: ERROR". The handler of `with_owner_error` can let the
  sink continue.
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

The programs use smol. With `--features tokio`, the default `smol` feature
stays on. Then the programs compile to a `main` that writes a message to
stderr and exits with status 1.

### Pack a directory

`FsSource` is a `Stream` of the nodes in a directory tree. `into_tar`
converts the nodes to tar entries.

Dependencies: `smol-tar`, `smol-tar-fs`, `smol`, `futures`.

<!-- example: examples/pack.rs -->
```rust
//! Pack a directory tree into a new tar archive with smol.
//!
//! Run: cargo run --example pack -- OUT.tar DIR

// With the tokio feature, smol-tar-fs uses the tokio traits, and the smol
// main below cannot compile. In that case, a short main writes a message to
// stderr and exits with status 1.
#![cfg_attr(feature = "tokio", allow(unused_imports))]

use futures::{future, StreamExt, TryStreamExt};
use smol::{fs::File, io::BufWriter};
use smol_tar::TarWriter;
use smol_tar_fs::{FsSource, FsStreamExt};

#[cfg(not(feature = "tokio"))]
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

#[cfg(feature = "tokio")]
fn main() {
    eprintln!("this example needs the smol feature without the tokio feature");
    std::process::exit(1);
}
```

### Unpack an archive into a directory

`FsSink` writes each tar entry to a directory. `FsSink::extract` writes
the entries of a stream, for example a `TarReader`.

Dependencies: `smol-tar`, `smol-tar-fs`, `smol`.

<!-- example: examples/unpack.rs -->
```rust
//! Unpack a tar archive into a directory with smol.
//!
//! Run: cargo run --example unpack -- IN.tar DIR

// With the tokio feature, smol-tar-fs uses the tokio traits, and the smol
// main below cannot compile. In that case, a short main writes a message to
// stderr and exits with status 1.
#![cfg_attr(feature = "tokio", allow(unused_imports))]

use smol::{fs::File, io::BufReader};
use smol_tar::TarReader;
use smol_tar_fs::FsSink;

#[cfg(not(feature = "tokio"))]
fn main() -> std::io::Result<()> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let [input, dir] = args.as_slice() else {
        panic!("usage: unpack IN.tar DIR");
    };
    smol::block_on(async {
        // DIR must exist. If it does not exist, the sink gives a NotFound
        // error at the first entry that it writes. extract() calls finish()
        // at the end of the stream. finish() writes the files of the batch
        // and sets the mode and the modification time of the directories.
        FsSink::new(dir)
            .extract(TarReader::new(BufReader::new(File::open(input).await?)))
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

Refer to [docs.rs](https://docs.rs/smol-tar-fs) for the full API.

`FsSource::new(dir)`
: A `Stream` of the nodes in a directory tree. Its options are
  `with_prefix`, `with_xattrs`, and `with_filter`.

`FsStreamExt::into_tar()`
: Converts the nodes to `TarEntry` values. The body of a regular file is an
  `FsFile`.

`FsSink::new(dir)`
: A `Sink` of `TarEntry` that writes the entries to a directory. It also
  has the methods `extract(stream)`, `write(entry)`, `flush()`, and
  `finish()`. After the last `write`, call `finish`. `extract` writes small
  files and directories in batches. With a `TarReader` stream and `forward`,
  each batch has at most one file. Its options are
  `with_missing_link_target`, `with_device_nodes`, `with_owner`,
  `with_owner_error`, `with_mode`, `with_special_bits`, `with_mtime`,
  `with_xattrs`, `with_unsupported_xattr`, and `with_xattr_filter`. With `with_owner_error`, the sink can continue
  after an owner change that the system refuses. Then the node keeps the
  user of the process as owner, and a node that is not a directory gets
  no setuid and setgid bits from the entry. The sink also does not write
  the `security.capability` attribute on this node.

## License

MIT
