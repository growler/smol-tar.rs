# smol-tar.rs
[![Tests](https://github.com/growler/smol-tar.rs/actions/workflows/test.yml/badge.svg)](https://github.com/growler/smol-tar.rs/actions/workflows/test.yml) ![Coverage](https://img.shields.io/endpoint?url=https://gist.githubusercontent.com/growler/afe6b562550658e432029a3999ff9a8d/raw/smol-tar.rs-coverage.json)

Async streaming tar archives for Rust. The crates work with smol and other
`futures-io` runtimes, and with tokio.

## Crates

[`smol-tar`](smol-tar/README.md)
: An async streaming tar reader and writer. `TarReader` is a `Stream` of tar
  entries from an `AsyncRead` source. `TarWriter` is a `Sink` of tar entries
  to an `AsyncWrite` destination. The crate does not access the filesystem.

[`smol-tar-fs`](smol-tar-fs/README.md)
: A filesystem source and sink for `smol-tar`. `FsSource` gives a `Stream` of
  tar entries from a directory tree. `FsSink` is a `Sink` that writes tar
  entries to a directory. Unix only.

[`picotar`](picotar/README.md)
: An example tar CLI. It creates, extracts, and lists archives on disk. It is
  not published.

All crates have the same version. The root `Cargo.toml` sets the version in
`[workspace.package]`.

## Other directories

- [`bench`](bench/README.md): CPU time and allocation benchmarks.
- `fuzz`: the cargo-fuzz target for the reader.
- `xtask`: generates the fuzz seed archives in `smol-tar/tests/data`.

These crates are not members of the workspace. Each one has its own
`Cargo.lock`.

## License

MIT
