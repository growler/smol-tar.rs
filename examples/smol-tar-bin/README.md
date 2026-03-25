# smol-tar-bin

`smol-tar-bin` is a small tar-like example CLI built on top of `smol-tar`.

## Runtime selection

The example defaults to the `smol` runtime:

```sh
cargo run --manifest-path examples/smol-tar-bin/Cargo.toml -- --help
```

Run the same example with Tokio:

```sh
cargo run --manifest-path examples/smol-tar-bin/Cargo.toml --no-default-features --features tokio -- --help
```

## Usage

```text
smol-tar-bin -c -f ARCHIVE [-C DIR] PATH
smol-tar-bin -x -f ARCHIVE [-C DIR] [PATH]
smol-tar-bin -t -f ARCHIVE
```

- `-c` creates an archive from exactly one input path.
- `-x` extracts the whole archive, or only entries under `PATH` when a filter
  is provided.
- `-t` lists archive member paths.
- `-C DIR` changes the source directory in create mode or the destination
  directory in extract mode.
- `-f -` reads the archive from stdin or writes it to stdout.

## Examples

List the contents of an archive:

```sh
cargo run --manifest-path examples/smol-tar-bin/Cargo.toml -- -tf archive.tar
```

Create an archive from `tree` beneath `src`:

```sh
cargo run --manifest-path examples/smol-tar-bin/Cargo.toml -- -cf archive.tar -C src tree
```

Extract an archive into `out`:

```sh
cargo run --manifest-path examples/smol-tar-bin/Cargo.toml -- -xf archive.tar -C out
```

Extract a single subtree:

```sh
cargo run --manifest-path examples/smol-tar-bin/Cargo.toml -- -xf archive.tar -C out tree/nested
```

Stream via stdout/stdin:

```sh
cargo run --manifest-path examples/smol-tar-bin/Cargo.toml -- -cf - -C src tree > archive.tar
cargo run --manifest-path examples/smol-tar-bin/Cargo.toml -- -xf - -C out < archive.tar
```

## Notes

- Input paths, archive member paths, and symlink targets are UTF-8 only.
- Extract mode sanitizes member paths and rejects absolute or parent-escaping
  paths.
- FIFO and device entries can be read by the library, but this example CLI does
  not extract them.
