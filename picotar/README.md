# picotar

`picotar` is a small tar-like example CLI built on top of `smol-tar` and
`smol-tar-fs`.

## Runtime selection

The example defaults to the `smol` runtime:

```sh
cargo run -p picotar -- --help
```

Run the same example with Tokio:

```sh
cargo run -p picotar --no-default-features --features tokio -- --help
```

## Usage

```text
picotar -c -f ARCHIVE [-C DIR] [OPTIONS] PATH...
picotar -x -f ARCHIVE [-C DIR] [OPTIONS] [PATH...]
picotar -t -f ARCHIVE [OPTIONS]
```

- `-c` creates an archive from one or more input paths.
- `-x` extracts the whole archive, or only the entries at or below each
  `PATH`.
- `-t` lists archive member paths.
- `-C DIR` changes the source directory in create mode or the destination
  directory in extract mode. Extract mode creates `DIR` if it does not exist.
- `-f -` reads the archive from stdin or writes it to stdout.

## Options

All modes:

`-z`, `--gzip`
: Use gzip compression.

`-J`, `--xz`, `--xzip`
: Use xz compression.

`--zstd`
: Use zstd compression.

Create and extract modes:

`-v`, `--verbose`
: Write the path of each entry to stderr. In extract mode, picotar writes
  the path after `--strip-components`. It does not write the path of an
  entry that `--strip-components` removes, or of a device entry without
  `--devices`.

`--exclude PATH`, `--exclude=PATH`
: Skip the entries at or below `PATH`. You can give this option more than
  one time. Create mode does not read or walk an excluded path.

`--xattrs`
: Read the extended attributes in create mode. Write them in extract mode.
  On Linux, extract mode writes only the names that start with `user.`.

Extract mode:

`-p`, `--preserve-permissions`
: Keep the setuid, setgid and sticky bits.

`--same-owner`
: Set the owner of each node to the uid and gid of the entry. This usually
  needs root.

`--devices`
: Create character and block device nodes. This usually needs root.

`--skip-missing-links`
: Do not create a hard link whose target is not available, and continue.

`--strip-components N`, `--strip-components=N`
: Remove the first `N` components from each entry path and from each hard
  link target. A `.` component counts. picotar skips an entry that has `N`
  or fewer components. A hard link whose target has `N` or fewer components
  stops extraction with an error, or picotar skips it with
  `--skip-missing-links`. A path that starts with `/` or that has a `..`
  component does not change. Thus extract mode rejects it.

## Examples

List the contents of an archive:

```sh
cargo run -p picotar -- -tf archive.tar
```

Create an archive from `tree` beneath `src`:

```sh
cargo run -p picotar -- -cf archive.tar -C src tree
```

Extract an archive into `out`:

```sh
cargo run -p picotar -- -xf archive.tar -C out
```

Extract a single subtree:

```sh
cargo run -p picotar -- -xf archive.tar -C out tree/nested
```

Create a zstd archive from two directories, without `.git`:

```sh
cargo run -p picotar -- -cf archive.tar.zst --zstd --exclude src/.git src docs
```

Extract a gzip archive without its top directory:

```sh
cargo run -p picotar -- -xzf archive.tar.gz -C out --strip-components 1
```

Stream via stdout/stdin:

```sh
cargo run -p picotar -- -cf - -C src tree > archive.tar
cargo run -p picotar -- -xf - -C out < archive.tar
```

## Notes

- Create mode uses `FsSource` and extract mode uses `FsSink` from
  `smol-tar-fs`.
- Input paths, archive member paths, and symlink targets are UTF-8 only.
- Path filters and `--exclude` ignore empty and `.` components. Thus
  `tree/file` also selects `./tree/file` and `tree/./file`.
- Create mode writes a regular archive file to a temporary file in the same
  directory. At the end, it renames the temporary file to the archive path.
  The new archive has the permissions of the old archive. If an error
  occurs, picotar removes the temporary file, and the old archive does not
  change. For an archive path that is not a regular file, for example a
  FIFO, picotar writes to that path directly.
- Create mode does not put the archive file, or the old archive at the same
  path, into the archive.
- Create mode archives directories, regular files, symlinks, hard links,
  FIFOs, and devices. It skips sockets below the input paths. A hard link
  between two input paths becomes a hard link entry.
- Extract mode rejects member paths that start with `/` or that have a `..`
  component. It stores symlink targets without change and does not follow
  them.
- Extract mode creates FIFO entries. It skips device entries without
  `--devices`.
- In extract mode, the target of a hard link must come before the link in the
  archive. If the target is not available, extraction stops with an error.
  `--skip-missing-links` changes this.
- picotar does not detect the compression. Give the same compression option
  for each mode.
- The decompression reads all members of a gzip file, all streams of an xz
  file, and all frames of a zstd file. For example, it reads the output of
  pigz and pzstd.
