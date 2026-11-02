# TLA+ specifications for smol-tar

This directory contains TLA+ models of the `smol-tar` crate (`../src/lib.rs`).
TLC checks each model. Each model has one or more main configurations that
must pass. Each finding has a separate configuration. That configuration
must fail with a counterexample.

The comments in each `.tla` file refer to `lib.rs` by line number. The line
numbers agree with `lib.rs` at commit `5f20010`.

## Modules

TarReader.tla
: The reader state machine: `Header`, `Extension`, `Entry`, `SkipEntry`,
  `Padding`, `Eof`, `Eoff`, `Done` and `Failed`. The model keeps `pos` and `nxt` as
  absolute byte offsets, the same as the code. The source gives short reads
  of any length and can end at any byte. The consumer reads, fills, consumes
  part of a body, drops a file, and polls again after an error.

CoreHandoff.tla
: The transfer of the reader core between `TarReader`, `TarRegularFileReader`
  and the shared slot. The model includes waker registration, a cancelled
  `next()`, a file that is dropped before the end of its body, and a dropped
  `TarReader`. Each critical section of the slot mutex is one atomic step.
  A wake after unlock is a separate step.

TarWriter.tla
: The writer sink: `poll_ready`, `start_send`, `poll_drain`, `poll_flush` and
  `poll_close`, with the writer states `Open`, `Marker` and `Closed`. The
  underlying writer gives short writes, `Pending`, zero-byte writes and
  errors. File bodies can be exact, short or long. The writer fills the rest
  of a short body with zero bytes and then returns `UnexpectedEof` one time.
  The caller can drop a future at each `Pending` and can retry after an
  error.

RoundTrip.tla
: The writer output is the reader input. The model encodes each entry as the
  writer does (PAX header, ustar header, body, padding, end marker). The model
  decodes it with the reader state machine. The path encoding follows
  `path_split_point`, `set_path` and `ustar_path_name`.

## Scale

The models use small constants so that TLC can examine all states:

- A block is 4 bytes, not 512.
- In RoundTrip, `NAME_LEN` is 4 and `PREFIX_LEN` is 6, not 100 and 155.
- The number of entries, files, wakers, faults and cancels has a small limit.

The comparisons in the models are the same as the comparisons in the code.
Each `.tla` file lists what it does not model.

## Configurations

Main configurations (expected result: no error):

- `TarReader.cfg`
- `TarReader_Ext.cfg` -- extension items at the block-size limits
- `TarReader_PaxBadRepoll.cfg` -- a bad PAX payload larger than one block
- `TarReader_LateFieldError.cfg` -- a field error after `nxt` moves
- `CoreHandoff.cfg`
- `TarWriter.cfg`
- `TarWriter_SendAfterClose.cfg` -- `write()` after `finish()`
- `TarWriter_SendAfterMarker.cfg` -- `write()` after a dropped `finish()`
  that put the end marker in the buffer
- `TarWriter_DoubleClose.cfg` -- `finish()` two times closes the underlying
  writer one time
- `RoundTrip.cfg`
- `RoundTrip_AbsPath101.cfg` -- a path of "/" and `NAME_LEN` bytes

Finding configurations (expected result: a violation):

- `TarWriter_CommitAmbiguity.cfg` -- `FinalOutput`

## Run the checks

The tools come from the `tlaplus` package (TLC 2.18). Put the configuration
in this directory, next to the `.tla` file. Put the TLC metadata directory
outside the repository:

```sh
cd smol-tar/spec
tlc -workers auto -noGenerateSpecTE -cleanup \
    -metadir "${TMPDIR:-/tmp}/tlc/TarReader" \
    -config "$PWD/TarReader.cfg" "$PWD/TarReader.tla"
```

Each run takes between less than 1 second and about 9 minutes.
`TarReader.cfg` and `RoundTrip.cfg` take the most time.

## Findings

The finding configuration shows this behavior of the code.

TarWriter_CommitAmbiguity
: `write()` calls `poll_ready`, `start_send` and `poll_flush` in sequence
  (`lib.rs:4130-4147`). After `start_send` accepts the entry, `poll_flush` can
  fail or the caller can drop the future. The caller cannot find out whether
  the entry was accepted. If the caller tries the entry again, the archive
  contains the entry two times. The `write()` documentation describes this
  behavior. A caller that must know the commit point calls `poll_ready` and
  `start_send`, or `SinkExt::feed`, and gets the result of `start_send`.
