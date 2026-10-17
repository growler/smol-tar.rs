# smol-tar-bench

CPU time and allocation benchmarks for smol-tar, with a comparison to other
Rust tar libraries. The crate also has a heap usage test for the smol-tar
reader.

The crate is not part of the smol-tar package. It has its own `Cargo.lock`.

## Run the benchmarks

Run the smol build:

```sh
cargo bench --manifest-path bench/Cargo.toml
```

Run the tokio build:

```sh
cargo bench --manifest-path bench/Cargo.toml --no-default-features --features tokio
```

Enable exactly one of the `smol` and `tokio` features. async-tar does not
compile with both runtime features.

To run only some benchmarks, give a name pattern after `--`:

```sh
cargo bench --manifest-path bench/Cargo.toml -- read_all
```

## Run the tests

```sh
cargo test --release --manifest-path bench/Cargo.toml
cargo test --release --manifest-path bench/Cargo.toml --no-default-features --features tokio
```

`tests/corpus.rs` makes sure that every library reads, writes and filters
every corpus with the same entry count and the same body byte count.

`tests/alloc.rs` uses dhat to examine the smol-tar reader. It makes sure that:

- The peak heap does not change with the file size (1 MiB and 64 MiB).
- The peak heap does not change with the entry count (1,000 to 3,000).
- The allocation count increases linearly with the entry count.

The test prints the allocations per entry and the peak heap for each format.

## Libraries

The smol build compares:

- smol-tar with the `smol` feature
- async-tar with the `runtime-async-std` feature (futures-io traits)
- the `tar` crate (synchronous)

The tokio build compares:

- smol-tar with the `tokio` feature
- async-tar with the `runtime-tokio` feature
- astral-tokio-tar
- the `tar` crate (synchronous)

## Corpora

All archives are in memory. The results do not include disk I/O.

small
: 10,000 files of 1 KiB, ustar.

large
: 4 files of 64 MiB, ustar.

pax
: 10,000 files of 1 KiB with 123-byte paths and two extended attributes each,
  PAX. smol-tar writes this archive.

gnu-long
: 10,000 files of 1 KiB with 123-byte paths, GNU long-name headers.

mixed
: 100 directories, 9,000 files of 0 to 16 KiB and 1,000 symbolic links, ustar.

The `tar` crate writes all archives except pax.

## Benchmarks

read_all
: Read every entry, and read every file body into a 64 KiB buffer.

read_skip
: Read every entry and skip every file body.

write
: Write the entries of the small, large and mixed corpora into a `Vec<u8>`.
  The other corpora need different extension headers in each library, so
  they are not in this benchmark.

filter
: Copy every second entry of the small and mixed corpora into a `Vec<u8>`.

## Measurement rules

- Every library uses the same 64 KiB read buffer and the same output
  `Vec<u8>`. The benchmark makes them before the timed loop.
- The benchmark makes the async runtime before the timed loop. The smol
  build uses `smol::future::block_on`, which does not start the I/O reactor.
  The tokio build uses a `current_thread` runtime.
- The astral-tokio-tar writer uses `Builder::new_non_terminated` and
  `Builder::finish`. `Builder::new` spawns a task for each archive.

## Read the results

divan shows the time and the throughput in bytes and in entries. The
`alloc` rows show the allocation count and the allocated bytes for one
iteration. Divide the allocation count by the entry count of the corpus to
get the allocations per entry.
