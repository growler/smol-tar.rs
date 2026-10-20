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

## Results

Test environment: Intel Core i9-14900KF, Linux 6.18 x86_64, Rust 1.89.0.

The numbers in the lists below come from the smol build.

Time:

- With the large corpus (4 files of 64 MiB), all libraries have the same
  time within 5%. The memory copy sets the time.
- In `read_all` and `read_skip`, smol-tar has the lowest time for the pax,
  gnu-long, and mixed corpora in both builds. There is one exception: in
  the tokio build, `tar` has the lowest time in `read_all` with the
  gnu-long corpus. For pax, smol-tar reads the archive in 9.2 ms and `tar`
  reads it in 19.8 ms.
- In `read_all` with the small corpus, `tar` has the lowest time: 4.0 ms.
  smol-tar takes 4.6 ms.
- In `write` with the small corpus, smol-tar takes 2.1 ms, `tar` takes
  3.3 ms, and async-tar takes 4.3 ms.
- In `filter` with the small corpus, `tar` has the lowest time: 4.6 ms.
  smol-tar takes 5.3 ms. With the mixed corpus, `tar` takes 10.9 ms and
  smol-tar takes 11.1 ms.

Allocations:

- `read_all`: smol-tar makes 1 allocation for each ustar entry, 2 for each
  GNU long-name entry, and 4 for each PAX entry. The other libraries make 2
  for each ustar entry, 5 for each GNU long-name entry, and 5 or 6 for each
  PAX entry.
- `write` with the small corpus: smol-tar makes 1 allocation for each entry,
  `tar` makes 5, and async-tar and astral-tokio-tar make 6.
- `filter` with the small corpus: smol-tar allocates 206 KB in each
  iteration, `tar` allocates 400 KB, and async-tar and astral-tokio-tar
  allocate 41 MB.
- The peak live heap of smol-tar is less than 1.5 KB in `read_all` and
  approximately 66 KB in `read_skip` and `filter`. The 66 KB include the
  64 KiB skip buffer.

The tables show the median time in milliseconds for 100 samples.

smol build (`--features smol`):

| Benchmark | Corpus   | smol-tar | async-tar | tar   |
| --------- | -------- | -------: | --------: | ----: |
| read_all  | small    |     4.57 |      4.85 |  4.00 |
| read_all  | large    |    13.61 |     13.49 | 13.46 |
| read_all  | pax      |     9.25 |     14.76 | 19.79 |
| read_all  | gnu-long |     7.68 |     10.74 |  7.91 |
| read_all  | mixed    |     7.67 |     10.63 |  9.40 |
| read_skip | small    |     3.83 |      4.72 |  3.87 |
| read_skip | large    |    13.43 |     13.46 | 13.38 |
| read_skip | pax      |     8.55 |     14.33 | 19.39 |
| read_skip | gnu-long |     6.91 |     10.27 |  7.54 |
| read_skip | mixed    |     7.06 |      8.35 |  7.34 |
| write     | small    |     2.06 |      4.34 |  3.26 |
| write     | large    |    25.93 |     26.34 | 25.88 |
| write     | mixed    |     6.77 |      9.22 |  7.95 |
| filter    | small    |     5.30 |      7.12 |  4.63 |
| filter    | mixed    |    11.07 |     13.03 | 10.89 |

tokio build (`--no-default-features --features tokio`):

| Benchmark | Corpus   | smol-tar | async-tar | astral-tokio-tar | tar   |
| --------- | -------- | -------: | --------: | ---------------: | ----: |
| read_all  | small    |     4.96 |      5.22 |             5.58 |  4.06 |
| read_all  | large    |    13.80 |     13.58 |            13.63 | 13.55 |
| read_all  | pax      |     9.43 |     14.92 |            13.72 | 19.43 |
| read_all  | gnu-long |     8.17 |     10.78 |            11.93 |  7.88 |
| read_all  | mixed    |     8.26 |     10.67 |            11.64 |  9.43 |
| read_skip | small    |     4.00 |      4.75 |             5.30 |  3.87 |
| read_skip | large    |    13.80 |     13.57 |            13.61 | 13.98 |
| read_skip | pax      |     8.49 |     13.98 |            13.25 | 19.42 |
| read_skip | gnu-long |     7.18 |     10.05 |            11.34 |  8.44 |
| read_skip | mixed    |     7.27 |      8.36 |             9.07 |  7.68 |
| write     | small    |     2.04 |      4.30 |             4.13 |  3.21 |
| write     | large    |    25.93 |     26.29 |            25.51 | 25.39 |
| write     | mixed    |     6.72 |      9.12 |             9.07 |  8.00 |
| filter    | small    |     5.67 |      7.00 |             8.12 |  4.64 |
| filter    | mixed    |    11.49 |     12.95 |            13.95 | 10.84 |
