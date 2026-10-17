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
  gnu-long, and mixed corpora in both builds. For pax, smol-tar reads the
  archive in 9.9 ms and `tar` reads it in 21.0 ms.
- In `read_all` with the small corpus, `tar` has the lowest time: 3.9 ms.
  smol-tar takes 4.5 ms.
- In `write` with the small corpus, smol-tar takes 1.9 ms, `tar` takes
  3.2 ms, and async-tar takes 4.2 ms.
- In `filter` with the small corpus, `tar` has the lowest time: 4.6 ms.
  smol-tar takes 5.1 ms. With the mixed corpus, smol-tar and `tar` both take
  10.9 ms.

Allocations:

- `read_all`: smol-tar makes 1 allocation for each ustar entry, 2 for each
  GNU long-name entry, and 8 for each PAX entry. The other libraries make 2
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
| read_all  | small    |     4.46 |      4.89 |  3.91 |
| read_all  | large    |    13.82 |     13.78 | 13.80 |
| read_all  | pax      |     9.90 |     14.74 | 20.99 |
| read_all  | gnu-long |     7.44 |     10.84 |  7.81 |
| read_all  | mixed    |     7.52 |     10.55 |  9.36 |
| read_skip | small    |     3.61 |      4.56 |  3.73 |
| read_skip | large    |    13.73 |     13.77 | 13.77 |
| read_skip | pax      |     9.03 |     14.11 | 20.86 |
| read_skip | gnu-long |     6.64 |     10.15 |  7.67 |
| read_skip | mixed    |     6.79 |      8.21 |  7.41 |
| write     | small    |     1.85 |      4.22 |  3.19 |
| write     | large    |    25.78 |     25.17 | 25.47 |
| write     | mixed    |     6.55 |      9.17 |  8.01 |
| filter    | small    |     5.13 |      6.95 |  4.60 |
| filter    | mixed    |    10.86 |     12.65 | 10.93 |

tokio build (`--no-default-features --features tokio`):

| Benchmark | Corpus   | smol-tar | async-tar | astral-tokio-tar | tar   |
| --------- | -------- | -------: | --------: | ---------------: | ----: |
| read_all  | small    |     4.91 |      4.72 |             5.75 |  4.15 |
| read_all  | large    |    14.09 |     13.89 |            13.94 | 13.88 |
| read_all  | pax      |    10.17 |     15.12 |            14.65 | 19.64 |
| read_all  | gnu-long |     7.76 |     10.42 |            12.52 |  8.09 |
| read_all  | mixed    |     8.14 |     10.65 |            11.57 |  9.54 |
| read_skip | small    |     3.73 |      4.69 |             5.27 |  3.80 |
| read_skip | large    |    13.87 |     13.93 |            13.95 | 13.97 |
| read_skip | pax      |     9.04 |     14.62 |            14.33 | 19.22 |
| read_skip | gnu-long |     6.55 |     10.08 |            12.05 |  7.69 |
| read_skip | mixed    |     6.86 |      8.58 |             9.04 |  7.49 |
| write     | small    |     1.91 |      4.21 |             4.18 |  3.37 |
| write     | large    |    25.32 |     26.31 |            25.44 | 25.12 |
| write     | mixed    |     6.59 |      9.25 |             9.47 |  8.12 |
| filter    | small    |     5.99 |      6.91 |             8.06 |  4.62 |
| filter    | mixed    |    11.73 |     12.76 |            13.76 | 10.92 |
