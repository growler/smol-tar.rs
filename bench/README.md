# smol-tar-bench

Benchmarks for smol-tar, with a comparison to other Rust tar libraries. The
crate has two benchmark targets:

- `benches/tar.rs` measures the CPU time and the allocations of in-memory
  archive operations.
- `benches/fs.rs` measures the wall time of directory encoding and archive
  extraction on disk.

The crate also has a heap usage test for the smol-tar reader.

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

To run one benchmark target, use `--bench tar` or `--bench fs`. To run only
some benchmarks, give a name pattern after `--`:

```sh
cargo bench --manifest-path bench/Cargo.toml --bench tar -- read_all
```

The disk benchmarks write 73.5 MB of file data for each extract sample of
the mixed tree. On the test machine, one run of the disk benchmarks in the
tokio build takes approximately 10 minutes.

## Run the tests

```sh
cargo test --release --manifest-path bench/Cargo.toml
cargo test --release --manifest-path bench/Cargo.toml --no-default-features --features tokio
```

`tests/corpus.rs` makes sure that every library reads, writes and filters
every corpus with the same entry count and the same body byte count.

`tests/fs.rs` makes sure of these results for every tree:

- The archive of each encoder has the same entry count and the same body
  byte count.
- Each extractor makes the same number of directories, files and symbolic
  links, with the same file byte count. The test examines the default and
  the minimal settings.

`tests/alloc.rs` uses dhat to examine the smol-tar reader. It makes sure that:

- The peak heap does not change with the file size (1 MiB and 64 MiB).
- The peak heap does not change with the entry count (1,000 to 3,000).
- The allocation count increases linearly with the entry count.

The test prints the allocations per entry and the peak heap for each format.

## Libraries

The smol build compares:

- smol-tar with the `smol` feature, and smol-tar-fs for the disk benchmarks
- async-tar with the `runtime-async-std` feature (futures-io traits)
- the `tar` crate (synchronous)

The tokio build compares:

- smol-tar with the `tokio` feature, and smol-tar-fs for the disk benchmarks
- async-tar with the `runtime-tokio` feature
- astral-tokio-tar
- tar-codec
- the `tar` crate (synchronous)

tar-codec supports only tokio.

## In-memory benchmarks

### Corpora

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

### Benchmarks

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

### Measurement rules

- Every library uses the same 64 KiB read buffer and the same output
  `Vec<u8>`. The benchmark makes them before the timed loop.
- The benchmark makes the async runtime before the timed loop. The smol
  build uses `smol::future::block_on`, which does not start the I/O reactor.
  The tokio build uses a `current_thread` runtime.
- The astral-tokio-tar writer uses `Builder::new_non_terminated` and
  `Builder::finish`. `Builder::new` spawns a task for each archive.
- tar-codec gives each part of a file body in a `Vec<u8>` that it fills.
  The benchmark gives it a `Vec<u8>` with a capacity of 64 KiB and asks for
  parts of 64 KiB.
- The default tar-codec decode policy rejects unknown vendor PAX records.
  The tar-codec reader ignores the `SCHILY` records, so that it can read the
  extended attributes of the pax corpus.
- The tar-codec writer writes a PAX header for each entry. It does not write
  the mode or the modification time.
- The tar-codec builder has no method for a symbolic link, so `write` with
  tar-codec uses only the small and large corpora.
- tar-codec has no method to copy an entry from a reader to a builder, so it
  is not in `filter`.

## Disk benchmarks

The disk benchmarks use the method of the tar-codec comparison benchmark
(`crates/tar-codec/benches/comparison.rs` in
<https://github.com/astral-sh/astral-codec>). The mixed tree is an
addition.

### Trees

The benchmark makes each tree in a new temporary directory before it
measures the tree. The temporary directory is in the directory that
`std::env::temp_dir` gives.

large-16m
: 1 file of 16 MiB.

many-small
: 1,024 files of 1 KiB in 32 directories.

mixed
: The entries of the mixed corpus: 100 directories, 9,000 files of 0 to
  16 KiB and 1,000 symbolic links.

The archives of the large-16m and many-small trees have no directory
entries. Each extractor makes the directories from the file paths.

### Benchmarks

encode_dir
: Read the tree on disk and write an archive into a sink that counts the
  bytes and drops them. The `smol_tar_slow_sink` case uses smol-tar and a
  sink that also busy-waits 100 ns for each KiB. Its time shows if the
  reads of the file data run while the sink takes the data.

extract
: Extract a ustar archive in memory into a new directory. Each library
  uses its default settings.

extract_minimal
: Extract a ustar archive in memory into a new directory. Each library
  does not set the mode or the modification time when it has an option for
  this.

extract_bare
: Extract a ustar archive in memory into a new directory with no metadata
  updates. Only smol-tar and tar-codec are in this benchmark, and only in
  the tokio build. It compares the two libraries with the same work for
  each entry.

### Measurement rules

- The encoders store each symbolic link as a link entry. The `tar`,
  async-tar and astral-tokio-tar encoders use `follow_symlinks(false)`.
  tar-codec rejects symbolic links by default, so its encoder uses
  `SymlinkPolicy::Preserve`.
- Each extract sample has one iteration. The benchmark makes the temporary
  parent directory before the sample and removes it after the sample.
- smol-tar-fs writes into an existing directory, so the smol-tar extractor
  makes the destination directory in the timed part. The other extractors
  make the destination directory themselves.
- In `extract_minimal`:
  - smol-tar-fs uses `ModePolicy::Skip` and `with_mtime(false)`.
  - The `tar` crate, async-tar and astral-tokio-tar use
    `set_preserve_mtime(false)`. They have no option to keep the mode from
    the archive unset.
  - tar-codec uses the same settings as in `extract`. It does not set the
    modification time, and it makes each file with mode 0o666 or 0o777 less
    the umask.
- In `extract_bare`:
  - smol-tar-fs uses `ModePolicy::Skip` and `with_mtime(false)`. The owner,
    the extended attributes and the special mode bits are off by default.
    Each new file and directory gets the default mode less the umask.
  - tar-codec uses its default settings, as in `extract`.
- smol-tar-fs, async-tar, astral-tokio-tar and tar-codec do file system
  operations on the threads of a blocking thread pool. divan does not count
  the allocations of these threads.

## Read the results

divan shows the time and the throughput in bytes and in entries. The
`alloc` rows show the allocation count and the allocated bytes for one
iteration. Divide the allocation count by the entry count of the corpus to
get the allocations per entry.

In the disk benchmarks, the byte throughput uses the file body bytes of the
tree.

## Results

Test environment: Intel Core i9-14900KF, Linux 6.18 x86_64, Rust 1.89.0.
The temporary directory is on btrfs on NVMe for the disk results, and on
tmpfs for the tmpfs results. No other load was on the machine during the
runs.

The tables show the median time in milliseconds for 100 samples. `--` shows
a benchmark that the library does not have.

### In-memory results

Time:

- In both builds, smol-tar has the lowest time in all rows, except
  `read_all` and `read_skip` with the large corpus.
- In `read_all` and `read_skip` with the large corpus (4 files of 64 MiB),
  all libraries except tar-codec have the same time within 2%. The memory
  copy sets the time. The other libraries take 13.9 to 14.2 ms. tar-codec
  takes 14.4 ms in `read_all` and 15.2 ms in `read_skip`.
- In `write` with the large corpus, smol-tar takes 12.9 ms in both builds.
  async-tar, astral-tokio-tar and `tar` take 24.6 to 26.1 ms. tar-codec
  takes 34.3 ms.
- For pax in the smol build, smol-tar reads the archive in 5.2 ms,
  async-tar reads it in 15.2 ms and `tar` reads it in 19.5 ms. tar-codec
  reads it in 14.2 ms.
- In `read_all` with the small corpus, smol-tar takes 2.2 ms in the smol
  build and 2.3 ms in the tokio build. `tar` takes 3.9 ms and 4.0 ms.
  tar-codec takes 4.3 ms.
- In `read_all` in the tokio build, smol-tar takes 3.9 ms for gnu-long
  and 5.8 ms for mixed. tar-codec takes 7.7 ms and 8.0 ms.
- In `write` with the small corpus in the smol build, smol-tar takes
  1.9 ms, `tar` takes 3.3 ms and async-tar takes 4.2 ms. In the tokio
  build, tar-codec takes 5.7 ms.
- In `filter` with the small corpus in the smol build, smol-tar takes
  3.5 ms and `tar` takes 4.6 ms. With the mixed corpus, smol-tar takes
  8.5 ms and `tar` takes 10.8 ms.

Allocations:

- `read_all`: smol-tar makes 1 allocation for each ustar entry, 2 for each
  GNU long-name entry, and 4 for each PAX entry. tar-codec makes 1, 2 and
  13. The other libraries make 2 for each ustar entry, 5 for each GNU
  long-name entry, and 5 or 6 for each PAX entry.
- `write` with the small corpus: smol-tar makes 1 allocation for each entry,
  tar-codec makes 3, `tar` makes 5, and async-tar and astral-tokio-tar
  make 6. The tar-codec builder keeps the path of each entry. Its peak live
  heap is 1.7 MB in 10,206 allocations.
- `filter` with the small corpus: smol-tar allocates 210 KB in each
  iteration, `tar` allocates 400 KB, and async-tar and astral-tokio-tar
  allocate 41 MB.
- The peak live heap of smol-tar is less than 1.5 KB in `read_all`,
  approximately 66 KB in `read_skip` and approximately 70 KB in `filter`.
  The values for `read_skip` and `filter` include the 64 KiB skip buffer.

smol build (`--features smol`):

| Benchmark | Corpus   | smol-tar | async-tar |   tar |
| --------- | -------- | -------: | --------: | ----: |
| read_all  | small    |     2.21 |      4.83 |  3.87 |
| read_all  | large    |    14.00 |     13.99 | 13.96 |
| read_all  | pax      |     5.19 |     15.21 | 19.48 |
| read_all  | gnu-long |     3.92 |     10.71 |  7.71 |
| read_all  | mixed    |     5.67 |     10.63 |  9.28 |
| read_skip | small    |     2.04 |      4.55 |  3.71 |
| read_skip | large    |    13.94 |     13.98 | 13.99 |
| read_skip | pax      |     5.02 |     14.65 | 19.14 |
| read_skip | gnu-long |     3.79 |     10.00 |  7.59 |
| read_skip | mixed    |     5.31 |      8.21 |  7.42 |
| write     | small    |     1.94 |      4.24 |  3.29 |
| write     | large    |    12.92 |     25.34 | 24.64 |
| write     | mixed    |     6.14 |      9.06 |  7.97 |
| filter    | small    |     3.45 |      6.89 |  4.60 |
| filter    | mixed    |     8.50 |     12.67 | 10.80 |

tokio build (`--no-default-features --features tokio`):

| Benchmark | Corpus   | smol-tar | async-tar | astral-tokio-tar | tar-codec |   tar |
| --------- | -------- | -------: | --------: | ---------------: | --------: | ----: |
| read_all  | small    |     2.32 |      5.04 |             5.64 |      4.28 |  3.95 |
| read_all  | large    |    14.09 |     14.16 |            14.04 |     14.40 | 14.08 |
| read_all  | pax      |     5.27 |     15.15 |            13.81 |     14.20 | 19.23 |
| read_all  | gnu-long |     3.89 |     10.57 |            11.92 |      7.72 |  7.84 |
| read_all  | mixed    |     5.78 |     10.55 |            11.67 |      7.95 |  9.38 |
| read_skip | small    |     2.07 |      4.64 |             5.43 |      4.22 |  3.74 |
| read_skip | large    |    13.99 |     14.05 |            14.04 |     15.23 | 14.05 |
| read_skip | pax      |     5.06 |     14.29 |            13.41 |     14.21 | 18.82 |
| read_skip | gnu-long |     3.63 |     10.11 |            11.55 |      7.84 |  7.96 |
| read_skip | mixed    |     5.37 |      8.32 |             9.17 |      7.87 |  7.43 |
| write     | small    |     1.93 |      4.16 |             4.07 |      5.66 |  3.36 |
| write     | large    |    12.89 |     26.11 |            25.19 |     34.33 | 25.25 |
| write     | mixed    |     6.17 |      8.96 |             9.00 |        -- |  8.10 |
| filter    | small    |     3.75 |      6.99 |             8.12 |        -- |  4.63 |
| filter    | mixed    |     8.71 |     12.88 |            14.02 |        -- | 10.84 |

### Disk results

The numbers in the lists below come from the tokio build.

- `encode_dir`: smol-tar and tar-codec have the same time within 5% for
  many-small (2.4 ms and 2.5 ms). For mixed, smol-tar takes 30.2 ms and
  tar-codec takes 33.7 ms. For large-16m, `tar` takes 1.19 ms, smol-tar
  takes 1.23 ms and tar-codec takes 2.13 ms. async-tar and astral-tokio-tar
  take 8 to 11 times the smol-tar time.
- `extract` with many-small: tar-codec takes 12.7 ms, smol-tar takes
  14.7 ms, `tar` takes 17.0 ms and astral-tokio-tar takes 25.2 ms. In
  `extract_minimal`, smol-tar takes 12.8 ms.
- `extract` with mixed: tar-codec takes 148 ms, smol-tar takes 149 ms,
  `tar` takes 181 ms and astral-tokio-tar takes 270 ms. In
  `extract_minimal`, smol-tar takes 136 ms.
- `extract_bare`: with many-small, smol-tar takes 12.7 ms and tar-codec
  takes 12.6 ms. With mixed, smol-tar takes 137 ms and tar-codec takes
  147 ms. With large-16m, smol-tar takes 7.2 ms and tar-codec takes
  7.0 ms.
- With large-16m, all libraries except async-tar take 5.9 to 7.3 ms. The
  write of the file data sets the time.
- tar-codec uses the same settings in `extract` and `extract_minimal`. The
  difference between its two rows is less than 4%.
- Both builds run the smol-tar and `tar` extractors. For the mixed tree,
  the times of the two builds agree within 1%: 150.3 ms and 149.3 ms for
  smol-tar, and 182.2 ms and 180.6 ms for `tar`.

smol build (`--features smol`):

| Benchmark       | Tree       | smol-tar | async-tar |    tar |
| --------------- | ---------- | -------: | --------: | -----: |
| encode_dir      | large-16m  |     1.41 |     12.51 |   1.21 |
| encode_dir      | many-small |     2.29 |     40.13 |   6.14 |
| encode_dir      | mixed      |    29.95 |    398.30 |  65.74 |
| extract         | large-16m  |     6.58 |     18.38 |   6.42 |
| extract         | many-small |    15.19 |     63.65 |  17.43 |
| extract         | mixed      |   150.30 |    661.10 | 182.20 |
| extract_minimal | large-16m  |     6.51 |     19.08 |   6.13 |
| extract_minimal | many-small |    13.05 |     63.24 |  16.76 |
| extract_minimal | mixed      |   139.20 |    652.10 | 175.10 |

tokio build (`--no-default-features --features tokio`):

| Benchmark       | Tree       | smol-tar | async-tar | astral-tokio-tar | tar-codec |    tar |
| --------------- | ---------- | -------: | --------: | ---------------: | --------: | -----: |
| encode_dir      | large-16m  |     1.23 |     12.11 |            12.13 |      2.13 |   1.19 |
| encode_dir      | many-small |     2.44 |     22.38 |            26.63 |      2.54 |   5.94 |
| encode_dir      | mixed      |    30.19 |    244.50 |           295.20 |     33.74 |  64.06 |
| extract         | large-16m  |     7.17 |     17.45 |             7.17 |      6.99 |   6.10 |
| extract         | many-small |    14.65 |     48.37 |            25.17 |     12.65 |  17.04 |
| extract         | mixed      |   149.30 |    509.10 |           269.90 |    148.10 | 180.60 |
| extract_minimal | large-16m  |     7.09 |     16.97 |             7.33 |      6.95 |   5.91 |
| extract_minimal | many-small |    12.78 |     45.41 |            23.16 |     13.09 |  16.51 |
| extract_minimal | mixed      |   135.80 |    492.30 |           250.80 |    147.40 | 175.30 |
| extract_bare    | large-16m  |     7.17 |        -- |               -- |      6.98 |     -- |
| extract_bare    | many-small |    12.72 |        -- |               -- |     12.64 |     -- |
| extract_bare    | mixed      |   136.90 |        -- |               -- |    147.20 |     -- |

### tmpfs results

These results come from the disk benchmarks with the temporary directory
on tmpfs. On tmpfs, the file data stays in memory and no writeback occurs.
To run the disk benchmarks on tmpfs, set `TMPDIR` to a directory on tmpfs:

```sh
TMPDIR=$(mktemp -d /dev/shm/smol-tar-bench.XXXXXX) \
  cargo bench --manifest-path bench/Cargo.toml --bench fs
```

Remove the directory after the run.

The numbers in the lists below come from the tokio build.

- `extract` with mixed: smol-tar takes 65.1 ms, tar-codec takes 74.0 ms,
  `tar` takes 115 ms and astral-tokio-tar takes 183 ms. In
  `extract_minimal`, smol-tar takes 61.7 ms and tar-codec takes 74.4 ms.
- For smol-tar, tar-codec, `tar` and astral-tokio-tar, the extract of the
  mixed tree takes 65 to 87 ms more on disk than on tmpfs.
- `extract` with many-small: tar-codec takes 5.2 ms, smol-tar takes
  5.4 ms and `tar` takes 10.1 ms. In `extract_bare`, smol-tar takes 5.1 ms
  and tar-codec takes 5.2 ms.
- With large-16m, `tar` takes 3.3 ms, tar-codec takes 4.3 ms,
  astral-tokio-tar takes 4.5 ms and smol-tar takes 4.6 ms. In the smol
  build, smol-tar takes 3.8 ms.
- `encode_dir`: for mixed and many-small, the times of smol-tar and
  tar-codec on tmpfs are within 4% of the disk times. For mixed, `tar`
  takes 47.4 ms on tmpfs and 64.1 ms on disk.

smol build (`--features smol`):

| Benchmark       | Tree       | smol-tar | async-tar |    tar |
| --------------- | ---------- | -------: | --------: | -----: |
| encode_dir      | large-16m  |     1.49 |     13.00 |   1.39 |
| encode_dir      | many-small |     2.38 |     37.80 |   4.17 |
| encode_dir      | mixed      |    29.82 |    392.00 |  49.21 |
| extract         | large-16m  |     3.75 |     15.59 |   3.33 |
| extract         | many-small |     5.51 |     55.54 |  10.21 |
| extract         | mixed      |    65.40 |    573.10 | 115.20 |
| extract_minimal | large-16m  |     3.77 |     16.04 |   3.33 |
| extract_minimal | many-small |     5.23 |     52.25 |   9.90 |
| extract_minimal | mixed      |    61.68 |    551.00 | 112.40 |

tokio build (`--no-default-features --features tokio`):

| Benchmark       | Tree       | smol-tar | async-tar | astral-tokio-tar | tar-codec |    tar |
| --------------- | ---------- | -------: | --------: | ---------------: | --------: | -----: |
| encode_dir      | large-16m  |     1.33 |     12.38 |            11.85 |      2.21 |   1.39 |
| encode_dir      | many-small |     2.36 |     22.49 |            27.43 |      2.55 |   3.96 |
| encode_dir      | mixed      |    29.91 |    243.10 |           290.30 |     33.98 |  47.35 |
| extract         | large-16m  |     4.56 |     14.37 |             4.50 |      4.27 |   3.25 |
| extract         | many-small |     5.37 |     41.01 |            15.95 |      5.25 |  10.12 |
| extract         | mixed      |    65.10 |    435.90 |           183.00 |     74.02 | 115.10 |
| extract_minimal | large-16m  |     4.38 |     14.62 |             4.69 |      4.06 |   3.26 |
| extract_minimal | many-small |     5.04 |     38.98 |            15.23 |      5.20 |   9.88 |
| extract_minimal | mixed      |    61.68 |    425.70 |           177.10 |     74.42 | 112.80 |
| extract_bare    | large-16m  |     4.47 |        -- |               -- |      4.21 |     -- |
| extract_bare    | many-small |     5.08 |        -- |               -- |      5.19 |     -- |
| extract_bare    | mixed      |    62.33 |        -- |               -- |     74.00 |     -- |
