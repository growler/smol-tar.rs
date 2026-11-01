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

Test environment: Intel Core i9-14900KF, Linux 6.18 x86_64, Rust 1.89.0,
btrfs on NVMe for the temporary directory.

The tables show the median time in milliseconds for 100 samples. `--` shows
a benchmark that the library does not have.

### In-memory results

Time:

- In both builds, smol-tar has the lowest time in all rows, except
  `read_all` and `read_skip` with the large corpus.
- In `read_all` and `read_skip` with the large corpus (4 files of 64 MiB),
  all libraries except tar-codec have the same time within 3%. The memory
  copy sets the time. The other libraries take 13.4 to 13.7 ms. tar-codec
  takes 14.0 ms in `read_all` and 14.9 ms in `read_skip`.
- In `write` with the large corpus, smol-tar takes 12.8 ms in the smol
  build and 12.9 ms in the tokio build. async-tar, astral-tokio-tar and
  `tar` take 24.5 to 25.9 ms. tar-codec takes 34.1 ms.
- For pax in the smol build, smol-tar reads the archive in 5.1 ms,
  async-tar reads it in 14.4 ms and `tar` reads it in 19.4 ms. tar-codec
  reads it in 14.2 ms.
- In `read_all` with the small corpus, smol-tar takes 2.2 ms in the smol
  build and 2.3 ms in the tokio build. `tar` takes 4.0 ms and 4.3 ms.
  tar-codec takes 4.1 ms.
- In `read_all` in the tokio build, smol-tar takes 3.7 ms for gnu-long
  and 5.7 ms for mixed. tar-codec takes 7.4 ms and 7.8 ms.
- In `write` with the small corpus in the smol build, smol-tar takes
  1.9 ms, `tar` takes 3.3 ms and async-tar takes 4.2 ms. In the tokio
  build, tar-codec takes 5.6 ms.
- In `filter` with the small corpus in the smol build, smol-tar takes
  3.4 ms and `tar` takes 4.8 ms. With the mixed corpus, smol-tar takes
  8.3 ms and `tar` takes 10.9 ms.

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
| read_all  | small    |     2.24 |      4.85 |  4.04 |
| read_all  | large    |    13.42 |     13.42 | 13.44 |
| read_all  | pax      |     5.14 |     14.36 | 19.40 |
| read_all  | gnu-long |     3.87 |     10.59 |  7.95 |
| read_all  | mixed    |     5.56 |     10.62 |  9.34 |
| read_skip | small    |     2.06 |      4.78 |  3.75 |
| read_skip | large    |    13.46 |     13.50 | 13.43 |
| read_skip | pax      |     5.02 |     14.23 | 19.05 |
| read_skip | gnu-long |     3.67 |     10.26 |  7.65 |
| read_skip | mixed    |     5.22 |      8.26 |  7.34 |
| write     | small    |     1.94 |      4.20 |  3.34 |
| write     | large    |    12.79 |     24.46 | 24.65 |
| write     | mixed    |     5.97 |      9.07 |  8.12 |
| filter    | small    |     3.36 |      6.91 |  4.84 |
| filter    | mixed    |     8.27 |     12.71 | 10.91 |

tokio build (`--no-default-features --features tokio`):

| Benchmark | Corpus   | smol-tar | async-tar | astral-tokio-tar | tar-codec |   tar |
| --------- | -------- | -------: | --------: | ---------------: | --------: | ----: |
| read_all  | small    |     2.30 |      5.22 |             5.68 |      4.12 |  4.34 |
| read_all  | large    |    13.63 |     13.67 |            13.60 |     13.98 | 13.63 |
| read_all  | pax      |     5.18 |     14.75 |            13.83 |     14.21 | 19.45 |
| read_all  | gnu-long |     3.74 |     11.07 |            12.02 |      7.38 |  8.44 |
| read_all  | mixed    |     5.74 |     10.74 |            11.43 |      7.82 |  9.89 |
| read_skip | small    |     2.00 |      4.73 |             5.35 |      4.13 |  3.67 |
| read_skip | large    |    13.73 |     13.64 |            13.67 |     14.92 | 13.65 |
| read_skip | pax      |     4.92 |     14.18 |            13.02 |     14.44 | 18.50 |
| read_skip | gnu-long |     3.55 |     10.19 |            11.47 |      7.75 |  7.52 |
| read_skip | mixed    |     5.25 |      8.43 |             9.07 |      7.79 |  7.34 |
| write     | small    |     1.97 |      4.25 |             4.17 |      5.63 |  3.41 |
| write     | large    |    12.93 |     25.87 |            24.48 |     34.08 | 25.42 |
| write     | mixed    |     6.07 |      8.97 |             9.08 |        -- |  8.15 |
| filter    | small    |     3.63 |      7.01 |             8.00 |        -- |  4.63 |
| filter    | mixed    |     8.62 |     12.90 |            13.67 |        -- | 10.89 |

### Disk results

The numbers in the lists below come from the tokio build.

- `encode_dir`: smol-tar and tar-codec have the same time within 4% for
  many-small (2.5 ms). For mixed, smol-tar takes 30.5 ms and tar-codec
  takes 32.4 ms. For large-16m, `tar` takes 1.1 ms, smol-tar takes 1.2 ms
  and tar-codec takes 1.9 ms. async-tar and astral-tokio-tar take 8 to 11
  times the smol-tar time.
- `extract` with many-small: tar-codec takes 12.4 ms, smol-tar takes
  14.5 ms, `tar` takes 16.8 ms and astral-tokio-tar takes 25.5 ms. In
  `extract_minimal`, smol-tar takes 12.5 ms.
- `extract` with mixed: tar-codec takes 173 ms, smol-tar takes 174 ms,
  `tar` takes 198 ms and astral-tokio-tar takes 297 ms. In
  `extract_minimal`, smol-tar takes 161 ms.
- `extract_bare`: with many-small, smol-tar takes 12.9 ms and tar-codec
  takes 12.6 ms. With mixed, smol-tar takes 161 ms and tar-codec takes
  172 ms. With large-16m, smol-tar takes 6.8 ms and tar-codec takes
  6.5 ms.
- With large-16m, all libraries except async-tar take 6.0 to 7.3 ms. The
  write of the file data sets the time.
- tar-codec uses the same settings in `extract` and `extract_minimal`. The
  difference between its two rows is less than 2%. Between runs on the
  test machine, the extract time of the mixed tree changes by up to 25%.
  btrfs writeback sets most of this time.

smol build (`--features smol`):

| Benchmark       | Tree       | smol-tar | async-tar |    tar |
| --------------- | ---------- | -------: | --------: | -----: |
| encode_dir      | large-16m  |     1.24 |     12.32 |   1.25 |
| encode_dir      | many-small |     2.45 |     39.45 |   6.11 |
| encode_dir      | mixed      |    30.70 |    401.40 |  64.39 |
| extract         | large-16m  |     6.47 |     18.58 |   6.32 |
| extract         | many-small |    14.94 |     65.23 |  17.00 |
| extract         | mixed      |   182.10 |    693.20 | 199.30 |
| extract_minimal | large-16m  |     6.56 |     19.49 |   6.08 |
| extract_minimal | many-small |    12.77 |     65.86 |  16.49 |
| extract_minimal | mixed      |   165.10 |    698.10 | 190.90 |

tokio build (`--no-default-features --features tokio`):

| Benchmark       | Tree       | smol-tar | async-tar | astral-tokio-tar | tar-codec |    tar |
| --------------- | ---------- | -------: | --------: | ---------------: | --------: | -----: |
| encode_dir      | large-16m  |     1.15 |     11.63 |            12.26 |      1.85 |   1.09 |
| encode_dir      | many-small |     2.45 |     22.91 |            27.52 |      2.54 |   5.99 |
| encode_dir      | mixed      |    30.48 |    244.80 |           302.90 |     32.35 |  62.85 |
| extract         | large-16m  |     6.87 |     17.01 |             7.06 |      6.65 |   5.99 |
| extract         | many-small |    14.46 |     48.23 |            25.47 |     12.40 |  16.83 |
| extract         | mixed      |   174.10 |    539.30 |           296.80 |    172.60 | 197.60 |
| extract_minimal | large-16m  |     6.69 |     17.77 |             7.31 |      6.56 |   5.97 |
| extract_minimal | many-small |    12.50 |     45.76 |            23.18 |     12.53 |  16.49 |
| extract_minimal | mixed      |   161.20 |    526.70 |           281.20 |    172.20 | 189.80 |
| extract_bare    | large-16m  |     6.75 |        -- |               -- |      6.51 |     -- |
| extract_bare    | many-small |    12.87 |        -- |               -- |     12.55 |     -- |
| extract_bare    | mixed      |   161.10 |        -- |               -- |    171.80 |     -- |
