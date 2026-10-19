//! Copy the entries whose path starts with PREFIX into a new archive.
//!
//! Run: cargo run --example filter -- IN.tar OUT.tar PREFIX

use futures::{future, StreamExt, TryStreamExt};
use smol::{fs::File, io::BufReader};
use smol_tar::{TarReader, TarWriter};

fn main() -> std::io::Result<()> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let [input, output, prefix] = args.as_slice() else {
        panic!("usage: filter IN.tar OUT.tar PREFIX");
    };
    smol::block_on(async {
        let mut out = TarWriter::new(File::create(output).await?);
        // TarReader is a Stream and TarWriter is a Sink of the same entry type.
        // forward() closes the writer at the end of the stream.
        TarReader::new(BufReader::new(File::open(input).await?))
            .try_filter(|entry| future::ready(entry.path().starts_with(prefix.as_str())))
            .forward(&mut out)
            .await
    })
}
