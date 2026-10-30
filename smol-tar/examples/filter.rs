//! Copy the entries whose path starts with PREFIX into a new archive.
//!
//! Run: cargo run --example filter -- IN.tar OUT.tar PREFIX

// With the tokio feature, smol-tar uses the tokio traits, and the smol main
// below cannot compile. In that case, a short main writes a message to
// stderr and exits with status 1.
#![cfg_attr(feature = "tokio", allow(unused_imports))]

use futures::{future, StreamExt, TryStreamExt};
use smol::{fs::File, io::BufReader};
use smol_tar::{TarReader, TarWriter};

#[cfg(not(feature = "tokio"))]
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

#[cfg(feature = "tokio")]
fn main() {
    eprintln!("this example needs the smol feature without the tokio feature");
    std::process::exit(1);
}
