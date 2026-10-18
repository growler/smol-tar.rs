//! Unpack a tar archive into a directory with smol.
//!
//! Run: cargo run --example unpack --features fs -- IN.tar DIR

use futures::StreamExt;
use smol::{fs::File, io::BufReader};
use smol_tar::{fs::FsSink, TarReader};

fn main() -> std::io::Result<()> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let [input, dir] = args.as_slice() else {
        panic!("usage: unpack IN.tar DIR");
    };
    smol::block_on(async {
        // DIR must exist. If it does not exist, the sink gives a NotFound
        // error at the first entry. forward() calls finish() at the end of
        // the stream. finish() sets the mode and the modification time of
        // the directories.
        TarReader::new(BufReader::new(File::open(input).await?))
            .forward(FsSink::new(dir))
            .await
    })
}
