//! Unpack a tar archive into a directory with smol.
//!
//! Run: cargo run --example unpack -- IN.tar DIR

// With the tokio feature, smol-tar-fs uses the tokio traits, and the smol
// main below cannot compile. In that case, a short main writes a message to
// stderr and exits with status 1.
#![cfg_attr(feature = "tokio", allow(unused_imports))]

use smol::{fs::File, io::BufReader};
use smol_tar::TarReader;
use smol_tar_fs::FsSink;

#[cfg(not(feature = "tokio"))]
fn main() -> std::io::Result<()> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let [input, dir] = args.as_slice() else {
        panic!("usage: unpack IN.tar DIR");
    };
    smol::block_on(async {
        // DIR must exist. If it does not exist, the sink gives a NotFound
        // error at the first entry that it writes. extract() calls finish()
        // at the end of the stream. finish() writes the files of the batch
        // and sets the mode and the modification time of the directories.
        FsSink::new(dir)
            .extract(TarReader::new(BufReader::new(File::open(input).await?)))
            .await
    })
}

#[cfg(feature = "tokio")]
fn main() {
    eprintln!("this example needs the smol feature without the tokio feature");
    std::process::exit(1);
}
