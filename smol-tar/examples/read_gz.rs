//! List the files of a gzip-compressed tar archive with smol.
//!
//! Run: cargo run --example read_gz -- ARCHIVE.tar.gz

// With the tokio feature, smol-tar uses the tokio traits, and the smol main
// below cannot compile. In that case, a short main writes a message to
// stderr and exits with status 1.
#![cfg_attr(feature = "tokio", allow(unused_imports))]

use async_compression::futures::bufread::GzipDecoder;
use smol::{
    fs::File,
    io::{copy, sink, BufReader},
    stream::StreamExt,
};
use smol_tar::{TarEntry, TarReader};

#[cfg(not(feature = "tokio"))]
fn main() -> std::io::Result<()> {
    let path = std::env::args()
        .nth(1)
        .expect("usage: read_gz ARCHIVE.tar.gz");
    smol::block_on(async {
        let gz = GzipDecoder::new(BufReader::new(File::open(path).await?));
        let mut tar = TarReader::new(gz);
        while let Some(entry) = tar.next().await {
            if let TarEntry::File(mut file) = entry? {
                let size = copy(&mut file, &mut sink()).await?;
                println!("{} ({size} bytes)", file.path());
            }
        }
        Ok(())
    })
}

#[cfg(feature = "tokio")]
fn main() {
    eprintln!("this example needs the smol feature without the tokio feature");
    std::process::exit(1);
}
