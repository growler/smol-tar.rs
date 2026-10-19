//! List the files of a gzip-compressed tar archive with smol.
//!
//! Run: cargo run --example read_gz -- ARCHIVE.tar.gz

use async_compression::futures::bufread::GzipDecoder;
use smol::{
    fs::File,
    io::{copy, sink, BufReader},
    stream::StreamExt,
};
use smol_tar::{TarEntry, TarReader};

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
