//! List the entries of a tar archive with tokio.
//!
//! Run: cargo run --example read_tokio --no-default-features --features tokio -- ARCHIVE.tar

use smol_tar::{TarEntry, TarReader};
use tokio::{
    fs::File,
    io::{copy, sink, BufReader},
};
use tokio_stream::StreamExt;

#[tokio::main]
async fn main() -> std::io::Result<()> {
    let path = std::env::args()
        .nth(1)
        .expect("usage: read_tokio ARCHIVE.tar");
    // TarReader does not buffer the source. Wrap a file in a BufReader.
    let mut tar = TarReader::new(BufReader::new(File::open(path).await?));
    while let Some(entry) = tar.next().await {
        match entry? {
            TarEntry::File(mut file) => {
                // Read the body to the end or drop it before the next entry.
                let size = copy(&mut file, &mut sink()).await?;
                println!("file {} ({size} bytes)", file.path());
            }
            TarEntry::Directory(dir) => println!("dir {}", dir.path()),
            other => println!("other {}", other.path()),
        }
    }
    Ok(())
}
