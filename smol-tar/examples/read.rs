//! List the entries of a tar archive with smol.
//!
//! Run: cargo run --example read -- ARCHIVE.tar

// With the tokio feature, smol-tar uses the tokio traits, and the smol main
// below cannot compile. In that case, a short main writes a message to
// stderr and exits with status 1.
#![cfg_attr(feature = "tokio", allow(unused_imports))]

use smol::{
    fs::File,
    io::{copy, sink, BufReader},
    stream::StreamExt,
};
use smol_tar::{TarEntry, TarReader};

#[cfg(not(feature = "tokio"))]
fn main() -> std::io::Result<()> {
    let path = std::env::args().nth(1).expect("usage: read ARCHIVE.tar");
    smol::block_on(async {
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
                TarEntry::Symlink(link) => println!("symlink {} -> {}", link.path(), link.link()),
                other => println!("other {}", other.path()),
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
