//! Write files from disk into a new tar archive with smol.
//!
//! Run: cargo run --example write -- OUT.tar FILE...

// With the tokio feature, smol-tar uses the tokio traits, and the smol main
// below cannot compile. In that case, a short main writes a message to
// stderr and exits with status 1.
#![cfg_attr(feature = "tokio", allow(unused_imports))]

use smol::{fs::File, io::BufReader};
use smol_tar::{TarDirectory, TarRegularFile, TarWriter};

#[cfg(not(feature = "tokio"))]
fn main() -> std::io::Result<()> {
    let mut args = std::env::args().skip(1);
    let out = args.next().expect("usage: write OUT.tar FILE...");
    smol::block_on(async {
        // TarWriter writes each header and each part of a body separately.
        // To decrease the number of writes, wrap the file in a BufWriter.
        let mut tar = TarWriter::new(File::create(out).await?);
        tar.write(TarDirectory::new("files/").with_mode(0o755).into())
            .await?;
        for path in args {
            let file = File::open(&path).await?;
            let meta = file.metadata().await?;
            let name = format!("files/{}", path.trim_start_matches('/'));
            // TarWriter writes the body from the buffer of the body reader.
            // The buffer size sets the size of each body write.
            let body = BufReader::with_capacity(64 * 1024, file);
            // The size must be equal to the number of bytes in the body.
            let entry = TarRegularFile::new(name, meta.len(), body)
                .with_mode(0o644)
                .with_mtime(meta.modified()?);
            tar.write(entry.into()).await?;
        }
        // Write the two zero blocks that end the archive and close the file.
        tar.finish().await
    })
}

#[cfg(feature = "tokio")]
fn main() {
    eprintln!("this example needs the smol feature without the tokio feature");
    std::process::exit(1);
}
