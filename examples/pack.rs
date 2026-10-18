//! Pack a directory tree into a new tar archive with smol.
//!
//! Run: cargo run --example pack --features fs -- OUT.tar DIR

use futures::{future, StreamExt, TryStreamExt};
use smol::{fs::File, io::BufWriter};
use smol_tar::{
    fs::{FsSource, FsStreamExt},
    TarWriter,
};

fn main() -> std::io::Result<()> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let [output, dir] = args.as_slice() else {
        panic!("usage: pack OUT.tar DIR");
    };
    smol::block_on(async {
        // TarWriter writes each header and each part of a body separately.
        // A BufWriter decreases the number of writes to the file.
        let file = BufWriter::with_capacity(256 * 1024, File::create(output).await?);
        let mut out = TarWriter::new(file);
        // Filter the entries before into_tar(). Then each hard link in the
        // archive points to an entry that is in the archive.
        // forward() closes the writer at the end of the stream. The close
        // also flushes the BufWriter.
        FsSource::new(dir)
            .try_filter(|entry| future::ready(!entry.path().starts_with(".git/")))
            .into_tar()
            .forward(&mut out)
            .await
    })
}
