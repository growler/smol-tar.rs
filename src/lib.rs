#![deny(missing_docs)]
#![deny(rustdoc::broken_intra_doc_links)]
#![deny(rustdoc::invalid_rust_codeblocks)]

//! A minimal async streaming TAR reader and writer.
//!
//! The reader is fully streaming: [`TarReader`] yields [`TarEntry`] values from
//! any [`AsyncRead`] source with very little buffering. Regular file entries
//! expose their payload through [`TarRegularFile`], which also implements
//! [`AsyncRead`]. To move on to the next entry, either read the file body to
//! the end or drop the file reader.
//!
//! The writer is a [`futures_sink::Sink`] of [`TarEntry`] values and also
//! provides inherent [`TarWriter::write`] and [`TarWriter::finish`] helpers for
//! straightforward sequential writing without [`futures::SinkExt`]. Finish 
//! the archive with [`TarWriter::finish`] or [`futures::SinkExt::close`] 
//! so the trailing zero blocks are emitted.
//!
//! # Examples
//!
//! Runtime selection:
//!
//! ```toml
//! [dependencies]
//! smol-tar = "0.1"
//! ```
//!
//! ```toml
//! [dependencies]
//! smol-tar = { version = "0.1", default-features = false, features = ["tokio"] }
//! ```
//!
//! Reading an archive:
//!
//! ```rust
//! # #[cfg(feature = "smol")]
//! # use smol::{stream::StreamExt, io::{copy, sink, Cursor}};
//! # #[cfg(feature = "tokio")]
//! # use { std::io::Cursor, tokio_stream::StreamExt, tokio::io::{copy, sink} };
//! use smol_tar::{TarEntry, TarReader};
//! # #[cfg(feature = "smol")]
//! # fn block_on<F: std::future::Future>(fut: F) -> F::Output {
//! #   smol::block_on(fut)
//! # }
//! # #[cfg(feature = "tokio")]
//! # fn block_on<F: std::future::Future>(fut: F) -> F::Output {
//! #   tokio::runtime::Builder::new_current_thread().build().unwrap().block_on(fut)
//! # }
//! # block_on(async {
//!
//! let data = Cursor::new(vec![0; 1024]);
//! let mut tar = TarReader::new(data);
//!
//! while let Some(entry) = tar.next().await {
//!     match entry? {
//!         TarEntry::File(mut file) => {
//!             println!("file: {} ({} bytes)", file.path(), file.size());
//!             copy(&mut file, &mut sink()).await?;
//!         }
//!         TarEntry::Directory(dir) => {
//!             println!("dir: {}", dir.path());
//!         }
//!         other => {
//!             println!("other: {}", other.path());
//!         }
//!     }
//! }
//! # std::io::Result::Ok(())
//! # }).unwrap();
//! ```
//!
//! Writing an archive:
//!
//! ```rust
//! # #[cfg(feature = "smol")]
//! # use smol::io::Cursor;
//! # #[cfg(feature = "tokio")]
//! # use { std::io::Cursor, tokio_stream::StreamExt };
//! use smol_tar::{TarDirectory, TarEntry, TarRegularFile, TarWriter};
//! # #[cfg(feature = "smol")]
//! # fn block_on<F: std::future::Future>(fut: F) -> F::Output {
//! #   smol::future::block_on(fut)
//! # }
//! # #[cfg(feature = "tokio")]
//! # fn block_on<F: std::future::Future>(fut: F) -> F::Output {
//! #   tokio::runtime::Builder::new_current_thread().build().unwrap().block_on(fut)
//! # }
//!
//! # block_on(async {
//! let sink = Cursor::new(Vec::<u8>::new());
//! let mut tar = TarWriter::new(sink);
//!
//! tar.write(TarDirectory::new("bin/").into()).await?;
//!
//! let body = Cursor::new(b"hello\n");
//! tar.write(
//!     TarRegularFile::new(
//!         "bin/hello.txt", 6, body,
//!     ).into()
//! ).await?;
//!
//! tar.finish().await?;
//! # std::io::Result::Ok(())
//! # }).unwrap();
//! ```
//!
//! Alongside the direct API, the writer also implements the composable
//! [`futures_sink::Sink`] interface:
//!
//! ```rust
//! # #[cfg(feature = "smol")]
//! # use smol::io::Cursor;
//! # #[cfg(feature = "tokio")]
//! # use std::io::Cursor;
//! use {
//!     smol_tar::{TarDirectory, TarEntry, TarReader, TarRegularFile, TarWriter},
//!     futures::{future, SinkExt, StreamExt, TryStreamExt},
//! };
//! # #[cfg(feature = "smol")]
//! # fn block_on<F: std::future::Future>(fut: F) -> F::Output {
//! #   smol::future::block_on(fut)
//! # }
//! # #[cfg(feature = "tokio")]
//! # fn block_on<F: std::future::Future>(fut: F) -> F::Output {
//! #   tokio::runtime::Builder::new_current_thread().build().unwrap().block_on(fut)
//! # }
//! # block_on(async {
//!
//! let mut input = Cursor::new(Vec::<u8>::new());
//! let mut source = TarWriter::new(&mut input);
//! source.send(
//!     TarDirectory::new("bin/").into()
//! ).await?;
//! source.send(
//!     TarRegularFile::new(
//!         "bin/keep.txt", 5, Cursor::new(b"keep\n".as_ref())
//!     ).into()
//! ).await?;
//! source.send(
//!     TarRegularFile::new(
//!         "share/skip.txt", 5, Cursor::new(b"skip\n".as_ref())
//!     ).into()
//! ).await?;
//! source.send(
//!     TarRegularFile::new(
//!         "bin/run.sh", 8, Cursor::new(b"echo hi\n".as_ref())
//!     ).with_mode(0o755).into()
//! ).await?;
//! source.close().await?;
//! input.set_position(0);
//!
//! let mut output = Cursor::new(Vec::<u8>::new());
//! let mut filtered = TarWriter::new(&mut output);
//!
//! TarReader::new(&mut input)
//!     .try_filter(|entry| {
//!         future::ready(entry.path().starts_with("bin/"))
//!     })
//!     .forward(&mut filtered)
//!     .await?;
//!
//! filtered.close().await?;
//! output.set_position(0);
//!
//! let paths: Vec<String> = TarReader::new(output)
//!     .map_ok(|entry| entry.path().to_string())
//!     .try_collect()
//!     .await?;
//!
//! assert_eq!(paths, vec!["bin/", "bin/keep.txt", "bin/run.sh"]);
//! # std::io::Result::Ok(())
//! # }).unwrap();
//! ```
//!
//! # Supported formats
//!
//! Reads:
//! 
//! - Old tar
//! - GNU tar
//! - POSIX ustar/pax
//! - GNU long names and long links
//! - PAX path metadata, timestamps, numeric ids, sizes, and extended attributes
//! 
//! Writes:
//! 
//! - POSIX ustar/pax
//! - PAX records for long paths, long link targets, timestamps, numeric ids,
//!   symbolic names, sizes, and extended attributes
//!
//! # Not supported
//!
//! - Sparse files and multi-volume archives
//!

#[cfg(all(feature = "smol", feature = "tokio"))]
compile_error!("features `smol` and `tokio` are mutually exclusive");
#[cfg(not(any(feature = "smol", feature = "tokio")))]
compile_error!("either feature `smol` or `tokio` must be enabled");

use {
    async_lock::Mutex,
    futures_lite::Stream,
    futures_sink::Sink,
    pin_project_lite::pin_project,
    std::{
        future::{Future, poll_fn},
        io::{Error, ErrorKind, Result},
        pin::{pin, Pin},
        str::from_utf8,
        sync::Arc,
        task::{self, Context, Poll},
        time::{Duration, SystemTime, UNIX_EPOCH},
    },
};

#[cfg(feature = "smol")]
use futures_lite::io::{AsyncRead, AsyncWrite};
#[cfg(feature = "tokio")]
use {
    tokio::io::{AsyncRead, AsyncWrite, ReadBuf},
};

const BLOCK_SIZE: usize = 512;
const SKIP_BUFFER_SIZE: usize = 64 * 1024;
const PATH_MAX: usize = 4096;
const PAX_HEADER_MAX_SIZE: usize = 1024 * 1024;

#[cfg(feature = "smol")]
fn poll_read_compat<R: AsyncRead + ?Sized>(
    reader: Pin<&mut R>,
    cx: &mut Context<'_>,
    buf: &mut [u8],
) -> Poll<Result<usize>> {
    reader.poll_read(cx, buf)
}

#[cfg(feature = "tokio")]
fn poll_read_compat<R: AsyncRead + ?Sized>(
    reader: Pin<&mut R>,
    cx: &mut Context<'_>,
    buf: &mut [u8],
) -> Poll<Result<usize>> {
    let mut read_buf = ReadBuf::new(buf);
    match reader.poll_read(cx, &mut read_buf) {
        Poll::Ready(Ok(())) => Poll::Ready(Ok(read_buf.filled().len())),
        Poll::Ready(Err(err)) => Poll::Ready(Err(err)),
        Poll::Pending => Poll::Pending,
    }
}

#[cfg(feature = "smol")]
fn poll_close_compat<W: AsyncWrite + ?Sized>(
    writer: Pin<&mut W>,
    cx: &mut Context<'_>,
) -> Poll<Result<()>> {
    writer.poll_close(cx)
}

#[cfg(feature = "tokio")]
fn poll_close_compat<W: AsyncWrite + ?Sized>(
    writer: Pin<&mut W>,
    cx: &mut Context<'_>,
) -> Poll<Result<()>> {
    writer.poll_shutdown(cx)
}

fn poll_regular_file_reader<'a, R: AsyncRead>(
    this: Pin<&mut TarRegularFileReader<'a, R>>,
    ctx: &mut Context<'_>,
    buf: &mut [u8],
) -> Poll<Result<usize>> {
    let this = this.get_mut();
    let eof = this.eof;
    let fut = this.inner.lock();
    let mut g = task::ready!(pin!(fut).poll(ctx));
    let inner_pin: Pin<&mut TarReaderInner<'a, R>> = g.as_mut();
    let inner = inner_pin.project();
    let n;
    if *inner.pos > eof || (*inner.pos == eof && !matches!(*inner.state, Entry)) {
        return Poll::Ready(Ok(0));
    } else if *inner.pos < eof {
        let remain = *inner.nxt - *inner.pos;
        n = if remain > 0 {
            let size = std::cmp::min(remain, buf.len() as u64);
            let n = task::ready!(poll_read_compat(
                pin!(inner.reader),
                ctx,
                &mut buf[0..size as usize]
            ))?;
            if n == 0 {
                return Poll::Ready(Err(Error::new(
                    ErrorKind::UnexpectedEof,
                    "unexpected EOF while reading archive file",
                )));
            }
            n
        } else {
            0
        };
        *inner.pos += n as u64;
        if (n as u64) < remain {
            return Poll::Ready(Ok(n));
        }
    } else {
        n = 0;
    };
    ctx.waker().wake_by_ref();
    let nxt = padded_size(*inner.nxt);
    if *inner.pos == nxt {
        *inner.nxt = nxt + BLOCK_SIZE as u64;
        *inner.state = Header;
    } else {
        *inner.nxt = nxt;
        *inner.state = Padding;
    }
    Poll::Ready(Ok(n))
}

macro_rules! ready_opt {
    ($e:expr $(,)?) => {
        match $e {
            Poll::Ready(Ok(t)) => t,
            Poll::Ready(Err(err)) => return Poll::Ready(Some(Err(err))),
            Poll::Pending => return Poll::Pending,
        }
    };
}

#[repr(C)]
#[allow(missing_docs)]
struct Header {
    record: [u8; BLOCK_SIZE],
}

enum HeaderKind<'a> {
    Gnu(&'a GnuHeader),
    Ustar(&'a UstarHeader),
    Old(&'a OldHeader),
}

trait HeaderVariant {}

impl Header {
    fn new() -> Self {
        Self {
            record: [0u8; BLOCK_SIZE],
        }
    }
    unsafe fn cast<U: HeaderVariant>(&self) -> &U {
        &*(self as *const Self as *const U)
    }
    fn buf_mut<I>(&mut self, range: I) -> &mut [u8]
    where
        I: core::slice::SliceIndex<[u8], Output = [u8]>,
    {
        &mut self.record[range]
    }
    fn buf<I>(&self, range: I) -> &[u8]
    where
        I: core::slice::SliceIndex<[u8], Output = [u8]>,
    {
        &self.record[range]
    }
    fn as_str<I>(&self, range: I) -> Result<Box<str>>
    where
        I: core::slice::SliceIndex<[u8], Output = [u8]>,
    {
        from_utf8(self.buf(range))
            .map_err(|_| Error::new(ErrorKind::InvalidData, "invalid UTF-8"))
            .map(|p| p.to_string().into_boxed_str())
    }
    fn as_null_terminated_str<I>(&self, range: I) -> Result<Box<str>>
    where
        I: core::slice::SliceIndex<[u8], Output = [u8]>,
    {
        from_utf8(null_terminated(self.buf(range)))
            .map_err(|_| Error::new(ErrorKind::InvalidData, "invalid UTF-8"))
            .map(|p| p.to_string().into_boxed_str())
    }
    fn kind(&self) -> HeaderKind<'_> {
        let gnu = unsafe { self.cast::<GnuHeader>() };
        if gnu.magic == *b"ustar " && gnu.version == *b" \0" {
            HeaderKind::Gnu(gnu)
        } else if gnu.magic == *b"ustar\0" && gnu.version == *b"00" {
            HeaderKind::Ustar(unsafe { self.cast::<UstarHeader>() })
        } else {
            HeaderKind::Old(unsafe { self.cast::<OldHeader>() })
        }
    }
    fn entry_type(&self) -> std::result::Result<Kind, u8> {
        Kind::from_byte(unsafe { self.cast::<GnuHeader>() }.typeflag[0])
    }
    fn checksum(&self) -> Result<u32> {
        parse_octal(&unsafe { self.cast::<GnuHeader>() }.cksum)
            .map(|value| value as u32)
            .map_err(|err| {
                Error::new(
                    ErrorKind::InvalidData,
                    format!(
                        "invalid tar header checksum field: {:?}",
                        String::from_utf8_lossy(err)
                    ),
                )
            })
    }
    fn calculated_checksum(&self) -> u32 {
        self.record
            .iter()
            .enumerate()
            .map(|(index, byte)| {
                if (148..156).contains(&index) {
                    u32::from(b' ')
                } else {
                    u32::from(*byte)
                }
            })
            .sum()
    }
    fn validate_checksum(&self) -> Result<()> {
        let expected = self.checksum()?;
        let actual = self.calculated_checksum();
        if expected == actual {
            Ok(())
        } else {
            Err(Error::new(
                ErrorKind::InvalidData,
                format!("invalid tar header checksum: expected {expected}, got {actual}"),
            ))
        }
    }
    fn is_gnu(&self) -> bool {
        let gnu = unsafe { self.cast::<GnuHeader>() };
        gnu.magic == *b"ustar " && gnu.version == *b" \0"
    }
    #[allow(dead_code)]
    fn is_ustar(&self) -> bool {
        let ustar = unsafe { self.cast::<UstarHeader>() };
        ustar.magic == *b"ustar\0" && ustar.version == *b"00"
    }
    fn is_old(&self) -> bool {
        let gnu = unsafe { self.cast::<GnuHeader>() };
        gnu.magic[..5] != *b"ustar"
    }
    #[inline]
    fn mode(&self) -> Result<u32> {
        parse_octal(&unsafe { self.cast::<GnuHeader>() }.mode)
            .map(|r| r as u32)
            .map_err(|err| {
                Error::new(
                    ErrorKind::InvalidData,
                    format!("invalid octal digit: {:?}", String::from_utf8_lossy(err)),
                )
            })
    }
    #[inline]
    fn mtime(&self) -> Result<u64> {
        parse_octal(&unsafe { self.cast::<GnuHeader>() }.mtime).map_err(|err| {
            Error::new(
                ErrorKind::InvalidData,
                format!("invalid octal digit: {:?}", String::from_utf8_lossy(err)),
            )
        })
    }
    #[inline]
    fn size(&self) -> Result<u64> {
        parse_octal(&unsafe { self.cast::<GnuHeader>() }.size).map_err(|err| {
            Error::new(
                ErrorKind::InvalidData,
                format!("invalid octal digit: {:?}", String::from_utf8_lossy(err)),
            )
        })
    }
    #[inline]
    fn uid(&self) -> Result<u32> {
        parse_octal(&unsafe { self.cast::<GnuHeader>() }.uid)
            .map(|r| r as u32)
            .map_err(|err| {
                Error::new(
                    ErrorKind::InvalidData,
                    format!("invalid octal digit: {:?}", String::from_utf8_lossy(err)),
                )
            })
    }
    #[inline]
    fn gid(&self) -> Result<u32> {
        parse_octal(&unsafe { self.cast::<GnuHeader>() }.gid)
            .map(|r| r as u32)
            .map_err(|err| {
                Error::new(
                    ErrorKind::InvalidData,
                    format!("invalid octal digit: {:?}", String::from_utf8_lossy(err)),
                )
            })
    }
    #[inline]
    fn dev_major(&self) -> Result<u32> {
        parse_octal(&unsafe { self.cast::<GnuHeader>() }.dev_major)
            .map(|r| r as u32)
            .map_err(|err| {
                Error::new(
                    ErrorKind::InvalidData,
                    format!("invalid octal digit: {:?}", String::from_utf8_lossy(err)),
                )
            })
    }
    #[inline]
    fn dev_minor(&self) -> Result<u32> {
        parse_octal(&unsafe { self.cast::<GnuHeader>() }.dev_minor)
            .map(|r| r as u32)
            .map_err(|err| {
                Error::new(
                    ErrorKind::InvalidData,
                    format!("invalid octal digit: {:?}", String::from_utf8_lossy(err)),
                )
            })
    }
    fn uname(&self) -> Result<Option<Box<str>>> {
        match self.kind() {
            HeaderKind::Gnu(gnu) => parse_name_field(&gnu.uname),
            HeaderKind::Ustar(ustar) => parse_name_field(&ustar.uname),
            HeaderKind::Old(_) => Ok(None),
        }
    }
    fn gname(&self) -> Result<Option<Box<str>>> {
        match self.kind() {
            HeaderKind::Gnu(gnu) => parse_name_field(&gnu.gname),
            HeaderKind::Ustar(ustar) => parse_name_field(&ustar.gname),
            HeaderKind::Old(_) => Ok(None),
        }
    }
    fn is_zero(&self) -> bool {
        self.record.iter().all(|b| *b == b'\0')
    }
}

#[derive(Debug, PartialEq, Eq)]
#[repr(u8)]
enum Kind {
    File0 = b'\0',
    File = b'0',
    Link = b'1',
    Symlink = b'2',
    CharDevice = b'3',
    BlockDevice = b'4',
    Directory = b'5',
    Fifo = b'6',
    #[allow(dead_code)]
    Continous = b'7',
    GNULongLink = b'K',
    GNULongName = b'L',
    PAXLocal = b'x',
    PAXGlobal = b'g',
}
impl std::fmt::Display for Kind {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.write_str(match self {
            Self::File | Self::File0 | Self::Continous => "regular file",
            Self::Link => "link",
            Self::Symlink => "symlink",
            Self::CharDevice => "character device",
            Self::BlockDevice => "block device",
            Self::Directory => "directory",
            Self::Fifo => "FIFO",
            Self::GNULongName => "GNU long name extension",
            Self::GNULongLink => "GNU long link extension",
            Self::PAXLocal => "PAX next file extension",
            Self::PAXGlobal => "PAX global extension",
        })
    }
}

impl Kind {
    fn byte(self) -> u8 {
        self as u8
    }
    fn from_byte(b: u8) -> std::result::Result<Self, u8> {
        match b {
            v if v == Kind::File0.byte() => Ok(Kind::File0),
            v if v == Kind::File.byte() => Ok(Kind::File),
            v if v == Kind::Link.byte() => Ok(Kind::Link),
            v if v == Kind::Symlink.byte() => Ok(Kind::Symlink),
            v if v == Kind::Directory.byte() => Ok(Kind::Directory),
            v if v == Kind::GNULongName.byte() => Ok(Kind::GNULongName),
            v if v == Kind::GNULongLink.byte() => Ok(Kind::GNULongLink),
            v if v == Kind::PAXLocal.byte() => Ok(Kind::PAXLocal),
            v if v == Kind::PAXGlobal.byte() => Ok(Kind::PAXGlobal),
            v if v == Kind::CharDevice.byte() => Ok(Kind::CharDevice),
            v if v == Kind::BlockDevice.byte() => Ok(Kind::BlockDevice),
            v if v == Kind::Fifo.byte() => Ok(Kind::Fifo),
            v if v == Kind::Continous.byte() => Ok(Kind::Continous),
            v => Err(v),
        }
    }
}

#[repr(C)]
#[allow(missing_docs)]
struct OldHeader {
    name: [u8; 100],
    mode: [u8; 8],
    uid: [u8; 8],
    gid: [u8; 8],
    size: [u8; 12],
    mtime: [u8; 12],
    cksum: [u8; 8],
    linkflag: [u8; 1],
    linkname: [u8; 100],
    pad: [u8; 255],
}
impl HeaderVariant for OldHeader {}
impl OldHeader {
    fn path_name(&self) -> Result<Box<str>> {
        path_name(&self.name).map(|p| p.to_string().into_boxed_str())
    }
    fn link_name(&self) -> Result<Box<str>> {
        path_name(&self.linkname).map(|p| p.to_string().into_boxed_str())
    }
}

const NAME_LEN: usize = 100;
const PREFIX_LEN: usize = 155;

#[repr(C)]
#[allow(missing_docs)]
struct UstarHeader {
    name: [u8; NAME_LEN],
    mode: [u8; 8],
    uid: [u8; 8],
    gid: [u8; 8],
    size: [u8; 12],
    mtime: [u8; 12],
    cksum: [u8; 8],
    typeflag: [u8; 1],
    linkname: [u8; NAME_LEN],
    magic: [u8; 6],
    version: [u8; 2],
    uname: [u8; 32],
    gname: [u8; 32],
    dev_major: [u8; 8],
    dev_minor: [u8; 8],
    prefix: [u8; PREFIX_LEN],
    pad: [u8; 12],
}
impl HeaderVariant for UstarHeader {}
impl UstarHeader {
    fn path_name(&self) -> Result<Box<str>> {
        ustar_path_name(&self.name, &self.prefix)
    }
    fn link_name(&self) -> Result<Box<str>> {
        path_name(&self.linkname).map(|p| p.to_string().into_boxed_str())
    }
    unsafe fn from_buf(buf: &mut [u8]) -> &mut Self {
        buf[..BLOCK_SIZE].fill(0);
        let hdr = &mut *(buf.as_mut_ptr() as *mut Self);
        hdr.magic = *b"ustar\0";
        hdr.version = *b"00";
        hdr
    }
    fn set_dev_major(&mut self, major: u32) -> std::io::Result<()> {
        format_octal(major as u64, &mut self.dev_major)
    }
    fn set_dev_minor(&mut self, minor: u32) -> std::io::Result<()> {
        format_octal(minor as u64, &mut self.dev_minor)
    }
    fn set_uid(&mut self, uid: u32) -> std::io::Result<()> {
        format_octal(uid as u64, &mut self.uid)
    }
    fn set_gid(&mut self, gid: u32) -> std::io::Result<()> {
        format_octal(gid as u64, &mut self.gid)
    }
    fn set_uname(&mut self, uname: &str) {
        self.uname.fill(0);
        self.uname[..uname.len()].copy_from_slice(uname.as_bytes());
    }
    fn set_gname(&mut self, gname: &str) {
        self.gname.fill(0);
        self.gname[..gname.len()].copy_from_slice(gname.as_bytes());
    }
    fn set_mode(&mut self, mode: u32) -> std::io::Result<()> {
        format_octal(mode as u64, &mut self.mode)
    }
    fn set_mtime(&mut self, mtime: u64) -> std::io::Result<()> {
        format_octal(mtime, &mut self.mtime)
    }
    fn set_size(&mut self, size: u64) -> std::io::Result<()> {
        format_octal(size, &mut self.size)
    }
    fn set_typeflag(&mut self, kind: Kind) {
        self.typeflag[0] = kind.byte();
    }
    fn path_split_point(&mut self, path: &str) -> Option<usize> {
        let bytes = path.as_bytes();
        if bytes.len() <= self.name.len() {
            return None;
        }
        bytes
            .iter()
            .enumerate()
            .rfind(|(i, b)| **b == b'/' && i <= &self.prefix.len())
            .map(|(i, _)| i)
    }
    fn set_path(&mut self, path: &str, split_pos: Option<usize>) {
        if let Some(pos) = split_pos {
            self.prefix[..pos].copy_from_slice(&path.as_bytes()[..pos]);
            copy_utf8_truncate(&mut self.name, unsafe {
                // SAFETY: the source string was an str, and a break, if any, was made at '/',
                // which is a valid codepoint
                std::str::from_utf8_unchecked(&path.as_bytes()[pos + 1..])
            });
        } else {
            copy_utf8_truncate(&mut self.name, path);
        }
    }
    fn set_link_path(&mut self, name: &str) {
        copy_utf8_truncate(&mut self.linkname, name);
    }
    fn finalize(&mut self) -> std::io::Result<()> {
        self.cksum.fill(b' ');
        let buf =
            unsafe { std::slice::from_raw_parts(self as *const Self as *const u8, BLOCK_SIZE) };
        let checksum: u32 = buf.iter().map(|b| *b as u32).sum();
        format_octal(checksum as u64, &mut self.cksum)
    }
}

fn copy_utf8_truncate(field: &mut [u8], bytes: &str) {
    if bytes.len() <= field.len() {
        field[..bytes.len()].copy_from_slice(bytes.as_bytes());
        return;
    }
    let mut cut = 0;
    for (i, c) in bytes.char_indices() {
        if i <= field.len() {
            if c != '/' {
                cut = i;
            }
        } else {
            break;
        }
    }
    field[..cut].copy_from_slice(&bytes.as_bytes()[..cut]);
}

#[repr(C)]
#[allow(missing_docs)]
struct GnuHeader {
    name: [u8; 100],
    mode: [u8; 8],
    uid: [u8; 8],
    gid: [u8; 8],
    size: [u8; 12],
    mtime: [u8; 12],
    cksum: [u8; 8],
    typeflag: [u8; 1],
    linkname: [u8; 100],
    magic: [u8; 6],
    version: [u8; 2],
    uname: [u8; 32],
    gname: [u8; 32],
    dev_major: [u8; 8],
    dev_minor: [u8; 8],
    atime: [u8; 12],
    ctime: [u8; 12],
    offset: [u8; 12],
    longnames: [u8; 4],
    unused: [u8; 1],
    sparse: [u8; 96],
    isextended: [u8; 1],
    realsize: [u8; 12],
    pad: [u8; 17],
}
impl HeaderVariant for GnuHeader {}
impl GnuHeader {
    fn path_name(&self) -> Result<Box<str>> {
        path_name(&self.name).map(|p| p.to_string().into_boxed_str())
    }
    fn link_name(&self) -> Result<Box<str>> {
        path_name(&self.linkname).map(|p| p.to_string().into_boxed_str())
    }
    fn atime(&self) -> Result<u64> {
        parse_octal(&self.atime).map_err(|err| {
            Error::new(
                ErrorKind::InvalidData,
                format!("invalid octal digit: {:?}", String::from_utf8_lossy(err)),
            )
        })
    }
    fn ctime(&self) -> Result<u64> {
        parse_octal(&self.ctime).map_err(|err| {
            Error::new(
                ErrorKind::InvalidData,
                format!("invalid octal digit: {:?}", String::from_utf8_lossy(err)),
            )
        })
    }
}

enum Entry {
    File {
        path_name: Box<str>,
        size: u64,
        eof: u64,
        mode: u32,
        uid: u32,
        gid: u32,
        uname: Option<Box<str>>,
        gname: Option<Box<str>>,
        times: EntryTimes,
        attrs: AttrList,
    },
    Link(TarLink),
    Symlink(TarSymlink),
    Directory(TarDirectory),
    Device(TarDevice),
    Fifo(TarFifo),
}

fn effective_size(hdr: &Header, size: Option<u64>) -> Result<u64> {
    Ok(size.unwrap_or(hdr.size()?))
}

fn effective_uid(hdr: &Header, uid: Option<u32>) -> Result<u32> {
    Ok(uid.unwrap_or(hdr.uid()?))
}

fn effective_gid(hdr: &Header, gid: Option<u32>) -> Result<u32> {
    Ok(gid.unwrap_or(hdr.gid()?))
}

fn effective_uname(hdr: &Header, uname: Option<Box<str>>) -> Result<Option<Box<str>>> {
    uname.map_or_else(|| hdr.uname(), |uname| Ok(Some(uname)))
}

fn effective_gname(hdr: &Header, gname: Option<Box<str>>) -> Result<Option<Box<str>>> {
    gname.map_or_else(|| hdr.gname(), |gname| Ok(Some(gname)))
}

fn effective_times(hdr: &Header, info: &PaxInfo) -> Result<EntryTimes> {
    let mut times = EntryTimes::from_mtime(hdr.mtime()?)?;
    if let HeaderKind::Gnu(gnu) = hdr.kind() {
        let atime = gnu.atime()?;
        if atime != 0 {
            times.atime = Some(unix_epoch_checked_add(Duration::from_secs(atime))?);
        }
        let ctime = gnu.ctime()?;
        if ctime != 0 {
            times.ctime = Some(unix_epoch_checked_add(Duration::from_secs(ctime))?);
        }
    }
    if let Some(mtime) = info.mtime {
        times.mtime = mtime;
    }
    if let Some(atime) = info.atime {
        times.atime = Some(atime);
    }
    if let Some(ctime) = info.ctime {
        times.ctime = Some(ctime);
    }
    Ok(times)
}

#[derive(Debug, PartialEq, Eq)]
enum State {
    Header,
    Extension((u32, Kind)),
    Entry,
    SkipEntry,
    Padding,
    Eof,
    Eoff,
}
use State::*;

struct PosixExtension {
    inner: Box<str>,
}
impl PosixExtension {
    fn validate(ext: &str) -> Result<()> {
        parse_pax_records(ext, |_, _| Ok(()))
    }
    fn for_each_record(&self, cb: impl FnMut(&str, &str) -> Result<()>) -> Result<()> {
        parse_pax_records(&self.inner, cb)
    }
}

impl From<Box<str>> for PosixExtension {
    fn from(s: Box<str>) -> Self {
        Self { inner: s }
    }
}
impl std::ops::Deref for PosixExtension {
    type Target = str;
    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

enum ExtensionHeader {
    LongName(Box<str>),
    LongLink(Box<str>),
    PosixExtension(PosixExtension),
}

impl std::ops::Deref for ExtensionHeader {
    type Target = str;
    fn deref(&self) -> &Self::Target {
        match self {
            ExtensionHeader::LongName(name) => name,
            ExtensionHeader::LongLink(name) => name,
            ExtensionHeader::PosixExtension(pax) => pax,
        }
    }
}

struct ExtensionBuffer {
    buf: Vec<u8>,
}

impl ExtensionBuffer {
    fn new(size: usize) -> Self {
        ExtensionBuffer {
            buf: Vec::<u8>::with_capacity(size),
        }
    }
    fn as_str<I>(&self, range: I) -> Result<Box<str>>
    where
        I: core::slice::SliceIndex<[u8], Output = [u8]>,
    {
        from_utf8(&self.buf[range])
            .map_err(|_| Error::new(ErrorKind::InvalidData, "invalid UTF-8"))
            .map(|p| p.to_string().into_boxed_str())
    }
    fn as_null_terminated_str<I>(&self, range: I) -> Result<Box<str>>
    where
        I: core::slice::SliceIndex<[u8], Output = [u8]>,
    {
        from_utf8(null_terminated(&self.buf[range]))
            .map_err(|_| Error::new(ErrorKind::InvalidData, "invalid UTF-8"))
            .map(|p| p.to_string().into_boxed_str())
    }
    unsafe fn upto(&mut self, n: usize) -> &mut [u8] {
        std::slice::from_raw_parts_mut(self.buf.as_mut_ptr(), n)
    }
    unsafe fn remaining_buf(&mut self) -> &mut [u8] {
        let remaining = self.buf.spare_capacity_mut();
        std::slice::from_raw_parts_mut(remaining.as_mut_ptr() as *mut u8, remaining.len())
    }
    unsafe fn advance(&mut self, n: usize) {
        self.buf.set_len(self.buf.len() + n)
    }
}

pin_project! {
    /// Core state machine that walks the tar stream block-by-block.
    ///
    /// `pos` tracks how much of the current block has been consumed while
    /// `nxt` marks the boundary at which the next transition happens (end of
    /// header, file body, padding, etc.). The `state` enum plus the optional
    /// `ext` buffer describe what the reader is currently expecting: extension
    /// payloads, entry data that must be skipped, or archive EOF.
    struct TarReaderInner<'a, R> {
        // current position in the stream
        pos: u64,
        // end of the current record being processed
        nxt: u64,
        // current state
        state: State,
        // the buffer for the current extended header or for skipping a entry
        ext: Option<ExtensionBuffer>,
        // list of the current extended headers
        exts: Vec<ExtensionHeader>,
        // list of the global extended headers
        globs: Vec<PosixExtension>,
        // the current record buffer
        header: Header,
        #[pin]
        reader: R,
        marker: std::marker::PhantomData<&'a ()>,
    }
}

/// Async reader for the body of the current regular-file entry.
///
/// Instances are produced by [`TarReader`] while iterating
/// [`TarEntry::File`] values. If you drop the reader before reaching EOF, the
/// archive reader skips the remaining bytes for that file so iteration can
/// carry on with the next entry.
pub struct TarRegularFileReader<'a, R: AsyncRead + 'a> {
    eof: u64,
    inner: Arc<Mutex<Pin<Box<TarReaderInner<'a, R>>>>>,
}

impl<R: AsyncRead> Drop for TarRegularFileReader<'_, R> {
    fn drop(&mut self) {
        let inner = self.inner.clone();
        let eof = self.eof;
        let mut g = inner.lock_blocking();
        let this_pin = g.as_mut();
        let this = this_pin.project();
        if *this.pos < eof {
            *this.state = SkipEntry;
        } else if *this.pos == eof && matches!(*this.state, Entry) {
            let nxt = padded_size(*this.nxt);
            if *this.pos == nxt {
                *this.nxt = nxt + BLOCK_SIZE as u64;
                *this.state = Header;
            } else {
                *this.nxt = nxt;
                *this.state = Padding;
            }
        }
    }
}

#[cfg(feature = "smol")]
impl<'a, R: AsyncRead> AsyncRead for TarRegularFileReader<'a, R> {
    fn poll_read(
        self: Pin<&mut Self>,
        ctx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<Result<usize>> {
        poll_regular_file_reader(self, ctx, buf)
    }
}

#[cfg(feature = "tokio")]
impl<'a, R: AsyncRead> AsyncRead for TarRegularFileReader<'a, R> {
    fn poll_read(
        self: Pin<&mut Self>,
        ctx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<Result<()>> {
        match poll_regular_file_reader(self, ctx, buf.initialize_unfilled()) {
            Poll::Ready(Ok(n)) => {
                buf.advance(n);
                Poll::Ready(Ok(()))
            }
            Poll::Ready(Err(err)) => Poll::Ready(Err(err)),
            Poll::Pending => Poll::Pending,
        }
    }
}

/// Streaming tar reader over an [`AsyncRead`] source.
///
/// The reader implements [`Stream`] and yields [`TarEntry`] values one at a
/// time. File entries remain streaming; to continue past a
/// [`TarEntry::File`] entry, either read the file body to the end or drop it.
///
/// # Example
///
/// ```rust
/// # #[cfg(feature = "smol")] 
/// # use { smol::{ stream::StreamExt, io::{copy, sink, Cursor} } };
/// # #[cfg(feature = "tokio")] 
/// # use { std::io::Cursor, tokio_stream::StreamExt, tokio::io::{copy, sink} };
/// use smol_tar::{TarEntry, TarReader};
/// # #[cfg(feature = "smol")]
/// # fn block_on<F: std::future::Future>(fut: F) -> F::Output {
/// #   smol::block_on(fut)
/// # }
/// # #[cfg(feature = "tokio")]
/// # fn block_on<F: std::future::Future>(fut: F) -> F::Output {
/// #   tokio::runtime::Builder::new_current_thread().build().unwrap().block_on(fut)
/// # }
/// # block_on(async {
///
/// let data = Cursor::new(vec![0; 1024]);
/// let mut tar = TarReader::new(data);
///
/// while let Some(entry) = tar.next().await {
///     match entry? {
///         TarEntry::File(mut file) => {
///             println!("file: {}", file.path());
///             copy(&mut file, &mut sink()).await?;
///         },
///         other => println!("entry: {}", other.path()),
///     }
/// }
/// # std::io::Result::Ok(())
/// # }).unwrap();
/// ```
pub struct TarReader<'a, R: AsyncRead + 'a> {
    inner: Arc<Mutex<Pin<Box<TarReaderInner<'a, R>>>>>,
}

impl<'a, R: AsyncRead + 'a> TarReader<'a, R> {
    /// Construct a streaming reader that yields [`TarEntry`] values.
    pub fn new(r: R) -> Self {
        Self {
            inner: Arc::new(Mutex::new(Box::pin(TarReaderInner::new(r)))),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct EntryTimes {
    mtime: SystemTime,
    atime: Option<SystemTime>,
    ctime: Option<SystemTime>,
}

impl EntryTimes {
    fn from_mtime(mtime: u64) -> Result<Self> {
        Ok(Self {
            mtime: unix_epoch_checked_add(Duration::from_secs(mtime))?,
            atime: None,
            ctime: None,
        })
    }

    fn mtime(&self) -> SystemTime {
        self.mtime
    }

    fn with_mtime(mut self, mtime: SystemTime) -> Self {
        self.mtime = mtime;
        self
    }

    fn with_atime(mut self, atime: SystemTime) -> Self {
        self.atime = Some(atime);
        self
    }

    fn with_ctime(mut self, ctime: SystemTime) -> Self {
        self.ctime = Some(ctime);
        self
    }
}

impl Default for EntryTimes {
    fn default() -> Self {
        Self {
            mtime: UNIX_EPOCH,
            atime: None,
            ctime: None,
        }
    }
}

fn unix_epoch_checked_add(duration: Duration) -> Result<SystemTime> {
    UNIX_EPOCH
        .checked_add(duration)
        .ok_or_else(|| Error::new(ErrorKind::InvalidData, "timestamp out of range"))
}

fn header_mtime_value(time: SystemTime) -> Option<u64> {
    let duration = time.duration_since(UNIX_EPOCH).ok()?;
    if duration.subsec_nanos() != 0 {
        return None;
    }
    Some(duration.as_secs())
}

/// Hard-link entry stored in a TAR archive.
pub struct TarLink {
    path_name: Box<str>,
    link_name: Box<str>,
}
impl<R: AsyncRead> From<TarLink> for TarEntry<'_, R> {
    fn from(link: TarLink) -> Self {
        Self::Link(link)
    }
}
impl TarLink {
    /// Create a hard-link entry.
    ///
    /// `path_name` is the path of the link entry in the archive and
    /// `link_name` is the target path stored in the header.
    pub fn new<N: Into<Box<str>>, L: Into<Box<str>>>(path_name: N, link_name: L) -> TarLink {
        TarLink {
            path_name: path_name.into(),
            link_name: link_name.into(),
        }
    }
    /// Return the path stored for this entry.
    pub fn path(&'_ self) -> &'_ str {
        &self.path_name
    }
    /// Return the hard-link target path stored for this entry.
    pub fn link(&'_ self) -> &'_ str {
        &self.link_name
    }
    fn write_header(&self, buffer: &mut [u8]) -> std::io::Result<usize> {
        write_header(
            buffer,
            self.path_name.as_ref(),
            Some(self.link_name.as_ref()),
            Kind::Link,
            0,    // size
            0,    // mode
            0,    // uid
            0,    // gid
            None, // uname
            None, // gname
            &EntryTimes::default(),
            None, // device
            &AttrList::default(),
        )
    }
}
/// Device-node kind for [`TarDevice`].
pub enum DeviceKind {
    /// Character device entry.
    Char,
    /// Block device entry.
    Block,
}

/// Device-node metadata stored in a TAR archive.
pub struct TarDevice {
    path_name: Box<str>,
    mode: u32,
    uid: u32,
    gid: u32,
    uname: Option<Box<str>>,
    gname: Option<Box<str>>,
    times: EntryTimes,
    kind: DeviceKind,
    major: u32,
    minor: u32,
    attrs: AttrList,
}
impl<R: AsyncRead> From<TarDevice> for TarEntry<'_, R> {
    fn from(device: TarDevice) -> Self {
        Self::Device(device)
    }
}
impl TarDevice {
    /// Create a character-device entry.
    pub fn new_char<N: Into<Box<str>>>(path_name: N, major: u32, minor: u32) -> TarDevice {
        TarDevice {
            path_name: path_name.into(),
            mode: 0o600,
            uid: 0,
            gid: 0,
            uname: None,
            gname: None,
            times: EntryTimes::default(),
            major,
            minor,
            kind: DeviceKind::Char,
            attrs: AttrList::default(),
        }
    }
    /// Create a block-device entry.
    pub fn new_block<N: Into<Box<str>>>(path_name: N, major: u32, minor: u32) -> TarDevice {
        TarDevice {
            path_name: path_name.into(),
            mode: 0o600,
            uid: 0,
            gid: 0,
            uname: None,
            gname: None,
            times: EntryTimes::default(),
            major,
            minor,
            kind: DeviceKind::Block,
            attrs: AttrList::default(),
        }
    }
    /// Return the path stored for this entry.
    pub fn path(&'_ self) -> &'_ str {
        &self.path_name
    }
    /// Return the raw permission bits stored in the TAR header.
    pub fn mode(&self) -> u32 {
        self.mode
    }
    /// Return the modification time stored for this entry.
    pub fn mtime(&self) -> SystemTime {
        self.times.mtime()
    }
    /// Return the access time stored for this entry, if present.
    pub fn atime(&self) -> Option<SystemTime> {
        self.times.atime
    }
    /// Return the status-change time stored for this entry, if present.
    pub fn ctime(&self) -> Option<SystemTime> {
        self.times.ctime
    }
    /// Return the raw user id stored in the TAR header.
    pub fn uid(&self) -> u32 {
        self.uid
    }
    /// Return the symbolic user name stored in the TAR header, if present.
    pub fn uname(&self) -> &str {
        self.uname.as_deref().unwrap_or("")
    }
    /// Replace the raw user id stored in the TAR header.
    pub fn with_uid(mut self, uid: u32) -> Self {
        self.uid = uid;
        self
    }
    /// Replace the symbolic user name stored in the TAR header.
    pub fn with_uname<S: Into<Box<str>>>(mut self, name: S) -> Self {
        self.uname = Some(name.into());
        self
    }
    /// Return the raw group id stored in the TAR header.
    pub fn gid(&self) -> u32 {
        self.gid
    }
    /// Return the symbolic group name stored in the TAR header, if present.
    pub fn gname(&self) -> &str {
        self.gname.as_deref().unwrap_or("")
    }
    /// Replace the raw group id stored in the TAR header.
    pub fn with_gid(mut self, gid: u32) -> Self {
        self.gid = gid;
        self
    }
    /// Replace the symbolic group name stored in the TAR header.
    pub fn with_gname<S: Into<Box<str>>>(mut self, name: S) -> Self {
        self.gname = Some(name.into());
        self
    }
    /// Return `true` if this device entry is a character device.
    pub fn is_char(&self) -> bool {
        matches!(self.kind, DeviceKind::Char)
    }
    /// Return `true` if this device entry is a block device.
    pub fn is_block(&self) -> bool {
        matches!(self.kind, DeviceKind::Block)
    }
    /// Return the raw major device number stored in the TAR header.
    pub fn major(&self) -> u32 {
        self.major
    }
    /// Return the raw minor device number stored in the TAR header.
    pub fn minor(&self) -> u32 {
        self.minor
    }
    /// Replace the raw permission bits stored in the TAR header.
    pub fn with_mode(mut self, mode: u32) -> Self {
        self.mode = mode;
        self
    }
    /// Replace the modification time stored for this entry.
    pub fn with_mtime(mut self, mtime: SystemTime) -> Self {
        self.times = self.times.with_mtime(mtime);
        self
    }
    /// Replace the access time stored for this entry.
    pub fn with_atime(mut self, atime: SystemTime) -> Self {
        self.times = self.times.with_atime(atime);
        self
    }
    /// Replace the status-change time stored for this entry.
    pub fn with_ctime(mut self, ctime: SystemTime) -> Self {
        self.times = self.times.with_ctime(ctime);
        self
    }
    /// Return the extended attributes that will be encoded in PAX records.
    pub fn attrs(&self) -> &AttrList {
        &self.attrs
    }
    /// Return a mutable reference to the extended attributes.
    pub fn attrs_mut(&mut self) -> &mut AttrList {
        &mut self.attrs
    }
    /// Replace the extended-attribute list and return the updated entry.
    pub fn with_attrs(mut self, attrs: AttrList) -> Self {
        self.attrs = attrs;
        self
    }
    fn write_header(&self, buffer: &mut [u8]) -> std::io::Result<usize> {
        write_header(
            buffer,
            self.path_name.as_ref(),
            None,
            match self.kind {
                DeviceKind::Char => Kind::CharDevice,
                DeviceKind::Block => Kind::BlockDevice,
            },
            0, // size
            self.mode,
            self.uid,
            self.gid,
            self.uname.as_deref(),
            self.gname.as_deref(),
            &self.times,
            Some((self.major, self.minor)),
            &self.attrs,
        )
    }
}

/// FIFO (named pipe) metadata stored in a TAR archive.
pub struct TarFifo {
    path_name: Box<str>,
    mode: u32,
    uid: u32,
    gid: u32,
    uname: Option<Box<str>>,
    gname: Option<Box<str>>,
    times: EntryTimes,
    attrs: AttrList,
}
impl<R: AsyncRead> From<TarFifo> for TarEntry<'_, R> {
    fn from(fifo: TarFifo) -> Self {
        Self::Fifo(fifo)
    }
}
impl TarFifo {
    /// Create a FIFO entry.
    pub fn new<N: Into<Box<str>>>(path_name: N) -> TarFifo {
        TarFifo {
            path_name: path_name.into(),
            mode: 0o644,
            uid: 0,
            gid: 0,
            uname: None,
            gname: None,
            times: EntryTimes::default(),
            attrs: AttrList::default(),
        }
    }
    /// Return the path stored for this entry.
    pub fn path(&'_ self) -> &'_ str {
        &self.path_name
    }
    /// Return the raw permission bits stored in the TAR header.
    pub fn mode(&self) -> u32 {
        self.mode
    }
    /// Return the modification time stored for this entry.
    pub fn mtime(&self) -> SystemTime {
        self.times.mtime()
    }
    /// Return the access time stored for this entry, if present.
    pub fn atime(&self) -> Option<SystemTime> {
        self.times.atime
    }
    /// Return the status-change time stored for this entry, if present.
    pub fn ctime(&self) -> Option<SystemTime> {
        self.times.ctime
    }
    /// Return the raw user id stored in the TAR header.
    pub fn uid(&self) -> u32 {
        self.uid
    }
    /// Return the symbolic user name stored in the TAR header, if present.
    pub fn uname(&self) -> &str {
        self.uname.as_deref().unwrap_or("")
    }
    /// Replace the raw user id stored in the TAR header.
    pub fn with_uid(mut self, uid: u32) -> Self {
        self.uid = uid;
        self
    }
    /// Replace the symbolic user name stored in the TAR header.
    pub fn with_uname<S: Into<Box<str>>>(mut self, name: S) -> Self {
        self.uname = Some(name.into());
        self
    }
    /// Return the raw group id stored in the TAR header.
    pub fn gid(&self) -> u32 {
        self.gid
    }
    /// Return the symbolic group name stored in the TAR header, if present.
    pub fn gname(&self) -> &str {
        self.gname.as_deref().unwrap_or("")
    }
    /// Replace the raw group id stored in the TAR header.
    pub fn with_gid(mut self, gid: u32) -> Self {
        self.gid = gid;
        self
    }
    /// Replace the symbolic group name stored in the TAR header.
    pub fn with_gname<S: Into<Box<str>>>(mut self, name: S) -> Self {
        self.gname = Some(name.into());
        self
    }
    /// Replace the raw permission bits stored in the TAR header.
    pub fn with_mode(mut self, mode: u32) -> Self {
        self.mode = mode;
        self
    }
    /// Replace the modification time stored for this entry.
    pub fn with_mtime(mut self, mtime: SystemTime) -> Self {
        self.times = self.times.with_mtime(mtime);
        self
    }
    /// Replace the access time stored for this entry.
    pub fn with_atime(mut self, atime: SystemTime) -> Self {
        self.times = self.times.with_atime(atime);
        self
    }
    /// Replace the status-change time stored for this entry.
    pub fn with_ctime(mut self, ctime: SystemTime) -> Self {
        self.times = self.times.with_ctime(ctime);
        self
    }
    /// Return the extended attributes that will be encoded in PAX records.
    pub fn attrs(&self) -> &AttrList {
        &self.attrs
    }
    /// Return a mutable reference to the extended attributes.
    pub fn attrs_mut(&mut self) -> &mut AttrList {
        &mut self.attrs
    }
    /// Replace the extended-attribute list and return the updated entry.
    pub fn with_attrs(mut self, attrs: AttrList) -> Self {
        self.attrs = attrs;
        self
    }
    fn write_header(&self, buffer: &mut [u8]) -> std::io::Result<usize> {
        write_header(
            buffer,
            self.path_name.as_ref(),
            None,
            Kind::Fifo,
            0, // size
            self.mode,
            self.uid,
            self.gid,
            self.uname.as_deref(),
            self.gname.as_deref(),
            &self.times,
            None, // device
            &self.attrs,
        )
    }
}

/// Symbolic-link metadata stored in a TAR archive.
pub struct TarSymlink {
    path_name: Box<str>,
    link_name: Box<str>,
    mode: u32,
    uid: u32,
    gid: u32,
    uname: Option<Box<str>>,
    gname: Option<Box<str>>,
    times: EntryTimes,
    attrs: AttrList,
}
impl<R: AsyncRead> From<TarSymlink> for TarEntry<'_, R> {
    fn from(symlink: TarSymlink) -> Self {
        Self::Symlink(symlink)
    }
}
impl TarSymlink {
    /// Create a symbolic-link entry.
    ///
    /// `path_name` is the link path stored in the archive and `link_name` is
    /// the target path.
    pub fn new<N: Into<Box<str>>, L: Into<Box<str>>>(path_name: N, link_name: L) -> TarSymlink {
        TarSymlink {
            path_name: path_name.into(),
            link_name: link_name.into(),
            mode: 0o777,
            uid: 0,
            gid: 0,
            uname: None,
            gname: None,
            times: EntryTimes::default(),
            attrs: AttrList::default(),
        }
    }
    /// Return the path stored for this entry.
    pub fn path(&'_ self) -> &'_ str {
        &self.path_name
    }
    /// Return the symbolic-link target path stored for this entry.
    pub fn link(&'_ self) -> &'_ str {
        &self.link_name
    }
    /// Return the raw permission bits stored in the TAR header.
    pub fn mode(&self) -> u32 {
        self.mode
    }
    /// Return the modification time stored for this entry.
    pub fn mtime(&self) -> SystemTime {
        self.times.mtime()
    }
    /// Return the access time stored for this entry, if present.
    pub fn atime(&self) -> Option<SystemTime> {
        self.times.atime
    }
    /// Return the status-change time stored for this entry, if present.
    pub fn ctime(&self) -> Option<SystemTime> {
        self.times.ctime
    }
    /// Return the raw user id stored in the TAR header.
    pub fn uid(&self) -> u32 {
        self.uid
    }
    /// Return the symbolic user name stored in the TAR header, if present.
    pub fn uname(&self) -> &str {
        self.uname.as_deref().unwrap_or("")
    }
    /// Replace the raw user id stored in the TAR header.
    pub fn with_uid(mut self, uid: u32) -> Self {
        self.uid = uid;
        self
    }
    /// Replace the symbolic user name stored in the TAR header.
    pub fn with_uname<S: Into<Box<str>>>(mut self, name: S) -> Self {
        self.uname = Some(name.into());
        self
    }
    /// Return the raw group id stored in the TAR header.
    pub fn gid(&self) -> u32 {
        self.gid
    }
    /// Return the symbolic group name stored in the TAR header, if present.
    pub fn gname(&self) -> &str {
        self.gname.as_deref().unwrap_or("")
    }
    /// Replace the raw group id stored in the TAR header.
    pub fn with_gid(mut self, gid: u32) -> Self {
        self.gid = gid;
        self
    }
    /// Replace the symbolic group name stored in the TAR header.
    pub fn with_gname<S: Into<Box<str>>>(mut self, name: S) -> Self {
        self.gname = Some(name.into());
        self
    }
    /// Replace the raw permission bits stored in the TAR header.
    pub fn with_mode(mut self, mode: u32) -> Self {
        self.mode = mode;
        self
    }
    /// Replace the modification time stored for this entry.
    pub fn with_mtime(mut self, mtime: SystemTime) -> Self {
        self.times = self.times.with_mtime(mtime);
        self
    }
    /// Replace the access time stored for this entry.
    pub fn with_atime(mut self, atime: SystemTime) -> Self {
        self.times = self.times.with_atime(atime);
        self
    }
    /// Replace the status-change time stored for this entry.
    pub fn with_ctime(mut self, ctime: SystemTime) -> Self {
        self.times = self.times.with_ctime(ctime);
        self
    }
    /// Return the extended attributes that will be encoded in PAX records.
    pub fn attrs(&self) -> &AttrList {
        &self.attrs
    }
    /// Return a mutable reference to the extended attributes.
    pub fn attrs_mut(&mut self) -> &mut AttrList {
        &mut self.attrs
    }
    /// Replace the extended-attribute list and return the updated entry.
    pub fn with_attrs(mut self, attrs: AttrList) -> Self {
        self.attrs = attrs;
        self
    }
    fn write_header(&self, buffer: &mut [u8]) -> std::io::Result<usize> {
        write_header(
            buffer,
            self.path_name.as_ref(),
            Some(self.link_name.as_ref()),
            Kind::Symlink,
            0, // size
            self.mode,
            self.uid,
            self.gid,
            self.uname.as_deref(),
            self.gname.as_deref(),
            &self.times,
            None, // device
            &self.attrs,
        )
    }
}

/// Directory metadata stored in a TAR archive.
pub struct TarDirectory {
    path_name: Box<str>,
    mode: u32,
    uid: u32,
    gid: u32,
    uname: Option<Box<str>>,
    gname: Option<Box<str>>,
    times: EntryTimes,
    size: u64,
    attrs: AttrList,
}
impl<R: AsyncRead> From<TarDirectory> for TarEntry<'_, R> {
    fn from(dir: TarDirectory) -> Self {
        Self::Directory(dir)
    }
}
impl TarDirectory {
    /// Create a directory entry.
    ///
    /// New directory entries are created with a stored size of `0`.
    pub fn new<N: Into<Box<str>>>(path_name: N) -> TarDirectory {
        TarDirectory {
            path_name: path_name.into(),
            size: 0,
            mode: 0o755,
            uid: 0,
            gid: 0,
            uname: None,
            gname: None,
            times: EntryTimes::default(),
            attrs: AttrList::default(),
        }
    }
    /// Return the path stored for this entry.
    pub fn path(&'_ self) -> &'_ str {
        &self.path_name
    }
    /// Return the size stored in the TAR header.
    pub fn size(&self) -> u64 {
        self.size
    }
    /// Return the raw permission bits stored in the TAR header.
    pub fn mode(&self) -> u32 {
        self.mode
    }
    /// Return the modification time stored for this entry.
    pub fn mtime(&self) -> SystemTime {
        self.times.mtime()
    }
    /// Return the access time stored for this entry, if present.
    pub fn atime(&self) -> Option<SystemTime> {
        self.times.atime
    }
    /// Return the status-change time stored for this entry, if present.
    pub fn ctime(&self) -> Option<SystemTime> {
        self.times.ctime
    }
    /// Return the raw user id stored in the TAR header.
    pub fn uid(&self) -> u32 {
        self.uid
    }
    /// Return the symbolic user name stored in the TAR header, if present.
    pub fn uname(&self) -> &str {
        self.uname.as_deref().unwrap_or("")
    }
    /// Replace the raw user id stored in the TAR header.
    pub fn with_uid(mut self, uid: u32) -> Self {
        self.uid = uid;
        self
    }
    /// Replace the symbolic user name stored in the TAR header.
    pub fn with_uname<S: Into<Box<str>>>(mut self, name: S) -> Self {
        self.uname = Some(name.into());
        self
    }
    /// Return the raw group id stored in the TAR header.
    pub fn gid(&self) -> u32 {
        self.gid
    }
    /// Return the symbolic group name stored in the TAR header, if present.
    pub fn gname(&self) -> &str {
        self.gname.as_deref().unwrap_or("")
    }
    /// Replace the raw group id stored in the TAR header.
    pub fn with_gid(mut self, gid: u32) -> Self {
        self.gid = gid;
        self
    }
    /// Replace the symbolic group name stored in the TAR header.
    pub fn with_gname<S: Into<Box<str>>>(mut self, name: S) -> Self {
        self.gname = Some(name.into());
        self
    }
    /// Replace the raw permission bits stored in the TAR header.
    pub fn with_mode(mut self, mode: u32) -> Self {
        self.mode = mode;
        self
    }
    /// Replace the modification time stored for this entry.
    pub fn with_mtime(mut self, mtime: SystemTime) -> Self {
        self.times = self.times.with_mtime(mtime);
        self
    }
    /// Replace the access time stored for this entry.
    pub fn with_atime(mut self, atime: SystemTime) -> Self {
        self.times = self.times.with_atime(atime);
        self
    }
    /// Replace the status-change time stored for this entry.
    pub fn with_ctime(mut self, ctime: SystemTime) -> Self {
        self.times = self.times.with_ctime(ctime);
        self
    }
    /// Replace the size stored in the TAR header.
    pub fn with_size(mut self, size: u64) -> Self {
        self.size = size;
        self
    }
    /// Return the extended attributes that will be encoded in PAX records.
    pub fn attrs(&self) -> &AttrList {
        &self.attrs
    }
    /// Return a mutable reference to the extended attributes.
    pub fn attrs_mut(&mut self) -> &mut AttrList {
        &mut self.attrs
    }
    /// Replace the extended-attribute list and return the updated entry.
    pub fn with_attrs(mut self, attrs: AttrList) -> Self {
        self.attrs = attrs;
        self
    }
    fn write_header(&self, buffer: &mut [u8]) -> std::io::Result<usize> {
        write_header(
            buffer,
            self.path_name.as_ref(),
            None,
            Kind::Directory,
            self.size,
            self.mode,
            self.uid,
            self.gid,
            self.uname.as_deref(),
            self.gname.as_deref(),
            &self.times,
            None, // device
            &self.attrs,
        )
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
/// List of extended attributes stored in PAX records.
///
/// Names map to raw byte values. When serialized, attributes are emitted as
/// `SCHILY.xattr.*` PAX records. Replacing attributes with
/// [`TarRegularFile::with_attrs`] or similar helpers overwrites the whole list.
pub struct AttrList {
    inner: Vec<(Box<str>, Box<[u8]>)>,
}

impl AttrList {
    /// Create an empty attribute list.
    pub fn new() -> Self {
        Self { inner: Vec::new() }
    }

    /// Returns the number of attributes stored.
    pub fn len(&self) -> usize {
        self.inner.len()
    }

    /// Returns true if the list is empty.
    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    /// Append an attribute name/value pair.
    pub fn push<N: Into<Box<str>>, V: Into<Box<[u8]>>>(&mut self, name: N, value: V) {
        self.inner.push((name.into(), value.into()));
    }

    /// Append an attribute pair and return the list.
    pub fn with<N: Into<Box<str>>, V: Into<Box<[u8]>>>(mut self, name: N, value: V) -> Self {
        self.push(name, value);
        self
    }

    /// Iterate over stored attributes.
    pub fn iter(&self) -> impl Iterator<Item = (&str, &[u8])> {
        self.inner
            .iter()
            .map(|(name, value)| (name.as_ref(), value.as_ref()))
    }
}

impl From<Vec<(Box<str>, Box<[u8]>)>> for AttrList {
    fn from(inner: Vec<(Box<str>, Box<[u8]>)>) -> Self {
        Self { inner }
    }
}

pin_project! {
    /// Regular-file metadata paired with a reader for its payload bytes.
    ///
    /// Values of this type are yielded by [`TarReader`] and can also be built
    /// manually for [`TarWriter`]. The type implements [`AsyncRead`] so callers
    /// can stream file contents without buffering the full file in memory.
    pub struct TarRegularFile<'a, R> {
        path_name: Box<str>,
        size: u64,
        mode: u32,
        uid: u32,
        gid: u32,
        uname: Option<Box<str>>,
        gname: Option<Box<str>>,
        times: EntryTimes,
        attrs: AttrList,
        #[pin]
        inner: R,
        marker: std::marker::PhantomData<&'a ()>,
    }
}
impl<'a, R: AsyncRead + 'a> TarRegularFile<'a, R> {
    /// Build a regular-file entry with the provided body reader.
    ///
    /// `size` must match the exact number of bytes that `inner` will yield.
    pub fn new<N: Into<Box<str>>>(path_name: N, size: u64, inner: R) -> TarRegularFile<'a, R> {
        TarRegularFile {
            path_name: path_name.into(),
            size,
            mode: 0o644,
            uid: 0,
            gid: 0,
            uname: None,
            gname: None,
            times: EntryTimes::default(),
            attrs: AttrList::default(),
            inner,
            marker: std::marker::PhantomData,
        }
    }
    /// Return the size stored in the TAR header.
    pub fn size(&self) -> u64 {
        self.size
    }
    /// Return the path stored for this entry.
    pub fn path(&'_ self) -> &'_ str {
        &self.path_name
    }
    /// Return the raw permission bits stored in the TAR header.
    pub fn mode(&self) -> u32 {
        self.mode
    }
    /// Return the modification time stored for this entry.
    pub fn mtime(&self) -> SystemTime {
        self.times.mtime()
    }
    /// Return the access time stored for this entry, if present.
    pub fn atime(&self) -> Option<SystemTime> {
        self.times.atime
    }
    /// Return the status-change time stored for this entry, if present.
    pub fn ctime(&self) -> Option<SystemTime> {
        self.times.ctime
    }
    /// Return the raw user id stored in the TAR header.
    pub fn uid(&self) -> u32 {
        self.uid
    }
    /// Return the symbolic user name stored in the TAR header, if present.
    pub fn uname(&self) -> &str {
        self.uname.as_deref().unwrap_or("")
    }
    /// Replace the raw user id stored in the TAR header.
    pub fn with_uid(mut self, uid: u32) -> Self {
        self.uid = uid;
        self
    }
    /// Replace the symbolic user name stored in the TAR header.
    pub fn with_uname<S: Into<Box<str>>>(mut self, name: S) -> Self {
        self.uname = Some(name.into());
        self
    }
    /// Return the raw group id stored in the TAR header.
    pub fn gid(&self) -> u32 {
        self.gid
    }
    /// Return the symbolic group name stored in the TAR header, if present.
    pub fn gname(&self) -> &str {
        self.gname.as_deref().unwrap_or("")
    }
    /// Replace the raw group id stored in the TAR header.
    pub fn with_gid(mut self, gid: u32) -> Self {
        self.gid = gid;
        self
    }
    /// Replace the symbolic group name stored in the TAR header.
    pub fn with_gname<S: Into<Box<str>>>(mut self, name: S) -> Self {
        self.gname = Some(name.into());
        self
    }
    /// Replace the raw permission bits stored in the TAR header.
    pub fn with_mode(mut self, mode: u32) -> Self {
        self.mode = mode;
        self
    }
    /// Replace the modification time stored for this entry.
    pub fn with_mtime(mut self, mtime: SystemTime) -> Self {
        self.times = self.times.with_mtime(mtime);
        self
    }
    /// Replace the access time stored for this entry.
    pub fn with_atime(mut self, atime: SystemTime) -> Self {
        self.times = self.times.with_atime(atime);
        self
    }
    /// Replace the status-change time stored for this entry.
    pub fn with_ctime(mut self, ctime: SystemTime) -> Self {
        self.times = self.times.with_ctime(ctime);
        self
    }
    /// Return the extended attributes that will be encoded in PAX records.
    pub fn attrs(&self) -> &AttrList {
        &self.attrs
    }
    /// Return a mutable reference to the extended attributes.
    pub fn attrs_mut(&mut self) -> &mut AttrList {
        &mut self.attrs
    }
    /// Replace the extended-attribute list and return the updated entry.
    pub fn with_attrs(mut self, attrs: AttrList) -> Self {
        self.attrs = attrs;
        self
    }
    fn write_header(&self, buffer: &mut [u8]) -> std::io::Result<usize> {
        write_header(
            buffer,
            self.path_name.as_ref(),
            None,
            Kind::File,
            self.size,
            self.mode,
            self.uid,
            self.gid,
            self.uname.as_deref(),
            self.gname.as_deref(),
            &self.times,
            None, // device
            &self.attrs,
        )
    }
}
impl<'a, R: AsyncRead + 'a> From<TarRegularFile<'a, R>> for TarEntry<'a, R> {
    fn from(file: TarRegularFile<'a, R>) -> Self {
        Self::File(file)
    }
}
/// High-level TAR entry representation used by [`TarReader`] and [`TarWriter`].
pub enum TarEntry<'a, R: AsyncRead + 'a> {
    /// A regular file entry with a streaming body reader.
    File(TarRegularFile<'a, R>),
    /// A hard-link entry.
    Link(TarLink),
    /// A symbolic-link entry.
    Symlink(TarSymlink),
    /// A directory entry.
    Directory(TarDirectory),
    /// A character or block device entry.
    Device(TarDevice),
    /// A FIFO entry.
    Fifo(TarFifo),
}

impl<'a, R: AsyncRead + 'a> TarEntry<'a, R> {
    fn write_header(&self, buffer: &mut [u8]) -> std::io::Result<usize> {
        match self {
            Self::Directory(dir) => dir.write_header(buffer),
            Self::Device(device) => device.write_header(buffer),
            Self::Fifo(fifo) => fifo.write_header(buffer),
            Self::File(file) => file.write_header(buffer),
            Self::Link(link) => link.write_header(buffer),
            Self::Symlink(symlink) => symlink.write_header(buffer),
        }
    }
}

impl<'a, R: AsyncRead + 'a> std::fmt::Debug for TarEntry<'a, R> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::File(file) => f
                .debug_struct("TarEntry::File")
                .field("path_name", &file.path_name)
                .field("size", &file.size)
                .field("mode", &file.mode)
                .field("mtime", &file.mtime())
                .field("atime", &file.times.atime)
                .field("ctime", &file.times.ctime)
                .field("uid", &file.uid)
                .field("gid", &file.gid)
                .field("uname", &file.uname)
                .field("gname", &file.gname)
                .field("attrs", &file.attrs.len())
                .finish(),
            Self::Device(device) => f
                .debug_struct("TarEntry::Device")
                .field("path_name", &device.path_name)
                .field("mode", &device.mode)
                .field("mtime", &device.mtime())
                .field("atime", &device.times.atime)
                .field("ctime", &device.times.ctime)
                .field("uid", &device.uid)
                .field("gid", &device.gid)
                .field("uname", &device.uname)
                .field("gname", &device.gname)
                .field("attrs", &device.attrs.len())
                .field(
                    "kind",
                    match device.kind {
                        DeviceKind::Char => &"char",
                        DeviceKind::Block => &"block",
                    },
                )
                .field("major", &device.major)
                .field("minor", &device.minor)
                .finish(),
            Self::Fifo(fifo) => f
                .debug_struct("TarEntry::Fifo")
                .field("path_name", &fifo.path_name)
                .field("mode", &fifo.mode)
                .field("mtime", &fifo.mtime())
                .field("atime", &fifo.times.atime)
                .field("ctime", &fifo.times.ctime)
                .field("uid", &fifo.uid)
                .field("gid", &fifo.gid)
                .field("uname", &fifo.uname)
                .field("gname", &fifo.gname)
                .field("attrs", &fifo.attrs.len())
                .finish(),
            Self::Link(link) => f
                .debug_struct("TarEntry::Link")
                .field("path_name", &link.path_name)
                .field("link_name", &link.link_name)
                .finish(),
            Self::Symlink(symlink) => f
                .debug_struct("TarEntry::Symlink")
                .field("path_name", &symlink.path_name)
                .field("link_name", &symlink.link_name)
                .field("mode", &symlink.mode)
                .field("mtime", &symlink.mtime())
                .field("atime", &symlink.times.atime)
                .field("ctime", &symlink.times.ctime)
                .field("uid", &symlink.uid)
                .field("gid", &symlink.gid)
                .field("uname", &symlink.uname)
                .field("gname", &symlink.gname)
                .field("attrs", &symlink.attrs.len())
                .finish(),
            Self::Directory(dir) => f
                .debug_struct("TarEntry::Directory")
                .field("path_name", &dir.path_name)
                .field("size", &dir.size)
                .field("mode", &dir.mode)
                .field("mtime", &dir.mtime())
                .field("atime", &dir.times.atime)
                .field("ctime", &dir.times.ctime)
                .field("uid", &dir.uid)
                .field("gid", &dir.gid)
                .field("uname", &dir.uname)
                .field("gname", &dir.gname)
                .field("attrs", &dir.attrs.len())
                .finish(),
        }
    }
}

struct PaxInfo {
    path: Option<Box<str>>,
    linkpath: Option<Box<str>>,
    atime: Option<SystemTime>,
    ctime: Option<SystemTime>,
    mtime: Option<SystemTime>,
    size: Option<u64>,
    uid: Option<u32>,
    gid: Option<u32>,
    uname: Option<Box<str>>,
    gname: Option<Box<str>>,
    attrs: AttrList,
}

fn entry_path(hdr: &Header, path: Option<Box<str>>) -> Result<Box<str>> {
    match hdr.kind() {
        HeaderKind::Gnu(hdr) => Ok(path.map_or_else(|| hdr.path_name(), Ok)?),
        HeaderKind::Ustar(hdr) => Ok(path.map_or_else(|| hdr.path_name(), Ok)?),
        HeaderKind::Old(hdr) => path.map_or_else(|| hdr.path_name(), Ok),
    }
}

fn entry_path_link(
    hdr: &Header,
    path: Option<Box<str>>,
    link: Option<Box<str>>,
) -> Result<(Box<str>, Box<str>)> {
    match hdr.kind() {
        HeaderKind::Gnu(hdr) => Ok((
            path.map_or_else(|| hdr.path_name(), Ok)?,
            link.map_or_else(|| hdr.link_name(), Ok)?,
        )),
        HeaderKind::Ustar(hdr) => Ok((
            path.map_or_else(|| hdr.path_name(), Ok)?,
            link.map_or_else(|| hdr.link_name(), Ok)?,
        )),
        HeaderKind::Old(hdr) => Ok((
            path.map_or_else(|| hdr.path_name(), Ok)?,
            link.map_or_else(|| hdr.link_name(), Ok)?,
        )),
    }
}

fn ext_as_path(hdr: &Header, size: usize, ext: &Option<ExtensionBuffer>) -> Result<Box<str>> {
    if size <= BLOCK_SIZE {
        hdr.as_null_terminated_str(..size)
    } else {
        ext.as_ref().unwrap().as_null_terminated_str(..size)
    }
}
fn ext_as_str(hdr: &Header, size: usize, ext: &Option<ExtensionBuffer>) -> Result<Box<str>> {
    if size <= BLOCK_SIZE {
        hdr.as_str(..size)
    } else {
        ext.as_ref().unwrap().as_str(..size)
    }
    .map(|p| p.to_string().into_boxed_str())
}

fn take_pax_info(exts: &mut Vec<ExtensionHeader>, globs: &[PosixExtension]) -> Result<PaxInfo> {
    let mut info = PaxInfo {
        path: None,
        linkpath: None,
        atime: None,
        ctime: None,
        mtime: None,
        size: None,
        uid: None,
        gid: None,
        uname: None,
        gname: None,
        attrs: AttrList::default(),
    };
    for glob in globs {
        apply_pax_extension(glob, &mut info)?;
    }
    for ext in exts.drain(..) {
        match ext {
            ExtensionHeader::LongName(name) => info.path = Some(name),
            ExtensionHeader::LongLink(name) => info.linkpath = Some(name),
            ExtensionHeader::PosixExtension(ext) => apply_pax_extension(&ext, &mut info)?,
        }
    }
    Ok(info)
}

fn apply_pax_extension(ext: &PosixExtension, info: &mut PaxInfo) -> Result<()> {
    ext.for_each_record(|key, val| {
        if key == "path" {
            info.path = Some(val.to_string().into_boxed_str());
        } else if key == "linkpath" {
            info.linkpath = Some(val.to_string().into_boxed_str());
        } else if key == "atime" {
            info.atime = Some(parse_pax_time(val)?);
        } else if key == "ctime" {
            info.ctime = Some(parse_pax_time(val)?);
        } else if key == "mtime" {
            info.mtime = Some(parse_pax_time(val)?);
        } else if key == "size" {
            info.size = Some(parse_pax_u64(val, "size")?);
        } else if key == "uid" {
            info.uid = Some(parse_pax_u32(val, "uid")?);
        } else if key == "gid" {
            info.gid = Some(parse_pax_u32(val, "gid")?);
        } else if key == "uname" {
            info.uname = Some(val.to_string().into_boxed_str());
        } else if key == "gname" {
            info.gname = Some(val.to_string().into_boxed_str());
        } else if let Some(name) = key.strip_prefix("SCHILY.xattr.") {
            attr_set(&mut info.attrs, name, val.as_bytes());
        }
        Ok(())
    })
}

fn parse_pax_u64(val: &str, key: &str) -> Result<u64> {
    val.parse::<u64>().map_err(|_| {
        Error::new(
            ErrorKind::InvalidData,
            format!("invalid PAX {key} value: {val}"),
        )
    })
}

fn parse_pax_u32(val: &str, key: &str) -> Result<u32> {
    let parsed = parse_pax_u64(val, key)?;
    parsed.try_into().map_err(|_| {
        Error::new(
            ErrorKind::InvalidData,
            format!("PAX {key} value out of range: {val}"),
        )
    })
}

fn parse_pax_time(val: &str) -> Result<SystemTime> {
    let (seconds, fraction) = match val.split_once('.') {
        Some((seconds, fraction)) => (seconds, Some(fraction)),
        None => (val, None),
    };
    let nanos = match fraction {
        None => 0,
        Some(fraction) => {
            if fraction.is_empty() || !fraction.bytes().all(|b| b.is_ascii_digit()) {
                return Err(Error::new(
                    ErrorKind::InvalidData,
                    format!("invalid PAX timestamp value: {val}"),
                ));
            }
            if fraction.len() > 9 && fraction[9..].bytes().any(|b| b != b'0') {
                return Err(Error::new(
                    ErrorKind::InvalidData,
                    format!("PAX timestamp exceeds nanosecond precision: {val}"),
                ));
            }
            let digits = &fraction[..fraction.len().min(9)];
            let scale = 9 - digits.len();
            let raw = digits.parse::<u32>().map_err(|_| {
                Error::new(
                    ErrorKind::InvalidData,
                    format!("invalid PAX timestamp value: {val}"),
                )
            })?;
            raw.saturating_mul(10u32.pow(scale as u32))
        }
    };
    if let Some(seconds) = seconds.strip_prefix('-') {
        let whole = seconds.parse::<u64>().map_err(|_| {
            Error::new(
                ErrorKind::InvalidData,
                format!("invalid PAX timestamp value: {val}"),
            )
        })?;
        UNIX_EPOCH
            .checked_sub(Duration::new(whole, nanos))
            .ok_or_else(|| {
                Error::new(
                    ErrorKind::InvalidData,
                    format!("invalid PAX timestamp value: {val}"),
                )
            })
    } else {
        let whole = seconds.parse::<u64>().map_err(|_| {
            Error::new(
                ErrorKind::InvalidData,
                format!("invalid PAX timestamp value: {val}"),
            )
        })?;
        UNIX_EPOCH
            .checked_add(Duration::new(whole, nanos))
            .ok_or_else(|| {
                Error::new(
                    ErrorKind::InvalidData,
                    format!("invalid PAX timestamp value: {val}"),
                )
            })
    }
}

fn format_pax_time(time: SystemTime) -> String {
    let (negative, delta) = match time.duration_since(UNIX_EPOCH) {
        Ok(delta) => (false, delta),
        Err(_) => (true, UNIX_EPOCH.duration_since(time).unwrap()),
    };
    let whole = delta.as_secs();
    let nanos = delta.subsec_nanos();
    let sign = if negative { "-" } else { "" };
    if nanos == 0 {
        format!("{sign}{whole}")
    } else {
        format!("{sign}{whole}.{nanos:09}")
            .trim_end_matches('0')
            .to_string()
    }
}

fn parse_pax_records<'a>(
    mut ext: &'a str,
    mut cb: impl FnMut(&'a str, &'a str) -> Result<()>,
) -> Result<()> {
    while !ext.is_empty() {
        let space_pos = ext.find(' ').ok_or_else(|| {
            Error::new(
                ErrorKind::InvalidData,
                "malformed PAX record: missing length separator",
            )
        })?;
        let len = ext[..space_pos].parse::<usize>().map_err(|_| {
            Error::new(
                ErrorKind::InvalidData,
                "malformed PAX record: invalid length",
            )
        })?;
        if len == 0 || len > ext.len() || len <= space_pos + 2 || !ext.is_char_boundary(len) {
            return Err(Error::new(
                ErrorKind::InvalidData,
                "malformed PAX record: invalid length",
            ));
        }
        let record = &ext[..len];
        ext = &ext[len..];
        if !record.ends_with('\n') {
            return Err(Error::new(
                ErrorKind::InvalidData,
                "malformed PAX record: missing trailing newline",
            ));
        }
        let payload = &record[space_pos + 1..len - 1];
        let eq_pos = payload.find('=').ok_or_else(|| {
            Error::new(ErrorKind::InvalidData, "malformed PAX record: missing '='")
        })?;
        cb(&payload[..eq_pos], &payload[eq_pos + 1..])?;
    }
    Ok(())
}

fn attr_set(attrs: &mut AttrList, name: &str, value: &[u8]) {
    if let Some(pos) = attrs
        .inner
        .iter()
        .position(|(existing, _)| existing.as_ref() == name)
    {
        attrs.inner.remove(pos);
    }
    attrs.inner.push((
        name.to_string().into_boxed_str(),
        value.to_vec().into_boxed_slice(),
    ));
}

impl<'a, R: AsyncRead + 'a> TarReaderInner<'a, R> {
    fn new(r: R) -> Self {
        Self {
            state: Header,
            pos: 0,
            nxt: BLOCK_SIZE as u64,
            ext: None,
            exts: Vec::new(),
            globs: Vec::new(),
            header: Header::new(),
            reader: r,
            marker: std::marker::PhantomData,
        }
    }
    /// Advance the state machine until the next entry or EOF marker is decoded.
    fn poll_read_header(
        self: Pin<&mut Self>,
        ctx: &mut Context<'_>,
    ) -> Poll<Option<Result<Entry>>> {
        let mut this = self.project();
        loop {
            match this.state {
                Header => {
                    let remaining = *this.nxt - *this.pos;
                    let n = {
                        let filled = BLOCK_SIZE - remaining as usize;
                        let (hdr, reader) = (&mut this.header, &mut this.reader);
                        let n =
                            ready_opt!(poll_read_compat(pin!(reader), ctx, hdr.buf_mut(filled..)));
                        if n == 0 {
                            Err(Error::new(
                                ErrorKind::UnexpectedEof,
                                "Unexpected EOF while reading tar header",
                            ))
                        } else {
                            Ok(n)
                        }
                    }?;
                    *this.pos += n as u64;
                    if remaining != n as u64 {
                        continue;
                    }
                    if this.header.is_zero() {
                        *this.nxt += BLOCK_SIZE as u64;
                        *this.state = Eof;
                        continue;
                    }
                    this.header.validate_checksum()?;
                    let kind = this.header.entry_type().map_err(|t| {
                        Error::new(
                            ErrorKind::InvalidData,
                            format!("header type {} is not supported", t),
                        )
                    })?;
                    return Poll::Ready(Some(match kind {
                        Kind::File | Kind::File0 | Kind::Continous => {
                            let info = take_pax_info(this.exts, this.globs)?;
                            let path_name = entry_path(this.header, info.path.clone())?;
                            let size = effective_size(this.header, info.size)?;
                            let uid = effective_uid(this.header, info.uid)?;
                            let gid = effective_gid(this.header, info.gid)?;
                            let times = effective_times(this.header, &info)?;
                            let uname = effective_uname(this.header, info.uname)?;
                            let gname = effective_gname(this.header, info.gname)?;
                            Ok(if path_name.ends_with('/') && this.header.is_old() {
                                *this.nxt += BLOCK_SIZE as u64;
                                *this.state = Header;
                                Entry::Directory(TarDirectory {
                                    size,
                                    mode: this.header.mode()?,
                                    uid,
                                    gid,
                                    uname,
                                    gname,
                                    times,
                                    path_name,
                                    attrs: info.attrs,
                                })
                            } else {
                                *this.nxt += size;
                                *this.state = Entry;
                                Entry::File {
                                    size,
                                    mode: this.header.mode()?,
                                    uid,
                                    gid,
                                    uname,
                                    gname,
                                    times,
                                    eof: *this.nxt,
                                    path_name,
                                    attrs: info.attrs,
                                }
                            })
                        }
                        Kind::Directory => {
                            let info = take_pax_info(this.exts, this.globs)?;
                            let size = effective_size(this.header, info.size)?;
                            let uid = effective_uid(this.header, info.uid)?;
                            let gid = effective_gid(this.header, info.gid)?;
                            let times = effective_times(this.header, &info)?;
                            let uname = effective_uname(this.header, info.uname)?;
                            let gname = effective_gname(this.header, info.gname)?;
                            *this.nxt += BLOCK_SIZE as u64;
                            *this.state = Header;
                            let path_name = entry_path(this.header, info.path)?;
                            Ok(Entry::Directory(TarDirectory {
                                size,
                                mode: this.header.mode()?,
                                uid,
                                gid,
                                uname,
                                gname,
                                times,
                                path_name,
                                attrs: info.attrs,
                            }))
                        }
                        Kind::Fifo => {
                            let info = take_pax_info(this.exts, this.globs)?;
                            let uid = effective_uid(this.header, info.uid)?;
                            let gid = effective_gid(this.header, info.gid)?;
                            let times = effective_times(this.header, &info)?;
                            let uname = effective_uname(this.header, info.uname)?;
                            let gname = effective_gname(this.header, info.gname)?;
                            *this.nxt += BLOCK_SIZE as u64;
                            *this.state = Header;
                            let path_name = entry_path(this.header, info.path)?;
                            Ok(Entry::Fifo(TarFifo {
                                path_name,
                                mode: this.header.mode()?,
                                uid,
                                gid,
                                uname,
                                gname,
                                times,
                                attrs: info.attrs,
                            }))
                        }
                        Kind::CharDevice | Kind::BlockDevice => {
                            let info = take_pax_info(this.exts, this.globs)?;
                            let uid = effective_uid(this.header, info.uid)?;
                            let gid = effective_gid(this.header, info.gid)?;
                            let times = effective_times(this.header, &info)?;
                            let uname = effective_uname(this.header, info.uname)?;
                            let gname = effective_gname(this.header, info.gname)?;
                            *this.nxt += BLOCK_SIZE as u64;
                            *this.state = Header;
                            let path_name = entry_path(this.header, info.path)?;
                            Ok(Entry::Device(TarDevice {
                                path_name,
                                mode: this.header.mode()?,
                                uid,
                                gid,
                                uname,
                                gname,
                                times,
                                kind: match kind {
                                    Kind::CharDevice => DeviceKind::Char,
                                    Kind::BlockDevice => DeviceKind::Block,
                                    _ => unreachable!(),
                                },
                                major: this.header.dev_major()?,
                                minor: this.header.dev_minor()?,
                                attrs: info.attrs,
                            }))
                        }
                        Kind::Link => {
                            let info = take_pax_info(this.exts, this.globs)?;
                            *this.nxt += BLOCK_SIZE as u64;
                            *this.state = Header;
                            let (path_name, link_name) =
                                entry_path_link(this.header, info.path, info.linkpath)?;
                            let _ = info.attrs;
                            Ok(Entry::Link(TarLink {
                                path_name,
                                link_name,
                            }))
                        }
                        Kind::Symlink => {
                            let info = take_pax_info(this.exts, this.globs)?;
                            let uid = effective_uid(this.header, info.uid)?;
                            let gid = effective_gid(this.header, info.gid)?;
                            let times = effective_times(this.header, &info)?;
                            let uname = effective_uname(this.header, info.uname)?;
                            let gname = effective_gname(this.header, info.gname)?;
                            *this.nxt += BLOCK_SIZE as u64;
                            *this.state = Header;
                            let (path_name, link_name) =
                                entry_path_link(this.header, info.path, info.linkpath)?;
                            Ok(Entry::Symlink(TarSymlink {
                                mode: this.header.mode()?,
                                uid,
                                gid,
                                uname,
                                gname,
                                times,
                                path_name,
                                link_name,
                                attrs: info.attrs,
                            }))
                        }
                        Kind::PAXLocal | Kind::PAXGlobal if this.header.is_ustar() => {
                            let size = this.header.size().and_then(|size| {
                                if size as usize > PAX_HEADER_MAX_SIZE {
                                    Err(Error::new(
                                        ErrorKind::InvalidData,
                                        format!(
                                            "PAX extension exceeds {PAX_HEADER_MAX_SIZE} bytes"
                                        ),
                                    ))
                                } else {
                                    Ok(size as usize)
                                }
                            })?;
                            *this.state = Extension((size as u32, kind));
                            let padded = padded_size(size as u64);
                            *this.nxt += padded;
                            if size > BLOCK_SIZE {
                                this.ext.replace(ExtensionBuffer::new(padded as usize));
                            }
                            continue;
                        }
                        Kind::GNULongName | Kind::GNULongLink if this.header.is_gnu() => {
                            let size = this.header.size().and_then(|size| {
                                if size as usize > PATH_MAX {
                                    Err(Error::new(
                                        ErrorKind::InvalidData,
                                        format!("long filename exceeds {PATH_MAX} bytes"),
                                    ))
                                } else {
                                    Ok(size as usize)
                                }
                            })?;
                            *this.state = Extension((size as u32, kind));
                            let padded = padded_size(size as u64);
                            *this.nxt += padded;
                            if size > BLOCK_SIZE {
                                this.ext.replace(ExtensionBuffer::new(padded as usize));
                            }
                            continue;
                        }
                        kind => Err(Error::new(
                            ErrorKind::InvalidData,
                            format!("invalid tar entry header {}", kind),
                        )),
                    }));
                }
                Extension((size, kind)) => {
                    let (ext, reader) = (&mut this.ext, &mut this.reader);
                    let n = if *size as usize <= BLOCK_SIZE {
                        let remaining = *this.nxt - *this.pos;
                        let filled = BLOCK_SIZE - remaining as usize;
                        let (hdr, reader) = (&mut this.header, &mut this.reader);
                        ready_opt!(poll_read_compat(pin!(reader), ctx, hdr.buf_mut(filled..)))
                    } else {
                        let buf = ext.as_mut().unwrap();
                        let n = ready_opt!(poll_read_compat(pin!(reader), ctx, unsafe {
                            buf.remaining_buf()
                        }));
                        unsafe { buf.advance(n) };
                        n
                    };
                    *this.pos += if n == 0 {
                        Err(Error::new(
                            ErrorKind::UnexpectedEof,
                            "unexpected end of tar file",
                        ))
                    } else {
                        Ok(n as u64)
                    }?;
                    if *this.pos == *this.nxt {
                        match kind {
                            Kind::GNULongName => this.exts.push(ExtensionHeader::LongName(
                                ext_as_path(this.header, *size as usize, ext)?,
                            )),
                            Kind::GNULongLink => this.exts.push(ExtensionHeader::LongLink(
                                ext_as_path(this.header, *size as usize, ext)?,
                            )),
                            Kind::PAXLocal => {
                                let ext = ext_as_str(this.header, *size as usize, ext)?;
                                PosixExtension::validate(&ext)?;
                                this.exts.push(ExtensionHeader::PosixExtension(ext.into()));
                            }
                            Kind::PAXGlobal => {
                                let ext = ext_as_str(this.header, *size as usize, ext)?;
                                PosixExtension::validate(&ext)?;
                                this.globs.push(ext.into());
                            }
                            _ => unreachable!(),
                        };
                        *this.nxt += BLOCK_SIZE as u64;
                        *this.state = Header;
                    }
                    continue;
                }
                Padding => {
                    let remaining = *this.nxt - *this.pos;
                    let (hdr, reader) = (&mut this.header, &mut this.reader);
                    let n = match ready_opt!(poll_read_compat(
                        pin!(reader),
                        ctx,
                        hdr.buf_mut(..remaining as usize)
                    )) {
                        0 => Err(Error::new(
                            ErrorKind::UnexpectedEof,
                            "unexpected end of tar file",
                        )),
                        n => Ok(n as u64),
                    }?;
                    *this.pos += n;
                    if remaining == n {
                        *this.nxt = *this.pos + BLOCK_SIZE as u64;
                        *this.state = Header;
                    }
                    continue;
                }
                Entry => {
                    // there is a file entry that must be either dropped or
                    // read fully to move further
                    return Poll::Pending;
                }
                SkipEntry => {
                    // skipping a entry
                    let nxt = padded_size(*this.nxt);
                    let remaining =
                        std::cmp::min(SKIP_BUFFER_SIZE as u64, nxt - *this.pos) as usize;
                    let n = if remaining > 0 {
                        let buf = if let Some(buf) = this.ext.as_mut() {
                            buf
                        } else {
                            this.ext.replace(ExtensionBuffer::new(SKIP_BUFFER_SIZE));
                            this.ext.as_mut().unwrap()
                        };
                        let reader = &mut this.reader;
                        match ready_opt!(poll_read_compat(pin!(reader), ctx, unsafe {
                            buf.upto(remaining)
                        })) {
                            0 => Err(Error::new(
                                ErrorKind::UnexpectedEof,
                                "unexpected end of tar file",
                            )),

                            n => Ok(n as u64),
                        }
                    } else {
                        Ok(0)
                    }?;
                    *this.pos += n;
                    if *this.pos == nxt {
                        this.ext.take();
                        *this.nxt = *this.pos + BLOCK_SIZE as u64;
                        *this.state = Header;
                    }
                    continue;
                }
                Eof => {
                    let remaining = *this.nxt - *this.pos;
                    let filled = BLOCK_SIZE - remaining as usize;
                    let (hdr, reader) = (&mut this.header, &mut this.reader);
                    let n = match ready_opt!(poll_read_compat(
                        pin!(reader),
                        ctx,
                        hdr.buf_mut(filled..)
                    )) {
                        0 => Err(Error::new(
                            ErrorKind::UnexpectedEof,
                            "unexpected end of tar file",
                        )),
                        n => Ok(n as u64),
                    }?;
                    *this.pos += n;
                    if remaining > n {
                        continue;
                    }
                    return Poll::Ready(if hdr.is_zero() {
                        *this.state = Eoff;
                        None
                    } else {
                        *this.state = Eoff;
                        Some(Err(Error::new(
                            ErrorKind::InvalidData,
                            "unexpected data after first zero block",
                        )))
                    });
                }
                Eoff => {
                    return Poll::Ready(Some(Err(Error::new(
                        ErrorKind::InvalidData,
                        "unexpected read after EOF",
                    ))));
                }
            }
        }
    }
}

impl<'a, R: AsyncRead + 'a> Stream for TarReader<'a, R> {
    type Item = Result<TarEntry<'a, TarRegularFileReader<'a, R>>>;
    fn poll_next(mut self: Pin<&mut Self>, ctx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.as_mut();
        let fut = this.inner.lock();
        let mut g = task::ready!(pin!(fut).poll(ctx));
        let inner: Pin<&mut TarReaderInner<R>> = g.as_mut();
        let entry = {
            match inner.poll_next(ctx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(None) => return Poll::Ready(None),
                Poll::Ready(Some(Err(err))) => return Poll::Ready(Some(Err(err))),
                Poll::Ready(Some(Ok(data))) => data,
            }
        };
        Poll::Ready(Some(Ok(match entry {
            Entry::File {
                size,
                mode,
                uid,
                gid,
                uname,
                gname,
                times,
                eof,
                path_name,
                attrs,
            } => TarEntry::File(TarRegularFile {
                path_name,
                size,
                mode,
                uid,
                gid,
                uname,
                gname,
                times,
                attrs,
                inner: TarRegularFileReader {
                    eof,
                    inner: Arc::clone(&this.inner),
                },
                marker: std::marker::PhantomData,
            }),
            Entry::Directory(d) => TarEntry::Directory(d),
            Entry::Link(l) => TarEntry::Link(l),
            Entry::Symlink(l) => TarEntry::Symlink(l),
            Entry::Device(d) => TarEntry::Device(d),
            Entry::Fifo(f) => TarEntry::Fifo(f),
        })))
    }
}

impl<'a, R: AsyncRead + 'a> Stream for TarReaderInner<'a, R> {
    type Item = Result<Entry>;
    fn poll_next(mut self: Pin<&mut Self>, ctx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.as_mut().poll_read_header(ctx)
    }
}
impl<'a, R: AsyncRead + 'a> TarEntry<'a, R> {
    /// Path of the entry regardless of concrete variant.
    pub fn path(&'_ self) -> &'_ str {
        match self {
            Self::File(f) => &f.path_name,
            Self::Link(l) => &l.path_name,
            Self::Symlink(l) => &l.path_name,
            Self::Directory(d) => &d.path_name,
            Self::Device(d) => &d.path_name,
            Self::Fifo(f) => &f.path_name,
        }
    }
}

#[cfg(feature = "smol")]
impl<'a, R: AsyncRead + 'a> AsyncRead for TarRegularFile<'a, R> {
    fn poll_read(
        self: Pin<&mut Self>,
        ctx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<std::io::Result<usize>> {
        let this = self.project();
        poll_read_compat(pin!(this.inner), ctx, buf)
    }
}

#[cfg(feature = "tokio")]
impl<'a, R: AsyncRead + 'a> AsyncRead for TarRegularFile<'a, R> {
    fn poll_read(
        self: Pin<&mut Self>,
        ctx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        let this = self.project();
        match poll_read_compat(pin!(this.inner), ctx, buf.initialize_unfilled()) {
            Poll::Ready(Ok(n)) => {
                buf.advance(n);
                Poll::Ready(Ok(()))
            }
            Poll::Ready(Err(err)) => Poll::Ready(Err(err)),
            Poll::Pending => Poll::Pending,
        }
    }
}

fn null_terminated(bytes: &[u8]) -> &[u8] {
    &bytes[..bytes
        .iter()
        .position(|b| *b == b'\0')
        .unwrap_or(bytes.len())]
}

fn ustar_path_name(name: &[u8; 100], prefix: &[u8; 155]) -> Result<Box<str>> {
    let (mut size, prefix) = if prefix[0] != b'\0' {
        let prefix = path_name(prefix)?;
        (prefix.len() + 1, Some(prefix))
    } else {
        (0, None)
    };
    let name = path_name(name)?;
    size += name.len();
    let mut path = String::with_capacity(size);
    if let Some(prefix) = prefix {
        path.push_str(prefix);
        path.push('/');
    }
    path.push_str(name);
    Ok(path.into_boxed_str())
}
fn path_name(name: &'_ [u8]) -> Result<&'_ str> {
    from_utf8(null_terminated(name))
        .map_err(|_| Error::new(ErrorKind::InvalidData, "invalid utf8 in file path"))
}

fn parse_name_field(bytes: &[u8]) -> Result<Option<Box<str>>> {
    let bytes = null_terminated(bytes);
    if bytes.is_empty() {
        Ok(None)
    } else {
        from_utf8(bytes)
            .map(|name| Some(name.to_string().into_boxed_str()))
            .map_err(|_| Error::new(ErrorKind::InvalidData, "invalid UTF-8"))
    }
}

fn ustar_name_fits(name: &str) -> bool {
    name.len() < 31
}

fn parse_octal(field: &'_ [u8]) -> std::result::Result<u64, &'_ [u8]> {
    let mut n = 0u64;
    let mut rest = field;
    while let [d, r @ ..] = rest {
        if d == &0 || d == &b' ' {
            break;
        }
        if !(&b'0'..=&b'7').contains(&d) {
            return Err(field);
        }
        rest = r;
        if d == &b'0' && n == 0 {
            continue;
        }
        n = (n << 3) | (u64::from(*d) - u64::from(b'0'));
    }
    Ok(n)
}

const fn padded_size(n: u64) -> u64 {
    if n == 0 {
        0
    } else {
        n.saturating_add(511) & !511
    }
}

#[allow(clippy::too_many_arguments)]
/// Write a single header, plus an optional PAX prefix, into `buffer`.
///
/// The function encodes metadata using the ustar layout and, if either the path
/// or link name is too long, or if PAX attributes are supplied (for example
/// extended attributes), prepends a PAX header containing the full values
/// before duplicating the original header so downstream tools can still read
/// the entry.
fn write_header(
    buffer: &mut [u8],
    name: &str,
    link_name: Option<&str>,
    kind: Kind,
    size: u64,
    mode: u32,
    uid: u32,
    gid: u32,
    uname: Option<&str>,
    gname: Option<&str>,
    times: &EntryTimes,
    device: Option<(u32, u32)>,
    attrs: &AttrList,
) -> std::io::Result<usize> {
    if buffer.len() < BLOCK_SIZE {
        return Err(std::io::Error::other("buffer too small for tar header"));
    }
    let (header_buf, data_buf) = buffer.split_at_mut(BLOCK_SIZE);
    let header = unsafe { UstarHeader::from_buf(header_buf) };
    let mut total = BLOCK_SIZE;

    let split_pos = header.path_split_point(name);
    let path_truncated = if let Some(pos) = split_pos {
        name.len() - pos - 1 > NAME_LEN
    } else {
        name.len() > NAME_LEN
    };
    let link_path_truncated = link_name
        .as_ref()
        .is_some_and(|link_name| link_name.len() > NAME_LEN);
    let supports_pax_metadata = !matches!(kind, Kind::Link);
    let supports_pax_xattrs = !matches!(kind, Kind::Link);

    let mut records = Vec::new();
    if path_truncated {
        records.push(PaxRecord::new("path", name.as_bytes()));
    }
    if link_path_truncated {
        let name = link_name.unwrap();
        records.push(PaxRecord::new("linkpath", name.as_bytes()));
    }
    if supports_pax_metadata {
        if let Some(atime) = times.atime {
            records.push(PaxRecord::string("atime", format_pax_time(atime)));
        }
        if let Some(ctime) = times.ctime {
            records.push(PaxRecord::string("ctime", format_pax_time(ctime)));
        }
    }
    let header_mtime = header_mtime_value(times.mtime);
    let mtime_requires_pax = supports_pax_metadata
        && (header_mtime.is_none() || !octal_fits(header_mtime.unwrap_or(0), 12));
    let stored_header_mtime = if mtime_requires_pax {
        0
    } else {
        header_mtime.unwrap_or(0)
    };
    if mtime_requires_pax {
        records.push(PaxRecord::string("mtime", format_pax_time(times.mtime)));
    }
    let uid_requires_pax = supports_pax_metadata && !octal_fits(u64::from(uid), 8);
    if uid_requires_pax {
        records.push(PaxRecord::string("uid", uid.to_string()));
    }
    let gid_requires_pax = supports_pax_metadata && !octal_fits(u64::from(gid), 8);
    if gid_requires_pax {
        records.push(PaxRecord::string("gid", gid.to_string()));
    }
    let uname_requires_pax =
        supports_pax_metadata && uname.is_some_and(|uname| !ustar_name_fits(uname));
    if let Some(uname) = uname.filter(|uname| !ustar_name_fits(uname)) {
        records.push(PaxRecord::string("uname", uname.to_string()));
    }
    let gname_requires_pax =
        supports_pax_metadata && gname.is_some_and(|gname| !ustar_name_fits(gname));
    if let Some(gname) = gname.filter(|gname| !ustar_name_fits(gname)) {
        records.push(PaxRecord::string("gname", gname.to_string()));
    }
    let size_requires_pax = supports_pax_metadata && !octal_fits(size, 12);
    if size_requires_pax {
        records.push(PaxRecord::string("size", size.to_string()));
    }
    if supports_pax_xattrs && !attrs.is_empty() {
        for (name, value) in attrs.iter() {
            records.push(pax_xattr_record(name, value)?);
        }
    }

    if records.is_empty() {
        header.set_uid(uid)?;
        header.set_gid(gid)?;
        if let Some(uname) = uname.filter(|uname| ustar_name_fits(uname)) {
            header.set_uname(uname);
        }
        if let Some(gname) = gname.filter(|gname| ustar_name_fits(gname)) {
            header.set_gname(gname);
        }
        header.set_mode(mode)?;
        header.set_mtime(stored_header_mtime)?;
        header.set_size(size)?;
        header.set_path(name, split_pos);
        if let Some(link_name) = link_name {
            header.set_link_path(link_name);
        }
        if let Some((major, minor)) = device {
            header.set_dev_major(major)?;
            header.set_dev_minor(minor)?;
        }
        header.set_typeflag(kind);
        header.finalize()?;
    } else {
        header.set_typeflag(Kind::PAXLocal);
        header.set_path("././@PaxHeader", None);
        header.set_uid(0)?;
        header.set_gid(0)?;
        header.set_mode(0)?;
        header.set_mtime(0)?;
        let mut ext_size = 0;
        for record in &records {
            ext_size += pax_record_len(&record.key, record.value.len());
        }
        if data_buf.len() < ext_size {
            return Err(std::io::Error::other("buffer too small for pax header"));
        }
        let mut offset = 0;
        for record in records {
            let rec_len = pax_record_len(&record.key, record.value.len());
            if data_buf.len() < offset + rec_len {
                return Err(std::io::Error::other("buffer too small for pax header"));
            }
            write_pax_record(
                &mut data_buf[offset..offset + rec_len],
                &record.key,
                &record.value,
                rec_len,
            )?;
            offset += rec_len;
        }
        header.set_size(ext_size as u64)?;
        header.finalize()?;
        let padded = padded_size(ext_size as u64);
        if data_buf.len() < padded as usize {
            return Err(std::io::Error::other("buffer too small for pax header"));
        }
        data_buf[ext_size..padded as usize].fill(0);
        total += padded as usize;
        if data_buf.len() < padded as usize + BLOCK_SIZE {
            return Err(std::io::Error::other("buffer too small for pax header"));
        }
        let header = unsafe {
            UstarHeader::from_buf(&mut data_buf[padded as usize..padded as usize + BLOCK_SIZE])
        };
        total += BLOCK_SIZE;
        header.set_uid(if uid_requires_pax { 0 } else { uid })?;
        header.set_gid(if gid_requires_pax { 0 } else { gid })?;
        if !uname_requires_pax {
            if let Some(uname) = uname {
                header.set_uname(uname);
            }
        }
        if !gname_requires_pax {
            if let Some(gname) = gname {
                header.set_gname(gname);
            }
        }
        header.set_mode(mode)?;
        header.set_mtime(stored_header_mtime)?;
        header.set_size(if size_requires_pax { 0 } else { size })?;
        header.set_typeflag(kind);
        header.set_path(name, split_pos);
        if let Some(link_name) = link_name {
            header.set_link_path(link_name);
        }
        if let Some((major, minor)) = device {
            header.set_dev_major(major)?;
            header.set_dev_minor(minor)?;
        }
        header.finalize()?;
    }
    Ok(total)
}

fn pax_record_len(key: &str, val_len: usize) -> usize {
    // <LEN> SP <KEY>=<VALUE>\n
    let payload_len = key.len() + 1 + val_len + 1;
    let mut len = payload_len + 1 + 1;
    loop {
        let d = num_decimal_digits(len);
        let new_len = payload_len + 1 + d;

        if new_len == len {
            return len;
        }
        len = new_len;
    }
}
#[inline]
fn num_decimal_digits(mut n: usize) -> usize {
    let mut c = 1;
    while n >= 10 {
        n /= 10;
        c += 1;
    }
    c
}

fn write_pax_record(
    buf: &mut [u8],
    key: &str,
    value: &[u8],
    rec_len: usize,
) -> std::io::Result<()> {
    if buf.len() < rec_len {
        return Err(std::io::Error::other("buffer too small for pax record"));
    }
    let len_str = rec_len.to_string();
    let expected = len_str.len() + 1 + key.len() + 1 + value.len() + 1;
    if expected != rec_len {
        return Err(std::io::Error::other("pax record length mismatch"));
    }
    let mut offset = 0;
    buf[..len_str.len()].copy_from_slice(len_str.as_bytes());
    offset += len_str.len();
    buf[offset] = b' ';
    offset += 1;
    buf[offset..offset + key.len()].copy_from_slice(key.as_bytes());
    offset += key.len();
    buf[offset] = b'=';
    offset += 1;
    buf[offset..offset + value.len()].copy_from_slice(value);
    offset += value.len();
    buf[offset] = b'\n';
    Ok(())
}

struct PaxRecord {
    key: String,
    value: Vec<u8>,
}

impl PaxRecord {
    fn new(key: &str, value: &[u8]) -> Self {
        Self {
            key: key.to_string(),
            value: value.to_vec(),
        }
    }

    fn string(key: &str, value: String) -> Self {
        Self {
            key: key.to_string(),
            value: value.into_bytes(),
        }
    }
}

fn pax_xattr_record(name: &str, value: &[u8]) -> std::io::Result<PaxRecord> {
    if !xattr_name_is_pax_safe(name) {
        return Err(std::io::Error::other(
            "xattr name contains non-portable characters",
        ));
    }
    Ok(PaxRecord::new(&format!("SCHILY.xattr.{name}"), value))
}

fn xattr_name_is_pax_safe(name: &str) -> bool {
    name.bytes().all(|b| b.is_ascii_graphic() && b != b'=')
}

fn octal_fits(value: u64, field_len: usize) -> bool {
    let digits = if value == 0 {
        1
    } else {
        (64 - value.leading_zeros()).div_ceil(3)
    } as usize;
    digits < field_len
}

fn format_octal(val: u64, field: &mut [u8]) -> std::io::Result<()> {
    let mut value = val;
    let mut len = field.len() - 1;
    field[len] = 0; // null terminator
    while len > 0 {
        len -= 1;
        field[len] = b'0' + (value & 0o7) as u8;
        value >>= 3;
    }
    if value != 0 {
        return Err(std::io::Error::other(format!(
            "value {} too large to fit in octal field of len {}",
            val,
            field.len()
        )));
    }
    Ok(())
}

pin_project! {
    /// Streaming tar writer that implements `future_sink::Sink<TarEntry<...>>`.
    ///
    /// Headers and file payloads are staged inside `buf` until downstream I/O
    /// makes progress, which keeps memory usage predictable while preserving
    /// proper block alignment.
    ///
    /// The composable `Sink` interface is handy, but if bringing in the
    /// `futures` crate feels a bit much, the writer also provides inherent
    /// methods. Use [`TarWriter::write`] to send entries to the tar stream, and
    /// [`TarWriter::finish`] once all entries have been written so the
    /// terminating zero blocks required by the format are emitted.
    /// [`future_sink::Sink::close`] performs the same operation.
    ///
    /// Type inference usually determines `R` from the file entries you send. If
    /// you only send metadata-only entries such as directories, you may need an
    /// explicit `TarWriter::<_, SomeReaderType>::new(...)` annotation.
    ///
    /// # Example
    ///
    /// ```rust
    /// # #[cfg(feature = "smol")]
    /// # use {
    /// #     smol::io::{Cursor, Empty},
    /// # };
    /// # #[cfg(feature = "tokio")]
    /// # use {
    /// #     std::io::Cursor,
    /// #     tokio::io::Empty,
    /// # };
    /// use smol_tar::{TarDirectory, TarEntry, TarWriter};
    ///
    /// # #[cfg(feature = "smol")]
    /// # fn block_on<F: std::future::Future>(fut: F) -> F::Output {
    /// #   smol::future::block_on(fut)
    /// # }
    /// # #[cfg(feature = "tokio")]
    /// # fn block_on<F: std::future::Future>(fut: F) -> F::Output {
    /// #   tokio::runtime::Builder::new_current_thread().build().unwrap().block_on(fut)
    /// # }
    /// # block_on(async {
    /// let sink = Cursor::new(Vec::<u8>::new());
    /// let mut tar = TarWriter::<_, Empty>::new(sink);
    ///
    /// tar.write(TarEntry::from(TarDirectory::new("bin/"))).await?;
    /// tar.finish().await?;
    /// # std::io::Result::Ok(())
    /// # }).unwrap();
    /// ```
    pub struct TarWriter<'a, 'b, W, R> {
        // internal buffer for writing headers and file data
        buf: [u8; BLOCK_SIZE * 32],
        // length of valid data in the buffer
        len: usize,
        // current position in the buffer
        pos: usize,
        // current global position (number of bytes written)
        total: u64,
        // the end position of the current entry being written
        eof: u64,
        // closed
        closed: bool,
        // reader for the current file being written
        reader: Option<R>,
        marker_: std::marker::PhantomData<&'b ()>,
        // the underlying writer
        #[pin]
        writer: W,
        marker: std::marker::PhantomData<&'a ()>,
    }
}

impl<'a, 'b, W: AsyncWrite + 'a, R: AsyncRead + Unpin + 'b> TarWriter<'a, 'b, W, R> {
    /// Create a writer that targets the provided [`AsyncWrite`] sink.
    pub fn new(writer: W) -> Self {
        Self {
            buf: [0; BLOCK_SIZE * 32],
            len: 0,
            pos: 0,
            total: 0,
            eof: 0,
            closed: false,
            reader: None,
            marker_: std::marker::PhantomData,
            writer,
            marker: std::marker::PhantomData,
        }
    }

    /// Write a single TAR entry and flush it to the underlying writer.
    pub async fn write(&mut self, entry: TarEntry<'b, R>) -> std::io::Result<()> {
        poll_fn(|cx| {
            // SAFETY: the mutable borrow of `self` is held for the duration of
            // this async method, so the writer cannot be moved while polled.
            unsafe { Pin::new_unchecked(&mut *self) }.poll_ready(cx)
        })
        .await?;

        // SAFETY: same as above; `start_send` does not move out of `self`.
        unsafe { Pin::new_unchecked(&mut *self) }.start_send(entry)?;

        poll_fn(|cx| {
            // SAFETY: the mutable borrow of `self` is held for the duration of
            // this async method, so the writer cannot be moved while polled.
            unsafe { Pin::new_unchecked(&mut *self) }.poll_flush(cx)
        })
        .await
    }

    /// Finish the archive by emitting the trailing zero blocks and closing.
    pub async fn finish(&mut self) -> std::io::Result<()> {
        poll_fn(|cx| {
            // SAFETY: the mutable borrow of `self` is held for the duration of
            // this async method, so the writer cannot be moved while polled.
            unsafe { Pin::new_unchecked(&mut *self) }.poll_close(cx)
        })
        .await
    }

    /// Drain internal buffer and current file reader to the underlying writer.
    ///
    /// The method alternates between flushing buffered headers, consuming the
    /// reader for the active file, and emitting zero padding so the next entry
    /// always starts on a block boundary.
    fn poll_drain(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        let mut this = self.project();
        loop {
            while *this.pos < *this.len {
                let n = task::ready!(this
                    .writer
                    .as_mut()
                    .poll_write(cx, &this.buf[*this.pos..*this.len]))?;
                if n == 0 {
                    return Poll::Ready(Err(std::io::Error::new(
                        std::io::ErrorKind::WriteZero,
                        "error writing buffer",
                    )));
                }
                *this.pos += n;
                *this.total += n as u64;
            }

            *this.pos = 0;
            *this.len = 0;

            if let Some(reader) = this.reader.as_mut() {
                let remain = this.eof.saturating_sub(*this.total);
                if remain == 0 {
                    *this.reader = None;
                    let padded = padded_size(*this.eof);
                    let padding = padded.saturating_sub(*this.eof);
                    if padding > 0 {
                        *this.len = padding as usize;
                        this.buf[..*this.len].fill(0);
                        *this.eof = padded;
                        continue;
                    } else {
                        return Poll::Ready(Ok(()));
                    }
                }
                let buf_len = std::cmp::min(this.buf.len() as u64, remain) as usize;
                let n = task::ready!(poll_read_compat(pin!(reader), cx, &mut this.buf[..buf_len]))?;
                if n == 0 {
                    return Poll::Ready(Err(std::io::Error::new(
                        std::io::ErrorKind::UnexpectedEof,
                        "unexpected EOF while reading file",
                    )));
                }
                *this.pos = 0;
                *this.len = n;
                continue;
            } else {
                return Poll::Ready(Ok(()));
            }
        }
    }
}

impl<'a, 'b, W: AsyncWrite + 'a, R: AsyncRead + Unpin + 'b> Sink<TarEntry<'b, R>>
    for TarWriter<'a, 'b, W, R>
{
    type Error = std::io::Error;

    fn poll_ready(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        self.poll_drain(cx)
    }

    fn start_send(self: Pin<&mut Self>, item: TarEntry<'b, R>) -> std::io::Result<()> {
        let this = self.project();

        if *this.len != 0 || this.reader.is_some() {
            return Err(std::io::Error::other(
                "start_send called while previous entry still in progress",
            ));
        }

        let header_len = item.write_header(this.buf)?;
        *this.len = header_len;

        if let TarEntry::File(file) = item {
            this.reader.replace(file.inner);
            *this.eof = *this.total + (header_len as u64) + file.size;
        }

        Ok(())
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        task::ready!(self.as_mut().poll_drain(cx))?;
        let mut this = self.project();
        task::ready!(this.writer.as_mut().poll_flush(cx))?;
        Poll::Ready(Ok(()))
    }

    fn poll_close(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        task::ready!(self.as_mut().poll_drain(cx))?;
        {
            let this = self.as_mut().project();
            if !*this.closed {
                this.buf[..BLOCK_SIZE * 2].fill(0);
                *this.len = BLOCK_SIZE * 2;
                *this.closed = true;
            }
        }
        task::ready!(self.as_mut().poll_drain(cx))?;
        let mut this = self.project();
        poll_close_compat(this.writer.as_mut(), cx)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use static_assertions::{assert_eq_align, assert_eq_size, assert_impl_all, assert_obj_safe};

    #[cfg(feature = "smol")]
    type AssertFile = smol::fs::File;
    #[cfg(feature = "tokio")]
    type AssertFile = tokio::fs::File;

    assert_impl_all!(TarReader<AssertFile>: Send, Sync);
    assert_impl_all!(TarEntry<AssertFile>: Send, Sync);
    assert_obj_safe!(TarReader<AssertFile>);
    assert_obj_safe!(TarEntry<AssertFile>);

    assert_eq_align!(Header, GnuHeader);
    assert_eq_size!(Header, GnuHeader);
    assert_eq_align!(Header, UstarHeader);
    assert_eq_size!(Header, UstarHeader);
    assert_eq_align!(Header, OldHeader);
    assert_eq_size!(Header, OldHeader);

    #[test]
    fn low_level_format_helpers_cover_edge_cases() {
        assert_eq!(padded_size(0), 0);
        assert_eq!(padded_size(1), BLOCK_SIZE as u64);

        let mut field = [0u8; 2];
        let err = format_octal(8, &mut field).unwrap_err();
        assert!(err.to_string().contains("too large to fit"));

        let rec_len = pax_record_len("path", 3);
        let err = write_pax_record(&mut vec![0; rec_len - 1], "path", b"abc", rec_len).unwrap_err();
        assert!(err.to_string().contains("buffer too small"));

        let err =
            write_pax_record(&mut vec![0; rec_len + 1], "path", b"abc", rec_len + 1).unwrap_err();
        assert!(err.to_string().contains("length mismatch"));
    }

    #[test]
    fn posix_extension_and_kind_helpers_are_exercised() {
        let ext = PosixExtension::from("13 path=file\n".to_string().into_boxed_str());
        assert_eq!(&*ext, "13 path=file\n");

        let long_name = ExtensionHeader::LongName("name".to_string().into_boxed_str());
        assert_eq!(&*long_name, "name");

        let long_link = ExtensionHeader::LongLink("link".to_string().into_boxed_str());
        assert_eq!(&*long_link, "link");

        let pax = ExtensionHeader::PosixExtension(ext);
        assert_eq!(&*pax, "13 path=file\n");

        for (kind, expected) in [
            (Kind::File, "regular file"),
            (Kind::Link, "link"),
            (Kind::Symlink, "symlink"),
            (Kind::CharDevice, "character device"),
            (Kind::BlockDevice, "block device"),
            (Kind::Directory, "directory"),
            (Kind::Fifo, "FIFO"),
            (Kind::GNULongName, "GNU long name extension"),
            (Kind::GNULongLink, "GNU long link extension"),
            (Kind::PAXLocal, "PAX next file extension"),
            (Kind::PAXGlobal, "PAX global extension"),
        ] {
            assert_eq!(kind.to_string(), expected);
        }
    }

    #[test]
    fn write_header_reports_small_buffers() {
        let err = write_header(
            &mut [0u8; BLOCK_SIZE - 1],
            "file.txt",
            None,
            Kind::File,
            0,
            0o644,
            0,
            0,
            None,
            None,
            &EntryTimes::default(),
            None,
            &AttrList::default(),
        )
        .unwrap_err();
        assert!(err.to_string().contains("buffer too small for tar header"));

        let mut small = vec![0u8; BLOCK_SIZE + 10];
        let err = write_header(
            &mut small,
            "file.txt",
            None,
            Kind::File,
            0,
            0o644,
            0,
            0,
            None,
            None,
            &EntryTimes::default(),
            None,
            &AttrList::new().with("user.comment", b"hello".as_slice()),
        )
        .unwrap_err();
        assert!(err.to_string().contains("buffer too small for pax header"));
    }
}
