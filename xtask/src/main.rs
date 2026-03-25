use std::{
    env, fs, io,
    path::{Path, PathBuf},
};

const BLOCK_SIZE: usize = 512;

const NAME_START: usize = 0;
const MODE_START: usize = 100;
const UID_START: usize = 108;
const GID_START: usize = 116;
const SIZE_START: usize = 124;
const MTIME_START: usize = 136;
const CKSUM_START: usize = 148;
const TYPEFLAG_START: usize = 156;
const LINKNAME_START: usize = 157;
const MAGIC_START: usize = 257;
const VERSION_START: usize = 263;
const UNAME_START: usize = 265;
const GNAME_START: usize = 297;
const GNU_ATIME_START: usize = 345;
const GNU_CTIME_START: usize = 357;

#[derive(Clone, Copy)]
enum HeaderFlavor {
    Old,
    Ustar,
    Gnu,
}

fn main() -> io::Result<()> {
    match env::args().nth(1).as_deref() {
        Some("gen-fuzz-seeds") => gen_fuzz_seeds(),
        Some(_) => {
            print_usage();
            Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "unknown xtask subcommand",
            ))
        }
        None => {
            print_usage();
            Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "missing xtask subcommand",
            ))
        }
    }
}

fn print_usage() {
    eprintln!("usage: cargo run -p xtask -- gen-fuzz-seeds");
}

fn gen_fuzz_seeds() -> io::Result<()> {
    let out_dir = repo_root().join("tests/data");
    fs::create_dir_all(&out_dir)?;

    write_seed(
        &out_dir,
        "seed_pax_extended_metadata.tar",
        seed_pax_extended_metadata(),
    )?;
    write_seed(
        &out_dir,
        "seed_symbolic_names_and_times.tar",
        seed_symbolic_names_and_times(),
    )?;
    write_seed(
        &out_dir,
        "seed_pax_symbolic_name_precedence.tar",
        seed_pax_symbolic_name_precedence(),
    )?;
    write_seed(
        &out_dir,
        "seed_pax_xattr_precedence.tar",
        seed_pax_xattr_precedence(),
    )?;
    write_seed(
        &out_dir,
        "seed_pax_overrides_gnu_times.tar",
        seed_pax_overrides_gnu_times(),
    )?;
    write_seed(
        &out_dir,
        "seed_hardlink_ignores_pax_metadata.tar",
        seed_hardlink_ignores_pax_metadata(),
    )?;
    write_seed(
        &out_dir,
        "seed_old_style_directory.tar",
        seed_old_style_directory(),
    )?;
    write_seed(
        &out_dir,
        "seed_invalid_malformed_pax.tar",
        seed_invalid_malformed_pax(),
    )?;
    write_seed(
        &out_dir,
        "seed_invalid_pax_uid_overflow.tar",
        seed_invalid_pax_uid_overflow(),
    )?;
    write_seed(
        &out_dir,
        "seed_invalid_header_uname_utf8.tar",
        seed_invalid_header_uname_utf8(),
    )?;

    Ok(())
}

fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("xtask crate should live under the repo root")
        .to_path_buf()
}

fn write_seed(out_dir: &Path, name: &str, bytes: Vec<u8>) -> io::Result<()> {
    fs::write(out_dir.join(name), bytes)
}

fn seed_pax_extended_metadata() -> Vec<u8> {
    let mut archive = Vec::new();
    let mut local = Vec::new();
    local.extend_from_slice(&pax_record_bytes("mtime", b"7.5"));
    local.extend_from_slice(&pax_record_bytes("atime", b"1.25"));
    local.extend_from_slice(&pax_record_bytes("ctime", b"-2.75"));
    local.extend_from_slice(&pax_record_bytes("size", b"5"));
    local.extend_from_slice(&pax_record_bytes("uid", b"123"));
    local.extend_from_slice(&pax_record_bytes("gid", b"456"));
    append_pax_entry(&mut archive, b'x', &local);
    append_entry(
        &mut archive,
        build_header(b"file.txt", b'0', 0, HeaderFlavor::Ustar),
        b"hello",
    );
    append_eof(&mut archive);
    archive
}

fn seed_symbolic_names_and_times() -> Vec<u8> {
    let mut archive = Vec::new();

    let mut ustar = build_header(b"ustar-file.txt", b'0', 0, HeaderFlavor::Ustar);
    set_bytes(&mut ustar, UNAME_START, 32, b"builder");
    set_bytes(&mut ustar, GNAME_START, 32, b"staff");
    finalize_checksum(&mut ustar);
    append_entry(&mut archive, ustar, b"");

    let mut gnu = build_header(b"gnu-file.txt", b'0', 0, HeaderFlavor::Gnu);
    set_bytes(&mut gnu, UNAME_START, 32, b"gnu-user");
    set_bytes(&mut gnu, GNAME_START, 32, b"gnu-group");
    write_octal(7, &mut gnu[MTIME_START..MTIME_START + 12]);
    write_octal(3, &mut gnu[GNU_ATIME_START..GNU_ATIME_START + 12]);
    write_octal(5, &mut gnu[GNU_CTIME_START..GNU_CTIME_START + 12]);
    finalize_checksum(&mut gnu);
    append_entry(&mut archive, gnu, b"");

    let mut local = Vec::new();
    local.extend_from_slice(&pax_record_bytes("uname", b"pax-user"));
    local.extend_from_slice(&pax_record_bytes("gname", b"pax-group"));
    append_pax_entry(&mut archive, b'x', &local);

    let mut pax_file = build_header(b"pax-file.txt", b'0', 0, HeaderFlavor::Ustar);
    set_bytes(&mut pax_file, UNAME_START, 32, b"header-user");
    set_bytes(&mut pax_file, GNAME_START, 32, b"header-group");
    finalize_checksum(&mut pax_file);
    append_entry(&mut archive, pax_file, b"");

    append_eof(&mut archive);
    archive
}

fn seed_pax_symbolic_name_precedence() -> Vec<u8> {
    let mut archive = Vec::new();
    let mut global = Vec::new();
    global.extend_from_slice(&pax_record_bytes("uname", b"global-user"));
    global.extend_from_slice(&pax_record_bytes("gname", b"global-group"));
    append_pax_entry(&mut archive, b'g', &global);

    let mut local = Vec::new();
    local.extend_from_slice(&pax_record_bytes("uname", b"local-user"));
    local.extend_from_slice(&pax_record_bytes("gname", b"local-group"));
    append_pax_entry(&mut archive, b'x', &local);

    append_entry(
        &mut archive,
        build_header(b"local.txt", b'0', 0, HeaderFlavor::Ustar),
        b"",
    );
    append_entry(
        &mut archive,
        build_header(b"global.txt", b'0', 0, HeaderFlavor::Ustar),
        b"",
    );

    append_eof(&mut archive);
    archive
}

fn seed_pax_xattr_precedence() -> Vec<u8> {
    let mut archive = Vec::new();
    let global = pax_record_bytes("SCHILY.xattr.user.scope", b"global");
    append_pax_entry(&mut archive, b'g', &global);

    let mut local = Vec::new();
    local.extend_from_slice(&pax_record_bytes("path", b"local-name.txt"));
    local.extend_from_slice(&pax_record_bytes("SCHILY.xattr.user.scope", b"local"));
    local.extend_from_slice(&pax_record_bytes("SCHILY.xattr.user.dupe", b"first"));
    local.extend_from_slice(&pax_record_bytes("SCHILY.xattr.user.dupe", b"second"));
    append_pax_entry(&mut archive, b'x', &local);

    append_entry(
        &mut archive,
        build_header(b"header-name.txt", b'0', 0, HeaderFlavor::Ustar),
        b"",
    );
    append_entry(
        &mut archive,
        build_header(b"second.txt", b'0', 0, HeaderFlavor::Ustar),
        b"",
    );
    append_eof(&mut archive);
    archive
}

fn seed_pax_overrides_gnu_times() -> Vec<u8> {
    let mut archive = Vec::new();
    let mut local = Vec::new();
    local.extend_from_slice(&pax_record_bytes("mtime", b"7.5"));
    local.extend_from_slice(&pax_record_bytes("atime", b"1.25"));
    local.extend_from_slice(&pax_record_bytes("ctime", b"-2"));
    append_pax_entry(&mut archive, b'x', &local);

    let mut header = build_header(b"file.txt", b'0', 0, HeaderFlavor::Gnu);
    write_octal(3, &mut header[MTIME_START..MTIME_START + 12]);
    write_octal(4, &mut header[GNU_ATIME_START..GNU_ATIME_START + 12]);
    write_octal(5, &mut header[GNU_CTIME_START..GNU_CTIME_START + 12]);
    finalize_checksum(&mut header);
    append_entry(&mut archive, header, b"");
    append_eof(&mut archive);
    archive
}

fn seed_hardlink_ignores_pax_metadata() -> Vec<u8> {
    let mut archive = Vec::new();
    let mut local = Vec::new();
    local.extend_from_slice(&pax_record_bytes("uname", b"ignored-user"));
    local.extend_from_slice(&pax_record_bytes("gname", b"ignored-group"));
    local.extend_from_slice(&pax_record_bytes("SCHILY.xattr.user.hard", b"ignored"));
    append_pax_entry(&mut archive, b'x', &local);

    let mut header = build_header(b"hard", b'1', 0, HeaderFlavor::Ustar);
    set_bytes(&mut header, LINKNAME_START, 100, b"target");
    set_bytes(&mut header, UNAME_START, 32, &[0xff]);
    set_bytes(&mut header, GNAME_START, 32, &[0xfe]);
    finalize_checksum(&mut header);
    append_entry(&mut archive, header, b"");
    append_eof(&mut archive);
    archive
}

fn seed_old_style_directory() -> Vec<u8> {
    let mut archive = Vec::new();
    append_entry(
        &mut archive,
        build_header(b"legacy-dir/", b'0', 0, HeaderFlavor::Old),
        b"",
    );
    append_eof(&mut archive);
    archive
}

fn seed_invalid_malformed_pax() -> Vec<u8> {
    let mut archive = Vec::new();
    append_pax_entry(&mut archive, b'x', b"9 path=aX");
    append_eof(&mut archive);
    archive
}

fn seed_invalid_pax_uid_overflow() -> Vec<u8> {
    let mut archive = Vec::new();
    let payload = pax_record_bytes("uid", b"4294967296");
    append_pax_entry(&mut archive, b'x', &payload);
    append_entry(
        &mut archive,
        build_header(b"file.txt", b'0', 0, HeaderFlavor::Ustar),
        b"",
    );
    append_eof(&mut archive);
    archive
}

fn seed_invalid_header_uname_utf8() -> Vec<u8> {
    let mut archive = Vec::new();
    let mut header = build_header(b"file.txt", b'0', 0, HeaderFlavor::Ustar);
    set_bytes(&mut header, UNAME_START, 32, &[0xff]);
    finalize_checksum(&mut header);
    append_entry(&mut archive, header, b"");
    append_eof(&mut archive);
    archive
}

fn append_pax_entry(archive: &mut Vec<u8>, typeflag: u8, payload: &[u8]) {
    append_entry(
        archive,
        build_header(
            b"././@PaxHeader",
            typeflag,
            payload.len() as u64,
            HeaderFlavor::Ustar,
        ),
        payload,
    );
}

fn write_octal(value: u64, field: &mut [u8]) {
    field.fill(0);
    let mut value = value;
    let mut index = field.len() - 1;
    while index > 0 {
        index -= 1;
        field[index] = b'0' + (value & 0o7) as u8;
        value >>= 3;
    }
    assert_eq!(value, 0, "value does not fit into tar field");
}

fn set_bytes(block: &mut [u8; BLOCK_SIZE], start: usize, len: usize, bytes: &[u8]) {
    assert!(bytes.len() <= len);
    block[start..start + len].fill(0);
    block[start..start + bytes.len()].copy_from_slice(bytes);
}

fn tar_checksum(block: &[u8; BLOCK_SIZE]) -> u32 {
    block
        .iter()
        .enumerate()
        .map(|(index, byte)| {
            if (CKSUM_START..CKSUM_START + 8).contains(&index) {
                u32::from(b' ')
            } else {
                u32::from(*byte)
            }
        })
        .sum()
}

fn finalize_checksum(block: &mut [u8; BLOCK_SIZE]) {
    block[CKSUM_START..CKSUM_START + 8].fill(b' ');
    write_octal(
        tar_checksum(block) as u64,
        &mut block[CKSUM_START..CKSUM_START + 8],
    );
}

fn build_header(name: &[u8], typeflag: u8, size: u64, flavor: HeaderFlavor) -> [u8; BLOCK_SIZE] {
    let mut block = [0u8; BLOCK_SIZE];
    set_bytes(&mut block, NAME_START, 100, name);
    write_octal(0o644, &mut block[MODE_START..MODE_START + 8]);
    write_octal(0, &mut block[UID_START..UID_START + 8]);
    write_octal(0, &mut block[GID_START..GID_START + 8]);
    write_octal(size, &mut block[SIZE_START..SIZE_START + 12]);
    write_octal(0, &mut block[MTIME_START..MTIME_START + 12]);
    block[TYPEFLAG_START] = typeflag;
    match flavor {
        HeaderFlavor::Old => {}
        HeaderFlavor::Ustar => {
            set_bytes(&mut block, MAGIC_START, 6, b"ustar\0");
            set_bytes(&mut block, VERSION_START, 2, b"00");
        }
        HeaderFlavor::Gnu => {
            set_bytes(&mut block, MAGIC_START, 6, b"ustar ");
            set_bytes(&mut block, VERSION_START, 2, b" \0");
        }
    }
    finalize_checksum(&mut block);
    block
}

fn append_entry(archive: &mut Vec<u8>, header: [u8; BLOCK_SIZE], data: &[u8]) {
    archive.extend_from_slice(&header);
    archive.extend_from_slice(data);
    let padded_len = archive.len().next_multiple_of(BLOCK_SIZE);
    archive.resize(padded_len, 0);
}

fn append_eof(archive: &mut Vec<u8>) {
    archive.extend_from_slice(&[0u8; BLOCK_SIZE * 2]);
}

fn pax_record_bytes(key: &str, value: &[u8]) -> Vec<u8> {
    let payload_len = key.len() + 1 + value.len() + 1;
    let mut len = payload_len + 2;
    loop {
        let digits = len.to_string().len();
        let next = payload_len + 1 + digits;
        if next == len {
            break;
        }
        len = next;
    }
    let mut record = Vec::with_capacity(len);
    record.extend_from_slice(len.to_string().as_bytes());
    record.push(b' ');
    record.extend_from_slice(key.as_bytes());
    record.push(b'=');
    record.extend_from_slice(value);
    record.push(b'\n');
    assert_eq!(record.len(), len);
    record
}
