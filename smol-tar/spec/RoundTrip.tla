----------------------------- MODULE RoundTrip -----------------------------
(***************************************************************************)
(* Round trip of smol-tar: TarWriter output goes into TarReader.           *)
(*                                                                         *)
(* Source: smol-tar/src/lib.rs.                                            *)
(*                                                                         *)
(* The writer and the reader share one pipe. The writer appends bytes to   *)
(* "out". The reader reads bytes from "out" at "rpos". When the reader is  *)
(* at the end of "out" and the writer did not close, the read is Pending.  *)
(* When the writer closed, the read gives 0 bytes (source EOF).            *)
(*                                                                         *)
(* A byte is a record with a tag "t":                                      *)
(*   "Z" -- zero byte (padding, end marker).                               *)
(*   "X" -- byte of a PAX 'x' header block.                                *)
(*   "P" -- byte k of the PAX data of entry e.                             *)
(*   "U" -- byte of the ustar header block of entry e.                     *)
(*   "B" -- byte k of the body of entry e.                                 *)
(* All bytes of one header block are equal. The reader decodes a header    *)
(* only when all BLOCK bytes in its buffer are equal (the checksum check). *)
(* A header that the reader assembles from wrong parts of the stream is a  *)
(* checksum error.                                                         *)
(*                                                                         *)
(* BLOCK is a scaled BLOCK_SIZE (lib.rs:301). NAME_LEN and PREFIX_LEN are  *)
(* scaled NAME_LEN and PREFIX_LEN (lib.rs:745-746). The model keeps all    *)
(* offset, padding and path-split arithmetic of the code.                  *)
(*                                                                         *)
(* Exclusions. The model does not check these items:                       *)
(*   - A PAX extension of size 0. The writer never writes one, because it  *)
(*     writes a PAX header only when it has records. PaxValid requires     *)
(*     data, but the code accepts size 0 (lib.rs:3249-3250).               *)
(*   - Each PAX metadata field. "meta" is one boolean for atime, ctime,    *)
(*     mtime, uid, gid, uname, gname and xattrs. The precedence of each    *)
(*     PAX field over its header field is not checked.                     *)
(*   - The header encoding by an independent second model. The "pax"      *)
(*     field of Expected and ArchiveLen use NeedsPax and HeaderBytes.      *)
(*   - The order rule of take_pax_info (a later record wins). The writer   *)
(*     writes at most one PAX set for each entry, so PaxFold never sees    *)
(*     two sets.                                                           *)
(*   - Record bytes, checksums, octal fields, GNU long names, global PAX   *)
(*     headers, PAX size limits and the SKIP_BUFFER_SIZE chunk limit.      *)
(***************************************************************************)
EXTENDS Integers, Sequences, FiniteSets, TLC

CONSTANTS
    BLOCK,          \* scaled BLOCK_SIZE, lib.rs:301
    NAME_LEN,       \* scaled NAME_LEN, lib.rs:745
    PREFIX_LEN,     \* scaled PREFIX_LEN, lib.rs:746
    K,              \* maximum number of entries in one archive
    FileSizes,      \* body sizes of regular files
    DirSizes,       \* size field of directories (TarDirectory::with_size)
    ConsumerDrops,  \* TRUE: the consumer can drop a file before its end
    WriterShort,    \* TRUE: writes and body chunks have any length
    AbsPath101      \* TRUE: input can have the path "/" + NAME_LEN bytes

ASSUME BLOCK \in Nat \ {0} /\ K \in Nat
ASSUME NAME_LEN \in Nat \ {0} /\ PREFIX_LEN \in Nat /\ NAME_LEN < PREFIX_LEN
ASSUME FileSizes \subseteq Nat /\ DirSizes \subseteq Nat
ASSUME ConsumerDrops \in BOOLEAN /\ WriterShort \in BOOLEAN
ASSUME AbsPath101 \in BOOLEAN

Min(a, b) == IF a < b THEN a ELSE b
MaxOf(S) == CHOOSE x \in S : \A y \in S : x >= y
MinOf(S) == CHOOSE x \in S : \A y \in S : x <= y

(* padded_size, lib.rs:3650-3656. *)
Padded(n) == IF n = 0 THEN 0 ELSE ((n + BLOCK - 1) \div BLOCK) * BLOCK

(***************************************************************************)
(* Paths. A path is a sequence of characters. Index i of the code (0-based) *)
(* is p[i + 1] here. "a" and "b" are ordinary characters, "/" is the       *)
(* separator, and NUL is the zero byte of a header field.                  *)
(***************************************************************************)
NUL == "NUL"
None == -1

(* A header field of length len: the content, then NUL bytes. from_buf    *)
(* fills the block with zero (lib.rs:777-783).                             *)
Field(len, content) == content \o [j \in 1..(len - Len(content)) |-> NUL]

(* null_terminated, lib.rs:3554-3559. *)
NullTerm(f) ==
    LET nuls == {i \in 1..Len(f) : f[i] = NUL}
    IN IF nuls = {} THEN f ELSE SubSeq(f, 1, MinOf(nuls) - 1)

(* path_split_point, lib.rs:818-829: None when len <= NAME_LEN, else the  *)
(* last index i > 0 with path[i] = '/' and i <= PREFIX_LEN.               *)
PathSplitPoint(p) ==
    IF Len(p) <= NAME_LEN THEN None
    ELSE LET s == {i \in 0..(Len(p) - 1) :
                       p[i + 1] = "/" /\ i > 0 /\ i <= PREFIX_LEN}
         IN IF s = {} THEN None ELSE MaxOf(s)

(* path_truncated, lib.rs:3685-3689. *)
PathTruncated(p) ==
    LET pos == PathSplitPoint(p)
    IN IF pos # None THEN Len(p) - pos - 1 > NAME_LEN
       ELSE Len(p) > NAME_LEN

(* link_path_truncated, lib.rs:3690-3692. *)
LinkTruncated(l) == Len(l) > NAME_LEN

(* copy_utf8_truncate, lib.rs:854-870, for one-byte characters. Gives the *)
(* bytes that it copies into a field of length flen.                       *)
CopyTrunc(flen, b) ==
    IF Len(b) <= flen THEN b
    ELSE LET c == {i \in 0..flen : i < Len(b) /\ b[i + 1] # "/"}
             cut == IF c = {} THEN 0 ELSE MaxOf(c)
         IN SubSeq(b, 1, cut)

(* set_path, lib.rs:830-841, with split_pos from path_split_point. The    *)
(* result is the prefix field and the name field.                          *)
SetPath(p) ==
    LET pos == PathSplitPoint(p)
    IN IF pos # None
       THEN [pfx |-> Field(PREFIX_LEN, SubSeq(p, 1, pos)),
             nm  |-> Field(NAME_LEN,
                           CopyTrunc(NAME_LEN, SubSeq(p, pos + 2, Len(p))))]
       ELSE [pfx |-> Field(PREFIX_LEN, <<>>),
             nm  |-> Field(NAME_LEN, CopyTrunc(NAME_LEN, p))]

(* set_link_path, lib.rs:842-844. *)
SetLinkPath(l) == Field(NAME_LEN, CopyTrunc(NAME_LEN, l))

(* ustar_path_name, lib.rs:3561-3577: prefix + "/" + name when prefix[0] *)
(* is not NUL, else name.                                                *)
UstarPath(pfx, nm) ==
    IF pfx[1] # NUL THEN NullTerm(pfx) \o <<"/">> \o NullTerm(nm)
    ELSE NullTerm(nm)

(* The path that the reader gives when the header has no PAX path. *)
HeaderPath(p) == LET f == SetPath(p) IN UstarPath(f.pfx, f.nm)

(* "/" and then NAME_LEN bytes without "/": no split point, PAX path. *)
IsAbsShape(p) ==
    /\ Len(p) = NAME_LEN + 1
    /\ p[1] = "/"
    /\ \A i \in 2..Len(p) : p[i] # "/"

(* Exhaustive check of the path codec for all paths up to a length. All  *)
(* paths come back. A path of the IsAbsShape shape gets a PAX path. TLC  *)
(* evaluates these ASSUMEs one time at start.                            *)
AllSeqs(chars, n) == UNION {[1..m -> chars] : m \in 0..n}
PathRoundTrips(p) == PathTruncated(p) \/ HeaderPath(p) = p
LinkRoundTrips(l) == LinkTruncated(l) \/ NullTerm(SetLinkPath(l)) = l
ASSUME \A p \in AllSeqs({"a", "/"}, NAME_LEN + PREFIX_LEN + 2) :
           /\ PathRoundTrips(p)
           /\ (IsAbsShape(p) => PathTruncated(p))
ASSUME \A l \in AllSeqs({"b", "/"}, NAME_LEN + 2) : LinkRoundTrips(l)

(***************************************************************************)
(* Input paths. The values assume NAME_LEN = 4 and PREFIX_LEN = 6.         *)
(*   PShort -- fits the name field, no split.                              *)
(*   PLong  -- no "/", longer than NAME_LEN: PAX "path".                   *)
(*   PSplit -- split at index 2, name part of 3 bytes, no PAX.             *)
(*   PDSlash-- "//" and 4 bytes: split at index 1, prefix "/", no PAX.     *)
(*   PTrail -- trailing "/": split at index 4, empty name, no PAX.         *)
(*   PAbs   -- "/" and NAME_LEN bytes: no split point, PAX "path".         *)
(* Link targets use "b", so a path value and a link value never agree.     *)
(*   LShort -- shorter than NAME_LEN.                                      *)
(*   LExact -- NAME_LEN bytes, the field has no NUL.                       *)
(*   LLong  -- longer than NAME_LEN: PAX "linkpath".                       *)
(***************************************************************************)
PShort  == <<"a", "a">>
PLong   == <<"a", "a", "a", "a", "a", "a", "a">>
PSplit  == <<"a", "a", "/", "a", "a", "a">>
PDSlash == <<"/", "/", "a", "a", "a", "a">>
PTrail  == <<"a", "a", "a", "a", "/">>
PAbs    == <<"/", "a", "a", "a", "a">>
LShort  == <<"b">>
LExact  == <<"b", "b", "b", "b">>
LLong   == <<"b", "b", "b", "b", "b">>

(* AbsPath101 = FALSE removes only the IsAbsShape path from the input. *)
Allowed(p) == AbsPath101 \/ ~IsAbsShape(p)
(* All kinds get these paths. *)
BasicPaths == {PShort, PLong}
(* set_path does not depend on the kind. To keep the state space small,  *)
(* these paths go only to a file of size 3 and to a directory of size 0. *)
ExtraPaths == {p \in {PSplit, PDSlash, PTrail, PAbs} : Allowed(p)}
LinkPaths == {LShort, LExact, LLong}

(***************************************************************************)
(* Abstract entries.                                                       *)
(*   kind -- "File", "Dir", "Symlink", "Link", "Fifo" (Fifo also stands    *)
(*           for a device: the code paths are the same).                   *)
(*   size -- body size of a file, size field of a directory, else 0.       *)
(*   path -- the entry path.                                               *)
(*   link -- the link target of Symlink and Link, else <<>>.               *)
(*   meta -- other PAX metadata: atime, ctime, mtime, uid, gid, uname,     *)
(*           gname, xattrs (lib.rs:3704-3749). Never for a hard link,      *)
(*           because TarLink has no such fields (lib.rs:1470-1473).        *)
(*   big  -- the size does not fit the octal field, so a PAX "size" record *)
(*           carries it (lib.rs:3741-3744). Only for File and Dir.         *)
(***************************************************************************)
HasLink(kind) == kind \in {"Symlink", "Link"}

Entries ==
    {[kind |-> "File", size |-> s, path |-> p, link |-> <<>>, meta |-> m,
      big |-> b] :
        s \in FileSizes, p \in BasicPaths, m \in BOOLEAN, b \in BOOLEAN}
    \cup
    {[kind |-> "Dir", size |-> s, path |-> p, link |-> <<>>, meta |-> m,
      big |-> b] :
        s \in DirSizes, p \in BasicPaths, m \in BOOLEAN, b \in BOOLEAN}
    \cup
    {[kind |-> ks[1], size |-> ks[2], path |-> p, link |-> <<>>, meta |-> m,
      big |-> FALSE] :
        ks \in {<<"File", 3>>, <<"Dir", 0>>}, p \in ExtraPaths,
        m \in BOOLEAN}
    \cup
    {[kind |-> "Symlink", size |-> 0, path |-> p, link |-> l, meta |-> m,
      big |-> FALSE] :
        p \in BasicPaths, l \in LinkPaths, m \in BOOLEAN}
    \cup
    {[kind |-> "Fifo", size |-> 0, path |-> p, link |-> <<>>, meta |-> m,
      big |-> FALSE] :
        p \in BasicPaths, m \in BOOLEAN}
    \cup
    {[kind |-> "Link", size |-> 0, path |-> p, link |-> l, meta |-> FALSE,
      big |-> FALSE] :
        p \in BasicPaths, l \in LinkPaths}

(* A size of 0 always fits the octal field. *)
ValidEntry(e) == e.big => e.size > 0

Inputs == UNION {[1..n -> {e \in Entries : ValidEntry(e)}] : n \in 0..K}

(***************************************************************************)
(* Writer encoding, write_header lib.rs:3669-3839.                         *)
(***************************************************************************)
PaxHasPath(e) == PathTruncated(e.path)
PaxHasLink(e) == HasLink(e.kind) /\ LinkTruncated(e.link)

NeedsPax(e) == PaxHasPath(e) \/ PaxHasLink(e) \/ e.meta \/ e.big

(* Abstract length of the PAX data (ext_size, lib.rs:3751-3754). It is    *)
(* the sum of the record lengths. The values give lengths below, equal to *)
(* and above BLOCK.                                                        *)
PaxLen(e) == (IF PaxHasPath(e) THEN 3 ELSE 0)
           + (IF PaxHasLink(e) THEN 2 ELSE 0)
           + (IF e.meta THEN 2 ELSE 0)
           + (IF e.big THEN 1 ELSE 0)

(* Size field of the ustar header: lib.rs:3783 and lib.rs:3826. *)
HdrSize(e) == IF e.kind \in {"File", "Dir"} /\ ~e.big THEN e.size ELSE 0

(* The fields of a byte. Unused fields keep the values of Byte0. *)
Byte0 == [t |-> "Z", e |-> 0, k |-> 0, n |-> 0, kind |-> "none", sz |-> 0,
          pfx |-> <<>>, nm |-> <<>>, lnk |-> <<>>,
          hp |-> FALSE, pp |-> <<>>, hl |-> FALSE, pl |-> <<>>,
          meta |-> FALSE]

Z == Byte0
Zeros(n) == [j \in 1..n |-> Z]

(* ustar header of entry i: lib.rs:3772-3793 or lib.rs:3815-3836. *)
(* set_path and set_link_path run in both branches.                *)
UHdr(e, i) ==
    LET f == SetPath(e.path)
        b == [Byte0 EXCEPT !.t = "U", !.e = i, !.kind = e.kind,
                           !.sz = HdrSize(e), !.pfx = f.pfx, !.nm = f.nm,
                           !.lnk = IF HasLink(e.kind) THEN SetLinkPath(e.link)
                                   ELSE Field(NAME_LEN, <<>>)]
    IN [j \in 1..BLOCK |-> b]
(* PAX 'x' header, typeflag PAXLocal, size ext_size: lib.rs:3795-3813. *)
XHdr(e, i) ==
    [j \in 1..BLOCK |-> [Byte0 EXCEPT !.t = "X", !.e = i, !.n = PaxLen(e),
                                      !.kind = "x"]]
(* PAX data bytes. Every byte carries the parsed result of the records:   *)
(* "path" (lib.rs:3697-3699), "linkpath" (lib.rs:3700-3703), "size" and   *)
(* the metadata marker.                                                   *)
PData(e, i) ==
    [j \in 1..PaxLen(e) |->
        [Byte0 EXCEPT !.t = "P", !.e = i, !.k = j, !.n = PaxLen(e),
                      !.sz = IF e.big THEN e.size ELSE -1,
                      !.hp = PaxHasPath(e),
                      !.pp = IF PaxHasPath(e) THEN e.path ELSE <<>>,
                      !.hl = PaxHasLink(e),
                      !.pl = IF PaxHasLink(e) THEN e.link ELSE <<>>,
                      !.meta = e.meta]]
BodyByte(i, j) == [Byte0 EXCEPT !.t = "B", !.e = i, !.k = j]

(* The bytes that write_header puts into the writer buffer. The length is *)
(* the "total" of lib.rs:3756-3760.                                      *)
HeaderBytes(e, i) ==
    IF NeedsPax(e)
    THEN XHdr(e, i) \o PData(e, i) \o Zeros(Padded(PaxLen(e)) - PaxLen(e))
            \o UHdr(e, i)
    ELSE UHdr(e, i)

(* Archive length, for the layout check. *)
RECURSIVE ArchiveLen(_, _)
ArchiveLen(in, i) ==
    IF i > Len(in) THEN 2 * BLOCK
    ELSE Len(HeaderBytes(in[i], i))
         + (IF in[i].kind = "File" THEN Padded(in[i].size) ELSE 0)
         + ArchiveLen(in, i + 1)

(***************************************************************************)
(* Variables.                                                              *)
(***************************************************************************)
VARIABLES
    input,      \* the entries that the user sends to the writer
    \* TarWriter, lib.rs:4022-4045
    out,        \* bytes that the underlying writer accepted
    wbuf,       \* buf[..len]; <<>> when len = 0
    wpos,       \* pos in buf
    wtotal,     \* total
    weof,       \* eof
    wfile,      \* index of the entry of the active body reader, 0 = None
    wnext,      \* index of the next entry to send (ghost)
    wclosed,    \* state # Open (Marker or Closed), lib.rs:3956-3963
    wshut,      \* state = Closed: the underlying close returned Ok
    \* TarReaderInner, lib.rs:1160-1182
    rstate,     \* state
    rpos,       \* pos
    rnxt,       \* nxt
    hbuf,       \* header record buffer
    ebuf,       \* buf[..filled] of the ExtensionBuffer, for PAX data
                \* larger than BLOCK (lib.rs:1119-1149)
    rext,       \* size in Extension((size, kind))
    rexts,      \* exts: parsed PAX sets
    \* TarReader and the consumer
    held,       \* "archive": TarReader owns the core; "file": the file owns it
    cur,        \* index of the current file entry (ghost)
    bodyRead,   \* body bytes that the consumer read from the current file
    yielded,    \* entries that the reader yielded (ghost)
    rerr,       \* "none" or the error message that the reader gave
    rbad        \* TRUE if the consumer read a wrong body byte (ghost)

wvars == <<out, wbuf, wpos, wtotal, weof, wfile, wnext, wclosed, wshut>>
rvars == <<rstate, rpos, rnxt, hbuf, ebuf, rext, rexts, held, cur,
           bodyRead, yielded, rerr, rbad>>
vars == <<input, wvars, rvars>>

Init ==
    /\ input \in Inputs
    /\ out = <<>>
    /\ wbuf = <<>>
    /\ wpos = 0
    /\ wtotal = 0
    /\ weof = 0
    /\ wfile = 0
    /\ wnext = 1
    /\ wclosed = FALSE
    /\ wshut = FALSE
    \* TarReaderInner::new, lib.rs:2919-2933
    /\ rstate = "Header"
    /\ rpos = 0
    /\ rnxt = BLOCK
    /\ hbuf = Zeros(BLOCK)
    /\ ebuf = <<>>
    /\ rext = 0
    /\ rexts = <<>>
    /\ held = "archive"
    /\ cur = 0
    /\ bodyRead = 0
    /\ yielded = <<>>
    /\ rerr = "none"
    /\ rbad = FALSE

(***************************************************************************)
(* Writer: Sink impl lib.rs:4289-4351 and poll_drain lib.rs:4227-4286.     *)
(* TarWriter::write is poll_ready, start_send, poll_flush. poll_ready and  *)
(* poll_flush call poll_drain. So a new entry starts only when the buffer  *)
(* and the body of the previous entry are written.                         *)
(***************************************************************************)
Chunks(rem) == IF WriterShort THEN 1..rem ELSE {rem}

(* start_send, lib.rs:4301-4323. *)
WStartSend ==
    /\ wbuf = <<>>
    /\ wfile = 0
    /\ ~wclosed
    /\ wnext <= Len(input)
    /\ LET e == input[wnext]
           h == HeaderBytes(e, wnext)
       IN /\ wbuf' = h
          /\ wpos' = 0
          /\ IF e.kind = "File"
             THEN /\ wfile' = wnext
                  /\ weof' = wtotal + Len(h) + e.size
             ELSE UNCHANGED <<wfile, weof>>
    /\ wnext' = wnext + 1
    /\ UNCHANGED <<out, wtotal, wclosed, wshut>>

(* Write buf[pos..len], lib.rs:4230-4246. At the end pos = len = 0. *)
WWriteBuf ==
    /\ wpos < Len(wbuf)
    /\ \E n \in Chunks(Len(wbuf) - wpos) :
        /\ out' = out \o SubSeq(wbuf, wpos + 1, wpos + n)
        /\ wtotal' = wtotal + n
        /\ IF wpos + n = Len(wbuf)
           THEN wbuf' = <<>> /\ wpos' = 0
           ELSE wpos' = wpos + n /\ UNCHANGED wbuf
    /\ UNCHANGED <<weof, wfile, wnext, wclosed, wshut>>

(* Body data, lib.rs:4251-4263. The source gives the written body bytes. *)
WBody ==
    /\ wbuf = <<>>
    /\ wfile # 0
    /\ weof > wtotal
    /\ LET remain == weof - wtotal
           done == input[wfile].size - remain
       IN \E n \in Chunks(remain) :
            /\ out' = out \o [j \in 1..n |-> BodyByte(wfile, done + j)]
            /\ wtotal' = wtotal + n
    /\ UNCHANGED <<wbuf, wpos, weof, wfile, wnext, wclosed, wshut>>

(* End of body and zero padding, lib.rs:4249-4276. *)
WBodyEnd ==
    /\ wbuf = <<>>
    /\ wfile # 0
    /\ weof = wtotal
    /\ wfile' = 0
    /\ LET pad == Padded(weof) - weof
       IN IF pad > 0
          THEN wbuf' = Zeros(pad) /\ weof' = Padded(weof)
          ELSE UNCHANGED <<wbuf, weof>>
    /\ UNCHANGED <<out, wpos, wtotal, wnext, wclosed, wshut>>

(* poll_close, lib.rs:4332-4350. When state is Open, poll_close puts two *)
(* zero blocks into the buffer and sets state to Marker (WClose). After  *)
(* the buffer is written and the underlying close returns Ok, state is   *)
(* Closed (WShutdown).                                                   *)
WClose ==
    /\ wbuf = <<>>
    /\ wfile = 0
    /\ wnext > Len(input)
    /\ ~wclosed
    /\ wbuf' = Zeros(2 * BLOCK)
    /\ wclosed' = TRUE
    /\ UNCHANGED <<out, wpos, wtotal, weof, wfile, wnext, wshut>>

WShutdown ==
    /\ wclosed
    /\ wbuf = <<>>
    /\ ~wshut
    /\ wshut' = TRUE
    /\ UNCHANGED <<out, wbuf, wpos, wtotal, weof, wfile, wnext, wclosed>>

WriterNext == WStartSend \/ WWriteBuf \/ WBody \/ WBodyEnd \/ WClose
              \/ WShutdown

(***************************************************************************)
(* Reader helpers.                                                         *)
(***************************************************************************)
Avail == Len(out) - rpos
(* A read gives 0: the writer closed and all bytes are read. *)
SrcEOF == rpos = Len(out) /\ wshut
(* Read n bytes into buf[off+1 .. off+n]. *)
Fill(buf, off, n) ==
    [j \in 1..BLOCK |->
        IF off < j /\ j <= off + n THEN out[rpos + j - off] ELSE buf[j]]

IsZero(h) == \A j \in 1..BLOCK : h[j].t = "Z"
SameBytes(h) == \A j \in 1..BLOCK : h[j] = h[1]

Fail(msg) ==
    /\ rerr' = msg
    /\ UNCHANGED <<rstate, rpos, rnxt, hbuf, ebuf, rext, rexts, held, cur,
                   bodyRead, yielded, rbad>>

(* take_pax_info, lib.rs:2705-2739, without globals (the writer never    *)
(* writes 'g' headers). Exts apply in order; a later record wins. The    *)
(* keys "path" and "linkpath" go to separate fields (apply_pax_set,      *)
(* lib.rs:2742-2760).                                                    *)
RECURSIVE PaxFold(_, _, _)
PaxFold(s, i, acc) ==
    IF i > Len(s) THEN acc
    ELSE PaxFold(s, i + 1,
            [hp   |-> acc.hp \/ s[i].hp,
             pp   |-> IF s[i].hp THEN s[i].pp ELSE acc.pp,
             hl   |-> acc.hl \/ s[i].hl,
             pl   |-> IF s[i].hl THEN s[i].pl ELSE acc.pl,
             sz   |-> IF s[i].sz >= 0 THEN s[i].sz ELSE acc.sz,
             meta |-> acc.meta \/ s[i].meta,
             from |-> Append(acc.from, s[i].from)])
PaxInfo(s) == PaxFold(s, 1, [hp |-> FALSE, pp |-> <<>>, hl |-> FALSE,
                             pl |-> <<>>, sz |-> -1, meta |-> FALSE,
                             from |-> <<>>])

(* Decode a complete ustar header of an entry, lib.rs:3004-3180.         *)
(* hb is the header. rpos' = rnxt here.                                  *)
YieldEntry(hb) ==
    LET info == PaxInfo(rexts)
        \* effective_size, lib.rs:945-947
        size == IF info.sz >= 0 THEN info.sz ELSE hb.sz
        \* entry_path and entry_path_link, lib.rs:2661-2688; the header
        \* fields go through ustar_path_name and path_name (lib.rs:771-776).
        path == IF info.hp THEN info.pp ELSE UstarPath(hb.pfx, hb.nm)
        link == IF info.hl THEN info.pl ELSE NullTerm(hb.lnk)
        ent  == [kind |-> hb.kind,
                 \* Fifo and device do not read the size. TarLink has no
                 \* size; the reader uses it only to skip data.
                 size |-> IF hb.kind \in {"Fifo", "Link"} THEN 0 ELSE size,
                 path |-> path,
                 link |-> IF HasLink(hb.kind) THEN link ELSE <<>>,
                 \* A link drops the PAX metadata: `let _ = info.attrs;`
                 \* (lib.rs:3126). TarLink has no times, ids or attrs.
                 meta |-> IF hb.kind = "Link" THEN FALSE ELSE info.meta,
                 pax  |-> info.from,
                 hdr  |-> hb.e]
    IN
    \* Symlink with size # 0 is an error, lib.rs:3146-3159.
    IF hb.kind = "Symlink" /\ size # 0
    THEN /\ rerr' = "symlink entry has a non-zero size"
         /\ UNCHANGED <<rstate, rnxt, ebuf, rext, rexts, held, cur,
                        bodyRead, yielded, rbad>>
    ELSE
    /\ yielded' = Append(yielded, ent)
    /\ rexts' = <<>>
    /\ CASE hb.kind = "File" ->
              \* lib.rs:3004-3048. The writer writes ustar magic, so
              \* is_old() is FALSE and the old-style directory branch
              \* (lib.rs:3014) does not apply.
              /\ rnxt' = rnxt + size
              /\ rstate' = "Entry"
              /\ held' = "file"
              /\ cur' = Len(yielded) + 1
              /\ bodyRead' = 0
         []   hb.kind \in {"Dir", "Fifo", "Symlink"} ->
              \* Dir lib.rs:3049-3071, Fifo 3072-3092, device 3093-3120,
              \* Symlink 3146-3180
              /\ rnxt' = rnxt + BLOCK
              /\ rstate' = "Header"
              /\ UNCHANGED <<held, cur, bodyRead>>
         []   hb.kind = "Link" ->
              \* lib.rs:3121-3145. Data can follow a link with size > 0.
              /\ IF size = 0
                 THEN rnxt' = rnxt + BLOCK /\ rstate' = "Header"
                 ELSE rnxt' = rnxt + size /\ rstate' = "SkipEntry"
              /\ UNCHANGED <<held, cur, bodyRead>>
    /\ UNCHANGED <<ebuf, rext, rerr, rbad>>

(* Decode a complete header block h, lib.rs:2989-3245. *)
Decode(h) ==
    IF IsZero(h)
    THEN \* lib.rs:2989-2995
         /\ rnxt' = rnxt + BLOCK
         /\ rstate' = "Eof"
         /\ UNCHANGED <<ebuf, rext, rexts, held, cur, bodyRead, yielded,
                        rerr, rbad>>
    ELSE IF ~SameBytes(h) \/ h[1].t \notin {"U", "X"}
    THEN \* validate_checksum lib.rs:2996; a block that is not a header
         \* has a wrong checksum.
         /\ rerr' = "invalid tar header checksum"
         /\ UNCHANGED <<rstate, rnxt, ebuf, rext, rexts, held, cur,
                        bodyRead, yielded, rbad>>
    ELSE IF h[1].t = "X"
    THEN \* PAXLocal with ustar magic, lib.rs:3181-3206
         /\ rstate' = "Extension"
         /\ rext' = h[1].n
         /\ rnxt' = rnxt + Padded(h[1].n)
         /\ ebuf' = <<>>
         /\ UNCHANGED <<rexts, held, cur, bodyRead, yielded, rerr, rbad>>
    ELSE YieldEntry(h[1])

(***************************************************************************)
(* Reader: poll_read_header, lib.rs:2937-3428.                             *)
(* The reader runs only while TarReader owns the core (poll_next,          *)
(* lib.rs:3433-3462). While the file owns it, poll_next is Pending.        *)
(* After an archive error the code sets state to Failed and the stream     *)
(* ends (lib.rs:2945-2957). The model keeps rstate, sets rerr, and RGuard  *)
(* stops the reader.                                                       *)
(***************************************************************************)
RGuard == held = "archive" /\ rerr = "none"

(* Header, lib.rs:2969-2995. *)
RHeader ==
    /\ RGuard
    /\ rstate = "Header"
    /\ LET remaining == rnxt - rpos
           filled == BLOCK - remaining
       IN \/ /\ SrcEOF
             /\ Fail("Unexpected EOF while reading tar header")
          \/ \E n \in 1..Min(remaining, Avail) :
                LET h == Fill(hbuf, filled, n)
                IN /\ hbuf' = h
                   /\ rpos' = rpos + n
                   /\ IF n < remaining
                      THEN UNCHANGED <<rstate, rnxt, ebuf, rext, rexts,
                                       held, cur, bodyRead, yielded,
                                       rerr, rbad>>
                      ELSE Decode(h)
    /\ UNCHANGED input

(* parse_pax_records accepts the data only when it is the complete PAX *)
(* data of one entry.                                                    *)
PaxValid(d) ==
    /\ Len(d) > 0
    /\ \A j \in 1..Len(d) :
          /\ d[j].t = "P"
          /\ d[j].e = d[1].e
          /\ d[j].k = j
          /\ d[j].n = Len(d)

(* End of an extension, lib.rs:3276-3324. h and e are the buffers after *)
(* the last read.                                                        *)
ExtDone(h, e) ==
    LET d == IF rext <= BLOCK THEN [j \in 1..rext |-> h[j]]
             ELSE SubSeq(e, 1, rext)
    IN IF PaxValid(d)
       THEN /\ rexts' = Append(rexts,
                          [from |-> d[1].e, hp |-> d[1].hp, pp |-> d[1].pp,
                           hl |-> d[1].hl, pl |-> d[1].pl,
                           sz |-> d[1].sz, meta |-> d[1].meta])
            /\ hbuf' = h
            /\ ebuf' = <<>>
            /\ rnxt' = rnxt + BLOCK
            /\ rstate' = "Header"
            /\ UNCHANGED <<rext, held, cur, bodyRead, yielded, rerr, rbad>>
       ELSE /\ rerr' = "invalid PAX records"
            /\ UNCHANGED <<rstate, rnxt, hbuf, ebuf, rext, rexts, held,
                           cur, bodyRead, yielded, rbad>>

(* Extension, lib.rs:3247-3326. Data of size <= BLOCK goes into the     *)
(* header buffer; larger data goes into the ExtensionBuffer.            *)
RExtension ==
    /\ RGuard
    /\ rstate = "Extension"
    /\ LET remaining == rnxt - rpos
       IN \/ /\ remaining = 0
             /\ ExtDone(hbuf, ebuf)
             /\ UNCHANGED rpos
          \/ /\ remaining > 0
             /\ SrcEOF
             /\ Fail("unexpected end of tar file")
          \/ /\ remaining > 0
             /\ \E n \in 1..Min(remaining, Avail) :
                  LET small == rext <= BLOCK
                      h == IF small THEN Fill(hbuf, BLOCK - remaining, n)
                           ELSE hbuf
                      e == IF small THEN ebuf
                           ELSE ebuf \o SubSeq(out, rpos + 1, rpos + n)
                  IN /\ rpos' = rpos + n
                     /\ IF n = remaining
                        THEN ExtDone(h, e)
                        ELSE /\ hbuf' = h
                             /\ ebuf' = e
                             /\ UNCHANGED <<rstate, rnxt, rext, rexts,
                                            held, cur, bodyRead, yielded,
                                            rerr, rbad>>
    /\ UNCHANGED input

(* Padding, lib.rs:3327-3347. The read goes into buf[..remaining]. *)
RPadding ==
    /\ RGuard
    /\ rstate = "Padding"
    /\ LET remaining == rnxt - rpos
       IN \/ /\ SrcEOF
             /\ Fail("unexpected end of tar file")
          \/ \E n \in 1..Min(remaining, Avail) :
                /\ hbuf' = Fill(hbuf, 0, n)
                /\ rpos' = rpos + n
                /\ IF n = remaining
                   THEN rnxt' = rpos + n + BLOCK /\ rstate' = "Header"
                   ELSE UNCHANGED <<rnxt, rstate>>
                /\ UNCHANGED <<ebuf, rext, rexts, held, cur, bodyRead,
                               yielded, rerr, rbad>>
    /\ UNCHANGED input

(* SkipEntry, lib.rs:3351-3382. The end is padded_size(nxt). The skip   *)
(* buffer limit SKIP_BUFFER_SIZE only limits one read; reads have any   *)
(* length here.                                                          *)
RSkip ==
    /\ RGuard
    /\ rstate = "SkipEntry"
    /\ LET nx == Padded(rnxt)
           remaining == nx - rpos
       IN \/ /\ remaining = 0
             /\ rnxt' = rpos + BLOCK
             /\ rstate' = "Header"
             /\ UNCHANGED <<rpos, hbuf, ebuf, rext, rexts, held, cur,
                            bodyRead, yielded, rerr, rbad>>
          \/ /\ remaining > 0
             /\ SrcEOF
             /\ Fail("unexpected end of tar file")
          \/ /\ remaining > 0
             /\ \E n \in 1..Min(remaining, Avail) :
                  /\ rpos' = rpos + n
                  /\ IF rpos + n = nx
                     THEN rnxt' = rpos + n + BLOCK /\ rstate' = "Header"
                     ELSE UNCHANGED <<rnxt, rstate>>
                  /\ UNCHANGED <<hbuf, ebuf, rext, rexts, held, cur,
                                 bodyRead, yielded, rerr, rbad>>
    /\ UNCHANGED input

(* Eof, lib.rs:3383-3413: the second zero block. *)
REof ==
    /\ RGuard
    /\ rstate = "Eof"
    /\ LET remaining == rnxt - rpos
           filled == BLOCK - remaining
       IN \/ /\ SrcEOF
             /\ Fail("unexpected end of tar file")
          \/ \E n \in 1..Min(remaining, Avail) :
                LET h == Fill(hbuf, filled, n)
                IN /\ hbuf' = h
                   /\ rpos' = rpos + n
                   /\ IF n < remaining
                      THEN UNCHANGED <<rstate, rerr>>
                      ELSE IF IsZero(h)
                      THEN rstate' = "Eoff" /\ UNCHANGED rerr
                      ELSE /\ rerr' = "unexpected data after first zero block"
                           /\ UNCHANGED rstate
                   /\ UNCHANGED <<rnxt, ebuf, rext, rexts, held, cur,
                                  bodyRead, yielded, rbad>>
    /\ UNCHANGED input

(* Eoff, lib.rs:3414-3424: read to source EOF, then Done. *)
REoff ==
    /\ RGuard
    /\ rstate = "Eoff"
    /\ \/ /\ SrcEOF
          /\ rstate' = "Done"
          /\ UNCHANGED <<rpos, hbuf>>
       \/ \E n \in 1..Min(BLOCK, Avail) :
             /\ rpos' = rpos + n
             /\ hbuf' = Fill(hbuf, 0, n)
             /\ UNCHANGED rstate
    /\ UNCHANGED <<input, rnxt, ebuf, rext, rexts, held, cur, bodyRead,
                   yielded, rerr, rbad>>

ReaderNext == RHeader \/ RExtension \/ RPadding \/ RSkip \/ REof \/ REoff

(***************************************************************************)
(* Consumer of a file entry: TarRegularFileReader, lib.rs:348-442 and     *)
(* lib.rs:1216-1244.                                                       *)
(***************************************************************************)

(* end_regular_file lib.rs:384-393, then return_core lib.rs:1218-1237. *)
(* p is the new pos.                                                    *)
EndFile(p) ==
    LET padded == Padded(rnxt)
    IN /\ IF p = padded
          THEN rnxt' = padded + BLOCK /\ rstate' = "Header"
          ELSE rnxt' = padded /\ rstate' = "Padding"
       /\ held' = "archive"

(* poll_read (lib.rs:348-380), or poll_fill_buf and consume             *)
(* (lib.rs:396-442). A read of n bytes with 1 <= n <= remain.           *)
CRead ==
    /\ held = "file"
    /\ rerr = "none"
    /\ LET remain == rnxt - rpos
           size == input[cur].size
       IN \/ \* remain = 0: lib.rs:358-360 and lib.rs:375-378, or
             \* lib.rs:404-409.
             /\ remain = 0
             /\ EndFile(rpos)
             /\ rbad' = (rbad \/ bodyRead # size)
             /\ UNCHANGED <<rpos, bodyRead, rerr>>
          \/ /\ remain > 0
             /\ SrcEOF
             /\ rerr' = "unexpected EOF while reading archive file"
             /\ UNCHANGED <<rstate, rpos, rnxt, held, bodyRead, rbad>>
          \/ /\ remain > 0
             /\ \E n \in 1..Min(remain, Avail) :
                  /\ rpos' = rpos + n
                  /\ bodyRead' = bodyRead + n
                  /\ rbad' = (rbad
                              \/ \E j \in 1..n :
                                    out[rpos + j] # BodyByte(cur, bodyRead + j)
                              \/ (n = remain /\ bodyRead + n # size))
                  /\ IF n = remain
                     THEN EndFile(rpos + n)
                     ELSE UNCHANGED <<rstate, rnxt, held>>
                  /\ UNCHANGED rerr
    /\ UNCHANGED <<input, hbuf, ebuf, rext, rexts, cur, yielded>>

(* Drop of the file before its end: return_core sets SkipEntry,          *)
(* lib.rs:1218-1224 and lib.rs:1240-1244.                                *)
CDrop ==
    /\ ConsumerDrops
    /\ held = "file"
    /\ rerr = "none"
    /\ held' = "archive"
    /\ rstate' = "SkipEntry"
    /\ UNCHANGED <<input, rpos, rnxt, hbuf, ebuf, rext, rexts, cur,
                   bodyRead, yielded, rerr, rbad>>

ConsumerNext == CRead \/ CDrop

(* Explicit terminal state: the stream ended and the writer closed. *)
Finished ==
    /\ rstate = "Done"
    /\ wshut
    /\ UNCHANGED vars

Next ==
    \/ WriterNext /\ UNCHANGED <<input, rvars>>
    \/ (ReaderNext \/ ConsumerNext) /\ UNCHANGED wvars
    \/ Finished

Spec ==
    /\ Init
    /\ [][Next]_vars
    /\ WF_vars(WriterNext /\ UNCHANGED <<input, rvars>>)
    /\ WF_vars((ReaderNext \/ ConsumerNext) /\ UNCHANGED wvars)

(***************************************************************************)
(* Properties.                                                             *)
(***************************************************************************)

(* The entry that the reader must give for input entry j. *)
Expected(j) ==
    [kind |-> input[j].kind,
     size |-> IF input[j].kind \in {"Fifo", "Symlink", "Link"}
              THEN 0 ELSE input[j].size,
     path |-> input[j].path,
     link |-> input[j].link,
     meta |-> input[j].meta,
     pax  |-> IF NeedsPax(input[j]) THEN <<j>> ELSE <<>>,
     hdr  |-> j]

(* The reader gives no error on writer output. *)
NoError == rerr = "none"

(* Each body byte that the consumer reads is the written byte, and a    *)
(* file that is read to its end has the written number of bytes.        *)
BodyOK == ~rbad

(* The yielded entries are a prefix of the written entries. Each has    *)
(* the written kind, size, path, link and metadata. Each carries the    *)
(* PAX set of its own entry and no other PAX set.                       *)
YieldOK ==
    /\ Len(yielded) <= Len(input)
    /\ \A j \in 1..Len(yielded) : yielded[j] = Expected(j)

(* A pending PAX set belongs to the next entry only. *)
PaxAttach ==
    /\ Len(rexts) <= 1
    /\ \A j \in 1..Len(rexts) : rexts[j].from = Len(yielded) + 1

(* Done only after all entries and the complete stream. *)
DoneComplete ==
    rstate = "Done" =>
        /\ Len(yielded) = Len(input)
        /\ wshut
        /\ rpos = Len(out)
        /\ rexts = <<>>

(* The writer output has the layout of the code. *)
WriterLayout ==
    /\ Len(out) = wtotal
    /\ wshut => /\ Len(out) % BLOCK = 0
                /\ Len(out) = ArchiveLen(input, 1)

(* The reader never reads bytes that the writer did not write. *)
ReadBound == rpos <= Len(out)

(* Liveness: under fairness the reader reaches Done. *)
ReachesDone == <>(rstate = "Done")
=============================================================================
