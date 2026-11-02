----------------------------- MODULE TarReader -----------------------------
(***************************************************************************)
(* State machine of the smol-tar archive reader (smol-tar/src/lib.rs).     *)
(*                                                                         *)
(* The model scales one tar block from 512 bytes to B bytes. The offsets   *)
(* pos and nxt are absolute byte offsets, as in TarReaderInner             *)
(* (lib.rs:1160). Padding and short reads are byte-exact.                  *)
(*                                                                         *)
(* The archive is a sequence of items. Each item is one header block and   *)
(* its data blocks. The model adds items one at a time: when the reader   *)
(* completes a block at the end of the known layout, the model picks the   *)
(* next item from ItemSet. A block that the reader decodes at an offset    *)
(* that is not an item start sets misaligned.                              *)
(*                                                                         *)
(* The source gives 1..MaxRead bytes per read. It can also end at any      *)
(* read (truncation). After MaxItems items the source has Trail more bytes *)
(* (Trail < B), then it ends. Pending is a stutter step and is not shown.  *)
(*                                                                         *)
(* One archive step is one pass of the loop in poll_read_header_inner      *)
(* (lib.rs:2962-3428). An Err for the archive data sets the state to       *)
(* Failed (lib.rs:2934-2959). In Failed each poll gives None               *)
(* (lib.rs:3425). The consumer polls again after each Err.                 *)
(*                                                                         *)
(* The model does not include these items:                                 *)
(* - Source I/O errors other than end of data. On such an error the code   *)
(*   keeps its state, and the next poll reads from the source again        *)
(*   (lib.rs:444-452, 1346-1350). ErrSticky applies only to errors for     *)
(*   the archive data.                                                     *)
(* - The model uses this assumption: poll_read into an empty buffer gives  *)
(*   Ready(Ok(0)).                                                         *)
(* - The PAX size record (effective_size, lib.rs:945). Header and data use *)
(*   the same size in the model.                                           *)
(* - The old-style directory: a File header with a path that ends in '/'   *)
(*   (lib.rs:3014-3016).                                                   *)
(* - The bytes in the extension buffer. extBuf only shows if ext is Some.  *)
(*   For an extension larger than one block, the code reads into the       *)
(*   unfilled part of a zeroed buffer of exactly padded bytes              *)
(*   (lib.rs:1127, 1143-1145, 3203, 3236, 3257-3263). Thus each read stops *)
(*   at nxt, as in ExtStep. A debug_assert checks pos <= nxt after the     *)
(*   read (lib.rs:3274).                                                   *)
(***************************************************************************)
EXTENDS Naturals, Sequences, FiniteSets

CONSTANTS
    B,          \* block size (512 in the code)
    MaxRead,    \* largest short read of the source
    SkipChunk,  \* SKIP_BUFFER_SIZE (lib.rs:302), scaled
    MaxItems,   \* largest number of archive items
    Trail,      \* bytes after the last item
    ItemSet     \* items that the archive can contain

ASSUME B >= 2 /\ MaxRead >= 1 /\ SkipChunk >= 1
ASSUME MaxItems >= 1 /\ Trail \in 0..(B - 1)

VARIABLES
    items, starts, lend,  \* archive layout: items, start offsets, end
    pos, nxt, st,         \* TarReaderInner pos, nxt, state (lib.rs:1162-1166)
    cur,                  \* index of the last decoded header item
    exts, globs, extBuf,  \* TarReaderInner exts, globs, ext (Some/None)
    owner,                \* "archive" or "file": who holds the core
    delivered, peek,      \* body bytes given to the consumer, fill_buf size
    ended,                \* the source returned 0 for a non-empty buffer
    errSeen, panicked,    \* an Err reached the consumer; the code panics
    misaligned,           \* the reader decoded a block that is no header
    yielded               \* Ok entries given by poll_next

vars == <<items, starts, lend, pos, nxt, st, cur, exts, globs, extBuf,
          owner, delivered, peek, ended, errSeen, panicked, misaligned,
          yielded>>
gen == <<items, starts, lend, misaligned>>

Min(a, b) == IF a < b THEN a ELSE b
Range(s) == {s[k] : k \in DOMAIN s}
\* padded_size (lib.rs:3650)
Padded(n) == IF n % B = 0 THEN n ELSE n + (B - (n % B))

(***************************************************************************)
(* Item kinds.                                                             *)
(*   File, Dir, Link, Sym  -- good entries. Dir also stands for Fifo and   *)
(*                            Device (arms with the same structure,       *)
(*                            lib.rs:3072, 3093).                         *)
(*   FileBad   -- File header with a field that does not parse.            *)
(*   DirBad    -- Directory, Fifo, Device or Symlink header with a bad     *)
(*                path, link name, mode or device number field.            *)
(*   Pax, Glob -- PAX local and global headers ('x', 'g').                 *)
(*   Gnu       -- GNU long name or long link ('L', 'K').                   *)
(*   PaxBad, GlobBad, GnuBad -- payload that does not parse.               *)
(*   Bad       -- bad checksum or unsupported type.                        *)
(*   Zero      -- a block of zero bytes.                                   *)
(* The field s is the size field of the header.                            *)
(***************************************************************************)
Kinds == {"File", "FileBad", "Dir", "DirBad", "Link", "Sym", "Pax",
          "PaxBad", "Glob", "GlobBad", "Gnu", "GnuBad", "Bad", "Zero"}
DataKinds == {"File", "FileBad", "Link", "Sym", "Pax", "PaxBad", "Glob",
              "GlobBad", "Gnu", "GnuBad"}
ExtKinds == {"Pax", "PaxBad", "Glob", "GlobBad", "Gnu", "GnuBad"}
\* Arms that call take_pax_info (lib.rs:2705) and drain exts.
TakeKinds == {"File", "FileBad", "Dir", "DirBad", "Link", "Sym"}

It(kind, size) == [k |-> kind, s |-> size]
ItemLen(it) == B + (IF it.k \in DataKinds THEN Padded(it.s) ELSE 0)

\* Item sets for the configurations.
ItemsMain ==
       {It("File", s) : s \in {0, 1, B, B + 1}}
  \cup {It("FileBad", 1), It("Dir", 0)}
  \cup {It("Link", s) : s \in {0, 1}}
  \cup {It("Sym", s) : s \in {0, 1}}
  \cup {It("Pax", s) : s \in {0, 1, B + 1}}
  \cup {It("PaxBad", 1), It("Glob", 1), It("GlobBad", 1)}
  \cup {It("Gnu", s) : s \in {0, 1, B + 1}}
  \cup {It("GnuBad", s) : s \in {1, B + 1}}
  \cup {It("Bad", 0), It("Zero", 0)}
\* Extension sizes at the size <= BLOCK_SIZE boundary (lib.rs:2691, 2698,
\* 3202, 3235, 3251) and a global header larger than one block.
ItemsExt == ItemsMain \cup {It("Pax", B), It("Gnu", B), It("Glob", B + 1)}
\* PaxBadRepoll: bad PAX payload larger than one block.
ItemsPaxBadRepoll == ItemsExt \cup {It("PaxBad", B + 1), It("GlobBad", B + 1)}
\* LateFieldError: field error after the Dir arm moves nxt.
ItemsLateFieldError == ItemsExt \cup {It("DirBad", 0)}

ASSUME ItemSet \subseteq [k : Kinds, s : Nat]

-----------------------------------------------------------------------------
(* Source model *)

\* The source can give bytes up to Cap. The next header can come after lend.
Cap == IF Len(items) < MaxItems THEN lend + B ELSE lend + Trail
Avail == IF ended \/ Cap <= pos THEN 0 ELSE Cap - pos
\* Possible results of one poll_read with a buffer of want bytes.
\* 0 for an empty buffer is Ok(0). 0 for a non-empty buffer is end of data.
Reads(want) == IF want = 0 THEN {0}
               ELSE {0} \cup 1..Min(want, Min(MaxRead, Avail))
EndedNext(want, n) == ended' = (ended \/ (want > 0 /\ n = 0))

-----------------------------------------------------------------------------
(* Result of one archive step. Apply writes it to the variables. *)

Out0 == [nxt |-> nxt, st |-> st, err |-> FALSE, yld |-> FALSE,
         take |-> FALSE, own |-> "archive", xb |-> extBuf, push |-> FALSE,
         glob |-> FALSE, panic |-> FALSE]
ErrOut == [Out0 EXCEPT !.err = TRUE]
PanicOut == [Out0 EXCEPT !.panic = TRUE]
HdrAt(p) == [Out0 EXCEPT !.nxt = p + B, !.st = "Header"]

\* p is the new pos, j is the new cur.
Apply(r, p, j) ==
    /\ pos' = p
    /\ nxt' = r.nxt
    /\ st' = IF r.err THEN "Failed" ELSE r.st
    /\ cur' = j
    /\ exts' = IF r.take THEN <<>>
               ELSE IF r.push THEN Append(exts, j) ELSE exts
    /\ globs' = IF r.glob THEN globs \cup {j} ELSE globs
    /\ extBuf' = r.xb
    /\ owner' = r.own
    /\ delivered' = 0
    /\ peek' = 0
    /\ yielded' = IF r.yld
                  THEN Append(yielded, [i |-> j, ext |-> exts, glob |-> globs,
                                        afterErr |-> errSeen])
                  ELSE yielded
    /\ errSeen' = (errSeen \/ r.err)
    /\ panicked' = (panicked \/ r.panic)

\* The block [o, o + B) is complete. Find or make its item, then decode it
\* with F. p is the new pos.
AtBlock(o, p, F(_, _)) ==
    \/ \E j \in DOMAIN starts :
          /\ starts[j] = o
          /\ Apply(F(j, items[j]), p, j)
          /\ UNCHANGED gen
    \/ /\ o = lend
       /\ Len(items) < MaxItems
       /\ \E it \in ItemSet :
            /\ items' = Append(items, it)
            /\ starts' = Append(starts, lend)
            /\ lend' = lend + ItemLen(it)
            /\ Apply(F(Len(items) + 1, it), p, Len(items) + 1)
       /\ UNCHANGED misaligned
    \/ /\ o \notin Range(starts)
       /\ ~(o = lend /\ Len(items) < MaxItems)
       /\ misaligned' = TRUE
       /\ Apply(ErrOut, p, cur)
       /\ UNCHANGED <<items, starts, lend>>

TakeYield(nx, s2) ==
    [Out0 EXCEPT !.nxt = nx, !.st = s2, !.take = TRUE, !.yld = TRUE]

\* Decode a complete header block (lib.rs:2989-3245).
HdrOut(j, it) ==
    CASE it.k = "Zero" ->                     \* lib.rs:2989-2995
            [Out0 EXCEPT !.nxt = nxt + B, !.st = "Eof"]
      [] it.k = "Bad" ->                      \* lib.rs:2996-3002, 3240
            ErrOut
      [] it.k = "File" ->                     \* lib.rs:3004-3048
            [TakeYield(nxt + it.s, "Entry") EXCEPT !.own = "file"]
      [] it.k = "FileBad" ->                  \* lib.rs:3005-3013, before nxt
            [ErrOut EXCEPT !.take = TRUE]
      [] it.k = "Dir" ->                      \* lib.rs:3049-3071
            TakeYield(nxt + B, "Header")
      [] it.k = "DirBad" ->                   \* lib.rs:3057-3062, after nxt
            [ErrOut EXCEPT !.take = TRUE, !.nxt = nxt + B]
      [] it.k = "Link" ->                     \* lib.rs:3121-3145
            IF it.s = 0 THEN TakeYield(nxt + B, "Header")
            ELSE TakeYield(nxt + it.s, "Skip")
      [] it.k = "Sym" ->                      \* lib.rs:3146-3180
            IF it.s = 0 THEN TakeYield(nxt + B, "Header")
            ELSE [ErrOut EXCEPT !.take = TRUE] \* lib.rs:3153-3159
      [] it.k \in {"Gnu", "GnuBad"} /\ it.s = 0 -> \* lib.rs:3209-3217
            ErrOut
      [] it.k \in ExtKinds /\ ~(it.k \in {"Gnu", "GnuBad"} /\ it.s = 0) ->
            \* lib.rs:3181-3206, 3207-3239. ext.replace when size > B.
            [Out0 EXCEPT !.nxt = nxt + Padded(it.s), !.st = "Ext",
                         !.xb = IF it.s > B THEN TRUE ELSE extBuf]

\* Finish an extension at pos = nxt (lib.rs:3276-3324).
\* lost: the payload is in ext but ext is None. ext_as_bytes
\* (lib.rs:2697-2703) and ext_as_path (lib.rs:2690-2696) then unwrap None.
Fin(it) ==
    LET lost == it.s > B /\ ~extBuf
        Ok(push, glob) == [Out0 EXCEPT !.nxt = nxt + B, !.st = "Header",
                                       !.xb = FALSE, !.push = push,
                                       !.glob = glob]
    IN CASE it.k = "Gnu" ->      IF lost THEN PanicOut ELSE Ok(TRUE, FALSE)
         [] it.k = "GnuBad" ->   IF lost THEN PanicOut ELSE ErrOut
         [] it.k = "Pax" ->      IF lost THEN PanicOut ELSE Ok(TRUE, FALSE)
         [] it.k = "Glob" ->     IF lost THEN PanicOut ELSE Ok(FALSE, TRUE)
         [] it.k \in {"PaxBad", "GlobBad"} ->
                \* ext_as_bytes takes ext before PaxSet::parse fails
                \* (lib.rs:3285-3287, 3299-3300).
                IF lost THEN PanicOut
                ELSE [ErrOut EXCEPT !.xb = IF it.s > B THEN FALSE ELSE extBuf]

\* Second end block (lib.rs:3402-3412).
EofOut(j, it) ==
    IF it.k = "Zero" THEN [Out0 EXCEPT !.st = "Eoff"] ELSE ErrOut

-----------------------------------------------------------------------------
(* Archive steps: one pass of the poll_read_header loop each. *)

ArcReady == owner = "archive" /\ ~panicked

\* Header arm (lib.rs:2969-2988): fill header[B - (nxt - pos)..].
HeaderStep ==
    /\ ArcReady /\ st = "Header"
    /\ LET rem == nxt - pos IN
       \E n \in Reads(rem) :
         /\ EndedNext(rem, n)
         /\ IF n = 0 THEN Apply(ErrOut, pos, cur) /\ UNCHANGED gen
            ELSE IF n < rem THEN Apply(Out0, pos + n, cur) /\ UNCHANGED gen
            ELSE AtBlock(nxt - B, pos + n, HdrOut)

\* Extension arm (lib.rs:3247-3326).
ExtStep ==
    /\ ArcReady /\ st = "Ext"
    /\ UNCHANGED gen
    /\ LET it == items[cur]
           rem == nxt - pos
       IN IF rem = 0
          THEN Apply(Fin(it), pos, cur) /\ UNCHANGED ended
          \* Unreachable: ext is Some while pos < nxt. ext.as_mut().unwrap()
          \* at lib.rs:3257 would panic.
          ELSE IF it.s > B /\ ~extBuf
          THEN Apply(PanicOut, pos, cur) /\ UNCHANGED ended
          ELSE \E n \in Reads(rem) :
                 /\ EndedNext(rem, n)
                 /\ IF n = 0 THEN Apply(ErrOut, pos, cur)
                    ELSE IF n < rem THEN Apply(Out0, pos + n, cur)
                    ELSE Apply(Fin(it), pos + n, cur)

\* Padding arm (lib.rs:3327-3347).
PaddingStep ==
    /\ ArcReady /\ st = "Padding"
    /\ UNCHANGED gen
    /\ LET rem == nxt - pos IN
       \E n \in Reads(rem) :
         /\ EndedNext(rem, n)
         /\ IF n = 0 THEN Apply(ErrOut, pos, cur)
            ELSE IF n = rem THEN Apply(HdrAt(pos + n), pos + n, cur)
            ELSE Apply(Out0, pos + n, cur)

\* SkipEntry arm (lib.rs:3351-3382).
SkipStep ==
    /\ ArcReady /\ st = "Skip"
    /\ UNCHANGED gen
    /\ LET tgt == Padded(nxt)
           r == Min(SkipChunk, tgt - pos)
       IN IF r = 0
          THEN Apply(HdrAt(pos), pos, cur) /\ UNCHANGED ended
          ELSE \E n \in Reads(r) :
                 /\ EndedNext(r, n)
                 /\ IF n = 0 THEN Apply(ErrOut, pos, cur)
                    ELSE IF pos + n = tgt THEN Apply(HdrAt(pos + n), pos + n, cur)
                    ELSE Apply(Out0, pos + n, cur)

\* Eof arm (lib.rs:3383-3413): read the second end block.
EofStep ==
    /\ ArcReady /\ st = "Eof"
    /\ LET rem == nxt - pos IN
       \E n \in Reads(rem) :
         /\ EndedNext(rem, n)
         /\ IF n = 0 THEN Apply(ErrOut, pos, cur) /\ UNCHANGED gen
            ELSE IF n < rem THEN Apply(Out0, pos + n, cur) /\ UNCHANGED gen
            ELSE AtBlock(nxt - B, pos + n, EofOut)

\* Eoff arm (lib.rs:3414-3424): drain the source, then return None.
EoffStep ==
    /\ ArcReady /\ st = "Eoff"
    /\ UNCHANGED gen
    /\ \E n \in Reads(B) :
         /\ EndedNext(B, n)
         /\ IF n = 0 THEN Apply([Out0 EXCEPT !.st = "Done"], pos, cur)
            ELSE Apply(Out0, pos + n, cur)

ArchiveStep ==
    HeaderStep \/ ExtStep \/ PaddingStep \/ SkipStep \/ EofStep \/ EoffStep

-----------------------------------------------------------------------------
(* File steps: the consumer holds TarRegularFileReader and the core. *)

FileKeep == UNCHANGED <<items, starts, lend, misaligned, cur, exts, globs,
                        extBuf, panicked, yielded>>

\* end_regular_file (lib.rs:384-393) and return_core (lib.rs:1218-1237).
EndFile(p) ==
    LET pd == Padded(nxt) IN
    /\ nxt' = IF p = pd THEN pd + B ELSE pd
    /\ st' = IF p = pd THEN "Header" ELSE "Padding"
    /\ owner' = "archive"

\* Move n body bytes. End the file at nxt (lib.rs:375-378, 438-441).
Advance(n) ==
    /\ pos' = pos + n
    /\ delivered' = delivered + n
    /\ IF pos + n = nxt THEN EndFile(pos + n) ELSE UNCHANGED <<nxt, st, owner>>

\* poll_regular_file_reader (lib.rs:348-380).
FileRead ==
    /\ owner = "file"
    /\ FileKeep
    /\ peek' = 0
    /\ IF nxt = pos
       THEN Advance(0) /\ UNCHANGED <<ended, errSeen>>
       ELSE \E n \in Reads(nxt - pos) :
              /\ EndedNext(nxt - pos, n)
              /\ IF n = 0
                 THEN errSeen' = TRUE /\ UNCHANGED <<pos, nxt, st, owner, delivered>>
                 ELSE Advance(n) /\ UNCHANGED errSeen

\* poll_regular_file_fill_buf (lib.rs:396-426). peek is the buffer length.
FileFill ==
    /\ owner = "file" /\ peek = 0 /\ nxt > pos
    /\ FileKeep
    /\ \E n \in Reads(nxt - pos) :
         /\ EndedNext(nxt - pos, n)
         /\ IF n = 0
            THEN errSeen' = TRUE /\ UNCHANGED <<pos, nxt, st, owner, delivered, peek>>
            ELSE peek' = n /\ UNCHANGED <<pos, nxt, st, owner, delivered, errSeen>>

\* regular_file_consume (lib.rs:429-442) with a partial or full amount.
FileConsume ==
    /\ owner = "file" /\ peek > 0
    /\ FileKeep
    /\ \E a \in 1..peek : Advance(a)
    /\ peek' = 0
    /\ UNCHANGED <<ended, errSeen>>

\* Drop of the file reader: return_core sets SkipEntry (lib.rs:1222-1224).
FileDrop ==
    /\ owner = "file"
    /\ FileKeep
    /\ owner' = "archive" /\ st' = "Skip" /\ peek' = 0
    /\ UNCHANGED <<pos, nxt, delivered, ended, errSeen>>

FileStep == FileRead \/ FileFill \/ FileConsume \/ FileDrop

\* Done and Failed return None forever (lib.rs:3425). A panic stops the
\* program.
Terminal == (st \in {"Done", "Failed"} \/ panicked) /\ UNCHANGED vars

-----------------------------------------------------------------------------

\* TarReaderInner::new (lib.rs:2919-2933).
Init ==
    /\ items = <<>> /\ starts = <<>> /\ lend = 0
    /\ pos = 0 /\ nxt = B /\ st = "Header" /\ cur = 0
    /\ exts = <<>> /\ globs = {} /\ extBuf = FALSE
    /\ owner = "archive" /\ delivered = 0 /\ peek = 0
    /\ ended = FALSE /\ errSeen = FALSE /\ panicked = FALSE
    /\ misaligned = FALSE /\ yielded = <<>>

Next == ArchiveStep \/ FileStep \/ Terminal

\* The consumer keeps polling and drops each file at some time.
Spec == Init /\ [][Next]_vars /\ WF_vars(ArchiveStep) /\ WF_vars(FileDrop)

-----------------------------------------------------------------------------
(* Properties *)

States == {"Header", "Ext", "Entry", "Skip", "Padding", "Eof", "Eoff", "Done",
           "Failed"}
Idx == 1..Len(items)
ItemEnd(j) == starts[j] + ItemLen(items[j])

TypeOK ==
    /\ items \in Seq(ItemSet) /\ Len(items) <= MaxItems
    /\ starts \in Seq(Nat) /\ Len(starts) = Len(items) /\ lend \in Nat
    /\ pos \in Nat /\ nxt \in Nat /\ st \in States /\ cur \in 0..Len(items)
    /\ exts \in Seq(Idx) /\ globs \subseteq Idx
    /\ extBuf \in BOOLEAN /\ owner \in {"archive", "file"}
    /\ delivered \in Nat /\ peek \in Nat
    /\ ended \in BOOLEAN /\ errSeen \in BOOLEAN
    /\ panicked \in BOOLEAN /\ misaligned \in BOOLEAN
    /\ yielded \in Seq([i : Idx, ext : Seq(Idx), glob : SUBSET Idx,
                        afterErr : BOOLEAN])

\* pos never passes nxt. SkipEntry reads on to Padded(nxt) (see SkipWindow).
\* In Eoff and Failed nxt has no use.
PosLeNxt == st \notin {"Skip", "Eoff", "Done", "Failed"} => pos <= nxt

\* A header read starts on a block boundary and reads at most one block.
HeaderAligned ==
    st \in {"Header", "Eof"} =>
        /\ nxt % B = 0
        /\ nxt - pos <= B
        /\ (nxt - pos = B => pos % B = 0)

\* The reader decodes headers only at item starts.
Aligned == ~misaligned

\* Only an active file holds the core (lib.rs:3348-3350).
OwnerState == (owner = "file") <=> (st = "Entry")

\* Body reads stay inside the data of the File item.
BodyWindow ==
    st = "Entry" =>
        LET ds == starts[cur] + B IN
        /\ items[cur].k = "File"
        /\ nxt = ds + items[cur].s
        /\ ds <= pos
        /\ delivered = pos - ds
        /\ peek <= nxt - pos

\* Extension, skip and padding end at the end of their item.
ExtWindow ==
    st = "Ext" =>
        /\ items[cur].k \in ExtKinds
        /\ nxt = ItemEnd(cur)
        /\ starts[cur] + B <= pos
SkipWindow ==
    st = "Skip" =>
        /\ items[cur].k \in {"File", "Link"}
        /\ Padded(nxt) = ItemEnd(cur)
        /\ pos <= Padded(nxt)
PaddingWindow ==
    st = "Padding" =>
        /\ items[cur].k = "File"
        /\ nxt = ItemEnd(cur)
        /\ pos < nxt
\* An unread header starts at the end of the last item.
NextHeader ==
    (st \in {"Header", "Eof"} /\ pos < nxt) =>
        nxt - B = IF cur = 0 THEN 0 ELSE ItemEnd(cur)

\* Ok entries come in archive order and are entry items.
YieldKinds == {"File", "Dir", "Link", "Sym"}
YieldOrder ==
    \A a \in DOMAIN yielded :
        /\ items[yielded[a].i].k \in YieldKinds
        /\ (items[yielded[a].i].k = "Sym" => items[yielded[a].i].s = 0)
        /\ \A b \in DOMAIN yielded : a < b => yielded[a].i < yielded[b].i

\* Before any Err, the Ok entries are all entries of the archive prefix.
YieldPrefix ==
    \A a \in DOMAIN yielded :
        ~yielded[a].afterErr =>
            {yielded[b].i : b \in 1..(a - 1)}
                = {j \in 1..(yielded[a].i - 1) : items[j].k \in TakeKinds}

\* Local extensions go to the next header that drains exts. Global
\* extensions go to all later entries.
LastTake(i) ==
    LET S == {j \in 1..(i - 1) : items[j].k \in TakeKinds}
    IN IF S = {} THEN 0 ELSE CHOOSE m \in S : \A x \in S : x <= m
GoodLocal(j) == items[j].k = "Pax" \/ (items[j].k = "Gnu" /\ items[j].s > 0)
\* Indices lo..hi as a sequence in increasing order.
Seq2(lo, hi) == [k \in 1..(IF hi < lo THEN 0 ELSE hi - lo + 1) |-> lo + k - 1]
\* The order is part of the check: take_pax_info applies exts in order and
\* a later LongName or PAX path wins (lib.rs:2724-2729).
ExtAttach ==
    \A a \in DOMAIN yielded :
        LET i == yielded[a].i IN
        /\ yielded[a].ext = SelectSeq(Seq2(LastTake(i) + 1, i - 1), GoodLocal)
        /\ yielded[a].glob = {j \in 1..(i - 1) : items[j].k = "Glob"}

\* None comes only after two zero blocks at header boundaries.
DoneAfterMarker ==
    st = "Done" =>
        \E j \in 1..(Len(items) - 1) :
            items[j].k = "Zero" /\ items[j + 1].k = "Zero"

\* End of data before the end marker gives an Err, never a clean None.
TruncIsErr == ended => (errSeen \/ st \in {"Eoff", "Done"})

\* An Err comes only from truncation or a faulty item.
Faulty(j) ==
    \/ items[j].k \in {"FileBad", "DirBad", "PaxBad", "GlobBad", "GnuBad", "Bad"}
    \/ items[j].k = "Sym" /\ items[j].s > 0
    \/ items[j].k = "Gnu" /\ items[j].s = 0
    \/ items[j].k = "Zero" /\ j < Len(items) /\ items[j + 1].k /= "Zero"
ErrHasCause == errSeen => (ended \/ \E j \in Idx : Faulty(j))

\* After an Err, more polls give no Ok entry. After an Err for the archive
\* data the state is Failed, and more polls give None. Done comes only
\* from the end marker, never after an Err.
ErrSticky ==
    /\ \A a \in DOMAIN yielded : ~yielded[a].afterErr
    /\ (errSeen => st /= "Done")

\* The code does not panic.
NoPanic == ~panicked

\* Liveness: each fair run ends in None, an Err, or a panic. With
\* ErrHasCause, an archive without faults and truncation ends in None.
Terminates == <>(st = "Done" \/ errSeen \/ panicked)
=============================================================================
