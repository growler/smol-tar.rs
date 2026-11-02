----------------------------- MODULE TarWriter -----------------------------
(***************************************************************************)
(* Model of the smol-tar archive writer: TarWriter and its Sink impl in    *)
(* smol-tar/src/lib.rs.                                                    *)
(*                                                                         *)
(* Code map (line numbers in smol-tar/src/lib.rs):                         *)
(*   enum WriterState ............ 3956-3963  -> state                     *)
(*   struct TarWriter ............ 4022-4045                               *)
(*   TarWriter::new .............. 4050-4064  -> Init                      *)
(*   TarWriter::write ............ 4130-4147  -> CallWrite, phases         *)
(*                                   "ready", "flush", "wflush"            *)
(*   TarWriter::flush ............ 4178-4185  -> CallRawSend, phases       *)
(*                                   "flush", "wflush"                     *)
(*   TarWriter::finish ........... 4211-4218  -> CallFinish, phases        *)
(*                                   "close1", "close2", "closew"          *)
(*   poll_drain .................. 4227-4286  -> BufWrite, PadStep,        *)
(*                                   BodyWrite, BodyEof, DrainDone         *)
(*   Sink::poll_ready ............ 4294-4299  -> CallWrite (state check)   *)
(*   Sink::start_send ............ 4301-4323  -> Accept, CallRawSend       *)
(*   Sink::poll_flush ............ 4325-4330                               *)
(*   Sink::poll_close ............ 4332-4350  -> CallFinish (Closed check) *)
(*   padded_size ................. 3650-3656  -> Padded                    *)
(*   write_header ................ 3669-3839  -> header of 1 or 3 blocks   *)
(*                                                                         *)
(* Abstractions:                                                           *)
(* - BLOCK is a small number. In the code, BLOCK_SIZE is 512.              *)
(* - A header is 1 block (ustar) or 3 blocks (PAX header, one block of PAX *)
(*   records, ustar header). Header bytes are not encoded. Each byte in    *)
(*   the output is a tag: <<"H", s>> header, <<"B", s>> body, <<"P", s>>   *)
(*   padding, <<"Z", 0>> end marker. The number s is the sequence number   *)
(*   of the start_send call that accepted the entry.                       *)
(* - A header error (write_header returns Err) is the flag "bad" on the    *)
(*   entry.                                                                *)
(* - One poll_write call accepts 1 to MaxChunk bytes. One poll_fill_buf    *)
(*   call gives 1 to MaxChunk bytes. A body write step joins poll_fill_buf *)
(*   and poll_write, because poll_fill_buf without consume has no effect.  *)
(* - Pending has no effect on the writer state. The model has no Pending   *)
(*   step. A future can stop (Cancel) at each I/O point, because the code  *)
(*   can return Pending there.                                             *)
(* - An I/O error and a zero-length write (WriteZero) have no effect on    *)
(*   the writer state. Fault models all of them. MaxFaults limits them.    *)
(* - The model assumes that a poll_write that returns Err took 0 bytes.    *)
(*   This is the usual AsyncWrite rule. The code does not enforce it.      *)
(* - poll_ready and start_send return Err when state is not "open". The    *)
(*   model does the poll_ready check in CallWrite. Only poll_close changes *)
(*   state, and the caller drives one future at a time. Thus state does    *)
(*   not change between the call of write() and its poll_ready.            *)
(* - poll_close returns Ok at once when state is "closed". The model does  *)
(*   this check in CallFinish. A failed or dropped close keeps state at    *)
(*   "marker", so the next close calls the underlying poll_close again.    *)
(* - A short body: poll_fill_buf gives an empty slice before the end of    *)
(*   the body. The writer drops the body reader and sets short. Then it    *)
(*   fills the rest of the entry with zero bytes from buf and returns      *)
(*   UnexpectedEof one time. The zero bytes have the padding tag. In the   *)
(*   code, one fill step uses all of buf (at least 4 KiB). The model uses  *)
(*   FillChunk bytes, so that a fill has more than one step.               *)
(* - Padded is a copy of padded_size. Only the code-side actions use it.   *)
(*   The expected layout (EntryBytes) uses a different padding formula.    *)
(* - The caller drives one future at a time (one task).                    *)
(***************************************************************************)
EXTENDS Naturals, Sequences, FiniteSets

CONSTANTS
    BLOCK,              \* tar block size, scaled down
    MaxChunk,           \* max bytes for one poll_write or poll_fill_buf
    Entries,            \* entries that the caller writes, in order
    BodyKinds,          \* body kinds: "exact", "short", "long"
    MaxFaults,          \* max number of I/O errors
    MaxCancels,         \* max number of dropped futures
    MaxRejects,         \* max number of rejected poll_ready and start_send
                        \* calls
    MaxFinish,          \* max number of finish calls that return Ok
    CallerKnowsCommit,  \* TRUE: after a failed or dropped write(), the
                        \* caller knows if start_send accepted the entry
    SendAfterClose,     \* TRUE: the caller can send entries after finish
    RawSink             \* TRUE: the caller can call start_send directly

(***************************************************************************)
(* Entry lists for the cfg files.                                          *)
(*   file: TRUE for TarEntry::File (it has a body reader).                 *)
(*   hdr:  header length in blocks (1 or 3).                               *)
(*   size: TarRegularFile::size.                                           *)
(*   bad:  write_header returns Err.                                       *)
(***************************************************************************)
MainEntries == <<
    [file |-> TRUE,  hdr |-> 1, size |-> 5, bad |-> FALSE],
    [file |-> FALSE, hdr |-> 3, size |-> 0, bad |-> FALSE],
    [file |-> TRUE,  hdr |-> 1, size |-> 2, bad |-> TRUE],
    [file |-> TRUE,  hdr |-> 3, size |-> 4, bad |-> FALSE],
    [file |-> TRUE,  hdr |-> 1, size |-> 0, bad |-> FALSE]
>>

SmallEntries == <<
    [file |-> TRUE,  hdr |-> 1, size |-> 1, bad |-> FALSE],
    [file |-> FALSE, hdr |-> 1, size |-> 0, bad |-> FALSE]
>>

VARIABLES
    \* TarWriter fields (lib.rs:4022-4045)
    pos,      \* buf position
    len,      \* length of data in buf
    buf,      \* tag of all bytes in buf[0..len]
    total,    \* bytes accepted by the underlying writer
    eof,      \* end offset of the current entry
    state,    \* WriterState: "open"; "marker": end marker is in buf or
              \* written; "closed": the underlying writer is closed
    reader,   \* body reader: on = Some, s = entry number, left = bytes left
    short,    \* the body ended before its size: return UnexpectedEof after
              \* the zero bytes
    \* environment
    out,      \* bytes accepted by the underlying writer, as tags
    wclosed,  \* number of Ok returns from the underlying poll_close
    bk,       \* body kind of each file entry
    \* caller
    op,       \* current future and its phase
    idx,      \* next entry that the caller sends
    cur,      \* entry of the current write future
    raw,      \* current op started with a direct start_send
    fin,      \* number of finish calls that returned Ok
    fstart,   \* finish was called
    faults, cancels, rejects,
    \* ghost
    sent,     \* entries accepted by start_send, in order
    layout,   \* bytes that the accepted entries and the marker must make
    markers,  \* number of times poll_close put the marker in buf
    eofErrs   \* number of UnexpectedEof returns for a short body

wvars == <<pos, len, buf, total, eof, state, reader, short>>
vars == <<pos, len, buf, total, eof, state, reader, short, out, wclosed, bk,
          op, idx, cur, raw, fin, fstart, faults, cancels, rejects,
          sent, layout, markers, eofErrs>>

Min(a, b) == IF a < b THEN a ELSE b

\* padded_size, lib.rs:3650-3656
Padded(n) == IF n = 0 THEN 0 ELSE ((n + BLOCK - 1) \div BLOCK) * BLOCK

Hdr(s)  == <<"H", s>>
Body(s) == <<"B", s>>
Pad(s)  == <<"P", s>>
Zero    == <<"Z", 0>>
NoTag   == <<"N", 0>>

Rep(t, n) == [i \in 1..n |-> t]
Count(sq, t) == Cardinality({i \in 1..Len(sq) : sq[i] = t})

\* Number of bytes that the body reader of entry i gives before EOF.
BodyLen(i) ==
    CASE bk[i] = "exact" -> Entries[i].size
      [] bk[i] = "short" -> IF Entries[i].size > 0
                               THEN Entries[i].size - 1 ELSE 0
      [] bk[i] = "long"  -> Entries[i].size + 1

\* The tar layout of entry i with sequence number s: header, body, zero
\* bytes for a short body, zero padding to a block.
\* The padding formula is independent of Padded (the code-side copy).
EntryBytes(s, i) ==
    LET e    == Entries[i]
        body == Min(e.size, BodyLen(i))
        pad  == (BLOCK - (e.size % BLOCK)) % BLOCK
    IN Rep(Hdr(s), e.hdr * BLOCK)
       \o (IF e.file
             THEN Rep(Body(s), body) \o Rep(Pad(s), (e.size - body) + pad)
             ELSE << >>)

\* End marker: two zero blocks.
Marker == Rep(Zero, 2 * BLOCK)

\* Archive of a list of entry indices, without the end marker.
RECURSIVE ArchiveUpTo(_, _)
ArchiveUpTo(sq, k) ==
    IF k = 0 THEN << >>
    ELSE ArchiveUpTo(sq, k - 1) \o EntryBytes(k, sq[k])
Archive(sq) == ArchiveUpTo(sq, Len(sq))

\* The entries that the caller wants in the archive.
Intended == SelectSeq([i \in 1..Len(Entries) |-> i],
                      LAMBDA i : ~Entries[i].bad)

FileIdx == {i \in 1..Len(Entries) : Entries[i].file}

NoReader == [on |-> FALSE, s |-> 0, left |-> 0]

\* eof.saturating_sub(total), lib.rs:4249
Remain == IF eof > total THEN eof - total ELSE 0

\* start_send busy check, lib.rs:4308. start_send checks state before it.
Busy == len # 0 \/ reader.on

DrainOps  == {"ready", "flush", "close1", "close2"}
WriteOps  == {"ready", "flush", "wflush"}
FinishOps == {"close1", "close2", "closew"}
Ops == {"idle", "wflush", "closew"} \cup DrainOps

\* Zero bytes in one fill step. The code uses buf.len().
FillChunk == 1

----------------------------------------------------------------------------
(* TarWriter::new, lib.rs:4050-4064 *)
Init ==
    /\ pos = 0 /\ len = 0 /\ buf = NoTag
    /\ total = 0 /\ eof = 0 /\ state = "open" /\ reader = NoReader
    /\ short = FALSE
    /\ out = << >> /\ wclosed = 0
    /\ bk \in [FileIdx -> BodyKinds]
    /\ op = "idle" /\ idx = 1 /\ cur = 0 /\ raw = FALSE
    /\ fin = 0 /\ fstart = FALSE
    /\ faults = 0 /\ cancels = 0 /\ rejects = 0
    /\ sent = << >> /\ layout = << >> /\ markers = 0 /\ eofErrs = 0

(***************************************************************************)
(* start_send after the busy check passed, lib.rs:4314-4322. The header   *)
(* goes into buf. A file entry stores its reader and its end offset.      *)
(* The model calls Accept only for an entry without a header error.       *)
(***************************************************************************)
Accept(i) ==
    LET e  == Entries[i]
        s  == Len(sent) + 1
        hl == e.hdr * BLOCK
    IN /\ pos' = 0
       /\ len' = hl                                         \* lib.rs:4315
       /\ buf' = Hdr(s)
       /\ reader' = IF e.file                               \* lib.rs:4318
                      THEN [on |-> TRUE, s |-> s, left |-> BodyLen(i)]
                      ELSE NoReader
       /\ eof' = IF e.file THEN total + hl + e.size ELSE eof \* lib.rs:4319
       /\ sent' = Append(sent, i)
       /\ layout' = layout \o EntryBytes(s, i)
       /\ UNCHANGED <<total, state>>

----------------------------------------------------------------------------
(* Caller actions *)

\* Call TarWriter::write (lib.rs:4130). The future starts with poll_ready.
\* When state is not "open", poll_ready returns Err and changes nothing
\* (lib.rs:4295-4297). Then write() returns the error.
CallWrite ==
    /\ op = "idle"
    /\ idx <= Len(Entries)
    /\ (~fstart \/ SendAfterClose)
    /\ IF state # "open"
         THEN /\ rejects < MaxRejects
              /\ rejects' = rejects + 1
              /\ UNCHANGED <<pos, len, buf, total, eof, state, reader, short,
                             out, wclosed, bk, op, idx, cur, raw, fin,
                             fstart, faults, cancels, sent, layout, markers,
                             eofErrs>>
         ELSE /\ op' = "ready" /\ cur' = idx /\ raw' = FALSE
              /\ UNCHANGED <<pos, len, buf, total, eof, state, reader, short,
                             out, wclosed, bk, idx, fin, fstart, faults,
                             cancels, rejects, sent, layout, markers,
                             eofErrs>>

(***************************************************************************)
(* A Sink user calls start_send without poll_ready (lib.rs:4301). When    *)
(* state is not "open" or the writer is busy, start_send returns Err and  *)
(* changes nothing (lib.rs:4304-4312). After Ok, the user calls           *)
(* poll_flush (or TarWriter::flush) or poll_close.                        *)
(* The user sees the Ok, so it goes to the next entry at once.            *)
(***************************************************************************)
CallRawSend ==
    /\ RawSink
    /\ op = "idle"
    /\ idx <= Len(Entries)
    /\ (~fstart \/ SendAfterClose)
    /\ IF state # "open" \/ Busy
         THEN /\ rejects < MaxRejects
              /\ rejects' = rejects + 1
              /\ UNCHANGED <<pos, len, buf, total, eof, state, reader, short,
                             out, wclosed, bk, op, idx, cur, raw, fin,
                             fstart, faults, cancels, sent, layout, markers,
                             eofErrs>>
         ELSE IF Entries[idx].bad
           THEN \* write_header returns Err, lib.rs:4314. The caller skips
                \* the entry.
                /\ idx' = idx + 1
                /\ UNCHANGED <<pos, len, buf, total, eof, state, reader,
                               short, out, wclosed, bk, op, cur, raw, fin,
                               fstart, faults, cancels, rejects, sent, layout,
                               markers, eofErrs>>
           ELSE /\ Accept(idx)
                /\ cur' = idx /\ raw' = TRUE /\ idx' = idx + 1
                /\ \/ op' = "flush" /\ UNCHANGED fstart     \* lib.rs:4325
                   \/ op' = "close1" /\ fstart' = TRUE      \* lib.rs:4332
                /\ UNCHANGED <<short, out, wclosed, bk, fin, faults,
                               cancels, rejects, markers, eofErrs>>

(***************************************************************************)
(* Call TarWriter::finish (lib.rs:4211). The future calls poll_close.     *)
(* The caller can call it before it sent all entries. When state is       *)
(* "closed", poll_close returns Ok at once and changes nothing            *)
(* (lib.rs:4333-4335). Then finish() returns Ok.                          *)
(***************************************************************************)
CallFinish ==
    /\ op = "idle"
    /\ fin < MaxFinish
    /\ IF state = "closed"
         THEN /\ fin' = fin + 1
              /\ UNCHANGED <<pos, len, buf, total, eof, state, reader, short,
                             out, wclosed, bk, op, idx, cur, raw, fstart,
                             faults, cancels, rejects, sent, layout, markers,
                             eofErrs>>
         ELSE /\ op' = "close1" /\ fstart' = TRUE
              /\ UNCHANGED <<pos, len, buf, total, eof, state, reader, short,
                             out, wclosed, bk, idx, cur, raw, fin, faults,
                             cancels, rejects, sent, layout, markers,
                             eofErrs>>

----------------------------------------------------------------------------
(* poll_drain, lib.rs:4227-4286 *)

(***************************************************************************)
(* Write buf[pos..len], lib.rs:4230-4246. When buf becomes empty, the     *)
(* code sets pos and len to 0 (lib.rs:4245-4246) before the next I/O       *)
(* point. This step does the same.                                        *)
(***************************************************************************)
BufWrite ==
    /\ op \in DrainOps
    /\ pos < len
    /\ \E n \in 1..Min(MaxChunk, len - pos) :
         /\ out' = out \o Rep(buf, n)
         /\ total' = total + n                              \* lib.rs:4242
         /\ IF pos + n = len
              THEN pos' = 0 /\ len' = 0
              ELSE pos' = pos + n /\ len' = len
    /\ UNCHANGED <<buf, eof, state, reader, short, wclosed, bk, op, idx, cur,
                   raw, fin, fstart, faults, cancels, rejects, sent, layout,
                   markers, eofErrs>>

(***************************************************************************)
(* Body done or ended early, zero bytes needed, lib.rs:4267-4276. The      *)
(* writer drops the body reader and puts the next part of the zero bytes   *)
(* in buf: the missing body bytes and the block padding. The current entry *)
(* is the last entry that Accept added. Its sequence number is Len(sent).  *)
(***************************************************************************)
PadStep ==
    /\ op \in DrainOps
    /\ len = 0
    /\ reader.on => Remain = 0
    /\ Padded(eof) > total                                  \* lib.rs:4273
    /\ reader' = NoReader                                   \* lib.rs:4267
    /\ len' = Min(Padded(eof) - total, FillChunk)           \* lib.rs:4274
    /\ pos' = 0
    /\ buf' = Pad(Len(sent))                                \* lib.rs:4275
    /\ UNCHANGED <<total, eof, state, short, out, wclosed, bk, op, idx, cur,
                   raw, fin, fstart, faults, cancels, rejects, sent, layout,
                   markers, eofErrs>>

\* poll_fill_buf gives data, poll_write accepts n bytes, lib.rs:4251-4263.
BodyWrite ==
    /\ op \in DrainOps
    /\ len = 0
    /\ reader.on
    /\ Remain > 0
    /\ reader.left > 0
    /\ \E n \in 1..Min(MaxChunk, Min(reader.left, Remain)) :
         /\ out' = out \o Rep(Body(reader.s), n)
         /\ total' = total + n                              \* lib.rs:4262
         /\ reader' = [reader EXCEPT !.left = @ - n]        \* lib.rs:4261
    /\ UNCHANGED <<pos, len, buf, eof, state, short, wclosed, bk, op, idx,
                   cur, raw, fin, fstart, faults, cancels, rejects, sent,
                   layout, markers, eofErrs>>

(***************************************************************************)
(* Body ends early: poll_fill_buf gives an empty slice, lib.rs:4251. The   *)
(* writer drops the body reader and sets short (lib.rs:4265-4267). Then    *)
(* PadStep fills the rest of the entry with zero bytes.                    *)
(***************************************************************************)
BodyEof ==
    /\ op \in DrainOps
    /\ len = 0
    /\ reader.on
    /\ Remain > 0
    /\ reader.left = 0
    /\ reader' = NoReader                                   \* lib.rs:4267
    /\ short' = TRUE                                        \* lib.rs:4265
    /\ UNCHANGED <<pos, len, buf, total, eof, state, out, wclosed, bk, op,
                   idx, cur, raw, fin, fstart, faults, cancels, rejects,
                   sent, layout, markers, eofErrs>>

(***************************************************************************)
(* poll_drain returns Ok (lib.rs:4284). Then the future goes on.           *)
(*   ready:  start_send runs in the same poll, lib.rs:4139.               *)
(*   flush:  writer.poll_flush is next, lib.rs:4328.                      *)
(*   close1: marker into buf when state is "open", lib.rs:4337-4344.      *)
(*   close2: writer poll_close is next, lib.rs:4347.                      *)
(***************************************************************************)
DrainDone ==
    /\ op \in DrainOps
    /\ len = 0
    /\ ~short
    /\ reader.on => Remain = 0
    /\ Padded(eof) <= total
    /\ CASE op = "ready" ->
              IF Entries[cur].bad
                THEN \* write_header returns Err, lib.rs:4314. The caller
                     \* skips the entry.
                     /\ op' = "idle" /\ idx' = cur + 1
                     /\ reader' = NoReader
                     /\ UNCHANGED <<pos, len, buf, total, eof, state, out,
                                    sent, layout, markers>>
                ELSE /\ Accept(cur)
                     /\ op' = "flush"
                     /\ UNCHANGED <<out, idx, markers>>
         [] op = "flush" ->
              /\ op' = "wflush"
              /\ reader' = NoReader
              /\ UNCHANGED <<pos, len, buf, total, eof, state, out, idx,
                             sent, layout, markers>>
         [] op = "close1" ->
              /\ op' = "close2"
              /\ reader' = NoReader
              /\ IF state = "open"                          \* lib.rs:4339
                   THEN /\ buf' = Zero                      \* lib.rs:4340
                        /\ len' = 2 * BLOCK                 \* lib.rs:4341
                        /\ pos' = 0
                        /\ state' = "marker"                \* lib.rs:4342
                        /\ markers' = markers + 1
                        /\ layout' = layout \o Marker
                   ELSE UNCHANGED <<pos, len, buf, state, markers, layout>>
              /\ UNCHANGED <<total, eof, out, idx, sent>>
         [] op = "close2" ->
              /\ op' = "closew"
              /\ reader' = NoReader
              /\ UNCHANGED <<pos, len, buf, total, eof, state, out, idx,
                             sent, layout, markers>>
    /\ UNCHANGED <<short, wclosed, bk, cur, raw, fin, fstart, faults,
                   cancels, rejects, eofErrs>>

\* writer.poll_flush returns Ok, lib.rs:4328. write() returns Ok.
WFlushOk ==
    /\ op = "wflush"
    /\ op' = "idle" /\ idx' = cur + 1
    /\ UNCHANGED <<pos, len, buf, total, eof, state, reader, short, out,
                   wclosed, bk, cur, raw, fin, fstart, faults, cancels,
                   rejects, sent, layout, markers, eofErrs>>

\* Underlying poll_close returns Ok, lib.rs:4347. The writer sets state to
\* Closed (lib.rs:4348). finish() returns Ok.
WCloseOk ==
    /\ op = "closew"
    /\ op' = "idle" /\ fin' = fin + 1 /\ wclosed' = wclosed + 1
    /\ state' = "closed"
    /\ UNCHANGED <<pos, len, buf, total, eof, reader, short, out, bk, idx,
                   cur, raw, fstart, faults, cancels, rejects, sent, layout,
                   markers, eofErrs>>

----------------------------------------------------------------------------
(* Errors and dropped futures *)

\* The code can return Pending or Err here.
IOPoint ==
    \/ /\ op \in DrainOps
       /\ \/ pos < len                                      \* lib.rs:4231
          \/ len = 0 /\ reader.on /\ Remain > 0             \* lib.rs:4251,4254
    \/ op \in {"wflush", "closew"}                          \* lib.rs:4328,4347

(***************************************************************************)
(* Next entry for the caller after write() fails or is dropped. The       *)
(* entry is in the archive only if start_send accepted it (phase "flush"   *)
(* or "wflush"). If CallerKnowsCommit is FALSE, the caller cannot see the  *)
(* phase, and it can send the entry again or skip it. A direct start_send *)
(* gives its result to the caller, so a raw caller always knows.          *)
(***************************************************************************)
NextIdx ==
    IF op \in WriteOps
      THEN IF CallerKnowsCommit \/ raw
             THEN {IF op \in {"flush", "wflush"} THEN cur + 1 ELSE cur}
             ELSE {cur, cur + 1}
      ELSE {idx}

\* poll_write returns Err or 0 (WriteZero), poll_fill_buf returns Err, or
\* poll_flush / poll_close returns Err. No field changes.
Fault ==
    /\ IOPoint
    /\ faults < MaxFaults
    /\ faults' = faults + 1
    /\ op' = "idle"
    /\ idx' \in NextIdx
    /\ UNCHANGED <<pos, len, buf, total, eof, state, reader, short, out,
                   wclosed, bk, cur, raw, fin, fstart, cancels, rejects, sent,
                   layout, markers, eofErrs>>

\* The code returns Pending and the caller drops the future.
Cancel ==
    /\ IOPoint
    /\ cancels < MaxCancels
    /\ cancels' = cancels + 1
    /\ op' = "idle"
    /\ idx' \in NextIdx
    /\ UNCHANGED <<pos, len, buf, total, eof, state, reader, short, out,
                   wclosed, bk, cur, raw, fin, fstart, faults, rejects, sent,
                   layout, markers, eofErrs>>

(***************************************************************************)
(* The zero bytes of a short body are written. poll_drain clears short     *)
(* and returns UnexpectedEof, lib.rs:4278-4282. The future returns the     *)
(* error. The writer state is the same as after a complete entry. The      *)
(* caller goes on as after Fault.                                          *)
(***************************************************************************)
ShortErr ==
    /\ op \in DrainOps
    /\ len = 0
    /\ ~reader.on
    /\ short
    /\ Padded(eof) <= total
    /\ short' = FALSE
    /\ eofErrs' = eofErrs + 1
    /\ op' = "idle"
    /\ idx' \in NextIdx
    /\ UNCHANGED <<pos, len, buf, total, eof, state, reader, out, wclosed,
                   bk, cur, raw, fin, fstart, faults, cancels, rejects, sent,
                   layout, markers>>

----------------------------------------------------------------------------
\* The state disjunct matters only when MaxFinish = 0: fin >= 1 implies
\* state # "open", but a raw sink close can change state with fin = 0.
Terminal ==
    /\ op = "idle" /\ fin = MaxFinish
    /\ (idx > Len(Entries) \/ ~SendAfterClose \/ state # "open")

Done == Terminal /\ UNCHANGED vars

Progress == BufWrite \/ PadStep \/ BodyWrite \/ BodyEof \/ DrainDone
            \/ ShortErr \/ WFlushOk \/ WCloseOk
CallerAct == CallWrite \/ CallRawSend \/ CallFinish

Next == Progress \/ CallerAct \/ Fault \/ Cancel \/ Done

\* Fairness: the underlying writer and the body reader make progress, and
\* the caller keeps calling. Faults, drops, and rejects are bounded.
Spec == Init /\ [][Next]_vars /\ WF_vars(Progress) /\ WF_vars(CallerAct)

----------------------------------------------------------------------------
(* Properties *)

TypeOK ==
    /\ op \in Ops
    /\ idx \in 1..(Len(Entries) + 1)
    /\ cur \in 0..Len(Entries)
    /\ pos \in Nat /\ len \in 0..(3 * BLOCK)
    /\ total \in Nat /\ eof \in Nat
    /\ state \in {"open", "marker", "closed"}
    /\ raw \in BOOLEAN /\ fstart \in BOOLEAN /\ short \in BOOLEAN
    /\ reader \in [on : BOOLEAN, s : Nat, left : Nat]
    /\ buf[1] \in {"H", "P", "Z", "N"}
    /\ \A i \in 1..Len(out) : out[i][1] \in {"H", "B", "P", "Z"}
    /\ fin \in 0..MaxFinish /\ wclosed \in Nat
    /\ faults \in 0..MaxFaults /\ cancels \in 0..MaxCancels
    /\ rejects \in 0..MaxRejects /\ eofErrs \in Nat
    /\ \A i \in 1..Len(sent) : sent[i] \in 1..Len(Entries)

\* Between polls, buf is empty (pos = len = 0) or has bytes to write.
PosLenOK == pos < len \/ (pos = 0 /\ len = 0)

\* total counts the bytes that the underlying writer accepted.
TotalIsOutput == total = Len(out)

\* The output is a prefix of the tar layout of the accepted entries (and
\* the marker, in the order that poll_close put it in buf).
OutIsPrefix ==
    /\ Len(out) <= Len(layout)
    /\ SubSeq(layout, 1, Len(out)) = out

\* Each header and the end marker start at a block boundary.
BoundaryAligned ==
    \A i \in 2..Len(out) :
        (out[i] # out[i - 1] /\ out[i][1] \in {"H", "Z"})
            => (i - 1) % BLOCK = 0

\* No body has more bytes than its size (long bodies are cut).
BodyNotLonger ==
    \A s \in 1..Len(sent) :
        Count(out, Body(s)) <= Entries[sent[s]].size

\* The end marker goes into buf once and is written once.
MarkerOnce == markers <= 1 /\ Count(out, Zero) <= 2 * BLOCK

\* Only zero bytes of the marker follow the first marker byte.
NothingAfterMarker ==
    \A i \in 1..(Len(out) - 1) : out[i] = Zero => out[i + 1] = Zero

(***************************************************************************)
(* After finish() returns Ok, the output is the tar archive of the        *)
(* entries that start_send accepted, in order, then the marker.           *)
(***************************************************************************)
FinalWellFormed == fin >= 1 => out = Archive(sent) \o Marker

(***************************************************************************)
(* After finish() returns Ok and the caller went past the last entry, the *)
(* output is the archive of the entries without a header error, each one  *)
(* time and in list order, then the marker. The check covers all runs     *)
(* with drops and errors in the bounds. Thus a drop or an error does not  *)
(* lose, repeat, or reorder an entry, and does not corrupt the layout.    *)
(***************************************************************************)
FinalOutput ==
    (fin >= 1 /\ idx > Len(Entries)) => out = Archive(Intended) \o Marker

\* The body reader of entry i ends before the size of the entry.
ShortBody(i) ==
    Entries[i].file /\ bk[i] = "short" /\ BodyLen(i) < Entries[i].size

(***************************************************************************)
(* After finish() returns Ok, the writer returned UnexpectedEof one time   *)
(* for each accepted entry with a short body. After that, start_send       *)
(* accepts no entry.                                                       *)
(***************************************************************************)
ShortErrOnce ==
    fin >= 1 =>
        eofErrs = Cardinality({s \in 1..Len(sent) : ShortBody(sent[s])})

\* The underlying writer is closed one time. A second finish() returns Ok
\* and does not call poll_close on the underlying writer.
CloseOnce == wclosed <= 1

\* state agrees with the marker and the close of the underlying writer.
StateOK ==
    /\ (state = "open") <=> (markers = 0)
    /\ (state = "closed") <=> (wclosed >= 1)

\* Sanity check of the spec, not of the code: CallWrite and CallRawSend
\* model the reject branches of poll_ready and start_send as UNCHANGED, so
\* this is true by construction. It guards the model against later edits.
SpecSanityReject ==
    [][rejects' > rejects => UNCHANGED <<wvars, out, sent, layout>>]_vars

\* Liveness: finish() returns Ok.
FinishCompletes == <>(fin >= 1)
Terminates == <>Terminal
=============================================================================
