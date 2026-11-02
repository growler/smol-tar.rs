---------------------------- MODULE CoreHandoff ----------------------------
(***************************************************************************)
(* Hand-off of the reader core in smol-tar/src/lib.rs.                     *)
(*                                                                         *)
(* One reader core (ReaderCore, lib.rs:1187) exists per archive. It moves  *)
(* between three kinds of holder:                                          *)
(*   - the archive: TarReader.core (lib.rs:1389-1394),                     *)
(*   - a file: TarRegularFileReader.core (lib.rs:1209-1214),               *)
(*   - the shared slot: ReturnedReader.core (lib.rs:1191-1194).            *)
(* The slot also holds the waker of the archive task                       *)
(* (ReturnedReader.waiter).                                                *)
(*                                                                         *)
(* Processes:                                                              *)
(*   - The archive consumer. It calls next(), polls it, can cancel the     *)
(*     next() future, and can drop the TarReader.                          *)
(*   - Files 1..NFiles. Each file handle has its own task. The task reads  *)
(*     the body, finishes it, or drops the handle.                         *)
(*                                                                         *)
(* Each critical section under the slot mutex is one atomic step. Work     *)
(* that touches only data that one thread owns through &mut is merged into *)
(* the adjacent step. The wake after unlock (lib.rs:1234-1236) and the     *)
(* drop of the old waker (lib.rs:3450-3451) are separate steps.            *)
(*                                                                         *)
(* Waker model. A waker is an abstract identity in Wakers. Each poll of    *)
(* next() uses a waker of its choice. This covers a move of the TarReader  *)
(* to another task and a new context after a cancel. will_wake is waker    *)
(* identity. A wake has an effect only on a pending poll with the same     *)
(* waker. A wake of any other waker is lost. This is the strictest model:  *)
(* the protocol must not need wakes that go to other futures.              *)
(* A task that returned Pending runs again only after a wake of its waker. *)
(* A spurious poll is a separate action. The constant Spurious enables it. *)
(*                                                                         *)
(* Abstractions:                                                           *)
(*   - The body of file i has fLen[i] steps (0..MaxLen). One read step     *)
(*     covers poll_read (lib.rs:348-378), and poll_fill_buf plus consume   *)
(*     (lib.rs:396-442). The three paths call return_core at the end of    *)
(*     the body in the same call.                                          *)
(*   - Source I/O is not modeled. A source Pending inside poll_read_header *)
(*     has no own step. The model has only a stop between the take step    *)
(*     and the header step, after which the user can cancel.               *)
(*   - Header parsing gives one of: File, other entry, error, end.         *)
(*     Reader states Eof, Eoff, Done and Failed are folded into "Done".    *)
(*     Extension is folded into Header.                                    *)
(*   - Errors in the Eof state (lib.rs:3383-3413) are not modeled. The     *)
(*     end step goes directly to "Done".                                   *)
(*   - An error for the archive data sets the state Failed                 *)
(*     (lib.rs:2945-2957). The archive keeps the core, and the next poll   *)
(*     returns Ready(None) (lib.rs:3425). The error step of the model      *)
(*     keeps the state, and a later end step gives "Done". The model also  *)
(*     lets next() yield more entries after this error.                    *)
(*   - A new next() call after Ready(None) is not modeled. Phase "done"    *)
(*     allows only the drop of the TarReader.                              *)
(*   - Body read errors leave the file with the core in state Entry. They  *)
(*     change no modeled variable, so they are not actions.                *)
(*   - A read on a file without core returns Ok(0) (lib.rs:354-356). It    *)
(*     changes no modeled variable, so it is not an action.                *)
(*   - Mutex poisoning is not modeled. A panic in the waker clone          *)
(*     (lib.rs:3448) or in the assert (lib.rs:1230) occurs while the slot  *)
(*     lock is held and poisons the slot mutex. return_core and poll_next  *)
(*     ignore the poison flag (lib.rs:1226-1229, 3439-3442).               *)
(*   - Panics in user waker code (wake, drop of a waker) are not modeled.  *)
(***************************************************************************)
EXTENDS Naturals, FiniteSets

CONSTANTS
    NFiles,     \* number of File entries the archive can yield
    MaxLen,     \* maximum number of read steps in a file body
    Wakers,     \* waker identities, a set of positive integers
    Spurious    \* TRUE: the executor can poll a pending next() without a wake

ASSUME NFiles \in Nat \ {0}
ASSUME MaxLen \in Nat
ASSUME Wakers \subseteq (Nat \ {0}) /\ Wakers # {}
ASSUME Spurious \in BOOLEAN

NoWaker == 0
Files == 1..NFiles
WakerOrNone == Wakers \cup {NoWaker}

(* Reader states (lib.rs:994-1004) that matter for the hand-off.           *)
RStates == {"Header", "Padding", "SkipEntry", "Entry", "Done"}

(* Phases of the archive consumer.                                         *)
(*   idle -- not inside next(). The TarReader exists.                      *)
(*   run  -- a next() future is runnable. The task will poll it.           *)
(*   reg  -- inside poll_next after the slot lock released, with the old   *)
(*           waker not yet dropped (lib.rs:3450-3452).                     *)
(*   wait -- poll_next returned Pending. Runs again only after a wake.     *)
(*   done -- next() returned None.                                         *)
(*   gone -- the TarReader is dropped.                                     *)
Phases == {"idle", "run", "reg", "wait", "done", "gone"}

VARIABLES
    aPhase,     \* phase of the archive consumer
    aCore,      \* TarReader.core is Some
    aWaker,     \* waker of the pending poll (in reg and wait)
    aPrev,      \* old waker that poll_next drops after unlock
    aNotified,  \* a wake came while the poll was in reg
    slotCore,   \* ReturnedReader.core is Some
    slotWaiter, \* ReturnedReader.waiter
    rstate,     \* state of the one reader core
    coreGone,   \* the core was dropped with the TarReader
    nextFile,   \* index of the next File entry
    fHandle,    \* file handle: "none" (not yielded), "live", "dropped"
    fCore,      \* TarRegularFileReader.core is Some
    fPos,       \* body read steps done
    fLen,       \* body length in read steps
    fAligned,   \* the body ends on a block boundary
    fBusy,      \* the file thread is between unlock and wake in return_core
    fWake,      \* the waker that return_core took from the slot
    fHow,       \* history: how the file returned the core
    fRetState,  \* history: reader state when the file returned the core
    assertFail, \* assert!(slot.core.is_none()) failed (lib.rs:1230)
    panicEntry, \* unreachable!() in state Entry was hit (lib.rs:3349)
    yieldBad    \* a File was yielded while another holder had the core

aVars == <<aPhase, aCore, aWaker, aPrev, aNotified>>
fVars == <<fHandle, fCore, fPos, fLen, fAligned, fBusy, fWake, fHow,
          fRetState>>
vars == <<aVars, slotCore, slotWaiter, rstate, coreGone, nextFile, fVars,
          assertFail, panicEntry, yieldBad>>

Init ==
    \* TarReader::new (lib.rs:1398-1406): the archive has the core.
    /\ aPhase = "idle"
    /\ aCore = TRUE
    /\ aWaker = NoWaker
    /\ aPrev = NoWaker
    /\ aNotified = FALSE
    /\ slotCore = FALSE
    /\ slotWaiter = NoWaker
    /\ rstate = "Header"
    /\ coreGone = FALSE
    /\ nextFile = 1
    /\ fHandle = [i \in Files |-> "none"]
    /\ fCore = [i \in Files |-> FALSE]
    /\ fPos = [i \in Files |-> 0]
    /\ fLen = [i \in Files |-> 0]
    /\ fAligned = [i \in Files |-> TRUE]
    /\ fBusy = [i \in Files |-> FALSE]
    /\ fWake = [i \in Files |-> NoWaker]
    /\ fHow = [i \in Files |-> "none"]
    /\ fRetState = [i \in Files |-> "none"]
    /\ assertFail = FALSE
    /\ panicEntry = FALSE
    /\ yieldBad = FALSE

-----------------------------------------------------------------------------
(* Archive consumer                                                        *)

(* The user calls next() and the task polls the new future.                *)
ACall ==
    /\ aPhase = "idle"
    /\ aPhase' = "run"
    /\ UNCHANGED <<aCore, aWaker, aPrev, aNotified, slotCore, slotWaiter,
                   rstate, coreGone, nextFile, fVars,
                   assertFail, panicEntry, yieldBad>>

(* poll_next with no local core, slot has a core (lib.rs:3439-3444).       *)
(* One critical section: take slot.core into TarReader.core.               *)
(* poll_read_header runs in the same poll after unlock. That is the        *)
(* separate step AHeader. Between the two steps the archive owns the core. *)
ATake ==
    /\ aPhase = "run"
    /\ ~aCore
    /\ slotCore
    /\ aCore' = TRUE
    /\ slotCore' = FALSE
    /\ UNCHANGED <<aPhase, aWaker, aPrev, aNotified, slotWaiter, rstate,
                   coreGone, nextFile, fVars, assertFail, panicEntry,
                   yieldBad>>

(* poll_next with no local core, slot empty (lib.rs:3445-3449).            *)
(* One critical section: keep the waiter if will_wake is true, else        *)
(* replace it and keep the old one to drop after unlock.                   *)
ARegister(w) ==
    /\ aPhase = "run"
    /\ ~aCore
    /\ ~slotCore
    /\ aPrev' = IF slotWaiter = w THEN NoWaker ELSE slotWaiter
    /\ slotWaiter' = w
    /\ aWaker' = w
    /\ aPhase' = "reg"
    /\ UNCHANGED <<aCore, aNotified, slotCore, rstate, coreGone, nextFile,
                   fVars, assertFail, panicEntry, yieldBad>>

(* After unlock: drop the old waker and return Pending (lib.rs:3450-3452). *)
(* A wake that came during the poll makes the executor poll again.         *)
AUnlock ==
    /\ aPhase = "reg"
    /\ aPrev' = NoWaker
    /\ aPhase' = IF aNotified THEN "run" ELSE "wait"
    /\ aWaker' = IF aNotified THEN NoWaker ELSE aWaker
    /\ aNotified' = FALSE
    /\ UNCHANGED <<aCore, slotCore, slotWaiter, rstate, coreGone, nextFile,
                   fVars, assertFail, panicEntry, yieldBad>>

(* poll_read_header with the local core (lib.rs:3456-3496).                *)
AHeaderUnchanged ==
    UNCHANGED <<aWaker, aPrev, aNotified, slotCore, slotWaiter, coreGone,
                fVars, assertFail, panicEntry>>

(* State Entry: unreachable!() (lib.rs:3348-3350).                         *)
AHeaderPanic ==
    /\ rstate = "Entry"
    /\ panicEntry' = TRUE
    /\ UNCHANGED <<aVars, slotCore, slotWaiter, rstate, coreGone, nextFile,
                   fVars, assertFail, yieldBad>>

(* States Eof, Eoff, Done, Failed: the stream ends (lib.rs:3383-3425).     *)
AHeaderDone ==
    /\ rstate = "Done"
    /\ aPhase' = "done"
    /\ UNCHANGED <<aCore, rstate, nextFile, yieldBad>>
    /\ AHeaderUnchanged

(* A regular file header (lib.rs:3004-3047). State becomes Entry           *)
(* (lib.rs:3035). poll_next moves the local core into the new file         *)
(* (lib.rs:3464-3488). The user gets the handle.                           *)
AYieldFile ==
    /\ rstate \in {"Header", "Padding", "SkipEntry"}
    /\ nextFile <= NFiles
    /\ LET k == nextFile IN
       \E len \in 0..MaxLen, al \in BOOLEAN :
          /\ len = 0 => al
          /\ yieldBad' = (yieldBad \/ slotCore \/ \E j \in Files : fCore[j])
          /\ aCore' = FALSE
          /\ rstate' = "Entry"
          /\ nextFile' = nextFile + 1
          /\ fHandle' = [fHandle EXCEPT ![k] = "live"]
          /\ fCore' = [fCore EXCEPT ![k] = TRUE]
          /\ fLen' = [fLen EXCEPT ![k] = len]
          /\ fAligned' = [fAligned EXCEPT ![k] = al]
          /\ aPhase' = "idle"
          /\ UNCHANGED <<aWaker, aPrev, aNotified, slotCore, slotWaiter,
                         coreGone, fPos, fBusy, fWake, fHow, fRetState,
                         assertFail, panicEntry>>

(* Any other entry. The archive keeps the core. A hard link with data      *)
(* leaves SkipEntry (lib.rs:3139). Other kinds leave Header.               *)
AYieldOther ==
    /\ rstate \in {"Header", "Padding", "SkipEntry"}
    /\ rstate' \in {"Header", "SkipEntry"}
    /\ aPhase' = "idle"
    /\ UNCHANGED <<aCore, nextFile, yieldBad>>
    /\ AHeaderUnchanged

(* An error item. The archive keeps the core and the state does not move   *)
(* to Entry: no fallible step follows lib.rs:3035. The user can call       *)
(* next() again.                                                           *)
AYieldErr ==
    /\ rstate \in {"Header", "Padding", "SkipEntry"}
    /\ aPhase' = "idle"
    /\ UNCHANGED <<aCore, rstate, nextFile, yieldBad>>
    /\ AHeaderUnchanged

(* Two zero blocks and source EOF: Ready(None) (lib.rs:3414-3423).         *)
AYieldEnd ==
    /\ rstate \in {"Header", "Padding", "SkipEntry"}
    /\ rstate' = "Done"
    /\ aPhase' = "done"
    /\ UNCHANGED <<aCore, nextFile, yieldBad>>
    /\ AHeaderUnchanged

AHeader ==
    /\ aPhase = "run"
    /\ aCore
    /\ \/ AHeaderPanic
       \/ AHeaderDone
       \/ AYieldFile
       \/ AYieldOther
       \/ AYieldErr
       \/ AYieldEnd

(* One poll step of the runnable next() future.                            *)
APoll == AHeader \/ ATake \/ \E w \in Wakers : ARegister(w)

(* The executor polls a pending future without a wake.                     *)
ASpurious ==
    /\ Spurious
    /\ aPhase = "wait"
    /\ aPhase' = "run"
    /\ aWaker' = NoWaker
    /\ UNCHANGED <<aCore, aPrev, aNotified, slotCore, slotWaiter, rstate,
                   coreGone, nextFile, fVars, assertFail, panicEntry,
                   yieldBad>>

(* The user drops the next() future between polls. The waker stays in      *)
(* the slot. No test checks the slot waiter after a cancel.                *)
(* tests/tar_tests.rs:5229 checks that the file can continue.              *)
ACancel ==
    /\ aPhase \in {"run", "wait"}
    /\ aPhase' = "idle"
    /\ aWaker' = NoWaker
    /\ UNCHANGED <<aCore, aPrev, aNotified, slotCore, slotWaiter, rstate,
                   coreGone, nextFile, fVars, assertFail, panicEntry,
                   yieldBad>>

(* The user drops the TarReader. A next() future borrows it, so no future  *)
(* exists. A local core is dropped. The slot stays alive in the Arc of     *)
(* every file (tests/tar_tests.rs:5449).                                   *)
ADrop ==
    /\ aPhase \in {"idle", "done"}
    /\ aPhase' = "gone"
    /\ coreGone' = (coreGone \/ aCore)
    /\ aCore' = FALSE
    /\ UNCHANGED <<aWaker, aPrev, aNotified, slotCore, slotWaiter, rstate,
                   nextFile, fVars, assertFail, panicEntry, yieldBad>>

-----------------------------------------------------------------------------
(* Files                                                                   *)

(* return_core critical section (lib.rs:1218-1233). The take of the file   *)
(* core and the Entry to SkipEntry change touch only data of the file, so  *)
(* they are part of this step. st is the reader state after that change.   *)
ReturnCore(i, st) ==
    /\ fCore' = [fCore EXCEPT ![i] = FALSE]
    /\ rstate' = st
    /\ assertFail' = (assertFail \/ slotCore)
    /\ slotCore' = TRUE
    /\ fWake' = [fWake EXCEPT ![i] = slotWaiter]
    /\ slotWaiter' = NoWaker
    /\ fBusy' = [fBusy EXCEPT ![i] = TRUE]
    /\ fRetState' = [fRetState EXCEPT ![i] = st]

(* One body read step. At the end of the body the same call runs           *)
(* end_regular_file (lib.rs:384-393) and return_core (lib.rs:375-378,      *)
(* 405-409, 438-441). A body of length 0 ends at the first read or         *)
(* fill_buf (lib.rs:404-409).                                              *)
FRead(i) ==
    /\ fHandle[i] = "live"
    /\ fCore[i]
    /\ ~fBusy[i]
    /\ IF fPos[i] + 1 < fLen[i]
       THEN /\ fPos' = [fPos EXCEPT ![i] = @ + 1]
            /\ UNCHANGED <<aVars, slotCore, slotWaiter, rstate, coreGone,
                           nextFile, fHandle, fCore, fLen, fAligned, fBusy,
                           fWake, fHow, fRetState, assertFail, panicEntry,
                           yieldBad>>
       ELSE /\ fPos' = [fPos EXCEPT ![i] = fLen[i]]
            /\ fHow' = [fHow EXCEPT ![i] = "finish"]
            /\ ReturnCore(i, IF fAligned[i] THEN "Header" ELSE "Padding")
            /\ UNCHANGED <<aVars, coreGone, nextFile, fHandle, fLen,
                           fAligned, panicEntry, yieldBad>>

(* Drop of the handle (lib.rs:1240-1244). With the core, return_core       *)
(* changes Entry to SkipEntry (lib.rs:1222-1224).                          *)
FDrop(i) ==
    /\ fHandle[i] = "live"
    /\ ~fBusy[i]
    /\ fHandle' = [fHandle EXCEPT ![i] = "dropped"]
    /\ IF fCore[i]
       THEN /\ fHow' = [fHow EXCEPT ![i] = "drop"]
            /\ ReturnCore(i, IF rstate = "Entry" THEN "SkipEntry" ELSE rstate)
            /\ UNCHANGED <<aVars, coreGone, nextFile, fPos, fLen, fAligned,
                           panicEntry, yieldBad>>
       ELSE UNCHANGED <<aVars, slotCore, slotWaiter, rstate, coreGone,
                        nextFile, fCore, fPos, fLen, fAligned, fBusy, fWake,
                        fHow, fRetState, assertFail, panicEntry, yieldBad>>

(* After unlock: waker.wake() (lib.rs:1234-1236).                          *)
(* A pending poll with the same waker becomes runnable. A poll in reg      *)
(* gets a re-poll after it returns. Any other wake is lost.                *)
FWake(i) ==
    /\ fBusy[i]
    /\ LET w == fWake[i] IN
       IF w # NoWaker /\ aWaker = w /\ aPhase = "wait"
       THEN /\ aPhase' = "run"
            /\ aWaker' = NoWaker
            /\ UNCHANGED aNotified
       ELSE IF w # NoWaker /\ aWaker = w /\ aPhase = "reg"
       THEN /\ aNotified' = TRUE
            /\ UNCHANGED <<aPhase, aWaker>>
       ELSE UNCHANGED <<aPhase, aWaker, aNotified>>
    /\ fBusy' = [fBusy EXCEPT ![i] = FALSE]
    /\ fWake' = [fWake EXCEPT ![i] = NoWaker]
    /\ UNCHANGED <<aCore, aPrev, slotCore, slotWaiter, rstate, coreGone,
                   nextFile, fHandle, fCore, fPos, fLen, fAligned, fHow,
                   fRetState, assertFail, panicEntry, yieldBad>>

-----------------------------------------------------------------------------

(* All handles are gone and no thread is inside return_core.               *)
Terminated ==
    /\ aPhase = "gone"
    /\ \A i \in Files : fHandle[i] # "live" /\ ~fBusy[i]

Next ==
    \/ ACall
    \/ APoll
    \/ AUnlock
    \/ ASpurious
    \/ ACancel
    \/ ADrop
    \/ \E i \in Files : FRead(i) \/ FDrop(i) \/ FWake(i)
    \/ (Terminated /\ UNCHANGED vars)

(* Code that runs without a user decision is weakly fair: a scheduled      *)
(* poll, the return of Pending, and the wake after unlock. User actions    *)
(* (call, cancel, drop, read) have no fairness.                            *)
Fairness ==
    /\ WF_vars(APoll)
    /\ WF_vars(AUnlock)
    /\ \A i \in Files : WF_vars(FWake(i))

Spec == Init /\ [][Next]_vars /\ Fairness

-----------------------------------------------------------------------------
(* Safety properties                                                       *)

TypeOK ==
    /\ aPhase \in Phases
    /\ aCore \in BOOLEAN
    /\ aWaker \in WakerOrNone
    /\ aPrev \in WakerOrNone
    /\ aNotified \in BOOLEAN
    /\ slotCore \in BOOLEAN
    /\ slotWaiter \in WakerOrNone
    /\ rstate \in RStates
    /\ coreGone \in BOOLEAN
    /\ nextFile \in 1..(NFiles + 1)
    /\ fHandle \in [Files -> {"none", "live", "dropped"}]
    /\ fCore \in [Files -> BOOLEAN]
    /\ fPos \in [Files -> 0..MaxLen]
    /\ fLen \in [Files -> 0..MaxLen]
    /\ fAligned \in [Files -> BOOLEAN]
    /\ fBusy \in [Files -> BOOLEAN]
    /\ fWake \in [Files -> WakerOrNone]
    /\ fHow \in [Files -> {"none", "finish", "drop"}]
    /\ fRetState \in [Files -> RStates \cup {"none"}]
    /\ assertFail \in BOOLEAN
    /\ panicEntry \in BOOLEAN
    /\ yieldBad \in BOOLEAN

(* Exactly one holder of the core. Zero holders only after the TarReader   *)
(* dropped its local core.                                                 *)
Holders ==
    (IF aCore THEN 1 ELSE 0) + (IF slotCore THEN 1 ELSE 0)
        + Cardinality({i \in Files : fCore[i]})
OneHolder == Holders = IF coreGone THEN 0 ELSE 1

(* assert!(slot.core.is_none()) never fails (lib.rs:1230).                 *)
NoAssertFail == ~assertFail

(* unreachable!() in poll_read_header is never hit (lib.rs:3349).          *)
NoEntryPanic == ~panicEntry

(* Only a file holds the core in state Entry.                              *)
EntryOnlyInFile ==
    /\ (aCore \/ slotCore) => rstate # "Entry"
    /\ \A i \in Files : fCore[i] => rstate = "Entry"

(* A file that returned the core never has it again. A handle that is not  *)
(* live has no core.                                                       *)
NoReturnBack ==
    \A i \in Files :
        /\ fHow[i] # "none" => ~fCore[i]
        /\ fHandle[i] # "live" => ~fCore[i]

(* The archive never yields a File while another holder has the core.      *)
NoYieldWhileHeld == ~yieldBad

(* Drop with the core leaves SkipEntry. End of body leaves Header or       *)
(* Padding (lib.rs:384-393).                                               *)
ReturnState ==
    \A i \in Files :
        /\ fHow[i] = "drop" => fRetState[i] = "SkipEntry"
        /\ fHow[i] = "finish" =>
              fRetState[i] = IF fAligned[i] THEN "Header" ELSE "Padding"

(* return_core takes the waiter in the same section that stores the core.  *)
SlotWaiterClear == slotCore => slotWaiter = NoWaker

(* A pending poll always has a way to be woken: its waker is in the slot,  *)
(* or a file thread holds it and will wake it, or the wake already came.   *)
WakePath ==
    LET held == slotWaiter = aWaker
                \/ \E i \in Files : fBusy[i] /\ fWake[i] = aWaker
    IN  /\ aPhase = "wait" => held
        /\ aPhase = "reg" => (held \/ aNotified)

(* poll_next drops a replaced waker after unlock (lib.rs:3446-3451). The   *)
(* old waker exists only inside the poll, and the will_wake check never    *)
(* replaces the waker of the current poll with itself.                     *)
OldWakerDropped ==
    aPrev # NoWaker => (aPhase = "reg" /\ aPrev # aWaker)

-----------------------------------------------------------------------------
(* Liveness                                                                *)

(* No lost wakeup. When the core is in the slot while next() is pending,   *)
(* the archive gets the core, unless the user cancels next() or drops the  *)
(* TarReader.                                                              *)
NoLostWakeup ==
    (aPhase \in {"run", "reg", "wait"} /\ slotCore)
        ~> (aCore \/ aPhase \in {"idle", "gone"})

(* No hang. When next() is pending and no file handle is live, the archive *)
(* gets the core, unless the user cancels next() or drops the TarReader.   *)
(* This also covers a core that a file loses instead of returning it.      *)
NoHang ==
    (aPhase \in {"run", "reg", "wait"} /\ \A i \in Files : fHandle[i] # "live")
        ~> (aCore \/ aPhase \in {"idle", "gone"})

=============================================================================
