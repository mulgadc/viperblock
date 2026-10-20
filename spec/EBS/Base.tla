----------------------------------------------------- MODULE Base -----------------------------------------------------
EXTENDS Naturals, Sequences, FiniteSets, TLC

-----------------------------------------------------------------------------------------------------------------------
\* Constants

CONSTANTS
  VolumeIds,
  InstanceIds,
  MaxHistory

ASSUME VolumeIds /= {}
ASSUME
  /\ MaxHistory \in Nat
  /\ MaxHistory >= 2

-----------------------------------------------------------------------------------------------------------------------
\* Variables

VARIABLES
  \* Every DB that a replica may still be serving, oldest first.
  history,
  \* Every volume ID that has been allocated, so that IDs are never reused.
  usedIds

vars == <<history, usedIds>>

-----------------------------------------------------------------------------------------------------------------------
\* Miscellaneous

Partial(F) == {[x \in D |-> f[x]] : f \in F, D \in SUBSET DOMAIN f}

Merged(A, B) == {a @@ b : a \in A, b \in B}

When(cond, x) == IF cond THEN {x} ELSE {}

A ?? B == IF A /= {} THEN A ELSE B

-----------------------------------------------------------------------------------------------------------------------
\* Types

AttachmentStatuses == {"attaching", "attached", "detaching", "detached", "busy"}

Attachments == [status : AttachmentStatuses]

VolumeStatuses == {"creating", "available", "in-use", "deleting", "deleted", "error"}

Volumes == [status             : VolumeStatuses,
            encrypted          : BOOLEAN,
            multiAttachEnabled : {FALSE}]

DBs == [volumes            : Partial([VolumeIds -> Volumes]),
        attachments        : Partial([VolumeIds \X InstanceIds -> Attachments]),
        encryptedByDefault : BOOLEAN]

Requests(action, params) == Merged([action : {action}], params)

Errors(codes) == [code : codes]

-----------------------------------------------------------------------------------------------------------------------
\* Database helpers

\* The most recently committed DB state.
CurrentDB == history[Len(history)]

\* Every DB state that a request may currently execute against.
VisibleDBs == {history[i] : i \in DOMAIN history}

Commit(db) ==
  IF db = CurrentDB
  THEN UNCHANGED vars
       \* History is bounded by MaxHistory to keep the state space finite.
  ELSE /\ Len(history) < MaxHistory
       /\ history' = Append(history, db)
       /\ usedIds' = usedIds \union DOMAIN db.volumes

InsertKey(table, key, value) == table @@ (key :> value)

RemoveKey(table, key) == [k \in DOMAIN table \ {key} |-> table[k]]

KeyExists(table, key) == key \in DOMAIN table

-----------------------------------------------------------------------------------------------------------------------
\* Invariants

TypeOK ==
  /\ history \in Seq(DBs) \ {<<>>}
  /\ usedIds \subseteq VolumeIds

-----------------------------------------------------------------------------------------------------------------------
\* Initial state and replication

Init ==
  /\ history = <<[volumes            |-> <<>>,
                  attachments        |-> <<>>,
                  encryptedByDefault |-> FALSE]>>
  /\ usedIds = {}

\* The lagging replicas caught up, so the oldest DB is no longer visible.
SyncReplicas ==
  /\ Len(history) > 1
  /\ history' = Tail(history)
  /\ UNCHANGED usedIds

-----------------------------------------------------------------------------------------------------------------------
\* Model checking

Symmetry == Permutations(VolumeIds) \union Permutations(InstanceIds)

=======================================================================================================================
