-------------------------------------------------- MODULE Encryption --------------------------------------------------
EXTENDS Base

-----------------------------------------------------------------------------------------------------------------------
\* Types

EnableEbsEncryptionByDefaultRequests  == Requests("EnableEbsEncryptionByDefault", {<<>>})
EnableEbsEncryptionByDefaultResponses == [ebsEncryptionByDefault : {TRUE}]
                                         \union Errors({})

DisableEbsEncryptionByDefaultRequests  == Requests("DisableEbsEncryptionByDefault", {<<>>})
DisableEbsEncryptionByDefaultResponses == [ebsEncryptionByDefault : {FALSE}]
                                          \union Errors({})

GetEbsEncryptionByDefaultRequests  == Requests("GetEbsEncryptionByDefault", {<<>>})
GetEbsEncryptionByDefaultResponses == [ebsEncryptionByDefault : BOOLEAN]
                                      \union Errors({})

-----------------------------------------------------------------------------------------------------------------------
\* Database helpers

SetEncryptedByDefault(val) ==
  Commit([CurrentDB EXCEPT !.encryptedByDefault = val])

-----------------------------------------------------------------------------------------------------------------------
\* API endpoints

\* A client requested the encryption be enabled by default for all new volumes.
EnableEbsEncryptionByDefault(req, res) ==
  /\ req \in EnableEbsEncryptionByDefaultRequests
  /\ LET PossibleErrors(db) == Errors({})
        \* The request was accepted.
     IN \/ /\ PossibleErrors(CurrentDB) = {}
           /\ SetEncryptedByDefault(TRUE)
           /\ res = [ebsEncryptionByDefault |-> TRUE]
        \* The request was rejected, possibly by a stale replica.
        \/ /\ \E db \in VisibleDBs : res \in PossibleErrors(db)
           /\ UNCHANGED vars

\* A client requested the encryption be disabled by default for all new volumes.
DisableEbsEncryptionByDefault(req, res) ==
  /\ req \in DisableEbsEncryptionByDefaultRequests
  /\ LET PossibleErrors(db) == Errors({})
        \* The request was accepted.
     IN \/ /\ PossibleErrors(CurrentDB) = {}
           /\ SetEncryptedByDefault(FALSE)
           /\ res = [ebsEncryptionByDefault |-> FALSE]
        \* The request was rejected, possibly by a stale replica.
        \/ /\ \E db \in VisibleDBs : res \in PossibleErrors(db)
           /\ UNCHANGED vars

\* A client requested the current default volume encryption setting.
GetEbsEncryptionByDefault(req, res) ==
  /\ req \in GetEbsEncryptionByDefaultRequests
  /\ LET PossibleErrors(db) == Errors({})
        \* The results may be served from any replica, stale or not.
     IN /\ \E db \in VisibleDBs :
             \* The request was accepted.
             \/ /\ PossibleErrors(db) = {}
                /\ res = [ebsEncryptionByDefault |-> db.encryptedByDefault]
             \* The request was rejected.
             \/ /\ res \in PossibleErrors(db)
        /\ UNCHANGED vars

-----------------------------------------------------------------------------------------------------------------------
\* Specification

EncryptionNext ==
  \/ \E req \in EnableEbsEncryptionByDefaultRequests, res \in EnableEbsEncryptionByDefaultResponses :
       EnableEbsEncryptionByDefault(req, res)
  \/ \E req \in DisableEbsEncryptionByDefaultRequests, res \in DisableEbsEncryptionByDefaultResponses :
       DisableEbsEncryptionByDefault(req, res)
  \/ \E req \in GetEbsEncryptionByDefaultRequests, res \in GetEbsEncryptionByDefaultResponses :
       GetEbsEncryptionByDefault(req, res)

EncryptionSpec == Init /\ [][EncryptionNext \/ SyncReplicas]_vars

=======================================================================================================================
