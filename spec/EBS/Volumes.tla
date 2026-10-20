--------------------------------------------------- MODULE Volumes ----------------------------------------------------
EXTENDS Base

-----------------------------------------------------------------------------------------------------------------------
\* Types

CreateVolumeRequests  == Requests("CreateVolume", Partial([encrypted : BOOLEAN]))
CreateVolumeResponses == Merged(Volumes, [vId : VolumeIds])
                         \union Errors({})

AttachVolumeRequests  == Requests("AttachVolume", [vId : VolumeIds, iId : InstanceIds])
AttachVolumeResponses == Merged(Attachments, [vId : VolumeIds, iId : InstanceIds])
                         \union Errors({"InvalidVolume.NotFound", "IncorrectState", "VolumeInUse"})

DetachVolumeRequests  == Requests("DetachVolume", [vId : VolumeIds, iId : InstanceIds])
DetachVolumeResponses == Merged(Attachments, [vId : VolumeIds, iId : InstanceIds])
                         \union Errors({"InvalidVolume.NotFound", "InvalidAttachment.NotFound",
                                        "IncorrectState", "VolumeInUse"})

DeleteVolumeRequests  == Requests("DeleteVolume", [vId : VolumeIds])
DeleteVolumeResponses == {<<>>}
                         \union Errors({"InvalidVolume.NotFound", "IncorrectState", "VolumeInUse"})

-----------------------------------------------------------------------------------------------------------------------
\* Database helpers

VolumeExists(db, vId) == KeyExists(db.volumes, vId)

AttachmentExists(db, vId, iId) == KeyExists(db.attachments, <<vId, iId>>)

HasAttachments(db, vId) == \E iId \in InstanceIds : AttachmentExists(db, vId, iId)

SetVolumeStatus(vId, status) ==
  Commit([CurrentDB EXCEPT !.volumes[vId].status = status])

SetAttachmentStatus(vId, iId, status) ==
  Commit([CurrentDB EXCEPT !.attachments[vId, iId].status = status])

-----------------------------------------------------------------------------------------------------------------------
\* API endpoints

\* A client requested a new volume.
CreateVolume(req, res) ==
  /\ req \in CreateVolumeRequests
  /\ LET PossibleErrors(db) == Errors({})
         PossibleRecords    == [status             : {"creating"},
                                \* After enabling encryption by default, CreateVolume may still
                                \* see a stale value from another replica for a short window.
                                encrypted          : {db.encryptedByDefault : db \in VisibleDBs},
                                multiAttachEnabled : {FALSE}]
        \* The request was accepted.
     IN \/ /\ PossibleErrors(CurrentDB) = {}
           /\ \E vId \in VolumeIds \ usedIds, record \in PossibleRecords :
                /\ Commit([CurrentDB EXCEPT !.volumes = InsertKey(@, vId, record)])
                /\ res = [vId |-> vId] @@ record
        \* The request was rejected, possibly by a stale replica.
        \/ /\ \E db \in VisibleDBs : res \in PossibleErrors(db)
           /\ UNCHANGED vars

\* A client requested that a volume be attached to an instance.
AttachVolume(req, res) ==
  /\ req \in AttachVolumeRequests
  /\ LET PossibleErrors(db) ==
           Errors(UNION {When(~VolumeExists(db, req.vId),                     "InvalidVolume.NotFound")
                           ?? When(db.volumes[req.vId].status /= "available", "IncorrectState"),
                         When(HasAttachments(db, req.vId),                    "VolumeInUse")})
         record == [status |-> "attaching"]
        \* The request was accepted.
     IN \/ /\ PossibleErrors(CurrentDB) = {}
           /\ Commit([CurrentDB EXCEPT !.attachments = InsertKey(@, <<req.vId, req.iId>>, record),
                                       !.volumes[req.vId].status = "in-use"])
           /\ res = [vId |-> req.vId, iId |-> req.iId] @@ record
        \* The request was rejected, possibly by a stale replica.
        \/ /\ \E db \in VisibleDBs : res \in PossibleErrors(db)
           /\ UNCHANGED vars

\* A client requested that a volume be detached from an instance.
DetachVolume(req, res) ==
  /\ req \in DetachVolumeRequests
  /\ LET PossibleErrors(db) ==
           LET volume     == db.volumes[req.vId]
               attachment == db.attachments[req.vId, req.iId]
           IN Errors(UNION {When(~VolumeExists(db, req.vId),                              "InvalidVolume.NotFound")
                              ?? When(volume.status \notin {"in-use", "error"},           "IncorrectState"),
                            When(~AttachmentExists(db, req.vId, req.iId),                 "InvalidAttachment.NotFound")
                              ?? When(attachment.status \notin {"attaching", "attached"}, "IncorrectState")})
        \* The request was accepted.
     IN \/ /\ PossibleErrors(CurrentDB) = {}
           /\ SetAttachmentStatus(req.vId, req.iId, "detaching")
           /\ res = [vId    |-> req.vId,
                     iId    |-> req.iId,
                     status |-> "detaching"]
        \* The request was rejected, possibly by a stale replica.
        \/ /\ \E db \in VisibleDBs : res \in PossibleErrors(db)
           /\ UNCHANGED vars

\* A client requested that a volume be deleted.
DeleteVolume(req, res) ==
  /\ req \in DeleteVolumeRequests
  /\ LET PossibleErrors(db) ==
           Errors(UNION {When(~VolumeExists(db, req.vId),                                    "InvalidVolume.NotFound")
                           ?? When(db.volumes[req.vId].status \notin {"available", "error"}, "IncorrectState"),
                         When(HasAttachments(db, req.vId),                                   "VolumeInUse")})
        \* The request was accepted.
     IN \/ /\ PossibleErrors(CurrentDB) = {}
           /\ SetVolumeStatus(req.vId, "deleting")
           /\ res = <<>>
        \* The request was rejected, possibly by a stale replica.
        \/ /\ \E db \in VisibleDBs : res \in PossibleErrors(db)
           /\ UNCHANGED vars

-----------------------------------------------------------------------------------------------------------------------
\* Background events

\* A volume or one of its attachments changed state in the background.
ProcessVolumeEvent ==
  \E vId \in VolumeIds :
    /\ VolumeExists(CurrentDB, vId)
    /\ LET volume == CurrentDB.volumes[vId]
          \* An in-progress create operation was completed.
       IN \/ /\ volume.status = "creating"
             \* If the operation failed, the status skips straight to deleting while any stale
             \* records/modifications are cleaned up.
             /\ \E result \in {"available", "deleting"} : SetVolumeStatus(vId, result)
          \* An in-progress delete operation was completed.
          \/ /\ volume.status = "deleting"
             /\ SetVolumeStatus(vId, "deleted")
          \* Garbage collection deleted a stale volume record.
          \/ /\ volume.status = "deleted"
             /\ Commit([CurrentDB EXCEPT !.volumes = RemoveKey(@, vId)])
          \* A volume experienced a hardware failure.
          \/ /\ volume.status \in {"creating", "available", "in-use"}
             /\ SetVolumeStatus(vId, "error")
          \/ \E iId \in InstanceIds :
               /\ AttachmentExists(CurrentDB, vId, iId)
               /\ LET attachment == CurrentDB.attachments[vId, iId]
                     \* An in-progress attach operation was completed.
                  IN \/ /\ attachment.status = "attaching"
                        \* If the operation failed, the status skips straight to detaching while any stale
                        \* records/modifications are cleaned up.
                        /\ \E result \in {"attached", "detaching"} : SetAttachmentStatus(vId, iId, result)
                     \* An in-progress detach operation was blocked (e.g. a guest instance refused to unmount).
                     \/ /\ attachment.status = "detaching"
                        /\ SetAttachmentStatus(vId, iId, "busy")
                     \* An in-progress detach operation became unblocked.
                     \/ /\ attachment.status = "busy"
                        /\ SetAttachmentStatus(vId, iId, "detaching")
                     \* An in-progress detach operation was completed.
                     \/ /\ attachment.status = "detaching"
                        /\ LET lastActive == \A id \in InstanceIds \ {iId} :
                                               \/ ~AttachmentExists(CurrentDB, vId, id)
                                               \/ CurrentDB.attachments[vId, id].status = "detached"
                           IN Commit([CurrentDB EXCEPT !.attachments[vId, iId].status = "detached",
                                                       \* If this was the only active attachment belonging to
                                                       \* the in-use volume, its status is reset to available.
                                                       !.volumes[vId].status =
                                                         IF @ = "in-use" /\ lastActive THEN "available" ELSE @])
                     \* Garbage collection deleted a stale attachment record.
                     \/ /\ attachment.status = "detached"
                        /\ Commit([CurrentDB EXCEPT !.attachments = RemoveKey(@, <<vId, iId>>)])

-----------------------------------------------------------------------------------------------------------------------
\* Specification

VolumesNext ==
  \/ \E req \in CreateVolumeRequests, res \in CreateVolumeResponses :
       CreateVolume(req, res)
  \/ \E req \in AttachVolumeRequests, res \in AttachVolumeResponses :
       AttachVolume(req, res)
  \/ \E req \in DetachVolumeRequests, res \in DetachVolumeResponses :
       DetachVolume(req, res)
  \/ \E req \in DeleteVolumeRequests, res \in DeleteVolumeResponses :
       DeleteVolume(req, res)
  \/ ProcessVolumeEvent

VolumesSpec == Init /\ [][VolumesNext \/ SyncReplicas]_vars

=======================================================================================================================
