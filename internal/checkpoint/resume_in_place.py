"""Release only this launcher's lock for a matching full checkpoint round."""
import glob
import os
import sys

from fluidcr import ctrl, distributed, group_restore

checkpoint_id = sys.argv[1]
checkpoint = os.environ.get("FLUIDCR_CHECKPOINT_PATH", "")
if not checkpoint_id or not checkpoint or not os.path.isabs(checkpoint):
    raise RuntimeError("explicit checkpoint path and checkpoint ID required")
lock = os.path.join(os.path.dirname(checkpoint), "lock")

# Serialize with runtime checkpoint/group-control mutations on the shared PVC.
with group_restore.legacy_control():
    status = ctrl._read_status(checkpoint)
    if status.get("checkpointID") != checkpoint_id:
        raise RuntimeError("local checkpoint ID mismatch")
    manifest = distributed.read_manifest()
    if manifest:
        if (manifest.get("checkpointID") != checkpoint_id
                or manifest.get("targets") != "all"
                or manifest.get("restoreOwnedResume")
                or manifest.get("noPeriodicResume")):
            raise RuntimeError("not the requested in-place full checkpoint")
    elif os.path.exists(lock):
        raise RuntimeError("cannot release lock without its manifest")
    if os.path.exists(lock):
        if status.get("state") != "CheckpointReady":
            raise RuntimeError("launcher has not reached CheckpointReady")
        os.unlink(lock)
    # Preserve the manifest until every rank is released. Never touch survivor
    # locks, saved rounds, checkpoints, generations, or archived workspace data.
    base = distributed._base_dir()
    if manifest and not any(glob.glob(os.path.join(base, "*", name))
                            for name in ("lock", "pause-lock")):
        os.unlink(distributed.manifest_path())
print("IN_PLACE_RESUME_RELEASED")
