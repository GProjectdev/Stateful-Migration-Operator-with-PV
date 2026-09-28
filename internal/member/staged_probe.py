"""Read-only proof that a restored launcher is waiting for this partial round."""
import json
import sys
from pathlib import Path


def require(condition, message):
    if not condition:
        raise RuntimeError(message)


def probe(expected, proc=Path("/proc/1")):
    env = dict(entry.decode().split("=", 1)
               for entry in (proc / "environ").read_bytes().split(b"\0")
               if b"=" in entry)
    require(expected["checkpointID"] and expected["sourcePodUID"], "missing binding")
    require(env.get("FLUIDCR_POD_UID") == expected["sourcePodUID"], "source UID mismatch")
    require(env.get("FLUIDCR_POD_NAME") == expected["podName"], "source name mismatch")
    require(env.get("FLUIDCR_SOURCE_WORLD_UID") == expected["workloadUID"], "world UID mismatch")
    require(int(env.get("RANK", "-1")) == expected["rank"], "rank mismatch")
    if expected["sourceNode"]:
        require(env.get("FLUIDCR_NODE_NAME") == expected["sourceNode"], "source node mismatch")
    command = (proc / "cmdline").read_bytes().split(b"\0")
    require(b"/opt/fluidcr/bin/fluidcr-launcher" in command, "PID 1 is not the launcher")
    checkpoint = Path(env.get("FLUIDCR_CHECKPOINT_PATH", ""))
    base = Path(env.get("FLUIDCR_CHECKPOINT_DIR", ""))
    require(checkpoint.is_absolute() and base.is_absolute(), "explicit checkpoint paths required")
    require(checkpoint.is_file(), "checkpoint missing")
    directory = checkpoint.parent
    require((directory / "lock").read_text().strip() == "1", "launcher lock missing or invalid")
    require(not (directory / "pause-lock").exists(), "target has survivor lock")
    status = json.loads((directory / ".fluidcr-runtime.json").read_text())
    require(status.get("state") == "CheckpointReady", "launcher not checkpoint ready")
    require(status.get("checkpointID") == expected["checkpointID"], "checkpoint ID mismatch")
    manifest = json.loads((base / "migration-manifest.json").read_text())
    require(manifest.get("checkpointID") == expected["checkpointID"], "manifest checkpoint mismatch")
    require(manifest.get("generation") == expected["generation"], "manifest generation mismatch")
    require(manifest.get("restoreOwnedResume") is True and manifest.get("noPeriodicResume") is True,
            "not a restore-owned round")
    require(isinstance(manifest.get("targets"), list)
            and sorted(manifest["targets"]) == sorted(expected["targets"]), "target ranks mismatch")
    require(expected["rank"] in expected["targets"], "rank is not a target")


if __name__ == "__main__":
    probe(json.loads(sys.argv[1]))
    print("PARTIAL_RESTORE_STAGED")
