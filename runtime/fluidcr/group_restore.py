"""Immutable full-world checkpoint metadata and fenced restore control.

Metadata is trusted through shared-storage permissions, not a signature. The
caller owns source fencing and keeps targets stopped until prepare succeeds.
"""
import hashlib
import json
import os
import re
import shutil
import tempfile
import threading
import time
from contextlib import contextmanager
from fluidcr import distributed as _distributed

_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:-]{0,127}$")
_local_lock = threading.RLock()
_lock_depth = threading.local()


class GroupConflict(ValueError):
    """A valid request cannot safely change the current shared state."""


def _root():
    return os.path.realpath(_distributed._base_dir())


def _path(relative):
    if not isinstance(relative, str) or os.path.isabs(relative):
        raise GroupConflict("metadata path must be relative to checkpoint root")
    root = _root()
    path = os.path.realpath(os.path.join(root, relative))
    if os.path.commonpath([root, path]) != root or path == root:
        raise GroupConflict("metadata path escapes checkpoint root")
    return path


def _read(path):
    try:
        with open(path, encoding="utf-8") as stream:
            data = json.load(stream)
    except FileNotFoundError:
        return None
    except (ValueError, UnicodeError) as exc:
        raise GroupConflict("invalid control metadata: " + path) from exc
    if not isinstance(data, dict):
        raise GroupConflict("control metadata must be an object: " + path)
    return data


def _write(path, data):
    _distributed._atomic_write(path, json.dumps(data, sort_keys=True), ".group-")


def _active_path():
    return _path(".fluidcr-group-restore.json")


def _receipt_path(uid):
    return _path(".group-operations/" + uid + ".json")


def _recover_active():
    """Finish a completion publication interrupted after its durable receipt."""
    active = _read(_active_path())
    if active and active.get("state") == "resuming":
        receipt = _read(_receipt_path(active["request"]["operationUID"]))
        if receipt:
            _matching(receipt, active["request"])
            _write(_active_path(), receipt)
            return receipt
    return active


def _identifier(value, field):
    if not isinstance(value, str) or not _ID.fullmatch(value):
        raise ValueError(field + " must be a nonempty 1-128 character identifier")
    # Windows drive/stream syntax must never be used as a filename.
    if ":" in value:
        raise ValueError(field + " must not contain ':'")
    return value


@contextmanager
def control_lock(timeout=5):
    """Shared exclusive-create lock; never steal a possibly live owner's lock."""
    with _local_lock:
        if getattr(_lock_depth, "value", 0):
            _lock_depth.value += 1
            try:
                yield
            finally:
                _lock_depth.value -= 1
            return
        path = _path(".fluidcr-group-control.lock")
        os.makedirs(_root(), exist_ok=True)
        deadline = time.monotonic() + timeout
        while True:
            try:
                fd = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
                break
            except FileExistsError as exc:
                if time.monotonic() >= deadline:
                    raise GroupConflict("shared control lock busy; retry, do not steal a live lock") from exc
                time.sleep(0.01)
        try:
            os.close(fd)
            _lock_depth.value = 1
            yield
        finally:
            _lock_depth.value = 0
            os.unlink(path)


@contextmanager
def legacy_control(checkpoint=False):
    with control_lock():
        active = _recover_active()
        if active and active.get("state") not in ("completed", "retired"):
            raise GroupConflict("prepared group requires operationUID-scoped resume")
        if checkpoint and active and active.get("state") == "completed":
            active["state"] = "retired"
            _write(_active_path(), active)
        yield


def _digest(path):
    digest = hashlib.sha256()
    with open(path, "rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def capture_round(checkpoint_path):
    """Capture producer identity before serialization; absent opt-in is legacy."""
    read_manifest, read_generation = _distributed.read_manifest, _distributed.read_generation
    world_uid = os.environ.get("FLUIDCR_SOURCE_WORLD_UID", "").strip()
    if not world_uid:
        return None
    with control_lock(timeout=300):
        active = _read(_active_path())
        if active and active.get("state") not in ("completed", "retired"):
            raise GroupConflict("cannot checkpoint during a prepared group operation")
        manifest = read_manifest()
        if not manifest.get("checkpointID") or manifest.get("targets") != "all":
            return None
        checkpoint_id = _identifier(manifest["checkpointID"], "checkpointID")
        try:
            rank, world = int(os.environ["RANK"]), int(os.environ["WORLD_SIZE"])
        except (KeyError, ValueError) as exc:
            raise GroupConflict("producer requires RANK and WORLD_SIZE") from exc
        generation = read_generation()
        if not 0 <= rank < world or generation <= 0:
            raise GroupConflict("producer rank/world/generation invalid")
        relative = os.path.relpath(os.path.realpath(checkpoint_path), _root())
        live = _path(relative)
        # Lock and runtime status paths are per-rank directories.
        if os.path.dirname(live) == _root():
            raise GroupConflict("producer requires a per-rank checkpoint directory")
        artifact = os.path.join(os.path.dirname(relative), "rounds", checkpoint_id,
                                os.path.basename(relative))
        return {
            "schemaVersion": 1, "checkpointID": checkpoint_id,
            "sourceWorldUID": world_uid, "generation": generation,
            "worldSize": world, "rank": rank, "targets": "all",
            "checkpointPath": relative, "artifactPath": artifact,
        }


def publish_round(checkpoint_path, context):
    """Publish immutable bytes then JSON commit marker, never overwrite a round."""
    if context is None:
        return
    read_manifest, read_generation = _distributed.read_manifest, _distributed.read_generation
    with control_lock(timeout=300):
        manifest = read_manifest()
        if (manifest.get("checkpointID") != context["checkpointID"]
                or manifest.get("targets") != "all"
                or read_generation() != context["generation"]):
            raise GroupConflict("checkpoint round changed during serialization")
        if os.path.realpath(checkpoint_path) != _path(context["checkpointPath"]):
            raise GroupConflict("producer checkpoint path mismatch")
        metadata = dict(context, sha256=_digest(checkpoint_path),
                        sizeBytes=os.path.getsize(checkpoint_path))
        marker = _path("rounds/{}/ranks/{}.json".format(context["checkpointID"], context["rank"]))
        previous = _read(marker)
        if previous is not None and previous != metadata:
            raise GroupConflict("immutable rank metadata already exists with different contents")
        artifact = _path(context["artifactPath"])
        if os.path.exists(artifact):
            if _digest(artifact) != metadata["sha256"] or os.path.getsize(artifact) != metadata["sizeBytes"]:
                raise GroupConflict("immutable rank artifact already exists with different contents")
        else:
            os.makedirs(os.path.dirname(artifact), exist_ok=True)
            fd, tmp = tempfile.mkstemp(dir=os.path.dirname(artifact), prefix=".artifact-")
            try:
                with os.fdopen(fd, "wb") as out, open(checkpoint_path, "rb") as inp:
                    shutil.copyfileobj(inp, out)
                    out.flush()
                    os.fsync(out.fileno())
                os.replace(tmp, artifact)
            finally:
                if os.path.exists(tmp):
                    os.unlink(tmp)
        if previous is None:
            _write(marker, metadata)


def validate_request(payload):
    if not isinstance(payload, dict):
        raise ValueError("group request requires a JSON object")
    if set(payload) != {"all", "checkpointID", "operationUID", "sourceFenceProof"}:
        raise ValueError("group request requires exactly all, checkpointID, operationUID, sourceFenceProof")
    checkpoint_id = _identifier(payload.get("checkpointID"), "checkpointID")
    uid = _identifier(payload.get("operationUID"), "operationUID")
    if payload.get("all") is not True or payload.get("ppids"):
        raise ValueError("group request requires all=true without ppids")
    proof = payload.get("sourceFenceProof")
    if not isinstance(proof, dict) or proof.get("allRanksFenced") is not True:
        raise ValueError("sourceFenceProof requires allRanksFenced=true")
    if set(proof) != {"allRanksFenced", "checkpointID", "operationUID", "sourceWorldUID", "evidenceRef"}:
        raise ValueError("sourceFenceProof fields do not match the caller contract")
    for field, value in (("checkpointID", checkpoint_id), ("operationUID", uid)):
        if proof.get(field) != value:
            raise ValueError("sourceFenceProof " + field + " mismatch")
    for field in ("sourceWorldUID", "evidenceRef"):
        if not isinstance(proof.get(field), str) or not proof[field].strip():
            raise ValueError("sourceFenceProof requires " + field)
    return {"checkpointID": checkpoint_id, "operationUID": uid, "all": True,
            "sourceFenceProof": {key: proof[key] for key in
                ("checkpointID", "operationUID", "allRanksFenced", "sourceWorldUID", "evidenceRef")}}


def _metadata(request):
    cid = request["checkpointID"]
    directory = _path("rounds/" + cid + "/ranks")
    try:
        names = sorted(os.listdir(directory))
    except FileNotFoundError as exc:
        raise GroupConflict("trusted-round-metadata-unavailable: producer rank metadata required") from exc
    names = [name for name in names if name.endswith(".json")]
    records = [_read(_path("rounds/" + cid + "/ranks/" + name)) for name in names]
    if not records or any(record is None for record in records):
        raise GroupConflict("complete producer rank metadata required")
    first = records[0]
    world = first.get("worldSize")
    generation = first.get("generation")
    if type(world) is not int or world < 1 or type(generation) is not int or generation < 1:
        raise GroupConflict("invalid world size or generation")
    if len(records) != world:
        raise GroupConflict("incomplete full-group rank metadata")
    ranks, live_paths, directories = set(), set(), set()
    for name, record in zip(names, records):
        rank = record.get("rank")
        if (type(rank) is not int or rank < 0 or rank >= world or rank in ranks
                or name != str(rank) + ".json"):
            raise GroupConflict("duplicate or mismatched rank metadata")
        for field, expected in (("schemaVersion", 1), ("checkpointID", cid),
                                ("sourceWorldUID", request["sourceFenceProof"]["sourceWorldUID"]),
                                ("worldSize", world), ("generation", generation), ("targets", "all")):
            if type(record.get(field)) is not type(expected) or record.get(field) != expected:
                raise GroupConflict("rank metadata mismatch: " + field)
        live = _path(record.get("checkpointPath"))
        artifact = _path(record.get("artifactPath"))
        expected_artifact = os.path.join(os.path.dirname(live), "rounds", cid, os.path.basename(live))
        if artifact != expected_artifact or os.path.dirname(live) == _root():
            raise GroupConflict("rank checkpoint/artifact pointer mismatch")
        if live in live_paths or os.path.dirname(live) in directories:
            raise GroupConflict("rank checkpoint directories must be distinct")
        size = record.get("sizeBytes")
        digest = record.get("sha256")
        if type(size) is not int or size < 0 or not isinstance(digest, str) or not re.fullmatch(r"[a-f0-9]{64}", digest):
            raise GroupConflict("invalid artifact integrity metadata")
        if not os.path.isfile(artifact) or os.path.getsize(artifact) != size or _digest(artifact) != digest:
            raise GroupConflict("rank artifact integrity mismatch")
        ranks.add(rank)
        live_paths.add(live)
        directories.add(os.path.dirname(live))
    return sorted(records, key=lambda record: record["rank"])


def _result(record):
    return {"checkpointID": record["request"]["checkpointID"],
            "operationUID": record["request"]["operationUID"],
            "prepared": True, "state": record["state"],
            "generation": record["metadata"][0]["generation"],
            "checkpointPointers": {str(m["rank"]): _path(m["artifactPath"]) for m in record["metadata"]}}


def _matching(record, request):
    if record.get("request") != request:
        raise GroupConflict("operationUID conflicts with its original request")


def _manifest(record):
    first = record["metadata"][0]
    return {"targets": "all", "checkpointID": first["checkpointID"],
            "generation": first["generation"], "sourceWorldUID": first["sourceWorldUID"],
            "worldSize": first["worldSize"], "restoreOwnedResume": True,
            "noPeriodicResume": True, "operationUID": record["request"]["operationUID"]}


def prepare_group(payload):
    request = validate_request(payload)
    receipt = _read(_receipt_path(request["operationUID"]))
    if receipt:
        _matching(receipt, request)
        return _result(receipt)
    # Validate artifacts before even acquiring the shared write lock.
    metadata = _metadata(request)
    with control_lock():
        receipt = _read(_receipt_path(request["operationUID"]))
        if receipt:
            _matching(receipt, request)
            return _result(receipt)
        active = _recover_active()
        if active and active.get("request", {}).get("operationUID") == request["operationUID"]:
            _matching(active, request)
            if active.get("metadata") != metadata:
                raise GroupConflict("immutable metadata changed since prepare")
            if active["state"] in ("prepared", "resuming", "completed"):
                if active["state"] == "prepared":
                    if (_distributed.read_manifest() != _manifest(active)
                            or _distributed.read_generation() != metadata[0]["generation"]):
                        raise GroupConflict("prepared control state no longer matches operation")
                    for item in metadata:
                        lock = os.path.join(os.path.dirname(_path(item["checkpointPath"])), "lock")
                        try:
                            with open(lock, encoding="utf-8") as stream:
                                owner = stream.read()
                        except FileNotFoundError as exc:
                            raise GroupConflict("prepared rank lock missing") from exc
                        if owner != request["operationUID"]:
                            raise GroupConflict("prepared rank lock belongs to another operation")
                return _result(active)
        elif active and active.get("state") not in ("completed", "retired"):
            raise GroupConflict("another group operation owns shared control")
        # Revalidate under lock to exclude concurrent producer publication.
        if _metadata(request) != metadata:
            raise GroupConflict("metadata changed during prepare")
        record = {"request": request, "metadata": metadata, "state": "preparing"}
        _write(_active_path(), record)
        generation_path, manifest_path = _distributed.generation_path, _distributed.manifest_path
        _atomic_write = _distributed._atomic_write
        for item in metadata:
            lock = os.path.join(os.path.dirname(_path(item["checkpointPath"])), "lock")
            _atomic_write(lock, request["operationUID"], ".lock-")
        _atomic_write(generation_path(), str(metadata[0]["generation"]), ".generation-")
        _write(manifest_path(), _manifest(record))
        record["state"] = "prepared"
        _write(_active_path(), record)
        return _result(record)


def resume_group(payload):
    request = validate_request(payload)
    with control_lock():
        receipt = _read(_receipt_path(request["operationUID"]))
        if receipt:
            _matching(receipt, request)
            return _result(receipt)
        record = _read(_active_path())
        if not record:
            raise GroupConflict("group operation has not been prepared")
        _matching(record, request)
        if record.get("state") not in ("prepared", "resuming"):
            raise GroupConflict("group operation is not ready to resume")
        if _metadata(request) != record["metadata"]:
            raise GroupConflict("immutable metadata changed since prepare")
        manifest_path, read_manifest = _distributed.manifest_path, _distributed.read_manifest
        manifest = read_manifest()
        if _distributed.read_generation() != record["metadata"][0]["generation"]:
            raise GroupConflict("prepared generation no longer matches operation")
        if manifest != _manifest(record) and not (record["state"] == "resuming" and not manifest):
            raise GroupConflict("prepared manifest no longer matches operation")
        # Validate every lock before releasing any. A resuming retry may see missing locks.
        paths = []
        for item in record["metadata"]:
            path = os.path.join(os.path.dirname(_path(item["checkpointPath"])), "lock")
            try:
                with open(path, encoding="utf-8") as stream:
                    owner = stream.read()
            except FileNotFoundError:
                if record["state"] != "resuming":
                    raise GroupConflict("prepared rank lock missing")
            else:
                if owner != request["operationUID"]:
                    raise GroupConflict("rank lock belongs to another operation")
                paths.append(path)
        record["state"] = "resuming"
        _write(_active_path(), record)
        for path in paths:
            os.unlink(path)
        if manifest:
            os.unlink(manifest_path())
        record["state"] = "completed"
        _write(_receipt_path(request["operationUID"]), record)
        _write(_active_path(), record)
        return _result(record)


def checkpoint_binding(checkpoint_path):
    record = _read(_active_path())
    if not record or record.get("state") == "retired":
        return None
    if record.get("state") == "preparing":
        raise GroupConflict("group preparation incomplete; targets must remain stopped")
    live = os.path.realpath(checkpoint_path)
    for item in record["metadata"]:
        if _path(item["checkpointPath"]) == live:
            artifact = _path(item["artifactPath"])
            if not os.path.isfile(artifact):
                raise GroupConflict("prepared artifact missing")
            return {"checkpointID": item["checkpointID"], "artifactPath": artifact}
    return None
