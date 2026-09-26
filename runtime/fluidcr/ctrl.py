# Derived from FluidCR (Apache-2.0); upstream module content retained below.
# Local changes add controller telemetry, checkpointID propagation, and
# immutable per-round restore binding for Stateful-Migration-System.
"""
fluidcr.ctrl -- External control CLI and REST shim.

This module provides:

- ``fluidcr-ctrl`` CLI with:
  - ``checkpoint`` subcommand to send SIGUSR1 to target PIDs or all
    GPU-using worker PIDs registered by running Launcher instances.
  - ``resume`` subcommand to remove lock files under ``/checkpoint/<PPID>/lock``
    for specific PPIDs or for all pending checkpoints.
- A lightweight HTTP server that exposes the same actions for external
  controllers to call via REST. The server is intended to run *inside* the
  training container; the controller talks to it over the Pod IP.
- A worker PID registry that Launcher instances use to advertise their
  current worker PID. The registry is stored as individual files under
  ``<FLUIDCR_CHECKPOINT_DIR>/.workers/<launcher_pid>``.
"""

import argparse
import glob
import json
import math
import os
import re
import shutil
import signal
import tempfile
import threading
import time
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Dict, List, Optional, Tuple

from fluidcr._config import EXIT_CODE, log, warn

# Base directory for checkpoints (mirrors launcher.py behaviour).
_BASE_DIR: str = os.environ.get("FLUIDCR_CHECKPOINT_DIR", "/checkpoint")

# Default control port for the in-container REST API.
_DEFAULT_CTRL_PORT: int = int(os.environ.get("FLUIDCR_CTRL_PORT", "8298"))

# How long the controller should wait (in seconds) for checkpoint locks to
# appear after signalling workers.
_CHECKPOINT_WAIT_TIMEOUT: float = float(
    os.environ.get("FLUIDCR_CTRL_CHECKPOINT_TIMEOUT", "300")
)

# Poll interval used while waiting for locks.
_CHECKPOINT_WAIT_INTERVAL: float = float(
    os.environ.get("FLUIDCR_POLL_INTERVAL", "1")
)

_api_server: Optional[ThreadingHTTPServer] = None
_api_thread: Optional[threading.Thread] = None
_api_lock = threading.Lock()
_checkpoint_lock = threading.Lock()

_RUNTIME_STATUS_NAME = ".fluidcr-runtime.json"
_CHECKPOINT_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:-]{0,127}$")
_RUNTIME_TELEMETRY_INTERVAL_SECONDS = float(
    os.environ.get("FLUIDCR_RUNTIME_TELEMETRY_INTERVAL_SECONDS", "1")
)
_runtime_telemetry_lock = threading.Lock()
_last_runtime_status_write_at = 0.0
_restore_binding_lock = threading.Lock()
_restore_bindings: Dict[str, Dict[str, str]] = {}


# ---------------------------------------------------------------------------
# Worker PID registry
# ---------------------------------------------------------------------------
#
# The registry must be POD-LOCAL.  The default path lives under
# ``FLUIDCR_CHECKPOINT_DIR`` for backwards compatibility with single-pod setups
# where ``/checkpoint`` is an emptyDir.  In distributed setups where
# ``/checkpoint`` is a shared NFS volume, all pods would share the same
# registry directory -- and because container entrypoints commonly all run as
# PID 1, registry entries collide across pods (each pod overwrites the
# other's entry and then prunes it as "stale" because the foreign PID isn't
# in its own ``/proc``).
#
# Set ``FLUIDCR_REGISTRY_DIR`` to a pod-local path (e.g. ``/tmp/fluidcr-workers``
# or an emptyDir mount) whenever ``FLUIDCR_CHECKPOINT_DIR`` points at shared
# storage.

_WORKER_REGISTRY_DIR: str = os.environ.get(
    "FLUIDCR_REGISTRY_DIR",
    os.path.join(_BASE_DIR, ".workers"),
)


def register_worker_pid(launcher_pid: int, worker_pid: int) -> None:
    """Record that *launcher_pid* is currently supervising *worker_pid*.

    Called by the Launcher each time it spawns (or respawns) a worker so
    that ``fluidcr-ctrl checkpoint --rank`` can discover active workers without
    relying on external tools like ``nvidia-smi`` or ``lsof``.
    """
    os.makedirs(_WORKER_REGISTRY_DIR, exist_ok=True)
    path = os.path.join(_WORKER_REGISTRY_DIR, str(launcher_pid))
    with open(path, "w") as fh:
        fh.write(str(worker_pid))


def unregister_worker_pid(launcher_pid: int) -> None:
    """Remove the registry entry for *launcher_pid*.

    Called when the worker exits (regardless of exit code) so stale PIDs
    are never left behind.
    """
    path = os.path.join(_WORKER_REGISTRY_DIR, str(launcher_pid))
    try:
        os.remove(path)
    except FileNotFoundError:
        pass


def registered_worker_pids() -> Dict[int, int]:
    """Return ``{launcher_pid: worker_pid}`` for every live registration.

    Stale entries (launcher no longer running) are cleaned up automatically.
    """
    result: Dict[int, int] = {}
    try:
        entries = os.listdir(_WORKER_REGISTRY_DIR)
    except FileNotFoundError:
        return result
    for name in entries:
        try:
            launcher_pid = int(name)
        except ValueError:
            continue
        # Prune if the launcher itself is gone.
        if not os.path.exists(os.path.join("/proc", str(launcher_pid))):
            unregister_worker_pid(launcher_pid)
            continue
        path = os.path.join(_WORKER_REGISTRY_DIR, name)
        try:
            with open(path, "r") as fh:
                worker_pid = int(fh.read().strip())
        except (FileNotFoundError, ValueError):
            continue
        # Prune if the worker itself is gone.
        if not os.path.exists(os.path.join("/proc", str(worker_pid))):
            continue
        result[launcher_pid] = worker_pid
    return result


# ---------------------------------------------------------------------------
# GPU usage check via /proc/<pid>/fd
# ---------------------------------------------------------------------------


def _pid_uses_gpu(pid: int) -> bool:
    """Check whether *pid* has any open file descriptor pointing to ``/dev/nvidia*``.

    Pure-Python, requires no external tools -- only reads ``/proc/<pid>/fd/``.
    Covers all standard NVIDIA device files (``nvidia0``, ``nvidiactl``,
    ``nvidia-uvm``, ``nvidia-modeset``, ``nvidia-caps/...``) since CUDA opens
    at least one of these at context init.
    """
    fd_dir = os.path.join("/proc", str(pid), "fd")
    try:
        fds = os.listdir(fd_dir)
    except (FileNotFoundError, PermissionError):
        return False
    for fd in fds:
        try:
            target = os.readlink(os.path.join(fd_dir, fd))
        except (FileNotFoundError, PermissionError, OSError):
            continue
        if target.startswith("/dev/nvidia"):
            return True
    return False


# ---------------------------------------------------------------------------
# Checkpoint / resume primitives
# ---------------------------------------------------------------------------


def _launcher_pid_for_worker(pid: int) -> Optional[int]:
    """Return the parent PID (Launcher) for a given worker PID, if available."""
    status_path = os.path.join("/proc", str(pid), "status")
    try:
        with open(status_path, "r", encoding="utf-8") as fh:
            for line in fh:
                if line.startswith("PPid:"):
                    parts = line.split()
                    if len(parts) >= 2:
                        return int(parts[1])
                    break
    except FileNotFoundError:
        # Worker already exited.
        return None
    except Exception:
        return None
    return None


def _wait_for_parent_locks(
    parent_pids: List[int], timeout: Optional[float] = None,
) -> Dict[int, str]:
    """Block until lock files appear for all given parent PIDs or timeout.

    Returns a mapping of parent PID -> status string:
        - "lock-ready"            : lock file observed
        - "launcher-exited"       : parent process disappeared without a lock
        - "timeout-waiting-lock"  : timeout reached without lock or exit
    """
    statuses: Dict[int, str] = {}
    pending = set(parent_pids)
    budget = _CHECKPOINT_WAIT_TIMEOUT if timeout is None else timeout
    if not math.isfinite(budget) or budget <= 0:
        raise ValueError("checkpoint timeout must be positive and finite")
    deadline = time.monotonic() + min(budget, 300.0)

    while pending and time.monotonic() < deadline:
        finished: List[int] = []
        for ppid in list(pending):
            lock_path = _lock_path_for_ppid(ppid)
            if os.path.exists(lock_path):
                statuses[ppid] = "lock-ready"
                finished.append(ppid)
                continue

            # If the Launcher is gone and no lock is present, treat as failure.
            if not os.path.exists(os.path.join("/proc", str(ppid))):
                statuses[ppid] = "launcher-exited"
                finished.append(ppid)

        for ppid in finished:
            pending.discard(ppid)

        if pending:
            interval = min(max(_CHECKPOINT_WAIT_INTERVAL, 0.01), 1.0)
            time.sleep(min(interval, max(0.0, deadline - time.monotonic())))

    for ppid in pending:
        statuses[ppid] = "timeout-waiting-lock"

    return statuses


def _lock_path_for_ppid(ppid: int) -> str:
    """Return the lock file path for a given parent PID.

    Mirrors the Launcher's logic in [launcher.py]: when ``FLUIDCR_CHECKPOINT_PATH``
    is set explicitly (e.g. for distributed training where each rank uses a
    different path on shared storage), the lock lives next to the checkpoint
    file, NOT at ``/checkpoint/<launcher_pid>/lock``.
    """
    explicit_path = os.environ.get("FLUIDCR_CHECKPOINT_PATH", "")
    if explicit_path:
        return os.path.join(os.path.dirname(explicit_path), "lock")
    return os.path.join(_BASE_DIR, str(ppid), "lock")


def _checkpoint_path_for_ppid(ppid: int) -> str:
    explicit_path = os.environ.get("FLUIDCR_CHECKPOINT_PATH", "")
    if explicit_path:
        return explicit_path
    return os.path.join(_BASE_DIR, str(ppid), "latest.pt")


def _default_checkpoint_path() -> str:
    explicit_path = os.environ.get("FLUIDCR_CHECKPOINT_PATH", "")
    if explicit_path:
        return explicit_path
    return os.path.join(_BASE_DIR, str(os.getpid()), "latest.pt")


def _runtime_status_path(checkpoint_path: Optional[str] = None) -> str:
    path = checkpoint_path or _default_checkpoint_path()
    return os.path.join(os.path.dirname(path), _RUNTIME_STATUS_NAME)


def _restore_binding_key(checkpoint_path: str) -> str:
    return os.path.abspath(checkpoint_path)


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _env_int(name: str) -> Optional[int]:
    try:
        return int(os.environ[name])
    except (KeyError, TypeError, ValueError):
        return None


def _validate_checkpoint_id(checkpoint_id: Optional[str]) -> Optional[str]:
    if checkpoint_id is None:
        return None
    value = str(checkpoint_id).strip()
    if not _CHECKPOINT_ID_RE.fullmatch(value):
        raise ValueError(
            "checkpointID must be 1-128 chars: letters, digits, '.', '_', ':', '-'"
        )
    return value


def _round_artifact_path(checkpoint_path: str, checkpoint_id: str) -> str:
    return os.path.join(
        os.path.dirname(checkpoint_path),
        "rounds",
        checkpoint_id,
        os.path.basename(checkpoint_path),
    )


def _atomic_json_write(path: str, payload: Dict) -> None:
    parent = os.path.dirname(path)
    if parent:
        os.makedirs(parent, exist_ok=True)
    fd, tmp = tempfile.mkstemp(dir=parent or None, prefix=".fluidcr-", suffix=".tmp")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as fh:
            json.dump(payload, fh, sort_keys=True)
            fh.flush()
            os.fsync(fh.fileno())
        os.replace(tmp, path)
    except Exception:
        try:
            os.remove(tmp)
        except OSError:
            pass
        raise


def _read_status(checkpoint_path: Optional[str] = None) -> Dict:
    try:
        with open(_runtime_status_path(checkpoint_path), "r", encoding="utf-8") as fh:
            data = json.load(fh)
            return data if isinstance(data, dict) else {}
    except (FileNotFoundError, ValueError):
        return {}


def _write_status(checkpoint_path: str, updates: Dict) -> None:
    data = _read_status(checkpoint_path)
    data.update(updates)
    data["observedAt"] = _utc_now()
    _atomic_json_write(_runtime_status_path(checkpoint_path), data)


def _runtime_status_write_deadline(
    checkpoint_path: str, updates: Dict, force: bool
) -> Optional[float]:
    state = str(updates.get("state") or "")
    durable = force or "checkpointDurationSeconds" in updates
    durable = durable or (bool(state) and state != "Running")
    if durable or not os.path.exists(_runtime_status_path(checkpoint_path)):
        return time.monotonic()

    interval = max(0.0, _RUNTIME_TELEMETRY_INTERVAL_SECONDS)
    now = time.monotonic()
    with _runtime_telemetry_lock:
        if interval and _last_runtime_status_write_at:
            if now - _last_runtime_status_write_at < interval:
                return None
        return now


def _record_runtime_status_write(timestamp: float) -> None:
    global _last_runtime_status_write_at
    with _runtime_telemetry_lock:
        _last_runtime_status_write_at = timestamp


def bind_restore_checkpoint(
    checkpoint_path: str, checkpoint_id: str, artifact_path: str
) -> None:
    """Pin a restored launcher to the round artifact captured before CRIU.

    This is intentionally process memory, not only the JSON sidecar.  The HTTP
    control server runs in the launcher process, so CRIU snapshots this binding
    together with the launcher after a confirmed round.  Later rounds may
    overwrite both ``latest.pt`` and the sidecar; the restored launcher must
    still spawn workers against the artifact captured for its archive.
    """
    with _restore_binding_lock:
        _restore_bindings[_restore_binding_key(checkpoint_path)] = {
            "checkpointID": checkpoint_id,
            "artifactPath": artifact_path,
        }


def restore_checkpoint_binding(checkpoint_path: Optional[str] = None) -> Optional[Dict[str, str]]:
    explicit_restore = os.environ.get("FLUIDCR_RESTORE_CHECKPOINT_PATH", "").strip()
    if explicit_restore:
        if not os.path.isfile(explicit_restore):
            raise FileNotFoundError(
                f"pinned restore checkpoint is missing: {explicit_restore}"
            )
        return {
            "checkpointID": os.environ.get("FLUIDCR_RESTORE_CHECKPOINT_ID", "").strip(),
            "artifactPath": explicit_restore,
        }

    live_path = checkpoint_path or _default_checkpoint_path()
    with _restore_binding_lock:
        binding = _restore_bindings.get(_restore_binding_key(live_path))
        binding = dict(binding) if binding else None
    if binding:
        artifact = binding.get("artifactPath", "")
        if not os.path.isfile(artifact):
            raise FileNotFoundError(f"pinned restore checkpoint is missing: {artifact}")
        return binding
    return None


def restore_checkpoint_path(checkpoint_path: Optional[str] = None) -> str:
    """Return the immutable checkpoint artifact a restored worker should load.

    ``FLUIDCR_CHECKPOINT_PATH`` remains the live write target, usually
    ``latest.pt``.  Confirmed checkpoint rounds record the request-owned
    ``checkpointID`` and copied round artifact before CRIU snapshots the
    launcher.  Restored workers load that immutable artifact so later
    ``latest.pt`` overwrites cannot change the historical model state paired
    with the CRIU archive.
    """
    live_path = checkpoint_path or _default_checkpoint_path()
    binding = restore_checkpoint_binding(live_path)
    if binding:
        return binding["artifactPath"]
    status = _read_status(live_path)
    artifact = str(status.get("artifactPath") or "").strip()
    if artifact and os.path.isfile(artifact):
        return artifact
    checkpoint_id = str(status.get("checkpointID") or "").strip()
    if checkpoint_id:
        candidate = _round_artifact_path(live_path, checkpoint_id)
        if os.path.isfile(candidate):
            return candidate
    return live_path


def record_runtime_status(
    *,
    global_step: Optional[int] = None,
    state: Optional[str] = None,
    iteration_time_seconds: Optional[float] = None,
    force: bool = False,
) -> None:
    """Persist runtime telemetry for the in-pod ``GET /runtime`` collector.

    The launcher and worker are separate processes.  They communicate runtime
    observations through this small sidecar JSON next to ``FLUIDCR_CHECKPOINT_PATH``.
    Existing fields, especially the request-owned checkpointID, are preserved.
    """
    updates: Dict = {}
    if global_step is not None:
        updates["globalStep"] = int(global_step)
    if state is not None:
        updates["state"] = state
    if iteration_time_seconds is not None:
        updates["iterationTimeSeconds"] = float(iteration_time_seconds)
    if updates:
        checkpoint_path = _default_checkpoint_path()
        binding = restore_checkpoint_binding(checkpoint_path)
        if binding:
            if binding.get("checkpointID"):
                updates["checkpointID"] = binding["checkpointID"]
            updates["artifactPath"] = binding["artifactPath"]
        deadline = _runtime_status_write_deadline(checkpoint_path, updates, force)
        if deadline is None:
            return
        _write_status(checkpoint_path, updates)
        _record_runtime_status_write(deadline)


def _read_checkpoint_global_step(checkpoint_path: str) -> Optional[int]:
    if not os.path.isfile(checkpoint_path):
        return None
    try:
        import torch  # type: ignore

        try:
            payload = torch.load(checkpoint_path, map_location="cpu", weights_only=False)
        except TypeError:
            payload = torch.load(checkpoint_path, map_location="cpu")
    except Exception:
        return None
    if not isinstance(payload, dict):
        return None
    step_counts = payload.get("step_counts")
    if isinstance(step_counts, list) and step_counts:
        try:
            return int(step_counts[0])
        except (TypeError, ValueError):
            return None
    for key in ("global_step", "globalStep"):
        if key in payload:
            try:
                return int(payload[key])
            except (TypeError, ValueError):
                return None
    return None


def _preserve_round_artifacts(parent_pids: List[int], checkpoint_id: str) -> Dict[int, str]:
    results: Dict[int, str] = {}
    for ppid in parent_pids:
        src = _checkpoint_path_for_ppid(ppid)
        dst = _round_artifact_path(src, checkpoint_id)
        if os.path.exists(dst):
            results[ppid] = "artifact-exists"
            continue
        if not os.path.isfile(src):
            results[ppid] = "checkpoint-missing"
            continue
        os.makedirs(os.path.dirname(dst), exist_ok=True)
        fd, tmp = tempfile.mkstemp(
            dir=os.path.dirname(dst),
            prefix="." + os.path.basename(dst) + "-",
            suffix=".tmp",
        )
        try:
            with os.fdopen(fd, "wb") as out, open(src, "rb") as inp:
                shutil.copyfileobj(inp, out)
                out.flush()
                os.fsync(out.fileno())
            os.replace(tmp, dst)
            bind_restore_checkpoint(src, checkpoint_id, dst)
            _write_status(
                src,
                {
                    "checkpointID": checkpoint_id,
                    "artifactPath": dst,
                    "state": "CheckpointReady",
                    "globalStep": _read_checkpoint_global_step(src) or 0,
                },
            )
            results[ppid] = "artifact-preserved"
        except Exception as exc:
            try:
                os.remove(tmp)
            except OSError:
                pass
            results[ppid] = f"artifact-error: {exc}"
    return results


def runtime_status() -> Dict:
    checkpoint_path = _default_checkpoint_path()
    status = _read_status(checkpoint_path)
    rank = _env_int("RANK")
    world_size = _env_int("WORLD_SIZE")
    if rank is None or world_size is None:
        raise ValueError("runtime unavailable: RANK and WORLD_SIZE must be set")
    observed_at = status.get("observedAt")
    if not observed_at:
        raise ValueError("runtime unavailable: no worker status observation")
    lock_path = os.path.join(os.path.dirname(checkpoint_path), "lock")
    workers = registered_worker_pids()
    if os.path.exists(lock_path):
        state = "CheckpointReady"
    else:
        if not workers:
            raise ValueError("runtime unavailable: no live worker registry")
        state = status.get("state") or "Running"
    try:
        global_step = int(status.get("globalStep", 0))
    except (TypeError, ValueError):
        global_step = 0
    if "globalStep" not in status:
        checkpoint_step = _read_checkpoint_global_step(checkpoint_path)
        if checkpoint_step is not None:
            global_step = checkpoint_step
    payload = {
        "globalStep": global_step,
        "checkpointID": status.get("checkpointID", ""),
        "rank": rank,
        "worldSize": world_size,
        "observedAt": observed_at,
        "state": state,
    }
    if "iterationTimeSeconds" in status:
        payload["iterationTimeSeconds"] = status["iterationTimeSeconds"]
    if "checkpointDurationSeconds" in status:
        payload["checkpointDurationSeconds"] = status["checkpointDurationSeconds"]
    return payload


def checkpoint_pids(
    pids: List[int],
    worker_to_launcher: Optional[Dict[int, int]] = None,
) -> Dict[int, str]:
    """Send SIGUSR1 to each target PID and wait for checkpoints.

    *worker_to_launcher* is an optional pre-computed mapping of worker PID to
    its Launcher PID (as recorded in the worker registry).  When provided it
    takes precedence over the ``/proc`` parent-walk so that intermediate
    processes injected by virtual-GPU layers (e.g. HAMi) do not cause the
    wrong lock path to be polled.
    """
    # ``--pid`` means a full checkpoint of the whole world; write an "all"
    # manifest so a leftover partial manifest from an aborted round cannot make
    # this trigger act as a partial migration (spec: --pid degrades to full).
    from fluidcr.distributed import bump_generation, write_manifest

    bump_generation()
    write_manifest("all")

    results: Dict[int, str] = {}
    for pid in pids:
        try:
            os.kill(pid, signal.SIGUSR1)
            results[pid] = "signalled"
        except ProcessLookupError:
            results[pid] = "no-such-process"
        except PermissionError:
            results[pid] = "permission-denied"
        except Exception as exc:  # pragma: no cover - defensive
            results[pid] = f"error: {exc}"

    # Resolve the Launcher PID for each successfully signalled worker.
    # Prefer the registry mapping when available; fall back to /proc walk.
    parent_pids: List[int] = []
    pid_to_parent: Dict[int, int] = {}
    for pid, status in results.items():
        if status != "signalled":
            continue
        ppid = (worker_to_launcher or {}).get(pid) or _launcher_pid_for_worker(pid)
        if ppid is None:
            continue
        parent_pids.append(ppid)
        pid_to_parent[pid] = ppid

    if parent_pids:
        parent_statuses = _wait_for_parent_locks(sorted(set(parent_pids)))
        # Propagate parent lock statuses back to worker PIDs.
        for pid, ppid in pid_to_parent.items():
            p_status = parent_statuses.get(ppid)
            if p_status == "lock-ready":
                results[pid] = "checkpoint-ready"
            elif p_status is not None:
                results[pid] = f"signalled ({p_status})"

    return results


def _parse_rank_spec(spec: str):
    """Parse a --rank spec into the manifest target set.

    ``"all"`` (any case) -> the ``"all"`` sentinel; ``"1,2,3"`` -> ``[1, 2, 3]``.
    Raises ValueError on an empty spec.
    """
    s = spec.strip().lower()
    if s == "all":
        return "all"
    ranks = [int(x) for x in spec.split(",") if x.strip() != ""]
    if not ranks:
        raise ValueError("empty --rank spec")
    return ranks


def checkpoint_ranks(
    targets, *, _workers=None, checkpoint_id: Optional[str] = None
) -> Dict[int, str]:
    """Declare a coordinated action: write the manifest, then trigger it.

    ``targets`` is the ``"all"`` sentinel or a list of rank ints. Advances the shared
    generation and writes the manifest first (so every rank observes both), then
    sends SIGUSR1 to the local GPU worker(s) as the trigger. The signal need only
    reach one pod -- ``all_reduce`` propagates "pending" to the whole world and the
    manifest decides roles.

    Order matters: the generation must be on the PVC before any rank can act on the
    trigger, or a survivor could rebuild its process group on the outgoing
    generation's rendezvous keys.
    """
    from fluidcr.distributed import bump_generation, write_manifest

    checkpoint_id = _validate_checkpoint_id(checkpoint_id)
    bump_generation()
    write_manifest(targets)
    workers = registered_worker_pids() if _workers is None else _workers
    if checkpoint_id:
        for launcher_pid in workers:
            _write_status(
                _checkpoint_path_for_ppid(launcher_pid),
                {"checkpointID": checkpoint_id, "state": "CheckpointRequested"},
            )
    results: Dict[int, str] = {}
    for _launcher_pid, worker_pid in workers.items():
        if not _pid_uses_gpu(worker_pid):
            results[worker_pid] = "skipped-no-gpu"
            continue
        try:
            os.kill(worker_pid, signal.SIGUSR1)
            results[worker_pid] = "checkpoint-signalled"
        except ProcessLookupError:
            results[worker_pid] = "gone"
        except PermissionError:
            results[worker_pid] = "permission-denied"
    return results


def checkpoint_ranks_and_wait(
    targets, timeout: float, checkpoint_id: Optional[str] = None
) -> Dict[int, str]:
    """Confirm local whole-workload checkpoints through new launcher locks."""
    if targets != "all":
        raise ValueError("wait requires ranks=all")
    if not math.isfinite(timeout) or timeout <= 0 or timeout > 300:
        raise ValueError("timeoutSeconds must be between 0 and 300")
    if not _checkpoint_lock.acquire(blocking=False):
        raise ValueError("checkpoint already in progress")
    try:
        checkpoint_id = _validate_checkpoint_id(checkpoint_id)
        registry = registered_worker_pids()
        workers = {
            launcher: worker for launcher, worker in registry.items()
            if _pid_uses_gpu(worker)
        }
        if not workers:
            raise ValueError("no local GPU workers registered")
        if len(set(workers.values())) != len(workers):
            raise ValueError("ambiguous worker-to-launcher registry")
        paths = [_lock_path_for_ppid(parent) for parent in workers]
        if len(set(paths)) != len(paths):
            raise ValueError("multiple launchers share one checkpoint lock path")
        if any(os.path.lexists(path) for path in paths):
            raise ValueError("stale checkpoint lock exists; resume before checkpoint")
        if checkpoint_id:
            for parent in workers:
                artifact = _round_artifact_path(
                    _checkpoint_path_for_ppid(parent), checkpoint_id
                )
                if os.path.exists(artifact):
                    raise ValueError(f"checkpointID already exists: {checkpoint_id}")

        # Keep the generation/manifest protocol and one registry snapshot
        # for both signalling and parent-lock confirmation.
        started = time.monotonic()
        results = checkpoint_ranks(
            targets, _workers=workers, checkpoint_id=checkpoint_id
        )
        parents = [
            parent for parent, worker in workers.items()
            if results.get(worker) == "checkpoint-signalled"
        ]
        statuses = _wait_for_parent_locks(parents, timeout=timeout)
        preserved: Dict[int, str] = {}
        ready_parents = [
            parent for parent in parents
            if statuses.get(parent) == "lock-ready"
        ]
        if checkpoint_id and ready_parents:
            preserved = _preserve_round_artifacts(ready_parents, checkpoint_id)
        for parent, worker in workers.items():
            if results.get(worker) == "checkpoint-signalled":
                status = statuses.get(parent, "timeout-waiting-lock")
                if status == "lock-ready" and checkpoint_id:
                    artifact_status = preserved.get(parent)
                    results[worker] = (
                        "checkpoint-ready"
                        if artifact_status == "artifact-preserved"
                        else artifact_status or "artifact-missing"
                    )
                else:
                    results[worker] = (
                        "checkpoint-ready" if status == "lock-ready" else status
                    )
                if status == "lock-ready":
                    checkpoint_path = _checkpoint_path_for_ppid(parent)
                    existing = _read_status(checkpoint_path).get("checkpointID", "")
                    _write_status(
                        checkpoint_path,
                        {
                            "checkpointID": checkpoint_id or existing,
                            "state": "CheckpointReady",
                            "checkpointDurationSeconds": round(time.monotonic() - started, 6),
                            "globalStep": _read_checkpoint_global_step(checkpoint_path) or 0,
                        },
                    )
        return results
    finally:
        _checkpoint_lock.release()


def pending_lock_paths() -> List[str]:
    """Return absolute path of every checkpoint lock file currently present.

    Works regardless of how the lock directory is named: a PID
    (``/checkpoint/1234/lock``) in single-pod mode, or a rank label
    (``/checkpoint/rank0/lock``) in distributed mode.
    """
    pattern = os.path.join(_BASE_DIR, "*", "lock")
    return sorted(glob.glob(pattern))


def pending_ppids() -> List[int]:
    """Return PPIDs that currently have a checkpoint lock file.

    Legacy: only finds locks under integer-named directories.  Lock dirs
    named like ``rank0`` (distributed mode) are silently skipped --
    use :func:`pending_lock_paths` for the general case.
    """
    ppids: List[int] = []
    for lock_path in pending_lock_paths():
        name = os.path.basename(os.path.dirname(lock_path))
        try:
            ppids.append(int(name))
        except ValueError:
            continue
    return sorted(set(ppids))


def resume_ppids(ppids: List[int]) -> Dict[int, str]:
    """Remove lock files for the given PPIDs to allow resume.

    Constructs the lock path directly from the PPID, ignoring
    ``FLUIDCR_CHECKPOINT_PATH``.  Use only for legacy single-pod setups
    where the lock lives at ``/checkpoint/<ppid>/lock``.
    """
    results: Dict[int, str] = {}
    for ppid in ppids:
        lock_path = os.path.join(_BASE_DIR, str(ppid), "lock")
        if not os.path.exists(lock_path):
            results[ppid] = "no-lock"
            continue
        try:
            os.remove(lock_path)
            results[ppid] = "lock-removed"
        except Exception as exc:  # pragma: no cover - defensive
            results[ppid] = f"error: {exc}"
    return results


def resume_all_pending() -> Dict[str, str]:
    """Remove every pending lock, pause-lock, and the migration manifest.

    Globs under BOTH the ctrl base dir and the shared checkpoint base
    (``distributed._base_dir()``) so survivor ``pause-lock`` files -- which live
    next to the per-rank checkpoint path -- are always found even when
    ``FLUIDCR_CHECKPOINT_DIR`` is unset and the PVC is not rooted at
    ``/checkpoint``. One call from any pod unblocks the whole world and clears the
    manifest so a stale target set cannot leak into a later round.

    Deliberately leaves ``migration-generation`` in place: the counter must stay
    monotonic for the lifetime of the rank-0 store server. Resetting it would point
    the next rebuild back at generation 0's rendezvous keys -- which that server
    still holds -- reintroducing the stale-``ncclUniqueId`` failure this counter
    exists to prevent.

    Returns a mapping of removed path -> status.
    """
    from fluidcr.distributed import manifest_path, _base_dir

    bases: List[str] = []
    for base in (_BASE_DIR, _base_dir()):
        if base not in bases:
            bases.append(base)

    paths: List[str] = []
    for base in bases:
        for name in ("lock", "pause-lock"):
            paths.extend(sorted(glob.glob(os.path.join(base, "*", name))))
    paths.append(manifest_path())

    results: Dict[str, str] = {}
    seen = set()
    for path in paths:
        if path in seen:
            continue
        seen.add(path)
        try:
            os.remove(path)
            results[path] = "removed"
        except FileNotFoundError:
            results[path] = "already-gone"
    return results


# ---------------------------------------------------------------------------
# REST API server
# ---------------------------------------------------------------------------


class _CtrlRequestHandler(BaseHTTPRequestHandler):
    """Minimal JSON REST handler for FluidCR control."""

    server_version = "FluidCR-Ctrl/1.0"

    def _read_json(self) -> Dict:
        length = int(self.headers.get("Content-Length", "0") or "0")
        if not length:
            return {}
        raw = self.rfile.read(length)
        try:
            return json.loads(raw.decode("utf-8"))
        except Exception:
            return {}

    def _send_json(self, status: int, payload: Dict) -> None:
        body = json.dumps(payload).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_POST(self) -> None:  # noqa: N802
        if self.path == "/checkpoint":
            self._handle_checkpoint()
        elif self.path == "/resume":
            self._handle_resume()
        else:
            self._send_json(404, {"error": "not-found"})

    def do_GET(self) -> None:  # noqa: N802
        if self.path == "/runtime":
            try:
                self._send_json(200, runtime_status())
            except ValueError as exc:
                self._send_json(503, {"error": str(exc)})
        else:
            self._send_json(404, {"error": "not-found"})

    def log_message(self, format: str, *args) -> None:  # noqa: A003
        # Route access logs through FluidCR logger at DEBUG level.
        msg = format % args
        log(f"ctrl-http {self.address_string()}: {msg}", level="DEBUG")

    def _handle_checkpoint(self) -> None:
        payload = self._read_json()
        pids = payload.get("pids") or []
        ranks = payload.get("ranks")  # "all" | [ints] | None
        wait = payload.get("wait", False)
        checkpoint_id = payload.get("checkpointID")
        if not isinstance(wait, bool):
            self._send_json(400, {"error": "wait must be a boolean"})
            return
        if wait:
            if pids or ranks not in (None, "all"):
                self._send_json(400, {"error": "wait supports only ranks=all"})
                return
            try:
                timeout = payload.get("timeoutSeconds", _CHECKPOINT_WAIT_TIMEOUT)
                if isinstance(timeout, bool):
                    raise ValueError("timeoutSeconds must be numeric")
                results = checkpoint_ranks_and_wait(
                    "all", float(timeout), checkpoint_id=checkpoint_id
                )
            except (ValueError, TypeError) as exc:
                self._send_json(400, {"error": str(exc)})
                return
            except Exception as exc:
                self._send_json(500, {"error": str(exc)})
                return
            self._send_json(200, {"results": results})
            return

        if pids and ranks is not None:
            self._send_json(
                400, {"error": "specify either 'pids' or 'ranks', not both"}
            )
            return

        if pids:
            if checkpoint_id:
                self._send_json(400, {"error": "checkpointID supports rank checkpoints only"})
                return
            try:
                pid_ints = [int(p) for p in pids]
            except Exception:
                self._send_json(400, {"error": "invalid 'pids' payload"})
                return
            results = checkpoint_pids(pid_ints)
        else:
            if isinstance(ranks, list) and not ranks:
                self._send_json(400, {"error": "empty 'ranks' list"})
                return
            targets = "all" if ranks in (None, "all") else [int(r) for r in ranks]
            try:
                results = checkpoint_ranks(targets, checkpoint_id=checkpoint_id)
            except ValueError as exc:
                self._send_json(400, {"error": str(exc)})
                return

        self._send_json(200, {"results": results})

    def _handle_resume(self) -> None:
        payload = self._read_json()
        ppids = payload.get("ppids") or []
        all_flag = bool(payload.get("all"))

        if ppids and all_flag:
            self._send_json(
                400, {"error": "specify either 'ppids' or 'all', not both"}
            )
            return

        if all_flag:
            results = resume_all_pending()
        else:
            try:
                ppid_ints = [int(p) for p in ppids]
            except Exception:
                self._send_json(400, {"error": "invalid 'ppids' payload"})
                return
            results = resume_ppids(ppid_ints)

        self._send_json(200, {"results": results})


def ensure_api_server_started(
    host: str = "0.0.0.0", port: int = _DEFAULT_CTRL_PORT
) -> Tuple[Optional[ThreadingHTTPServer], Optional[threading.Thread]]:
    """Start the control API server in the background if not already running.

    This is idempotent and safe to call from the Launcher. If the port is
    already in use, a warning is logged and no server is started (assumed to
    be running elsewhere in the container).
    """
    global _api_server, _api_thread

    with _api_lock:
        if _api_server is not None:
            return _api_server, _api_thread

        try:
            server = ThreadingHTTPServer((host, port), _CtrlRequestHandler)
        except OSError as exc:
            warn(f"control API server not started (port {port}): {exc}")
            return None, None

        thread = threading.Thread(
            target=server.serve_forever, name="fluidcr-ctrl-http", daemon=True
        )
        thread.start()

        _api_server = server
        _api_thread = thread

        log(f"control API server listening on {host}:{port}", level="INFO")
        return _api_server, _api_thread


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="fluidcr-ctrl",
        description="FluidCR external control CLI (checkpoint / resume).",
    )

    subparsers = parser.add_subparsers(dest="command", required=True)

    # checkpoint --------------------------------------------------------------
    p_checkpoint = subparsers.add_parser(
        "checkpoint", help="Trigger checkpoint via SIGUSR1."
    )
    p_checkpoint.add_argument(
        "-p",
        "--pid",
        dest="pids",
        type=int,
        nargs="+",
        help="Target worker PID(s) to signal.",
    )
    p_checkpoint.add_argument(
        "--rank",
        metavar="SPEC",
        help="Ranks to checkpoint/free: 'all' for a full checkpoint, or a comma "
        "list like '1,2,3' for partial migration (those ranks free their GPU; "
        "the rest keep VRAM and continue).",
    )

    # resume ------------------------------------------------------------------
    p_resume = subparsers.add_parser(
        "resume", help="Resume from checkpoints by removing lock files."
    )
    p_resume.add_argument(
        "-P",
        "--ppid",
        dest="ppids",
        type=int,
        nargs="+",
        help="Parent PID(s) whose lock files should be removed.",
    )
    p_resume.add_argument(
        "--all",
        action="store_true",
        help="Resume all pending checkpoints under the base directory.",
    )

    # api-server --------------------------------------------------------------
    p_api = subparsers.add_parser(
        "api-server", help="Run the in-container REST control API server."
    )
    p_api.add_argument(
        "--host",
        default="0.0.0.0",
        help="Bind address for the REST server (default: 0.0.0.0).",
    )
    p_api.add_argument(
        "--port",
        type=int,
        default=_DEFAULT_CTRL_PORT,
        help=f"TCP port for the REST server (default: {_DEFAULT_CTRL_PORT}).",
    )

    return parser


def main_cli(argv: Optional[List[str]] = None) -> int:
    """Entry point for the ``fluidcr-ctrl`` console script."""
    parser = _build_parser()
    args = parser.parse_args(argv)

    if args.command == "checkpoint":
        if args.pids and args.rank:
            parser.error("checkpoint: specify either --pid or --rank, not both")
        if args.rank is not None:
            try:
                targets = _parse_rank_spec(args.rank)
            except ValueError as exc:
                parser.error(f"checkpoint: {exc}")
            if targets != "all" and 0 in targets:
                print(
                    "WARNING: rank 0 hosts the rendezvous store; on resume, resume "
                    "rank 0 FIRST (or rely on TCPStore connect-retry)."
                )
            results = checkpoint_ranks(targets)
            if not results:
                workers = registered_worker_pids()
                print(
                    "No GPU-using workers detected.\n"
                    f"  Registry dir : {_WORKER_REGISTRY_DIR}\n"
                    f"  Registered   : {workers if workers else '(none)'}\n"
                    "  Hint: if Registered is empty, the launcher hasn't "
                    "registered yet -- check that the running wheel knows "
                    "about FLUIDCR_REGISTRY_DIR.  If workers are listed but "
                    "excluded, the worker isn't holding a /dev/nvidia* "
                    "file descriptor open -- inspect with: "
                    "`ls -la /proc/<worker_pid>/fd/ | grep /dev/`."
                )
                return 1
        elif args.pids:
            results = checkpoint_pids(args.pids)
        else:
            parser.error("checkpoint: one of --pid/--rank is required")
        for pid, status in results.items():
            print(f"PID {pid}: {status}")
        return 0

    if args.command == "resume":
        if args.ppids and args.all:
            parser.error("resume: specify either --ppid or --all, not both")
        if args.all:
            results = resume_all_pending()
            if not results:
                print(f"No pending checkpoint locks under {_BASE_DIR}/*/lock.")
                return 0
            for path, status in results.items():
                print(f"{path}: {status}")
        elif args.ppids:
            ppid_results = resume_ppids(args.ppids)
            for ppid, status in ppid_results.items():
                print(f"PPID {ppid}: {status}")
        else:
            parser.error("resume: one of --ppid/--all is required")
        return 0

    if args.command == "api-server":
        server, _thread = ensure_api_server_started(args.host, args.port)
        if server is None:
            return 1
        try:
            # Block forever; serve_forever() is already running in a daemon
            # thread. This just keeps the process alive until interrupted.
            threading.Event().wait()
        except KeyboardInterrupt:
            return 0

    return 1


if __name__ == "__main__":  # pragma: no cover - manual invocation
    raise SystemExit(main_cli())
