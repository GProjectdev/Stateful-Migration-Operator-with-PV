"""Survivor-side helpers for partial migration (pause + NCCL/DDP rebuild).

Also owns the shared migration *manifest* -- the JSON file that declares which
ranks are targets for a coordinated action. The manifest is the single source of
truth for role assignment, replacing per-rank signal inference (which raced when
more than one rank was a target).

And the shared migration *generation* -- the counter that gives each round its own
rendezvous key namespace. See :func:`generation_scoped_store` for why that is not
optional.
"""

import json
import os
import tempfile
import time
import weakref
from typing import Any, Dict, List, Optional, Tuple, Union

_MANIFEST_NAME = "migration-manifest.json"
_GENERATION_NAME = "migration-generation"

# Outermost store-key segment: "fluidcr-gen<N>". Bumping N moves the whole process
# group onto virgin keys. No trailing slash -- PrefixStore inserts the separator.
_STORE_PREFIX = "fluidcr-gen"

# A parsed manifest target set: the sentinel "all" or an explicit list of ranks.
Targets = Union[str, List[int]]


def _base_dir() -> str:
    """Shared PVC root common to every rank (parent of the per-rank dirs).

    Prefers the explicit ``FLUIDCR_CHECKPOINT_DIR``; otherwise derives it from the
    per-rank ``FLUIDCR_CHECKPOINT_PATH`` (``<base>/rankN/latest.pt`` -> ``<base>``).
    """
    d = os.environ.get("FLUIDCR_CHECKPOINT_DIR")
    if d:
        return d
    ckpt = os.environ.get("FLUIDCR_CHECKPOINT_PATH")
    if ckpt:
        return os.path.dirname(os.path.dirname(ckpt))
    return "/checkpoint"


def manifest_path() -> str:
    """Absolute path to the shared migration manifest."""
    return os.path.join(_base_dir(), _MANIFEST_NAME)


def _atomic_write(path: str, text: str, prefix: str) -> str:
    """Write *text* to *path* so readers never observe a partial file.

    Temp file in the same directory, fsync, then ``os.replace``. Returns *path*.
    """
    parent = os.path.dirname(path)
    if parent and not os.path.isdir(parent):
        os.makedirs(parent, exist_ok=True)
    fd, tmp = tempfile.mkstemp(dir=parent, prefix=prefix, suffix=".tmp")
    try:
        with os.fdopen(fd, "w") as fh:
            fh.write(text)
            fh.flush()
            os.fsync(fh.fileno())
        os.replace(tmp, path)
    except Exception:
        try:
            os.remove(tmp)
        except OSError:
            pass
        raise
    return path


def write_manifest(
    targets: Targets,
    *,
    checkpoint_id: Optional[str] = None,
    restore_owned_resume: bool = False,
    generation: Optional[int] = None,
) -> str:
    """Atomically write the manifest declaring the target set.

    ``targets`` is either the string ``"all"`` or a list of rank ints. Returns the
    manifest path.
    """
    payload = {"targets": "all" if targets == "all" else [int(r) for r in targets]}
    if checkpoint_id:
        payload["checkpointID"] = str(checkpoint_id)
    if generation is not None:
        payload["generation"] = int(generation)
    if restore_owned_resume:
        payload["restoreOwnedResume"] = True
        payload["noPeriodicResume"] = True
    return _atomic_write(manifest_path(), json.dumps(payload), ".manifest-")


def read_manifest() -> Dict[str, Any]:
    try:
        with open(manifest_path()) as fh:
            data = json.load(fh)
            return data if isinstance(data, dict) else {}
    except (FileNotFoundError, ValueError):
        return {}


def read_migration_targets() -> Targets:
    """Read the manifest. Returns ``"all"`` or a list of rank ints.

    A missing or unparseable manifest defaults to ``"all"`` (full checkpoint) --
    a bare trigger with no manifest means "everyone checkpoints".
    """
    data = read_manifest()
    if not data:
        return "all"
    t = data.get("targets", "all")
    if t == "all":
        return "all"
    if isinstance(t, list):
        try:
            return [int(r) for r in t]
        except (TypeError, ValueError):
            return "all"
    return "all"


# ---------------------------------------------------------------------------
# Migration generation -- one rendezvous key namespace per round
# ---------------------------------------------------------------------------


def generation_path() -> str:
    """Absolute path to the shared migration generation counter."""
    return os.path.join(_base_dir(), _GENERATION_NAME)


def read_generation() -> int:
    """Current migration generation.

    Returns 0 when the file is missing or unreadable -- the pre-migration state,
    which is also what a freshly submitted job sees.
    """
    try:
        with open(generation_path()) as fh:
            return int(fh.read().strip() or 0)
    except (FileNotFoundError, ValueError):
        return 0


def bump_generation() -> int:
    """Atomically advance the generation and return the new value.

    Called by ``fluidcr-ctrl`` once per round, *before* the manifest is written, so
    every rank that later (re-)initialises its process group -- survivors at unpark
    and restored targets on their next launcher iteration -- reads the same value.

    Seeded from the wall clock rather than counting 1, 2, 3..., because the value
    must never land on a namespace that rank-0's *live* store server already holds.
    A small counter breaks that if the file is lost while rank 0 keeps running --
    wiping the PVC between rounds sends it back to 1, and generation 1's keys are
    still in that server. Seconds-since-epoch puts the next value above anything an
    earlier round used, so losing the file is harmless in practice: rounds are
    seconds-to-minutes apart (a round saves the full checkpoint and CRIU-restores a
    pod), while the seed advances every second.

    ``max(previous + 1, now)`` keeps it strictly increasing even across a backwards
    clock step or repeated bumps inside one second. The value stays a plain integer,
    so a mixed fleet of FluidCR versions reads it identically.
    """
    nxt = max(read_generation() + 1, int(time.time()))
    _atomic_write(generation_path(), str(nxt), ".generation-")
    return nxt


def generation_scoped_store(rank: int, world_size: int, timeout: Any = None) -> Any:
    """The rendezvous store for this generation, namespaced by generation.

    Why this exists -- without it a rebuilt process group silently reuses the
    *previous* round's store keys, and three PyTorch behaviours make that
    deterministic rather than merely likely:

      * ``rendezvous._create_c10d_store`` builds rank-0's ``TCPStore`` with
        ``multi_tenant=True``, so ``destroy_process_group()`` + re-init hands back
        the same server -- every key from the previous round is still set.
      * ``destroy_process_group()`` resets ``_world.group_count = 0``, so the
        rebuilt default PG gets the same ``PrefixStore`` name as the first one.
      * ``ProcessGroupNCCL``'s ``ncclCommCounter_`` restarts at 0, so the key
        holding the ``ncclUniqueId`` is byte-identical to the previous round's.

    ``broadcastUniqueNCCLID`` then finds that key already populated and ``get()``
    returns the *stale* ID instead of blocking for the new one. A non-root survivor
    bootstraps against the root's already-destroyed socket and dies with
    ``socketPollConnect: ... Connection refused`` -> ``ncclRemoteError``. The root
    itself is unaffected because it *writes* that key rather than reading it, which
    is why the failure looks like "only rank N is broken".

    Prefixing with the generation keeps each round's namespace virgin, restoring the
    blocking ``get()`` the rendezvous protocol assumes.
    """
    import torch.distributed as dist
    from torch.distributed.rendezvous import _create_c10d_store

    if timeout is None:
        from datetime import timedelta

        # How long a rank will block waiting for the root to publish the new
        # ncclUniqueId. Matches the survivor parking budget in
        # _wait_for_pause_lock_removal: a survivor waiting on a target that is still
        # being restored should not give up sooner than it was willing to park.
        timeout = timedelta(seconds=1800)

    host = os.environ.get("MASTER_ADDR", "127.0.0.1")
    port = int(os.environ.get("MASTER_PORT", "29500"))
    base = _create_c10d_store(host, port, rank, world_size, timeout)

    # Mirrors torch's own layering (``PrefixStore("default_pg", ...)`` in
    # init_process_group) with the generation as the outermost segment, so keys
    # read ``fluidcr-gen<N>/default_pg/...``.
    scoped = dist.PrefixStore(f"{_STORE_PREFIX}{read_generation()}", base)
    return dist.PrefixStore("default_pg", scoped)


def _env_int(name: str) -> Optional[int]:
    try:
        return int(os.environ[name])
    except (KeyError, ValueError):
        return None


def scope_init_process_group_kwargs(args: tuple, kwargs: dict) -> Dict[str, Any]:
    """Return ``init_process_group`` kwargs carrying a generation-scoped store.

    Returns the kwargs unchanged (a plain no-op) unless every precondition holds:
    FluidCR distributed mode is on, the caller left peer discovery to the
    environment (no explicit ``store`` or ``init_method``), and RANK/WORLD_SIZE are
    both set. Anything else means the caller has its own rendezvous and we must not
    redirect it.

    Only ``backend`` may be positional. Past it the signature is
    ``(init_method, timeout, world_size, rank, store, ...)`` -- any of those given
    positionally would collide with the kwargs injected here, so bail out instead.
    """
    import fluidcr

    if not fluidcr.distributed_checkpoint_enabled():
        return kwargs
    if len(args) > 1:
        return kwargs
    if kwargs.get("store") is not None or kwargs.get("init_method") is not None:
        return kwargs

    rank = _env_int("RANK")
    world_size = _env_int("WORLD_SIZE")
    if rank is None or world_size is None:
        return kwargs

    scoped = dict(kwargs)
    scoped["store"] = generation_scoped_store(rank, world_size, kwargs.get("timeout"))
    scoped["rank"] = rank
    scoped["world_size"] = world_size
    return scoped


def rank_is_target(rank: int, targets: Targets) -> bool:
    """True if this rank should take the target (save+free+exit) path."""
    return targets == "all" or rank in targets


def is_full(targets: Targets, world_size: int) -> bool:
    """True if the target set covers the whole world (equivalent to full checkpoint).

    Ranks outside ``[0, world_size)`` are ignored so a typo cannot mask fullness.
    """
    if targets == "all":
        return True
    valid = {r for r in targets if 0 <= r < world_size}
    return valid >= set(range(world_size))


# Recorded DDP wrappers: (weakref to wrapper, positional args after module, kwargs)
_ddp_records: List[Tuple[Any, tuple, dict]] = []

# True while rebuild_tracked_ddp() is re-running DDP.__init__. The patched
# DDP.__init__ calls record_ddp(); without this guard each rebuild would
# re-append the same wrapper, growing _ddp_records every migration and
# re-initialising the same wrapper multiple times on later migrations.
_rebuilding = False


def record_ddp(wrapper: Any, args: tuple, kwargs: dict) -> None:
    """Record a DDP wrapper and its constructor arguments for later rebuild."""
    if _rebuilding:
        return
    _ddp_records.append((weakref.ref(wrapper), tuple(args), dict(kwargs)))


def rebuild_tracked_ddp() -> None:
    """Re-run DDP.__init__ on each live wrapper in place against the new PG.

    Object identity is preserved (the user's ``model`` variable keeps working);
    the inner module's parameters are untouched, so the optimizer stays valid.
    Dead weakrefs are pruned, and re-appends triggered by the patched
    ``DDP.__init__`` are suppressed via ``_rebuilding``.
    """
    global _rebuilding
    _rebuilding = True
    try:
        live: List[Tuple[Any, tuple, dict]] = []
        for ref, args, kwargs in list(_ddp_records):
            wrapper = ref()
            if wrapper is None:
                continue
            live.append((ref, args, kwargs))
            kw = dict(kwargs)
            kw.pop("process_group", None)  # MVP: rebind to the new default PG
            type(wrapper).__init__(wrapper, wrapper.module, *args, **kw)
        _ddp_records[:] = live
    finally:
        _rebuilding = False


def _pause_lock_path() -> str:
    """Path to this rank's pause-lock, next to its checkpoint file."""
    ckpt = os.environ.get("FLUIDCR_CHECKPOINT_PATH", "/checkpoint/latest.pt")
    return os.path.join(os.path.dirname(ckpt), "pause-lock")


def _survivor_proof_path() -> str:
    ckpt = os.environ.get("FLUIDCR_CHECKPOINT_PATH", "/checkpoint/latest.pt")
    return os.path.join(os.path.dirname(ckpt), ".fluidcr-survivor.json")


def _pod_uid() -> str:
    return (
        os.environ.get("FLUIDCR_POD_UID")
        or os.environ.get("POD_UID")
        or os.environ.get("K8S_POD_UID")
        or ""
    ).strip()


def _env_text(*names: str) -> str:
    for name in names:
        value = os.environ.get(name, "").strip()
        if value:
            return value
    return ""


def _utc_now() -> str:
    from datetime import datetime, timezone

    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _write_pause_lock(path: str) -> None:
    parent = os.path.dirname(path)
    if parent and not os.path.isdir(parent):
        os.makedirs(parent, exist_ok=True)
    manifest = read_manifest()
    rank = _env_int("RANK")
    proof = {
        "state": "SurvivorParked",
        "phase": "SurvivorPaused",
        "pid": os.getpid(),
        "pauseLockPID": os.getpid(),
        "rank": rank,
        "podName": _env_text("FLUIDCR_POD_NAME", "POD_NAME", "HOSTNAME"),
        "podUID": _pod_uid(),
        "nodeName": _env_text("FLUIDCR_NODE_NAME", "NODE_NAME"),
        "checkpointID": manifest.get("checkpointID", ""),
        "generation": read_generation(),
        "pauseLockPath": path,
        "observedAt": _utc_now(),
        "restoreOwnedResume": bool(manifest.get("restoreOwnedResume")),
        "noPeriodicResume": bool(manifest.get("noPeriodicResume")),
    }
    text = json.dumps(proof, sort_keys=True)
    with open(path, "w") as fh:
        fh.write(text)
    _atomic_write(_survivor_proof_path(), text, ".survivor-")


def read_survivor_proof() -> Dict[str, Any]:
    for path in (_survivor_proof_path(), _pause_lock_path()):
        try:
            with open(path) as fh:
                data = json.load(fh)
        except (FileNotFoundError, ValueError):
            continue
        if isinstance(data, dict):
            return data
    return {}


def _wait_for_pause_lock_removal(
    path: str, timeout: float = 1800.0, interval: float = 1.0
) -> None:
    """Block until the pause-lock is removed (the unpark signal).

    Raises TimeoutError after ``timeout`` seconds so a survivor cannot park
    forever (e.g. if the target never returns).
    """
    waited = 0.0
    while os.path.exists(path):
        if waited >= timeout:
            raise TimeoutError(f"pause-lock {path} not removed within {timeout}s")
        time.sleep(interval)
        waited += interval


def _destroy_pg_safely() -> None:
    """destroy_process_group(), falling back to abort() on hang/error."""
    import torch.distributed as dist

    try:
        dist.destroy_process_group()
        return
    except Exception as exc:  # pragma: no cover - defensive
        from fluidcr import warn

        warn(f"destroy_process_group failed ({exc}); attempting abort().")
    try:
        pg = dist.distributed_c10d._get_default_group()
        pg._abort()
    except Exception:  # pragma: no cover - defensive
        pass


def _reinit_process_group() -> None:
    """Re-create the default PG on this generation's rendezvous namespace.

    Explicit rather than relying on the patched ``init_process_group``: the scoping
    is what keeps the rebuild off the destroyed communicator's store keys (see
    :func:`generation_scoped_store`), so it must not depend on patches being
    installed. Double-scoping is harmless -- the patch is a no-op once ``store`` is
    present.
    """
    import torch.distributed as dist

    backend = "nccl" if dist.is_nccl_available() else "gloo"
    kwargs = scope_init_process_group_kwargs((), {})
    dist.init_process_group(backend=backend, **kwargs)


def survivor_pause_and_rebuild() -> None:
    """Tear down NCCL, park until unparked, then rebuild PG + DDP in place.

    Model and optimizer state remain resident in VRAM across the whole call.
    """
    from fluidcr import log, clear_migration_flags

    _destroy_pg_safely()
    lock = _pause_lock_path()
    _write_pause_lock(lock)
    log(f"Survivor parked (VRAM held). Waiting for unpark at {lock} ...")

    _wait_for_pause_lock_removal(lock)

    log("Unparked. Rebuilding process group and DDP wrappers ...")
    _reinit_process_group()
    rebuild_tracked_ddp()
    clear_migration_flags()
    proof = read_survivor_proof()
    owned = proof.get("restoreOwnedResume")
    if (proof.get("pid") != os.getpid() or proof.get("podUID") != _pod_uid()
            or (owned and not proof.get("releaseAuthorizedAt"))):
        raise RuntimeError("survivor rebuild lacks matching authorized release receipt")
    from fluidcr.ctrl import _read_status, _default_checkpoint_path
    loaded = _read_status(_default_checkpoint_path()).get("checkpointID", "")
    proof.update(state="SurvivorResumed" if owned else "SurvivorRejoined", phase="Running", resumedAt=_utc_now(),
                 loadedCheckpointID=loaded)
    _atomic_write(_survivor_proof_path(), json.dumps(proof, sort_keys=True), ".survivor-")
    log("Survivor rejoined. Resuming training.")
