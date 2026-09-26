# Derived from FluidCR (Apache-2.0); upstream PyTorch backend content retained below.
# Local changes add throttled runtime telemetry and immutable restore-path loading
# for Stateful-Migration-System.
"""
fluidcr.backends.pytorch -- PyTorch backend.

Handles monkey-patching of ``torch.nn.Module`` and ``torch.optim.Optimizer``,
checkpoint save/load, RNG state management, and GPU resource cleanup.
"""

import functools
import os
import sys
import threading
import time
import types
import weakref
from typing import Any, Dict, List, Optional, Set

from fluidcr._config import CHECKPOINT_PATH, log, warn
from fluidcr.backends import AbstractBackend, register

# ---------------------------------------------------------------------------
# Tracking state
# ---------------------------------------------------------------------------

_tracked_models: List[weakref.ref] = []
_tracked_optimizers: List[weakref.ref] = []

_checkpoint_loaded: bool = False
_patched: bool = False
_lock: threading.Lock = threading.Lock()

# Number of batches to skip on the next enumerate(DataLoader) call.
# Set by _try_load_checkpoint() and consumed by the patched enumerate().
_dataloader_skip: int = 0

# Epoch-mode: when set, checkpoint saves (epoch, step_in_epoch) and _dataloader_skip
# uses step_in_epoch (skip within current epoch only) instead of full global_step.
_steps_per_epoch: Optional[int] = None
_saved_epoch: int = 0

# range() patch: only patch the first matching range(stop) call (see option 3).
_epoch_range_patch_consumed: bool = False

# For Hugging Face Trainer: optimizer steps -> batches (step_count * gradient_accumulation_steps).
_gradient_accumulation_steps: int = 1

# ---------------------------------------------------------------------------
# Utility helpers
# ---------------------------------------------------------------------------


def _live_refs(ref_list: List[weakref.ref]) -> List[Any]:
    """Dereference weak-refs and return only live objects."""
    alive = []
    for r in ref_list:
        obj = r()
        if obj is not None:
            alive.append(obj)
    return alive


def _unwrap_model(model: Any) -> Any:
    """Unwrap ``DataParallel`` / ``DistributedDataParallel``."""
    while hasattr(model, "module"):
        model = model.module
    return model


def _has_parameters(module: Any) -> bool:
    """Return ``True`` if *module* owns at least one parameter."""
    try:
        next(module.parameters())
        return True
    except StopIteration:
        return False


def _root_models() -> List[Any]:
    """Return tracked models that are root-level and carry learnable parameters.

    Filters out sub-modules and stateless singletons (``nn.ReLU``, etc.).
    """
    all_models = _live_refs(_tracked_models)
    if not all_models:
        return []

    child_ids: Set[int] = set()
    for m in all_models:
        for child in m.modules():
            if child is not m:
                child_ids.add(id(child))

    return [
        m for m in all_models
        if id(m) not in child_ids and _has_parameters(m)
    ]


# ---------------------------------------------------------------------------
# Epoch-mode API
# ---------------------------------------------------------------------------


def set_gradient_accumulation_steps(n: int) -> None:
    """Set batches per optimizer step for Hugging Face Trainer.

    FluidCR counts optimizer steps; the Trainer consumes
    gradient_accumulation_steps batches per step. Call before trainer.train()::

        fluidcr.set_gradient_accumulation_steps(training_args.gradient_accumulation_steps)
        trainer.train()
    """
    global _gradient_accumulation_steps
    _gradient_accumulation_steps = max(1, int(n))


def set_steps_per_epoch(n: int) -> None:
    """Enable epoch-mode checkpointing.

    When set, the checkpoint stores (epoch, step_in_epoch) instead of
    only global_step. On resume, ``enumerate(DataLoader)`` skips only
    batches within the current epoch, and ``start_epoch()`` returns
    the epoch to resume from.

    Call before the epoch loop, e.g. after creating the dataloader::

        fluidcr.set_steps_per_epoch(len(dataloader))
        for epoch in range(fluidcr.start_epoch(), num_epochs):
            for step, batch in enumerate(dataloader):
                ...
    """
    global _steps_per_epoch
    _steps_per_epoch = n


def start_epoch() -> int:
    """Return the epoch index to start (or resume) from.

    On a fresh run returns 0. After checkpoint restore returns the
    saved epoch so the epoch loop can resume transparently.
    """
    return _saved_epoch


def _maybe_patch_range(stop: int) -> Optional[object]:
    """If epoch-resume mode and range(stop) matches, return range(saved_epoch, stop).

    Used by the transparent range() patch. Returns None if no patch applied.
    """
    global _epoch_range_patch_consumed
    if _epoch_range_patch_consumed:
        return None
    if _saved_epoch <= 0:
        return None
    if _steps_per_epoch is None:
        return None
    if stop <= _saved_epoch:
        return None
    _epoch_range_patch_consumed = True
    log("Patching epoch range: range(%d) -> range(%d, %d)" % (stop, _saved_epoch, stop))
    return range(_saved_epoch, stop)


# ---------------------------------------------------------------------------
# RNG state helpers
# ---------------------------------------------------------------------------


def _collect_rng_states() -> Dict[str, Any]:
    """Snapshot all RNG states for deterministic resumption."""
    import torch

    rng: Dict[str, Any] = {}
    rng["torch_cpu"] = torch.random.get_rng_state()
    if torch.cuda.is_available():
        rng["torch_cuda"] = torch.cuda.get_rng_state_all()
    if "random" in sys.modules:
        import random
        rng["python"] = random.getstate()
    if "numpy" in sys.modules:
        import numpy
        rng["numpy"] = numpy.random.get_state()
    return rng


def _restore_rng_states(rng: Dict[str, Any]) -> None:
    """Restore RNG states captured by :func:`_collect_rng_states`."""
    import torch

    if "torch_cpu" in rng:
        torch.random.set_rng_state(rng["torch_cpu"])
    if "torch_cuda" in rng and torch.cuda.is_available():
        torch.cuda.set_rng_state_all(rng["torch_cuda"])
    if "python" in rng:
        import random
        random.setstate(rng["python"])
    if "numpy" in rng:
        import numpy
        numpy.random.set_state(rng["numpy"])


# ---------------------------------------------------------------------------
# Checkpoint save
# ---------------------------------------------------------------------------


def _save_checkpoint(path: str) -> None:
    """Serialize all tracked state to *path*."""
    import torch

    models = _root_models()
    optimizers = _live_refs(_tracked_optimizers)

    step_counts = [getattr(opt, "_fluidcr_step", 0) for opt in optimizers]
    global_step = step_counts[0] if step_counts else 0

    payload: Dict[str, Any] = {
        "models": [],
        "optimizers": [],
        "step_counts": step_counts,
        "rng_states": _collect_rng_states(),
    }

    # Epoch-mode: save epoch and step_in_epoch for transparent resume.
    #
    # IMPORTANT:
    # - For "classic" training loops (optimizer.step() once per batch),
    #   global_step == batch_step so this matches the intuitive definition.
    # - For Hugging Face Trainer, global_step counts *optimizer* steps, while the
    #   dataloader is consumed in *batches*. Convert optimizer steps -> batches
    #   using gradient_accumulation_steps so resume skips the correct number of
    #   batches within the epoch.
    if _steps_per_epoch and _steps_per_epoch > 0:
        batch_step = int(global_step) * int(_gradient_accumulation_steps)
        epoch = batch_step // _steps_per_epoch
        step_in_epoch = batch_step % _steps_per_epoch
        payload["epoch"] = epoch
        payload["step_in_epoch"] = step_in_epoch
        payload["steps_per_epoch"] = _steps_per_epoch

    for idx, mdl in enumerate(models):
        inner = _unwrap_model(mdl)
        payload["models"].append({
            "idx": idx,
            "cls": type(inner).__qualname__,
            "state_dict": inner.state_dict(),
        })

    for idx, opt in enumerate(optimizers):
        payload["optimizers"].append({
            "idx": idx,
            "cls": type(opt).__qualname__,
            "state_dict": opt.state_dict(),
        })

    ckpt_dir = os.path.dirname(path)
    if ckpt_dir:
        os.makedirs(ckpt_dir, exist_ok=True)

    tmp_path = path + ".tmp"
    torch.save(payload, tmp_path)
    os.replace(tmp_path, path)


# ---------------------------------------------------------------------------
# Checkpoint load (auto-resume)
# ---------------------------------------------------------------------------


def _checkpoint_load_path() -> str:
    explicit_restore = os.environ.get("FLUIDCR_RESTORE_CHECKPOINT_PATH", "").strip()
    if explicit_restore:
        if not os.path.isfile(explicit_restore):
            raise FileNotFoundError(
                f"pinned restore checkpoint is missing: {explicit_restore}"
            )
        return explicit_restore
    try:
        from fluidcr.ctrl import restore_checkpoint_path
    except ImportError:
        return CHECKPOINT_PATH
    return restore_checkpoint_path(CHECKPOINT_PATH)


def _move_optimizer_state_to_device(optimizer: Any) -> None:
    """Move optimizer internal state tensors to match their parameter devices."""
    import torch

    for group in optimizer.param_groups:
        for p in group["params"]:
            if p not in optimizer.state:
                continue
            state = optimizer.state[p]
            for key, val in state.items():
                if isinstance(val, torch.Tensor) and val.device != p.device:
                    state[key] = val.to(p.device)


def peek_checkpoint_epoch_resume() -> Optional[tuple]:
    """Peek at checkpoint for epoch/step_in_epoch (no model/optimizer restore).

    Returns (epoch, steps_trained_in_current_epoch) if epoch-mode data exists,
    else None. Used by HF Trainer before optimizer exists.
    """
    load_path = _checkpoint_load_path()
    if not os.path.isfile(load_path):
        return None
    try:
        import torch

        ckpt = torch.load(
            load_path, map_location="cpu", weights_only=False
        )
    except (TypeError, Exception):
        try:
            import torch

            ckpt = torch.load(load_path, map_location="cpu")
        except Exception:
            return None
    epoch = ckpt.get("epoch")
    step_in_epoch = ckpt.get("step_in_epoch")
    steps_per_epoch = ckpt.get("steps_per_epoch")
    if (
        epoch is not None
        and step_in_epoch is not None
        and steps_per_epoch is not None
    ):
        return (int(epoch), int(step_in_epoch))
    return None


def _try_load_checkpoint() -> None:
    """Load checkpoint if it exists (called once on first optimizer creation)."""
    global _checkpoint_loaded

    with _lock:
        if _checkpoint_loaded:
            return
        _checkpoint_loaded = True

    load_path = _checkpoint_load_path()
    if not os.path.isfile(load_path):
        return

    import torch

    # Read checkpoint -------------------------------------------------------
    try:
        ckpt = torch.load(
            load_path, map_location="cpu", weights_only=False
        )
    except TypeError:
        ckpt = torch.load(load_path, map_location="cpu")
    except Exception as exc:
        warn(f"failed to read checkpoint: {exc}")
        return

    # Restore models --------------------------------------------------------
    models = _root_models()
    for entry in ckpt.get("models", []):
        idx = entry["idx"]
        if idx < len(models):
            inner = _unwrap_model(models[idx])
            try:
                inner.load_state_dict(entry["state_dict"])
                log(f"Restored model[{idx}] ({entry['cls']})")
            except Exception as exc:
                warn(f"model[{idx}] restore failed: {exc}")
        else:
            warn(f"checkpoint has model[{idx}] but only {len(models)} root model(s) tracked")

    # Restore optimizers ----------------------------------------------------
    optimizers = _live_refs(_tracked_optimizers)
    for entry in ckpt.get("optimizers", []):
        idx = entry["idx"]
        if idx < len(optimizers):
            try:
                optimizers[idx].load_state_dict(entry["state_dict"])
                _move_optimizer_state_to_device(optimizers[idx])
                log(f"Restored optimizer[{idx}] ({entry['cls']})")
            except Exception as exc:
                warn(f"optimizer[{idx}] restore failed: {exc}")
        else:
            warn(f"checkpoint has optimizer[{idx}] but only {len(optimizers)} optimizer(s) tracked")

    # Restore step counts ---------------------------------------------------
    global _dataloader_skip, _steps_per_epoch, _saved_epoch
    step_counts = ckpt.get("step_counts", [])
    for idx, count in enumerate(step_counts):
        if idx < len(optimizers):
            optimizers[idx]._fluidcr_step = count  # type: ignore[attr-defined]
            log(f"Optimizer[{idx}] resuming from global step {count}")

    # Epoch-mode: use step_in_epoch for skip (skip within current epoch only)
    epoch = ckpt.get("epoch")
    step_in_epoch = ckpt.get("step_in_epoch")
    steps_per_epoch = ckpt.get("steps_per_epoch")
    if (
        epoch is not None
        and step_in_epoch is not None
        and steps_per_epoch is not None
    ):
        _saved_epoch = int(epoch)
        _steps_per_epoch = int(steps_per_epoch)
        _dataloader_skip = int(step_in_epoch)
        log(f"Resuming from epoch {_saved_epoch} (step {step_in_epoch}/{steps_per_epoch})")
    elif step_counts:
        # step_counts are optimizer steps; multiply by gradient_accumulation_steps
        # to get batch count (for Hugging Face Trainer).
        _dataloader_skip = step_counts[0] * _gradient_accumulation_steps

    # Restore RNG states ----------------------------------------------------
    rng = ckpt.get("rng_states")
    if rng:
        try:
            _restore_rng_states(rng)
            log("RNG states restored (torch/CUDA/Python/NumPy)")
        except Exception as exc:
            warn(f"RNG state restore failed: {exc}")

    log(f"Checkpoint resumed from {load_path}")


# ---------------------------------------------------------------------------
# GPU resource release
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# Coordinated (distributed) checkpoint -- the optimizer-step rendezvous
# ---------------------------------------------------------------------------


def _device_for_optimizer(opt: Any) -> Any:
    """Device of the optimizer's first parameter (where the flag tensor lives)."""
    import torch

    for group in opt.param_groups:
        for p in group["params"]:
            if isinstance(p, torch.Tensor):
                return p.device
    return torch.device("cuda" if torch.cuda.is_available() else "cpu")


def _coordinate_checkpoint_request(device: Any, requested: bool) -> bool:
    """All-reduce (MAX) the per-rank "checkpoint requested" flag.

    Returns ``True`` on every rank iff *any* rank passed ``requested=True``.
    This is the one collective that makes coordinated checkpointing safe: it is
    issued at the optimizer-step boundary, so all ranks call it in the same
    order with the same shape -- it never races the in-flight DDP gradient
    all-reduce the way a signal-handler ``dist.barrier()`` did.

    Factored out (no CUDA-exit side effects) so it is unit-testable over gloo.
    """
    import torch
    import torch.distributed as dist

    flag = torch.tensor([1 if requested else 0], device=device, dtype=torch.int32)
    dist.all_reduce(flag, op=dist.ReduceOp.MAX)
    return bool(flag.item())


def _maybe_coordinated_checkpoint(opt: Any) -> None:
    """At each optimizer-step boundary, coordinate a manifest-driven action.

    Runs every step in distributed mode (the collective must be issued by every
    rank in lockstep). One tiny ``all_reduce(MAX)`` propagates the "coordinate
    now" trigger; when it fires, every rank reads the shared manifest and diverges
    by role:
      * target rank  -> save, free GPU, exit(99)   (full checkpoint if all ranks
                                                     are targets)
      * survivor     -> pause, keep VRAM, rebuild NCCL + DDP
    The manifest (not the signal) decides roles, so any number of targets is
    handled without a signalling race.
    """
    import fluidcr

    if not fluidcr.distributed_checkpoint_enabled():
        return

    import torch.distributed as dist

    if not dist.is_available() or not dist.is_initialized():
        return

    device = _device_for_optimizer(opt)
    try:
        pending = _coordinate_checkpoint_request(device, fluidcr.checkpoint_requested())
    except Exception as exc:
        warn(f"checkpoint coordination all-reduce failed: {exc}")
        return

    if not pending:
        return

    from fluidcr.distributed import (
        is_full,
        rank_is_target,
        read_migration_targets,
        survivor_pause_and_rebuild,
    )

    targets = read_migration_targets()
    if isinstance(targets, list) and not targets:
        warn("Migration triggered but manifest target list is empty -- no-op.")
        # This rank rendezvoused and will keep training, not exit -- so if it was
        # the signalled rank, disarm the watchdog before it force-exits us.
        fluidcr.cancel_checkpoint_watchdog()
        fluidcr.clear_migration_flags()
        return

    rank = dist.get_rank()
    world_size = dist.get_world_size()

    if rank_is_target(rank, targets):
        if is_full(targets, world_size):
            log("Coordinated full checkpoint: this rank saves and exits.")
        else:
            log(f"Partial migration: rank {rank} is a target -- save, free GPU, exit.")
        fluidcr.perform_checkpoint_and_exit(_active_backend_for_exit())
        return

    # Survivor: this rank parks for an unbounded time keeping its VRAM. If it was
    # the rank that received SIGUSR1, its watchdog would otherwise force-exit it
    # after the timeout -- disarm it now that we have safely rendezvoused.
    fluidcr.cancel_checkpoint_watchdog()
    log(f"Partial migration: rank {rank} is a survivor -- pause and rebuild NCCL.")
    survivor_pause_and_rebuild()


def _active_backend_for_exit() -> Any:
    """The registered backend instance used to save before exit."""
    from fluidcr.backends import get_active

    return get_active()


def _release_gpu() -> None:
    """Best-effort CUDA teardown before exit."""
    import torch

    if not torch.cuda.is_available():
        return

    try:
        torch.cuda.synchronize()
    except Exception:
        pass

    for ref in _tracked_models:
        m = ref()
        if m is not None:
            try:
                m.cpu()
            except Exception:
                pass

    try:
        torch.cuda.empty_cache()
    except Exception:
        pass

    try:
        torch.cuda.synchronize()
    except Exception:
        pass

    log("GPU resources released.")


# ---------------------------------------------------------------------------
# Monkey-patching
# ---------------------------------------------------------------------------


def _apply_patches(_module: Any) -> None:
    """Wrap ``nn.Module.__init__`` and ``optim.Optimizer.__init__``."""
    global _patched

    with _lock:
        if _patched:
            return
        _patched = True

    import torch.nn as nn
    import torch.optim as optim

    # Some virtual-GPU shims (e.g. HAMi's libvgpu.so loaded via LD_PRELOAD)
    # install their own SIGUSR1 handler when CUDA initialises, which silently
    # overrides the one FluidCR registered before ``import torch``.  Re-install
    # the FluidCR handler after torch has been imported so it always wins.
    try:
        import signal as _signal
        from fluidcr import _sigusr1_handler as _fluidcr_handler  # noqa: WPS433

        if hasattr(_signal, "SIGUSR1"):
            _signal.signal(_signal.SIGUSR1, _fluidcr_handler)
    except Exception as exc:  # pragma: no cover - defensive
        log(f"Could not re-install signal handlers: {exc}", level="DEBUG")

    # -- torch.distributed.init_process_group (generation-scoped rendezvous) --
    # A migration target comes back as a brand-new process and calls
    # init_process_group itself, while survivors rebuild in place.  Both must land
    # on the same store key namespace for the round, and it must not be the
    # namespace of the communicator that was just destroyed -- see
    # fluidcr.distributed.generation_scoped_store for why reusing it makes NCCL
    # bootstrap against a dead socket.
    import torch.distributed as _dist

    _original_init_pg = _dist.init_process_group

    @functools.wraps(_original_init_pg)
    def _patched_init_process_group(*args: Any, **kwargs: Any) -> Any:
        from fluidcr.distributed import scope_init_process_group_kwargs

        try:
            kwargs = scope_init_process_group_kwargs(args, kwargs)
        except Exception as exc:  # pragma: no cover - defensive
            warn(f"could not scope rendezvous to the migration generation: {exc}")
        return _original_init_pg(*args, **kwargs)

    _dist.init_process_group = _patched_init_process_group  # type: ignore[assignment]

    # -- nn.Module.__init__ -------------------------------------------------
    _original_module_init = nn.Module.__init__

    @functools.wraps(_original_module_init)
    def _patched_module_init(self: nn.Module, *args: Any, **kwargs: Any) -> None:
        _original_module_init(self, *args, **kwargs)
        _tracked_models.append(weakref.ref(self))

    nn.Module.__init__ = _patched_module_init  # type: ignore[method-assign]

    # -- DistributedDataParallel.__init__ (record for survivor rebuild) ------
    import torch.nn.parallel as _parallel

    _DDP = _parallel.DistributedDataParallel
    _original_ddp_init = _DDP.__init__

    @functools.wraps(_original_ddp_init)
    def _patched_ddp_init(self: Any, module: Any, *args: Any, **kwargs: Any) -> None:
        _original_ddp_init(self, module, *args, **kwargs)
        from fluidcr.distributed import record_ddp

        record_ddp(self, args, kwargs)

    _DDP.__init__ = _patched_ddp_init  # type: ignore[method-assign]

    # -- optim.Optimizer.__init__ -------------------------------------------
    _original_optimizer_init = optim.Optimizer.__init__

    @functools.wraps(_original_optimizer_init)
    def _patched_optimizer_init(
        self: optim.Optimizer, *args: Any, **kwargs: Any
    ) -> None:
        _original_optimizer_init(self, *args, **kwargs)
        _tracked_optimizers.append(weakref.ref(self))

        # Per-instance step() wrapper for step counting
        self._fluidcr_step = 0  # type: ignore[attr-defined]
        _orig_step = self.step

        # Must remain a bound method: PyTorch LRScheduler (LambdaLR, etc.) uses
        # optimizer.step.__func__ when wrapping step; assigning a plain function
        # breaks with AttributeError on newer torch.
        def _counted_step(opt: optim.Optimizer, *a: Any, **kw: Any) -> Any:
            started = time.monotonic()
            result = _orig_step(*a, **kw)
            opt._fluidcr_step += 1  # type: ignore[attr-defined]
            try:
                from fluidcr.ctrl import record_runtime_status

                record_runtime_status(
                    global_step=getattr(opt, "_fluidcr_step", 0),
                    state="Running",
                    iteration_time_seconds=time.monotonic() - started,
                )
            except Exception:
                pass
            # Coordinated-checkpoint rendezvous: a lockstep point across all
            # ranks where the backward all-reduce is done and no training
            # collective is in flight (no-op unless FLUIDCR_DISTRIBUTED=1).
            _maybe_coordinated_checkpoint(opt)
            return result

        self.step = types.MethodType(_counted_step, self)  # type: ignore[assignment]

        # Auto-resume on first optimizer creation
        _try_load_checkpoint()

    optim.Optimizer.__init__ = _patched_optimizer_init  # type: ignore[method-assign]

    log("Patches applied -- tracking torch.nn.Module & torch.optim.Optimizer")


# ---------------------------------------------------------------------------
# Backend class
# ---------------------------------------------------------------------------


class PyTorchBackend(AbstractBackend):
    """FluidCR backend for PyTorch."""

    @property
    def name(self) -> str:
        return "pytorch"

    @property
    def target_module(self) -> str:
        return "torch"

    def apply_patches(self, module: Any) -> None:
        _apply_patches(module)

    def save_checkpoint(self, path: str) -> None:
        _save_checkpoint(path)

    def load_checkpoint(self, path: str) -> None:
        _try_load_checkpoint()

    def release_gpu(self) -> None:
        _release_gpu()

    def global_step(self, optimizer: Any = None) -> int:
        if optimizer is not None:
            return getattr(optimizer, "_fluidcr_step", 0)
        live = _live_refs(_tracked_optimizers)
        return getattr(live[0], "_fluidcr_step", 0) if live else 0

    def set_steps_per_epoch(self, n: int) -> None:
        set_steps_per_epoch(n)

    def start_epoch(self) -> int:
        return start_epoch()

    def is_active(self) -> bool:
        return "torch" in sys.modules


# ---------------------------------------------------------------------------
# Auto-register
# ---------------------------------------------------------------------------

register(PyTorchBackend())
