# Derived from FluidCR (Apache-2.0); upstream package content retained below.
# Local changes add runtime telemetry hooks and immutable checkpoint restore
# binding for Stateful-Migration-System.
"""
FluidCR -- Heterogeneous GPU Migration Framework.

Importing this package:
    1. Patches ``builtins.enumerate`` to transparently skip DataLoader
       batches after a checkpoint restore (zero code changes required).
    2. Patches ``builtins.iter`` so frameworks using ``iter(dataloader)``
       (e.g. Hugging Face Trainer, SFTTrainer) also skip batches on resume.
    3. Patches Hugging Face Trainer when transformers is imported (progress
       bar resume, gradient_accumulation_steps). Disable with
       FLUIDCR_DISABLE_HF_INTEGRATION=1.
    4. Optionally patches ``builtins.range`` so epoch loops resume transparently
       (see FLUIDCR_DISABLE_EPOCH_RANGE_PATCH).
    5. Registers the SIGUSR1 signal handler.
    6. Installs PEP-451 import hooks for every registered backend
       (PyTorch today, TensorFlow in the future).
    7. Exposes the public API: :func:`global_step`, :func:`resumable`.

Typical training scripts need **no modifications** -- FluidCR is injected via
``sitecustomize.py`` / ``fluidcr-launcher`` and everything works transparently::

    # Unchanged user code:
    for epoch in range(num_epochs):
        for step, batch in enumerate(dataloader):
            ...   # epoch and step resume from checkpoint automatically

    # Hugging Face Trainer/SFTTrainer: progress bar and batch skip auto-resume
    trainer = SFTTrainer(model=..., args=...)
    trainer.train()
"""

import builtins as _builtins
import inspect
import itertools
import os
import signal
import sys
import threading
from typing import Any, Iterable, Iterator, Optional, Tuple

from fluidcr._config import CHECKPOINT_PATH, EXIT_CODE, log, warn
from fluidcr._hook import install_hook
from fluidcr.backends import get_active, get_backends

# Re-import all built-in backends so they auto-register.
# PyTorch backend is optional; FluidCR works without it (e.g. before user installs torch).
try:
    import fluidcr.backends.pytorch  # noqa: F401
except Exception as exc:
    log("PyTorch backend not loaded: %s" % exc, level="DEBUG")
# Transparent HF Trainer integration (patches Trainer when transformers is imported).
try:
    import fluidcr.hf_integration  # noqa: F401
except Exception as exc:
    log("HF integration not loaded: %s" % exc, level="DEBUG")

# ---------------------------------------------------------------------------
# Transparent enumerate() patch
# ---------------------------------------------------------------------------

_original_enumerate = _builtins.enumerate


def _make_enumerate_proxy():
    """Create a class-like proxy for ``enumerate`` that stays compatible with
    torch._dynamo while still injecting FluidCR's DataLoader skip behavior.
    """
    original = _original_enumerate

    class _EnumerateMeta(type):
        def __call__(self, iterable, start=0):  # type: ignore[override]
            try:
                from fluidcr.backends import pytorch as _pt  # type: ignore[attr-defined]
            except ImportError:
                return original(iterable, start)
            skip = _pt._dataloader_skip
            if skip >= 0:
                # Detect DataLoader by type without importing torch (avoids circular
                # import when enumerate is used during stdlib/torch bootstrap).
                cls = type(iterable)
                is_dataloader = (
                    cls.__module__ == "torch.utils.data.dataloader"
                    and cls.__name__ == "DataLoader"
                )
                if is_dataloader:
                    # Auto-infer steps_per_epoch for epoch-mode (option 3)
                    if _pt._steps_per_epoch is None and hasattr(iterable, "__len__"):
                        try:
                            _pt._steps_per_epoch = len(iterable)
                        except (TypeError, NotImplementedError):
                            pass
                    if skip > 0:
                        _pt._dataloader_skip = 0
                        log("Resuming dataloader -- skipping %d batches." % skip)
                        return original(
                            itertools.islice(iterable, skip, None),
                            start=start + skip,
                        )

            return original(iterable, start)

    # Create a dummy class object whose metaclass controls the call behavior.
    EnumerateProxy = _EnumerateMeta("enumerate", (), {})
    return EnumerateProxy


def _should_patch_enumerate() -> bool:
    """Return True if we should patch ``builtins.enumerate``.

    You can force-disable the patch by setting
    ``FLUIDCR_DISABLE_ENUMERATE_PATCH=1`` in the environment.
    """
    disable_env = os.environ.get("FLUIDCR_DISABLE_ENUMERATE_PATCH", "").strip().lower()
    if disable_env in {"1", "true", "yes"}:
        log(
            "FLUIDCR_DISABLE_ENUMERATE_PATCH set; leaving enumerate() unmodified. "
            "Use fluidcr.resumable(dataloader) for checkpoint-aware iteration.",
            level="INFO",
        )
        return False

    return True


# ---------------------------------------------------------------------------
# Transparent iter() patch (Hugging Face Trainer / SFTTrainer compatibility)
# ---------------------------------------------------------------------------
# The Trainer uses iter(train_dataloader) and next(), not enumerate().
# Without this patch, batches would restart from 0 on resume.

_original_iter = _builtins.iter


def _iter_proxy(obj, *args, **kwargs):
    """Proxy for iter() that skips DataLoader batches after checkpoint restore."""
    if args or kwargs:
        return _original_iter(obj, *args, **kwargs)
    # Detect DataLoader by type without importing torch (avoids circular import
    # when iter is used during stdlib/torch bootstrap, e.g. inspect.getsourcelines).
    cls = type(obj)
    is_dataloader = (
        (cls.__module__ == "torch.utils.data.dataloader" and cls.__name__ == "DataLoader")
        or ("accelerate" in cls.__module__ and "DataLoader" in cls.__name__)
    )
    if is_dataloader:
        try:
            from fluidcr.backends import pytorch as _pt
            # Capture steps_per_epoch on first DataLoader for epoch-mode checkpoint (Trainer)
            if _pt._steps_per_epoch is None and hasattr(obj, "__len__"):
                try:
                    _pt._steps_per_epoch = len(obj)
                except (TypeError, NotImplementedError):
                    pass
            if _pt._dataloader_skip > 0:
                skip = _pt._dataloader_skip
                _pt._dataloader_skip = 0
                log("Resuming dataloader (iter) -- skipping %d batches." % skip)
                return itertools.islice(obj, skip, None)
        except ImportError:
            pass
    return _original_iter(obj)


def _should_patch_iter() -> bool:
    """Return True if we should patch ``builtins.iter`` for DataLoader skip."""
    disable_env = os.environ.get("FLUIDCR_DISABLE_ITER_PATCH", "").strip().lower()
    if disable_env in {"1", "true", "yes"}:
        return False
    return True


def _should_patch_range() -> bool:
    """Return True if we should patch ``builtins.range`` for transparent epoch resume.

    Disable with ``FLUIDCR_DISABLE_EPOCH_RANGE_PATCH=1``.
    Uses AST to find training loops; only patches range() at those sites.
    Set FLUIDCR_EPOCH_RANGE_USE_AST=0 for legacy first-matching-call behavior.
    """
    disable_env = os.environ.get("FLUIDCR_DISABLE_EPOCH_RANGE_PATCH", "").strip().lower()
    if disable_env in {"1", "true", "yes"}:
        log(
            "FLUIDCR_DISABLE_EPOCH_RANGE_PATCH set; leaving range() unmodified.",
            level="INFO",
        )
        return False
    return True


_original_range = _builtins.range

# Fallback to "first matching call" when AST cannot determine (e.g. exec, -c).
_USE_AST_FOR_RANGE_PATCH = os.environ.get("FLUIDCR_EPOCH_RANGE_USE_AST", "1").strip().lower() in ("1", "true", "yes")


def _range_proxy(*args, **kwargs):
    """Proxy for range() that transparently resumes epoch loops.

    Uses AST to find training loops (for X in range(...): ... for Y, Z in enumerate(...)).
    Only patches range() calls at detected training-loop sites.
    Set FLUIDCR_EPOCH_RANGE_USE_AST=0 for legacy first-matching-call behavior (e.g. exec/-c).
    """
    if len(args) != 1 or len(kwargs) != 0:
        return _original_range(*args, **kwargs)
    stop = args[0]
    try:
        stop_int = int(stop)
    except (TypeError, ValueError):
        return _original_range(*args, **kwargs)

    try:
        from fluidcr.backends import pytorch as _pt
    except ImportError:
        return _original_range(*args, **kwargs)

    use_ast = _USE_AST_FOR_RANGE_PATCH
    should_patch = False

    if use_ast:
        try:
            frame = inspect.currentframe()
            if frame is not None and frame.f_back is not None:
                caller = frame.f_back
                filename = caller.f_code.co_filename
                lineno = caller.f_lineno
                from fluidcr._ast_scan import is_training_loop_site

                should_patch = is_training_loop_site(filename, lineno)
        except Exception:
            pass
    else:
        # Legacy: consider all range(stop) for patch (first matching wins in _maybe_patch_range)
        should_patch = True

    if not should_patch:
        return _original_range(*args, **kwargs)

    patched = _pt._maybe_patch_range(stop_int)
    if patched is not None:
        return patched
    return _original_range(*args, **kwargs)


if _should_patch_enumerate():
    _builtins.enumerate = _make_enumerate_proxy()  # type: ignore[assignment]

if _should_patch_iter():
    _builtins.iter = _iter_proxy  # type: ignore[assignment]

if _should_patch_range():
    _builtins.range = _range_proxy  # type: ignore[assignment]

# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def set_gradient_accumulation_steps(n: int) -> None:
    """Set batches per optimizer step for Hugging Face Trainer.

    FluidCR counts optimizer steps; the Trainer consumes
    gradient_accumulation_steps batches per step. Call before trainer.train()::

        fluidcr.set_gradient_accumulation_steps(training_args.gradient_accumulation_steps)
        trainer.train()
    """
    try:
        from fluidcr.backends import pytorch as _pt
        _pt.set_gradient_accumulation_steps(n)
    except ImportError:
        pass


def set_steps_per_epoch(n: int) -> None:
    """Enable epoch-mode checkpointing for transparent epoch-loop resume.

    When set, checkpoints store (epoch, step_in_epoch). On resume,
    ``start_epoch()`` returns the saved epoch and ``enumerate(DataLoader)``
    skips only batches within the current epoch.

    Call before the epoch loop::

        fluidcr.set_steps_per_epoch(len(dataloader))
        for epoch in range(fluidcr.start_epoch(), num_epochs):
            for step, batch in enumerate(dataloader):
                ...
    """
    try:
        from fluidcr.backends import pytorch as _pt
        _pt.set_steps_per_epoch(n)
    except ImportError:
        pass


def find_training_loop_sites(source_or_path):
    """Find range() call sites that are training loops (epoch + enumerate pattern).

    Parses Python source and returns a list of :class:`TrainingLoopSite` with
    lineno, col_offset, loop_var for each detected ``for X in range(...): ...
    for Y, Z in enumerate(...)`` pattern.

    Useful for debugging or tooling. Import the full API::

        from fluidcr._ast_scan import find_training_loop_sites, TrainingLoopSite
    """
    from fluidcr._ast_scan import find_training_loop_sites as _find

    return _find(source_or_path)


def start_epoch() -> int:
    """Return the epoch index to start (or resume) from.

    On a fresh run returns 0. After checkpoint restore returns the saved
    epoch so the epoch loop can resume transparently.
    """
    try:
        from fluidcr.backends import pytorch as _pt
        return _pt.start_epoch()
    except ImportError:
        return 0


def global_step(optimizer: Any = None) -> int:
    """Return the current global training step.

    Parameters
    ----------
    optimizer : optional
        If omitted, returns the step count of the **first** tracked
        optimizer (covers the single-optimizer common case).

    Returns
    -------
    int
        Number of ``optimizer.step()`` calls completed so far.
        Returns ``0`` on a fresh run (no checkpoint).
    """
    backend = get_active()
    if backend is None:
        return 0
    return backend.global_step(optimizer)


def resumable(
    dataloader: Iterable,
    optimizer: Any = None,
) -> Iterator[Tuple[int, Any]]:
    """Wrap a dataloader so iteration resumes from the last checkpoint.

    .. note::

        Since v0.2.0 FluidCR patches ``enumerate()`` transparently, so this
        helper is no longer required for the common case.  It is kept for
        backward compatibility and for advanced use cases (e.g. multiple
        dataloaders or custom skip logic).

    Drop-in replacement for ``enumerate(dataloader)``::

        # Before
        for step, batch in enumerate(dataloader):
            ...

        # After (checkpoint-aware)
        for step, batch in fluidcr.resumable(dataloader):
            ...

    On a fresh run this is identical to ``enumerate(dataloader)``.
    After a checkpoint restore it skips already-processed batches
    and yields ``(step, batch)`` tuples starting from the saved step.

    Parameters
    ----------
    dataloader : Iterable
        Any iterable (typically a ``torch.utils.data.DataLoader``).
    optimizer : optional
        Passed to :func:`global_step` to look up the step count.
        Omit for the common single-optimizer case.
    """
    start = global_step(optimizer)
    if start > 0:
        log("Resuming dataloader from step %d (skipping %d batches)." % (start, start))
    # islice is O(n) for the skipped items but avoids GPU work.
    it = itertools.islice(dataloader, start, None)
    yield from _original_enumerate(it, start=start)


# ---------------------------------------------------------------------------
# SIGUSR1 signal handler
# ---------------------------------------------------------------------------


def _release_gpu_best_effort(backend: Any) -> None:
    try:
        backend.release_gpu()
    except Exception:
        pass


# Hard upper bound on how long the SIGUSR1 handler is allowed to run before we
# forcibly terminate the process.  Anything stuck inside HAMi's libvgpu.so
# (CUDA copies during ``save_checkpoint`` or ``release_gpu``, DataLoader
# worker cleanup in ``sys.exit``, etc.) is bypassed by this watchdog so the
# Launcher always observes the checkpoint exit code.
_SIGUSR1_WATCHDOG_TIMEOUT: int = int(
    os.environ.get("FLUIDCR_CHECKPOINT_WATCHDOG_TIMEOUT", "60")
)


# The currently-armed watchdog's cancel switch. A survivor (partial migration)
# that safely rendezvouses at the step boundary and then parks indefinitely must
# cancel the watchdog via ``cancel_checkpoint_watchdog`` so the 60s force-exit does
# not kill it. A target/full-checkpoint rank leaves it armed -- it guards the
# save/release/exit from hanging inside HAMi.
_active_watchdog_cancel: Optional[threading.Event] = None


def _start_watchdog_thread() -> None:
    """Spawn a daemon thread that forcibly exits after the watchdog timeout.

    Why thread-based and not ``signal.alarm`` (SIGALRM):
      Python signal handlers only run between bytecode instructions in the
      MAIN thread.  If the main thread is stuck inside a C extension
      (e.g. a hung CUDA copy after a corrupted NCCL communicator), the
      kernel delivers SIGALRM but Python's handler never runs because no
      bytecode boundary is reached.  ``os._exit`` is therefore never called
      and the process hangs forever.

      A daemon thread waiting on an Event releases the GIL while it waits;
      CUDA C calls also release the GIL while waiting on streams, so the
      watchdog thread CAN reacquire the GIL when its timer expires and call
      ``os._exit`` directly -- bypassing Python's signal-handler machinery
      entirely.

    The wait is cancellable: a survivor cancels it once it has rendezvoused
    (see ``cancel_checkpoint_watchdog``), because a survivor parks for an
    unbounded time by design and must not be force-exited.
    """
    global _active_watchdog_cancel
    cancel = threading.Event()
    _active_watchdog_cancel = cancel

    def _watchdog() -> None:
        if cancel.wait(_SIGUSR1_WATCHDOG_TIMEOUT):
            return  # cancelled: this rank rendezvoused and is parking as a survivor
        warn(
            f"Checkpoint watchdog fired after {_SIGUSR1_WATCHDOG_TIMEOUT}s -- "
            "forcing exit."
        )
        os._exit(EXIT_CODE)

    threading.Thread(target=_watchdog, daemon=True).start()


def cancel_checkpoint_watchdog() -> None:
    """Cancel the armed SIGUSR1 watchdog, if any.

    Called by a rank that has safely rendezvoused at the coordinated step
    boundary and will NOT exit -- a partial-migration survivor (which parks and
    keeps its VRAM) or a no-op round (empty target set). Without this, the
    fire-and-forget watchdog would force-exit the rank ~60s after the signal,
    killing a survivor that is supposed to stay resident.
    """
    cancel = _active_watchdog_cancel
    if cancel is not None:
        cancel.set()


# ---------------------------------------------------------------------------
# Coordinated (distributed) checkpoint
# ---------------------------------------------------------------------------
#
# Why a flag and not a barrier in the signal handler:
#
#   NCCL is a lockstep protocol -- every rank must issue the *same* sequence of
#   collectives in the *same* order.  A SIGUSR1 handler runs asynchronously: the
#   kernel delivers the signal and Python runs the handler at an arbitrary
#   bytecode boundary, at a *different* point in the training loop on each rank.
#   If the handler calls ``dist.barrier()`` directly, that barrier gets matched
#   on the wire against whatever collective the *other* rank happens to be in --
#   typically an in-flight DDP gradient all-reduce -- producing
#   ``message truncated: receiving N bytes instead of M`` and a corrupted
#   communicator.  (This was the Phase-1 bug; see
#   examples/ray/with-fluidcr/README.md.)
#
#   The fix is *cooperative* checkpointing: the handler only records the request
#   (sets ``_checkpoint_requested``).  The per-optimizer ``step()`` hook -- a
#   point every rank reaches in lockstep, where the backward all-reduce is
#   already complete and no training collective is in flight -- all-reduces the
#   flag (``MAX``) every step.  If ANY rank asked to checkpoint, EVERY rank
#   agrees at the same step boundary and saves together.  See
#   ``fluidcr.backends.pytorch._maybe_coordinated_checkpoint``.

_checkpoint_requested = threading.Event()


def request_checkpoint() -> None:
    """Record that a coordinated checkpoint was requested (called from SIGUSR1)."""
    _checkpoint_requested.set()


def checkpoint_requested() -> bool:
    """True if SIGUSR1 has asked this rank to checkpoint at the next step boundary."""
    return _checkpoint_requested.is_set()


def clear_migration_flags() -> None:
    """Reset the coordination trigger after a coordinated action completes.

    Called by a survivor after it rebuilds so the trigger does not immediately
    re-fire on the next optimizer step.
    """
    _checkpoint_requested.clear()


def distributed_checkpoint_enabled() -> bool:
    """True when ``FLUIDCR_DISTRIBUTED`` opts into coordinated checkpointing."""
    return os.environ.get("FLUIDCR_DISTRIBUTED", "0").strip().lower() in (
        "1",
        "true",
        "yes",
    )


def perform_checkpoint_and_exit(backend: Any = None) -> None:
    """Save the checkpoint, best-effort release the GPU, then ``os._exit``.

    Only safe to call when CUDA/NCCL state is healthy: either single-GPU, or --
    in distributed mode -- *after* all ranks have rendezvoused at a step
    boundary (see ``_maybe_coordinated_checkpoint``).  Never call this from the
    async signal handler in distributed mode.
    """
    if backend is None:
        backend = get_active()
    if backend is None:
        os._exit(EXIT_CODE)

    log("Saving checkpoint...")
    try:
        from fluidcr.ctrl import record_runtime_status

        record_runtime_status(global_step=global_step(), state="CheckpointSaving")
    except Exception:
        pass
    try:
        backend.save_checkpoint(CHECKPOINT_PATH)
        try:
            from fluidcr.ctrl import record_runtime_status

            record_runtime_status(global_step=global_step(), state="CheckpointSaved")
        except Exception:
            pass
        log("Checkpoint saved.")
    except Exception as exc:
        warn(f"Error saving checkpoint: {exc}")

    # Best-effort GPU release in a background thread with a timeout.  The
    # checkpoint is already on disk; cleanup is best-effort only.
    log("Releasing GPU resources...")
    _GPU_RELEASE_TIMEOUT = 30
    t = threading.Thread(target=_release_gpu_best_effort, args=(backend,), daemon=True)
    t.start()
    t.join(timeout=_GPU_RELEASE_TIMEOUT)
    if t.is_alive():
        warn("GPU release timed out; proceeding with exit.")

    log("Exiting for migration.")

    # ``os._exit`` skips atexit hooks (which can deadlock inside a virtual-GPU
    # interceptor when shutting down DataLoader workers or freeing the CUDA
    # caching allocator).  The Launcher only needs the exit code.
    os._exit(EXIT_CODE)


def _sigusr1_handler(signum: int, frame: Any) -> None:
    """Checkpoint on SIGUSR1.

    Single-GPU: save and exit immediately.
    Distributed (``FLUIDCR_DISTRIBUTED=1``): record the request only and return.
    Touching NCCL/CUDA from this async handler would corrupt the communicator
    (see the module note above); the optimizer-step hook performs the actual
    coordinated save once every rank has agreed.
    """
    # Thread-based watchdog: GUARANTEED to fire even if the main thread is
    # stuck in a hung CUDA C call (which the SIGALRM approach cannot escape).
    # It also bounds the distributed path: if a peer has already died, the
    # next collective hangs, and this watchdog still forces the exit code.
    _start_watchdog_thread()

    backend = get_active()
    if backend is None:
        log("SIGUSR1 received but no ML framework imported -- nothing to save.")
        os._exit(EXIT_CODE)

    if distributed_checkpoint_enabled():
        log(
            "SIGUSR1 received. Distributed mode: deferring to the next "
            "optimizer-step boundary for a coordinated checkpoint."
        )
        request_checkpoint()
        return

    log("SIGUSR1 received.")
    perform_checkpoint_and_exit(backend)


if hasattr(signal, "SIGUSR1"):
    signal.signal(signal.SIGUSR1, _sigusr1_handler)


# ---------------------------------------------------------------------------
# Install import hooks for all registered backends
# ---------------------------------------------------------------------------

for _backend in get_backends():
    install_hook(_backend.target_module, _backend.apply_patches)
