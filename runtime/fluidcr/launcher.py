# Derived from FluidCR (Apache-2.0); upstream launcher content retained below.
# Local changes add runtime telemetry and immutable per-round restore binding
# for Stateful-Migration-System.
"""
fluidcr.launcher -- FluidCR Launcher

This module contains the core Launcher implementation. It is exposed as
the ``fluidcr-launcher`` console script entrypoint.
"""

import os
import subprocess
import sys
import time
from typing import Dict, List, Optional

from fluidcr.ctrl import (
    ensure_api_server_started,
    record_runtime_status,
    register_worker_pid,
    restore_checkpoint_binding,
    restore_checkpoint_path,
    unregister_worker_pid,
)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
SNAPSHOT_EXIT_CODE: int = int(os.environ.get("FLUIDCR_EXIT_CODE", "99"))
POLL_INTERVAL: float = float(os.environ.get("FLUIDCR_POLL_INTERVAL", "1"))

# Per-instance checkpoint path (PID-isolated by default).
_BASE_DIR: str = os.environ.get("FLUIDCR_CHECKPOINT_DIR", "/checkpoint")
_EXPLICIT_PATH: str = os.environ.get("FLUIDCR_CHECKPOINT_PATH", "")

if _EXPLICIT_PATH:
    # User set an explicit path -- single-instance mode, no PID isolation.
    CHECKPOINT_PATH: str = _EXPLICIT_PATH
else:
    CHECKPOINT_PATH = os.path.join(_BASE_DIR, str(os.getpid()), "latest.pt")

# Lock file always lives next to the checkpoint.
LOCK_PATH: str = os.path.join(os.path.dirname(CHECKPOINT_PATH), "lock")

# Directory that contains sitecustomize.py (this script lives beside it).
_PAYLOAD_DIR: str = os.path.dirname(os.path.abspath(__file__))

# Global debug flag controlled by the ``--debug`` CLI option.
DEBUG: bool = False


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _log(msg: str) -> None:
    """Print a launcher log line only when debug is enabled."""
    if DEBUG:
        print(msg, flush=True)


def _build_env() -> Dict[str, str]:
    """Return a copy of the environment with ``PYTHONPATH`` prepended.

    Also injects ``FLUIDCR_CHECKPOINT_PATH`` so the Worker's
    ``sitecustomize.py`` picks up the correct per-instance path.
    """
    env = os.environ.copy()
    existing = env.get("PYTHONPATH", "")
    if existing:
        env["PYTHONPATH"] = f"{_PAYLOAD_DIR}{os.pathsep}{existing}"
    else:
        env["PYTHONPATH"] = _PAYLOAD_DIR
    env["FLUIDCR_CHECKPOINT_PATH"] = CHECKPOINT_PATH
    restore_path = restore_checkpoint_path(CHECKPOINT_PATH)
    if restore_path != CHECKPOINT_PATH:
        env["FLUIDCR_RESTORE_CHECKPOINT_PATH"] = restore_path
        binding = restore_checkpoint_binding(CHECKPOINT_PATH) or {}
        checkpoint_id = binding.get("checkpointID", "")
        if checkpoint_id:
            env["FLUIDCR_RESTORE_CHECKPOINT_ID"] = checkpoint_id
        else:
            env.pop("FLUIDCR_RESTORE_CHECKPOINT_ID", None)
    else:
        env.pop("FLUIDCR_RESTORE_CHECKPOINT_PATH", None)
        env.pop("FLUIDCR_RESTORE_CHECKPOINT_ID", None)

    # Propagate logging verbosity to the Worker via FLUIDCR_LOG_LEVEL so that
    # ``fluidcr._config.log()`` respects the launcher's ``--debug`` flag.
    #
    # Without ``--debug`` we force QUIET mode so all [FluidCR] logs are suppressed.
    # With ``--debug`` we enable INFO-level logs.
    env["FLUIDCR_LOG_LEVEL"] = "INFO" if DEBUG else "QUIET"

    return env


def _create_lock() -> None:
    """Create the lock file that signals readiness for CRIU snapshot."""
    lock_dir = os.path.dirname(LOCK_PATH)
    if lock_dir and not os.path.isdir(lock_dir):
        os.makedirs(lock_dir, exist_ok=True)
    try:
        with open(LOCK_PATH, "w") as fh:
            fh.write(str(os.getpid()))
    except FileNotFoundError:
        os.makedirs(lock_dir, exist_ok=True)
        with open(LOCK_PATH, "w") as fh:
            fh.write(str(os.getpid()))


def _wait_for_lock_removal() -> None:
    """Block until the external controller removes the lock file."""
    while os.path.exists(LOCK_PATH):
        time.sleep(POLL_INTERVAL)


def _slurp_checkpoint() -> Optional[bytes]:
    """Read the checkpoint into heap memory (survives CRIU)."""
    if not os.path.isfile(CHECKPOINT_PATH):
        _log(f"[Launcher] No checkpoint at {CHECKPOINT_PATH} to buffer.")
        return None
    with open(CHECKPOINT_PATH, "rb") as fh:
        buf = fh.read()
    size_mb = len(buf) / (1024 * 1024)
    _log(f"[Launcher] Checkpoint buffered ({size_mb:.1f} MB).")
    return buf


def _flush_checkpoint(buf: bytes) -> None:
    """Write the buffered checkpoint back to disk with ``fsync``."""
    ckpt_dir = os.path.dirname(CHECKPOINT_PATH)
    if ckpt_dir and not os.path.isdir(ckpt_dir):
        os.makedirs(ckpt_dir, exist_ok=True)
    with open(CHECKPOINT_PATH, "wb") as fh:
        fh.write(buf)
        fh.flush()
        os.fsync(fh.fileno())
    size_mb = len(buf) / (1024 * 1024)
    _log(f"[Launcher] Checkpoint flushed to disk ({size_mb:.1f} MB).")


def _spawn_worker(command: List[str], env: Dict[str, str]) -> int:
    """Spawn the user command and return its exit code."""
    _log(f"[Launcher] Spawning: {' '.join(command)}")
    record_runtime_status(state="Running")
    launcher_pid = os.getpid()
    proc = subprocess.Popen(command, env=env)
    register_worker_pid(launcher_pid, proc.pid)
    try:
        proc.wait()
    finally:
        unregister_worker_pid(launcher_pid)
    return proc.returncode


# ---------------------------------------------------------------------------
# Main loop
# ---------------------------------------------------------------------------

def main(command: List[str]) -> int:
    """Run the Launcher-Worker loop.

    Returns the final exit code to propagate to the caller.
    """
    # Ensure the in-container REST control API server is running while at least
    # one Launcher is active. If the port is already in use we assume another
    # instance has started it.
    ensure_api_server_started()

    iteration = 0
    checkpoint_buffer: Optional[bytes] = None

    while True:
        # Flush RAM buffer -> disk before (re)spawning.
        if checkpoint_buffer is not None:
            _flush_checkpoint(checkpoint_buffer)
            checkpoint_buffer = None

        iteration += 1
        _log(f"[Launcher] ---- iteration {iteration} ----")

        env = _build_env()
        rc = _spawn_worker(command, env)
        _log(f"[Launcher] Worker exited with code {rc}")

        if rc == 0:
            _log("[Launcher] Training complete.")
            record_runtime_status(state="Completed")
            return 0

        if rc == SNAPSHOT_EXIT_CODE:
            _log("[Launcher] Worker ready for snapshot.")
            record_runtime_status(state="CheckpointSaving")

            checkpoint_buffer = _slurp_checkpoint()

            _create_lock()
            record_runtime_status(state="CheckpointReady")
            _log(f"[Launcher] Lock at {LOCK_PATH}. Waiting for controller...")

            _wait_for_lock_removal()
            _log("[Launcher] Snapshot complete. Resuming...")
            record_runtime_status(state="Running")
            continue

        # Unexpected failure.
        _log(f"[Launcher] Unexpected exit code {rc}. Aborting.")
        record_runtime_status(state="Failed")
        return rc


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def _normalize_command(argv: List[str]) -> List[str]:
    """Prepend the Python interpreter for bare ``.py`` scripts."""
    if argv and argv[0].endswith(".py"):
        return [sys.executable, "-u"] + argv
    return argv


def main_cli() -> int:
    """CLI entrypoint for the ``fluidcr-launcher`` console script."""
    argv = sys.argv[1:]

    # Show help without spawning a worker.
    if not argv or argv[0] in ("-h", "--help"):
        print(
            "Usage: fluidcr-launcher [--debug] <command> [args ...]\n"
            "Example: fluidcr-launcher --debug python train.py --epochs 100",
            file=sys.stderr,
        )
        # Treat help as success so shells don't think this is an error.
        return 0

    debug = False
    if argv and argv[0] == "--debug":
        debug = True
        argv = argv[1:]

    if not argv:
        # No command provided after processing options.
        print(
            "Usage: fluidcr-launcher [--debug] <command> [args ...]\n"
            "Example: fluidcr-launcher --debug python train.py --epochs 100",
            file=sys.stderr,
        )
        return 1

    global DEBUG
    DEBUG = debug

    user_command = _normalize_command(argv)
    return main(user_command)
