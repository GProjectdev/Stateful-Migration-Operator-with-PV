"""Tests the payload overlay without importing torch or a FluidCR installation."""
import importlib.util
import os
from pathlib import Path
import sys
import tempfile
import types
import unittest
from unittest.mock import Mock, patch


def load_ctrl():
    package = types.ModuleType("fluidcr")
    package.__path__ = []
    config = types.ModuleType("fluidcr._config")
    config.EXIT_CODE = 75
    config.log = Mock()
    config.warn = Mock()
    source = Path(__file__).resolve().parents[1] / "fluidcr" / "ctrl.py"
    spec = importlib.util.spec_from_file_location("ctrl_overlay", source)
    module = importlib.util.module_from_spec(spec)
    with patch.dict(sys.modules, {"fluidcr": package, "fluidcr._config": config}):
        spec.loader.exec_module(module)
    return module


ctrl = load_ctrl()


class ConfirmedCheckpointTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.distributed = types.ModuleType("fluidcr.distributed")
        self.distributed.bump_generation = Mock()
        self.distributed.write_manifest = Mock()
        patches = [
            patch.dict(sys.modules, {"fluidcr.distributed": self.distributed}),
            patch.dict(os.environ, {"FLUIDCR_CHECKPOINT_PATH": ""}),
            patch.object(ctrl, "_BASE_DIR", self.tmp.name),
            patch.object(ctrl, "_CHECKPOINT_WAIT_INTERVAL", 0.01),
            patch.object(ctrl.signal, "SIGUSR1", 10, create=True),
            patch.object(ctrl, "registered_worker_pids", return_value={11: 101}),
            patch.object(ctrl, "_pid_uses_gpu", return_value=True),
        ]
        for item in patches:
            item.start()
            self.addCleanup(item.stop)
        self.kill = patch.object(ctrl.os, "kill").start()
        self.addCleanup(patch.stopall)

    def create_lock(self, worker=101, sig=None):
        lock = Path(ctrl._lock_path_for_ppid(11))
        lock.parent.mkdir(parents=True, exist_ok=True)
        lock.write_text("new checkpoint")

    def test_signalled_becomes_ready_only_after_new_lock(self):
        self.kill.side_effect = self.create_lock
        self.assertEqual(ctrl.checkpoint_ranks_and_wait("all", 0.1),
                         {101: "checkpoint-ready"})
        self.distributed.bump_generation.assert_called_once()
        self.distributed.write_manifest.assert_called_once_with("all")
        self.kill.assert_called_once_with(101, ctrl.signal.SIGUSR1)

    def test_timeout_does_not_become_ready(self):
        real_exists = os.path.exists
        with patch.object(ctrl.os.path, "exists",
                          side_effect=lambda p: str(p).startswith("/proc") or real_exists(p)):
            self.assertEqual(ctrl.checkpoint_ranks_and_wait("all", 0.02),
                             {101: "timeout-waiting-lock"})

    def test_exited_launcher_does_not_become_ready(self):
        with patch.object(ctrl.os.path, "exists", return_value=False):
            self.assertEqual(ctrl.checkpoint_ranks_and_wait("all", 0.02),
                             {101: "launcher-exited"})

    def test_empty_registry_does_not_trigger_generation(self):
        with patch.object(ctrl, "registered_worker_pids", return_value={}):
            with self.assertRaisesRegex(ValueError, "no local GPU workers"):
                ctrl.checkpoint_ranks_and_wait("all", 0.1)
        self.distributed.bump_generation.assert_not_called()
        self.kill.assert_not_called()

    def test_stale_lock_rejected_before_trigger(self):
        self.create_lock()
        with self.assertRaisesRegex(ValueError, "stale checkpoint lock"):
            ctrl.checkpoint_ranks_and_wait("all", 0.1)
        self.distributed.bump_generation.assert_not_called()
        self.kill.assert_not_called()

    def test_ambiguous_parent_lock_path_rejected(self):
        with patch.object(ctrl, "registered_worker_pids", return_value={11: 101, 22: 202}), \
             patch.dict(os.environ, {"FLUIDCR_CHECKPOINT_PATH": self.tmp.name + "/shared.ckpt"}):
            with self.assertRaisesRegex(ValueError, "share one checkpoint lock"):
                ctrl.checkpoint_ranks_and_wait("all", 0.1)
        self.distributed.bump_generation.assert_not_called()

    def test_each_signalled_local_worker_requires_its_own_lock(self):
        with patch.object(ctrl, "registered_worker_pids", return_value={11: 101, 22: 202}), \
             patch.object(ctrl, "_wait_for_parent_locks",
                          return_value={11: "lock-ready", 22: "timeout-waiting-lock"}) as wait:
            self.assertEqual(ctrl.checkpoint_ranks_and_wait("all", 0.1),
                             {101: "checkpoint-ready", 202: "timeout-waiting-lock"})
            wait.assert_called_once_with([11, 22], timeout=0.1)

    def test_signal_failure_is_not_ready(self):
        self.kill.side_effect = PermissionError()
        self.assertEqual(ctrl.checkpoint_ranks_and_wait("all", 0.1),
                         {101: "permission-denied"})

    def test_concurrent_request_rejected(self):
        ctrl._checkpoint_lock.acquire()
        try:
            with self.assertRaisesRegex(ValueError, "already in progress"):
                ctrl.checkpoint_ranks_and_wait("all", 0.1)
        finally:
            ctrl._checkpoint_lock.release()
        self.distributed.bump_generation.assert_not_called()

    def request(self, payload):
        handler = ctrl._CtrlRequestHandler.__new__(ctrl._CtrlRequestHandler)
        handler._read_json = Mock(return_value=payload)
        handler._send_json = Mock()
        handler._handle_checkpoint()
        return handler._send_json.call_args.args

    def test_http_wait_returns_confirmed_result(self):
        self.kill.side_effect = self.create_lock
        self.assertEqual(self.request({"all": True, "wait": True, "timeoutSeconds": 0.1}),
                         (200, {"results": {101: "checkpoint-ready"}}))

    def test_http_empty_returns_error(self):
        with patch.object(ctrl, "registered_worker_pids", return_value={}):
            status, body = self.request({"wait": True})
        self.assertEqual(status, 400)
        self.assertIn("no local GPU workers", body["error"])

    def test_http_default_preserves_async_protocol(self):
        self.assertEqual(self.request({"all": True}),
                         (200, {"results": {101: "checkpoint-signalled"}}))

    def test_invalid_wait_requests_do_not_trigger(self):
        for payload in ({"wait": "yes"}, {"wait": True, "ranks": [1]},
                        {"wait": True, "timeoutSeconds": 0},
                        {"wait": True, "timeoutSeconds": float("inf")},
                        {"wait": True, "timeoutSeconds": float("nan")},
                        {"wait": True, "timeoutSeconds": 301},
                        {"wait": True, "timeoutSeconds": True}):
            with self.subTest(payload=payload):
                self.assertEqual(self.request(payload)[0], 400)
        self.distributed.bump_generation.assert_not_called()


if __name__ == "__main__":
    unittest.main()
