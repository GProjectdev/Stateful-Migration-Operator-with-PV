"""Tests the payload overlay without importing torch or a FluidCR installation."""
import importlib.util
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import types
import unittest
import uuid
from unittest.mock import Mock, patch


def load_ctrl():
    package = types.ModuleType("fluidcr")
    package.__path__ = [str(Path(__file__).resolve().parents[1] / "fluidcr")]
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


class RoundPublicationTests(unittest.TestCase):
    def test_follower_waits_for_atomic_publication(self):
        distributed = types.ModuleType("fluidcr.distributed")
        expected = {"checkpointID": "new", "targets": "all"}
        distributed.read_manifest = Mock(side_effect=[
            {"checkpointID": "old"}, {}, expected,
        ])
        with patch.dict(sys.modules, {"fluidcr.distributed": distributed}), \
             patch.object(ctrl.time, "sleep") as sleep:
            self.assertEqual(ctrl._wait_for_round_manifest("new", 1), expected)
        self.assertEqual(sleep.call_count, 2)

    def test_stale_marker_cannot_authorize_another_round(self):
        distributed = types.ModuleType("fluidcr.distributed")
        distributed.read_manifest = Mock(return_value={"checkpointID": "old"})
        with patch.dict(sys.modules, {"fluidcr.distributed": distributed}):
            with self.assertRaisesRegex(ValueError, "publication timed out"):
                ctrl._wait_for_round_manifest("new", 0)

    def test_matching_round_does_not_wait(self):
        distributed = types.ModuleType("fluidcr.distributed")
        distributed.read_manifest = Mock(return_value={"checkpointID": "new"})
        with patch.dict(sys.modules, {"fluidcr.distributed": distributed}), \
             patch.object(ctrl.time, "sleep") as sleep:
            ctrl._wait_for_round_manifest("new", 0)
        sleep.assert_not_called()


def load_launcher(checkpoint_path):
    package = types.ModuleType("fluidcr")
    package.__path__ = []
    source = Path(__file__).resolve().parents[1] / "fluidcr" / "launcher.py"
    spec = importlib.util.spec_from_file_location(
        "launcher_overlay_" + uuid.uuid4().hex, source
    )
    module = importlib.util.module_from_spec(spec)
    with patch.dict(sys.modules, {"fluidcr": package, "fluidcr.ctrl": ctrl}), \
         patch.dict(os.environ, {"FLUIDCR_CHECKPOINT_PATH": str(checkpoint_path)}):
        spec.loader.exec_module(module)
    return module


def load_pytorch_backend(checkpoint_path):
    package = types.ModuleType("fluidcr")
    package.__path__ = []
    package.ctrl = ctrl
    package.group_restore = ctrl.group_restore
    config = types.ModuleType("fluidcr._config")
    config.CHECKPOINT_PATH = str(checkpoint_path)
    config.log = Mock()
    config.warn = Mock()
    backends = types.ModuleType("fluidcr.backends")

    class AbstractBackend:
        pass

    backends.AbstractBackend = AbstractBackend
    backends.register = Mock()
    source = Path(__file__).resolve().parents[1] / "fluidcr" / "backends" / "pytorch.py"
    spec = importlib.util.spec_from_file_location(
        "pytorch_overlay_" + uuid.uuid4().hex, source
    )
    module = importlib.util.module_from_spec(spec)
    with patch.dict(sys.modules, {
        "fluidcr": package,
        "fluidcr._config": config,
        "fluidcr.backends": backends,
        "fluidcr.ctrl": ctrl,
    }):
        spec.loader.exec_module(module)
    return module


def make_dir(path):
    try:
        Path(path).mkdir(parents=True, exist_ok=True)
        return
    except OSError:
        pass
    safe_path = str(path).replace("'", "''")
    subprocess.run([
        "powershell",
        "-NoProfile",
        "-Command",
        f"New-Item -ItemType Directory -Force -Path '{safe_path}' | Out-Null",
    ], check=True)


class ConfirmedCheckpointTests(unittest.TestCase):
    def setUp(self):
        base_tmp = Path(os.environ.get("FLUIDCR_TEST_TMPDIR", r"C:\tmp\fluidcr-runtime-tests"))
        make_dir(base_tmp)
        self.tmp_path = base_tmp / uuid.uuid4().hex
        make_dir(self.tmp_path)
        self.addCleanup(lambda: shutil.rmtree(self.tmp_path, ignore_errors=True))
        self.distributed = types.ModuleType("fluidcr.distributed")
        self.distributed.bump_generation = Mock()
        self.distributed.bump_generation.return_value = 123
        self.distributed.write_manifest = Mock()
        self.distributed._base_dir = lambda: str(self.tmp_path)
        self.distributed.manifest_path = lambda: str(self.tmp_path / "migration-manifest.json")
        self.distributed.read_manifest = Mock(return_value={})
        self.distributed.read_survivor_proof = Mock(return_value={})
        patches = [
            patch.object(ctrl.group_restore, "_root", return_value=str(self.tmp_path)),
            patch.dict(sys.modules, {"fluidcr.distributed": self.distributed}),
            patch.dict(os.environ, {"FLUIDCR_CHECKPOINT_PATH": ""}),
            patch.object(ctrl, "_BASE_DIR", str(self.tmp_path)),
            patch.object(ctrl, "_CHECKPOINT_WAIT_INTERVAL", 0.01),
            patch.object(ctrl.signal, "SIGUSR1", 10, create=True),
            patch.object(ctrl, "registered_worker_pids", return_value={11: 101}),
            patch.object(ctrl, "_pid_uses_gpu", return_value=True),
        ]
        for item in patches:
            item.start()
            self.addCleanup(item.stop)
        self.old_last_runtime_status_write_at = ctrl._last_runtime_status_write_at
        ctrl._last_runtime_status_write_at = 0.0
        self.addCleanup(
            lambda: setattr(
                ctrl,
                "_last_runtime_status_write_at",
                self.old_last_runtime_status_write_at,
            )
        )
        self.old_restore_bindings = dict(ctrl._restore_bindings)
        ctrl._restore_bindings.clear()
        self.addCleanup(
            lambda: (
                ctrl._restore_bindings.clear(),
                ctrl._restore_bindings.update(self.old_restore_bindings),
            )
        )
        self.kill = patch.object(ctrl.os, "kill").start()
        self.addCleanup(patch.stopall)

    def create_lock(self, worker=101, sig=None):
        lock = Path(ctrl._lock_path_for_ppid(11))
        make_dir(lock.parent)
        lock.write_text("new checkpoint")

    def test_overlay_save_publishes_full_round_but_not_partial_group_metadata(self):
        group = ctrl.group_restore
        for targets in ("all", [1]):
            with self.subTest(targets=targets):
                checkpoint_id = "full-round" if targets == "all" else "partial-round"
                latest = self.tmp_path / "trainer-1" / "latest.pt"
                backend = load_pytorch_backend(latest)
                torch = types.ModuleType("torch")
                torch.save = lambda payload, path: Path(path).write_bytes(b"checkpoint-state")
                with patch.dict(sys.modules, {"torch": torch}), \
                     patch.dict(os.environ, {"FLUIDCR_SOURCE_WORLD_UID": "world-uid", "RANK": "1", "WORLD_SIZE": "2"}), \
                     patch.object(group._distributed, "read_manifest", return_value={"checkpointID": checkpoint_id, "targets": targets}), \
                     patch.object(group._distributed, "read_generation", return_value=42), \
                     patch.object(backend, "_collect_rng_states", return_value={}):
                    backend._save_checkpoint(str(latest))
                self.assertEqual(latest.read_bytes(), b"checkpoint-state")
                marker = self.tmp_path / "rounds" / checkpoint_id / "ranks" / "1.json"
                if targets != "all":
                    self.assertFalse(marker.exists())
                    continue
                metadata = json.loads(marker.read_text())
                self.assertEqual(metadata["rank"], 1)
                self.assertEqual(metadata["sourceWorldUID"], "world-uid")
                self.assertEqual(metadata["generation"], 42)
                self.assertEqual(metadata["checkpointID"], checkpoint_id)
                self.assertEqual(metadata["sha256"], hashlib.sha256(latest.read_bytes()).hexdigest())
                self.assertEqual((self.tmp_path / metadata["artifactPath"]).read_bytes(), latest.read_bytes())

    def test_failed_serialization_does_not_publish_rank_metadata(self):
        latest = self.tmp_path / "trainer-1" / "latest.pt"
        backend = load_pytorch_backend(latest)
        torch = types.ModuleType("torch")
        torch.save = Mock(side_effect=OSError("storage unavailable"))
        with patch.dict(sys.modules, {"torch": torch}), \
             patch.object(backend, "_collect_rng_states", return_value={}), \
             patch.object(ctrl.group_restore, "capture_round", return_value={"checkpointID": "round"}), \
             patch.object(ctrl.group_restore, "publish_round") as publish:
            with self.assertRaisesRegex(OSError, "storage unavailable"):
                backend._save_checkpoint(str(latest))
            publish.assert_not_called()

    def test_overlay_restore_failure_never_claims_checkpoint_loaded(self):
        latest = self.tmp_path / "latest.pt"
        latest.write_bytes(b"corrupt")
        backend = load_pytorch_backend(latest)
        torch = types.ModuleType("torch")
        torch.load = Mock(side_effect=ValueError("corrupt checkpoint"))
        with patch.dict(sys.modules, {"torch": torch}), \
             patch.object(backend, "_checkpoint_load_path", return_value=str(latest)):
            with self.assertRaisesRegex(RuntimeError, "failed to read checkpoint"):
                backend._try_load_checkpoint()
        self.assertFalse(backend._checkpoint_loaded)
        self.assertFalse(any("Checkpoint resumed" in str(c) for c in backend.log.call_args_list))

    def create_checkpoint_and_lock(self, worker=101, sig=None):
        checkpoint = Path(ctrl._checkpoint_path_for_ppid(11))
        make_dir(checkpoint.parent)
        checkpoint.write_bytes(b"application checkpoint")
        self.create_lock(worker, sig)

    def test_signalled_becomes_ready_only_after_new_lock(self):
        self.kill.side_effect = self.create_lock
        self.assertEqual(ctrl.checkpoint_ranks_and_wait("all", 0.1),
                         {101: "checkpoint-ready"})
        self.distributed.bump_generation.assert_called_once()
        self.distributed.write_manifest.assert_called_once_with(
            "all", checkpoint_id=None, restore_owned_resume=False
        )
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
             patch.dict(os.environ, {"FLUIDCR_CHECKPOINT_PATH": str(self.tmp_path / "shared.ckpt")}):
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

    def resume_request(self, payload):
        handler = ctrl._CtrlRequestHandler.__new__(ctrl._CtrlRequestHandler)
        handler._read_json = Mock(return_value=payload)
        handler._send_json = Mock()
        handler._handle_resume()
        return handler._send_json.call_args.args

    def test_http_wait_returns_confirmed_result(self):
        self.kill.side_effect = self.create_lock
        self.assertEqual(self.request({"all": True, "wait": True, "timeoutSeconds": 0.1}),
                         (200, {"results": {101: "checkpoint-ready"}}))

    def test_checkpoint_id_preserves_round_artifact(self):
        make_dir(self.tmp_path / "11" / "rounds" / "round-001")
        self.kill.side_effect = self.create_checkpoint_and_lock
        self.assertEqual(ctrl.checkpoint_ranks_and_wait("all", 0.1, checkpoint_id="round-001"),
                         {101: "checkpoint-ready"})
        artifact = self.tmp_path / "11" / "rounds" / "round-001" / "latest.pt"
        self.assertEqual(artifact.read_bytes(), b"application checkpoint")
        status = ctrl._read_status(str(self.tmp_path / "11" / "latest.pt"))
        self.assertEqual(status["checkpointID"], "round-001")
        self.assertEqual(status["state"], "CheckpointReady")
        self.assertNotIn("iterationTimeSeconds", status)
        self.assertIn("checkpointDurationSeconds", status)

    def test_partial_wait_returns_target_checkpoint_evidence(self):
        make_dir(self.tmp_path / "11" / "rounds" / "round-001")
        self.kill.side_effect = self.create_checkpoint_and_lock
        with patch.dict(os.environ, {
            "FLUIDCR_CONTAINER_NAME": "trainer",
            "FLUIDCR_NODE_NAME": "node-a",
            "FLUIDCR_POD_NAME": "trainer-1",
            "FLUIDCR_POD_UID": "uid-target",
            "RANK": "1",
            "WORLD_SIZE": "2",
        }), patch.object(ctrl, "registered_worker_pids", return_value={11: 101}):
            response = ctrl.checkpoint_ranks_and_wait(
                [1], 0.1, checkpoint_id="round-001", restore_owned_resume=True, contract=True,
            )

        artifact = self.tmp_path / "11" / "rounds" / "round-001" / "latest.pt"
        self.assertEqual(response["checkpointID"], "round-001")
        self.assertEqual(response["targetRanks"], [1])
        self.assertTrue(response["partial"])
        self.assertTrue(response["restoreOwnedResume"])
        self.assertTrue(response["noPeriodicResume"])
        evidence = response["appCheckpointEvidence"]["1"]
        self.assertEqual(evidence["podUID"], "uid-target")
        self.assertEqual(evidence["phase"], "AppCheckpointReady")
        self.assertEqual(evidence["appArtifact"]["path"], str(artifact))
        self.assertTrue(evidence["appArtifact"]["sha256"])
        self.assertNotIn("durableRef", evidence)
        self.distributed.write_manifest.assert_called_with(
            [1], checkpoint_id="round-001", restore_owned_resume=True, generation=123
        )

    def test_partial_rank0_target_returns_owned_evidence(self):
        make_dir(self.tmp_path / "11" / "rounds" / "round-rank0")
        self.kill.side_effect = self.create_checkpoint_and_lock
        with patch.dict(os.environ, {
            "RANK": "0", "WORLD_SIZE": "2",
            "FLUIDCR_POD_NAME": "trainer-0", "FLUIDCR_POD_UID": "rank-zero-uid",
            "FLUIDCR_NODE_NAME": "node-zero", "FLUIDCR_CONTAINER_NAME": "trainer",
        }), patch.object(ctrl, "registered_worker_pids", return_value={11: 101}):
            response = ctrl.checkpoint_ranks_and_wait(
                [0], 0.1, checkpoint_id="round-rank0",
                restore_owned_resume=True, contract=True,
            )
        self.assertEqual(response["targetRanks"], [0])
        self.assertTrue(response["noPeriodicResume"])
        self.assertTrue(response["restoreOwnedResume"])
        evidence = response["appCheckpointEvidence"]["0"]
        self.assertEqual(evidence["podUID"], "rank-zero-uid")
        self.assertEqual(evidence["phase"], "AppCheckpointReady")
        self.assertTrue(evidence["appArtifact"]["sha256"])
        self.distributed.write_manifest.assert_called_with(
            [0], checkpoint_id="round-rank0", restore_owned_resume=True, generation=123
        )

    def test_partial_wait_replay_recovers_after_worker_exit_99_unregistered(self):
        marker = self.tmp_path / "rounds" / "round-007" / ".triggered"
        make_dir(marker.parent)
        marker.write_text("already")
        checkpoint = self.tmp_path / "rank1" / "latest.pt"
        make_dir(checkpoint.parent)
        checkpoint.write_bytes(b"exited-worker-state")
        (checkpoint.parent / "lock").write_text("ready")
        self.distributed.read_manifest.return_value = {
            "checkpointID": "round-007",
            "restoreOwnedResume": True,
            "targets": [1],
        }
        with patch.object(ctrl, "registered_worker_pids", return_value={}), \
             patch.dict(os.environ, {
                 "FLUIDCR_CHECKPOINT_PATH": str(checkpoint),
                 "FLUIDCR_NODE_NAME": "node-a",
                 "FLUIDCR_POD_NAME": "trainer-1",
                 "FLUIDCR_POD_UID": "uid-target",
                 "RANK": "1",
                 "WORLD_SIZE": "2",
             }):
            response = ctrl.checkpoint_ranks_and_wait(
                [1], 0.1, checkpoint_id="round-007", restore_owned_resume=True, contract=True,
            )
        artifact = checkpoint.parent / "rounds" / "round-007" / "latest.pt"
        self.assertEqual(response["results"], {"replay": "checkpoint-ready"})
        self.assertEqual(response["appCheckpointEvidence"]["1"]["phase"], "AppCheckpointReady")
        self.assertEqual(response["appCheckpointEvidence"]["1"]["appArtifact"]["path"], str(artifact))
        self.assertEqual(artifact.read_bytes(), b"exited-worker-state")
        self.kill.assert_not_called()
        self.distributed.bump_generation.assert_not_called()

    def test_checkpoint_id_is_immutable(self):
        artifact = self.tmp_path / "11" / "rounds" / "round-001" / "latest.pt"
        make_dir(artifact.parent)
        artifact.write_bytes(b"old")
        with self.assertRaisesRegex(ValueError, "checkpointID already exists"):
            ctrl.checkpoint_ranks_and_wait("all", 0.1, checkpoint_id="round-001")
        self.distributed.bump_generation.assert_not_called()
        self.kill.assert_not_called()

    def test_http_empty_returns_error(self):
        with patch.object(ctrl, "registered_worker_pids", return_value={}):
            status, body = self.request({"wait": True})
        self.assertEqual(status, 400)
        self.assertIn("no local GPU workers", body["error"])

    def test_http_invalid_checkpoint_id_returns_error(self):
        status, body = self.request({"wait": True, "checkpointID": "../bad"})
        self.assertEqual(status, 400)
        self.assertIn("checkpointID", body["error"])

    def test_http_runtime_returns_training_runtime_contract(self):
        checkpoint = self.tmp_path / "rank0" / "latest.pt"
        with patch.dict(os.environ, {
            "FLUIDCR_CHECKPOINT_PATH": str(checkpoint),
            "RANK": "0",
            "WORLD_SIZE": "2",
        }):
            make_dir(checkpoint.parent)
            ctrl._write_status(str(checkpoint), {
                "checkpointID": "round-001",
                "globalStep": 42,
                "state": "Running",
                "iterationTimeSeconds": 1.25,
            })
            observed_at = ctrl._read_status(str(checkpoint))["observedAt"]
            handler = ctrl._CtrlRequestHandler.__new__(ctrl._CtrlRequestHandler)
            handler.path = "/runtime"
            handler._send_json = Mock()
            with patch.object(ctrl, "_utc_now", return_value="2999-01-01T00:00:00Z"), \
                 patch.object(ctrl, "_read_checkpoint_global_step", return_value=7):
                handler.do_GET()
        status, body = handler._send_json.call_args.args
        self.assertEqual(status, 200)
        self.assertEqual(body["globalStep"], 42)
        self.assertEqual(body["checkpointID"], "round-001")
        self.assertEqual(body["rank"], 0)
        self.assertEqual(body["worldSize"], 2)
        self.assertEqual(body["state"], "Running")
        self.assertEqual(body["iterationTimeSeconds"], 1.25)
        self.assertEqual(body["observedAt"], observed_at)

    def test_http_runtime_returns_survivor_pause_evidence(self):
        checkpoint = self.tmp_path / "rank0" / "latest.pt"
        pause_lock = self.tmp_path / "rank0" / "pause-lock"
        make_dir(checkpoint.parent)
        pause_lock.write_text("parked")
        self.distributed.read_survivor_proof = Mock(return_value={
            "state": "SurvivorPaused",
            "rank": 0,
            "podUID": "survivor-pod-uid",
            "checkpointID": "round-survivor",
            "generation": 7,
            "pauseLockPath": str(pause_lock),
            "pauseLockPID": 1234,
            "observedAt": "survivor-observed",
        })
        ctrl._write_status(str(checkpoint), {"globalStep": 42, "state": "Running", "checkpointID": "round-survivor"})
        with patch.dict(os.environ, {
            "FLUIDCR_CHECKPOINT_PATH": str(checkpoint),
            "FLUIDCR_NODE_NAME": "node-survivor",
            "FLUIDCR_POD_NAME": "trainer-0",
            "FLUIDCR_POD_UID": "survivor-pod-uid",
            "RANK": "0",
            "WORLD_SIZE": "2",
        }), patch.object(ctrl, "registered_worker_pids", return_value={11: 101}):
            handler = ctrl._CtrlRequestHandler.__new__(ctrl._CtrlRequestHandler)
            handler.path = "/runtime"
            handler._send_json = Mock()
            handler.do_GET()
        status, body = handler._send_json.call_args.args
        self.assertEqual(status, 200)
        self.assertEqual(body["survivorEvidence"], {
            "generation": 7,
            "pauseLockPath": str(pause_lock),
            "pauseLockPID": 1234,
            "observedAt": "survivor-observed",
        })
        self.assertEqual(body["phase"], "SurvivorPaused")
        self.assertEqual(body["checkpointID"], "round-survivor")

    def test_http_runtime_unavailable_without_rank_world_or_live_worker(self):
        checkpoint = self.tmp_path / "rank0" / "latest.pt"
        make_dir(checkpoint.parent)
        ctrl._write_status(str(checkpoint), {"globalStep": 42, "state": "Running"})
        with patch.dict(os.environ, {"FLUIDCR_CHECKPOINT_PATH": str(checkpoint)}, clear=True):
            handler = ctrl._CtrlRequestHandler.__new__(ctrl._CtrlRequestHandler)
            handler.path = "/runtime"
            handler._send_json = Mock()
            handler.do_GET()
        status, body = handler._send_json.call_args.args
        self.assertEqual(status, 503)
        self.assertIn("RANK and WORLD_SIZE", body["error"])

        with patch.dict(os.environ, {
            "FLUIDCR_CHECKPOINT_PATH": str(checkpoint),
            "RANK": "0",
            "WORLD_SIZE": "2",
        }), patch.object(ctrl, "registered_worker_pids", return_value={}):
            handler = ctrl._CtrlRequestHandler.__new__(ctrl._CtrlRequestHandler)
            handler.path = "/runtime"
            handler._send_json = Mock()
            handler.do_GET()
        status, body = handler._send_json.call_args.args
        self.assertEqual(status, 503)
        self.assertIn("no live worker registry", body["error"])

    def test_record_runtime_status_throttles_step_telemetry_without_fake_observation(self):
        checkpoint = self.tmp_path / "rank0" / "latest.pt"
        make_dir(checkpoint.parent)
        with patch.dict(os.environ, {"FLUIDCR_CHECKPOINT_PATH": str(checkpoint)}), \
             patch.object(ctrl, "_RUNTIME_TELEMETRY_INTERVAL_SECONDS", 1.0), \
             patch.object(ctrl.time, "monotonic", side_effect=[10.0, 10.2, 10.3]), \
             patch.object(ctrl, "_utc_now", side_effect=["t1", "t2"]):
            ctrl.record_runtime_status(
                global_step=1,
                state="Running",
                iteration_time_seconds=0.1,
            )
            first = ctrl._read_status(str(checkpoint))
            ctrl.record_runtime_status(
                global_step=2,
                state="Running",
                iteration_time_seconds=0.2,
            )
            skipped = ctrl._read_status(str(checkpoint))
            ctrl.record_runtime_status(state="CheckpointSaving")
            transition = ctrl._read_status(str(checkpoint))

        self.assertEqual(first["globalStep"], 1)
        self.assertEqual(first["observedAt"], "t1")
        self.assertEqual(skipped, first)
        self.assertEqual(transition["globalStep"], 1)
        self.assertEqual(transition["state"], "CheckpointSaving")
        self.assertEqual(transition["observedAt"], "t2")

    def test_restore_uses_round_artifact_after_latest_overwrite(self):
        latest = self.tmp_path / "11" / "latest.pt"
        artifact = self.tmp_path / "11" / "rounds" / "round-001" / "latest.pt"
        future_artifact = self.tmp_path / "11" / "rounds" / "round-002" / "latest.pt"
        make_dir(artifact.parent)
        make_dir(future_artifact.parent)
        make_dir(latest.parent)
        artifact.write_bytes(b"round1-state")
        future_artifact.write_bytes(b"round2-state")
        latest.write_bytes(b"future-state")
        ctrl.bind_restore_checkpoint(str(latest), "round-001", str(artifact))
        ctrl._write_status(str(latest), {
            "checkpointID": "round-002",
            "artifactPath": str(future_artifact),
            "globalStep": 2,
            "state": "CheckpointReady",
        })

        self.assertEqual(ctrl.restore_checkpoint_path(str(latest)), str(artifact))
        launcher = load_launcher(latest)
        env = launcher._build_env()
        self.assertEqual(env["FLUIDCR_CHECKPOINT_PATH"], str(latest))
        self.assertEqual(env["FLUIDCR_RESTORE_CHECKPOINT_PATH"], str(artifact))
        self.assertEqual(env["FLUIDCR_RESTORE_CHECKPOINT_ID"], "round-001")

        backend = load_pytorch_backend(latest)
        with patch.dict(os.environ, {
            "FLUIDCR_CHECKPOINT_PATH": str(latest),
            "FLUIDCR_RESTORE_CHECKPOINT_PATH": env["FLUIDCR_RESTORE_CHECKPOINT_PATH"],
            "FLUIDCR_RESTORE_CHECKPOINT_ID": env["FLUIDCR_RESTORE_CHECKPOINT_ID"],
        }), patch.object(ctrl.time, "monotonic", return_value=100.0), \
             patch.object(ctrl, "_utc_now", return_value="restored-observation"):
            self.assertEqual(backend._checkpoint_load_path(), str(artifact))
            self.assertEqual(Path(backend._checkpoint_load_path()).read_bytes(), b"round1-state")
            ctrl.record_runtime_status(global_step=3, state="Running")
        self.assertEqual(latest.read_bytes(), b"future-state")
        status = ctrl._read_status(str(latest))
        self.assertEqual(status["checkpointID"], "round-001")
        self.assertEqual(status["artifactPath"], str(artifact))
        self.assertEqual(status["globalStep"], 3)
        self.assertEqual(status["observedAt"], "restored-observation")

    def test_pinned_restore_artifact_missing_fails_closed(self):
        latest = self.tmp_path / "11" / "latest.pt"
        missing = self.tmp_path / "11" / "rounds" / "round-001" / "latest.pt"
        make_dir(latest.parent)
        latest.write_bytes(b"future-state")
        ctrl.bind_restore_checkpoint(str(latest), "round-001", str(missing))

        with self.assertRaisesRegex(FileNotFoundError, "pinned restore checkpoint"):
            ctrl.restore_checkpoint_path(str(latest))

        backend = load_pytorch_backend(latest)
        with patch.dict(os.environ, {"FLUIDCR_RESTORE_CHECKPOINT_PATH": str(missing)}):
            with self.assertRaisesRegex(FileNotFoundError, "pinned restore checkpoint"):
                backend._checkpoint_load_path()

    def test_restore_owned_partial_manifest_rejects_ordinary_resume_all(self):
        pause_lock = self.tmp_path / "rank0" / "pause-lock"
        make_dir(pause_lock.parent)
        pause_lock.write_text("parked")
        self.distributed.read_manifest.return_value = {
            "checkpointID": "round-resume",
            "generation": 123,
            "restoreOwnedResume": True,
            "targets": [1],
        }
        with self.assertRaisesRegex(ValueError, "explicit restore resume"):
            ctrl.resume_all_pending()
        self.assertTrue(pause_lock.exists())

    def test_restore_owned_partial_manifest_rejects_unscoped_ppid_resume(self):
        lock = self.tmp_path / "11" / "lock"
        make_dir(lock.parent)
        lock.write_text("parked-target")
        self.distributed.read_manifest.return_value = {
            "checkpointID": "round-resume",
            "generation": 123,
            "restoreOwnedResume": True,
            "targets": [1],
        }
        with self.assertRaisesRegex(ValueError, "scoped restore resume"):
            ctrl.resume_ppids([11])
        self.assertTrue(lock.exists())

    def test_restore_owned_resume_requires_matching_checkpoint_and_generation(self):
        pause_lock = self.tmp_path / "rank0" / "pause-lock"
        make_dir(pause_lock.parent)
        pause_lock.write_text("parked")
        (pause_lock.parent / ".fluidcr-survivor.json").write_text(json.dumps({"checkpointID": "round-resume", "generation": 123}))
        self.distributed._atomic_write = Mock()
        self.distributed.read_manifest.return_value = {
            "checkpointID": "round-resume",
            "generation": 123,
            "restoreOwnedResume": True,
            "targets": [1],
        }
        with self.assertRaisesRegex(ValueError, "generation mismatch"):
            ctrl.resume_all_pending(
                checkpoint_id="round-resume",
                generation=124,
                restore_owned_resume=True,
            )
        results = ctrl.resume_all_pending(
            checkpoint_id="round-resume",
            generation=123,
            restore_owned_resume=True,
        )
        self.assertEqual(results[str(pause_lock)], "removed")

    def test_http_restore_owned_resume_rejects_plain_all_and_accepts_scoped_release(self):
        pause_lock = self.tmp_path / "rank0" / "pause-lock"
        make_dir(pause_lock.parent)
        pause_lock.write_text("parked")
        (pause_lock.parent / ".fluidcr-survivor.json").write_text(json.dumps({"checkpointID": "round-http-resume", "generation": 123}))
        self.distributed._atomic_write = Mock()
        self.distributed.read_manifest.return_value = {
            "checkpointID": "round-http-resume",
            "generation": 123,
            "restoreOwnedResume": True,
            "targets": [1],
        }
        status, body = self.resume_request({"all": True})
        self.assertEqual(status, 400)
        self.assertIn("explicit restore resume", body["error"])
        self.assertTrue(pause_lock.exists())

        status, body = self.resume_request({
            "all": True,
            "checkpointID": "round-http-resume",
            "generation": 123,
            "restoreOwnedResume": True,
        })
        self.assertEqual(status, 200)
        self.assertEqual(body["results"][str(pause_lock)], "removed")

    def test_restore_owned_resume_is_idempotent_after_manifest_removed(self):
        self.distributed.read_manifest.return_value = {}
        results = ctrl.resume_all_pending(
            checkpoint_id="round-resume",
            generation=123,
            restore_owned_resume=True,
        )
        self.assertEqual(results[self.distributed.manifest_path()], "already-complete")

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


class FluidCRPayloadParityTests(unittest.TestCase):
    EXPECTED_SHA256 = {
        # Keep checkpoint producer, load validation, and control API paired.
        "ctrl.py": "b36ed1c556d254782c6b410b160df44d34e858a04a59b0a8adca61491102f3d2",
        "backends/pytorch.py": "530e5fe1c3c2786a6f4564fd6ed18087aa55a383a159757c885a076b03166615",
        "group_restore.py": "0d22ab37c6ed05fcf44d3b5cc5a7b37288de2f57f4ab725bca338dbb3c7917d8",
        "distributed.py": "291afd14df82362e7a76e60c292ae7a3c3fa29ae4cda922fd3287018c50f500d",
    }

    def test_stateful_overlay_matches_pinned_fluidcr_payload_hashes(self):
        repo = Path(__file__).resolve().parents[1]
        for name, expected in self.EXPECTED_SHA256.items():
            with self.subTest(name=name):
                actual = hashlib.sha256((repo / "fluidcr" / name).read_bytes().replace(b"\r\n", b"\n")).hexdigest()
                self.assertEqual(actual, expected)

    def test_stateful_overlay_matches_local_fluidcr_source_when_available(self):
        repo = Path(__file__).resolve().parents[1]
        source = repo.parents[1] / "My_FluidCR-work" / "fluidcr"
        if not source.exists():
            self.skipTest("local My_FluidCR-work source tree is not available")
        for name in self.EXPECTED_SHA256:
            with self.subTest(name=name):
                self.assertEqual(
                    (repo / "fluidcr" / name).read_bytes().replace(b"\r\n", b"\n"),
                    (source / name).read_bytes().replace(b"\r\n", b"\n"),
                )


if __name__ == "__main__":
    unittest.main()
