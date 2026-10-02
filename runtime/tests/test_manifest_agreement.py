"""Manifest agreement and rank-role regressions without CUDA."""
import ast
import importlib.util
import json
import os
from pathlib import Path
import sys
import tempfile
import types
import unittest
from unittest.mock import Mock, patch

ROOT = Path(__file__).resolve().parents[1] / "fluidcr"


def load_distributed():
    spec = importlib.util.spec_from_file_location("manifest_under_test", ROOT / "distributed.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class ManifestAgreementTests(unittest.TestCase):
    def setUp(self):
        self.d = load_distributed()
        self.tmp = tempfile.TemporaryDirectory()
        self.env = patch.dict(os.environ, {"FLUIDCR_CHECKPOINT_DIR": self.tmp.name})
        self.env.start()
        self.addCleanup(self.env.stop)
        self.addCleanup(self.tmp.cleanup)

    def test_missing_malformed_and_invalid_targets_fail_closed(self):
        for text in ("", "{", "{}", '{"targets":null}', '{"targets":[true]}',
                     '{"targets":[2]}', '{"targets":[1,1]}', '{"targets":["1"]}'):
            with self.subTest(text=text):
                Path(self.d.manifest_path()).write_text(text)
                with self.assertRaises(ValueError):
                    self.d.validated_checkpoint_manifest(2)

    def test_owned_partial_requires_identity(self):
        self.d.write_manifest([1], restore_owned_resume=True)
        with self.assertRaises(ValueError):
            self.d.validated_checkpoint_manifest(2)
        self.d.write_manifest([1], checkpoint_id="partial", restore_owned_resume=True, generation=2)
        self.assertEqual(self.d.validated_checkpoint_manifest(2)["targets"], [1])

    def test_explicit_full_and_legacy_partial_still_supported(self):
        for targets in ("all", [1], []):
            self.d.write_manifest(targets)
            self.assertEqual(self.d.validated_checkpoint_manifest(2)["targets"], targets)

    def test_disagreement_holds_then_accepts_same_round(self):
        self.d.write_manifest([1], checkpoint_id="partial", restore_owned_resume=True, generation=2)
        calls = []
        def gather(out, local):
            calls.append(local)
            out[:] = [local, {"manifest": {"targets": "all"}}] if len(calls) == 1 else [local, local]
        dist = types.ModuleType("torch.distributed")
        dist.all_gather_object = gather
        torch = types.ModuleType("torch")
        torch.distributed = dist
        with patch.dict(sys.modules, {"torch": torch, "torch.distributed": dist}), patch.object(self.d.time, "sleep") as sleep:
            result = self.d.agreed_checkpoint_manifest(2)
        self.assertEqual(result["checkpointID"], "partial")
        self.assertEqual(len(calls), 2)
        sleep.assert_called_once_with(0.25)

    def test_survivor_proof_uses_agreed_snapshot_not_replaced_manifest(self):
        agreed = {"targets": [1], "checkpointID": "partial-A", "generation": 7,
                  "restoreOwnedResume": True, "noPeriodicResume": True}
        self.d.write_manifest("all", checkpoint_id="unrelated-B", generation=8)
        checkpoint = str(Path(self.tmp.name) / "rank0" / "latest.pt")
        with patch.dict(os.environ, {"FLUIDCR_CHECKPOINT_PATH": checkpoint, "RANK": "0"}):
            self.d._write_pause_lock(self.d._pause_lock_path(), agreed)
            proof = self.d.read_survivor_proof()
        self.assertEqual(proof["checkpointID"], "partial-A")
        self.assertEqual(proof["generation"], 7)

    def test_periodic_then_partial_keeps_rank_zero_alive(self):
        tree = ast.parse((ROOT / "backends" / "pytorch.py").read_text())
        function = next(n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == "_maybe_coordinated_checkpoint")
        scope = {"Any": object, "_device_for_optimizer": lambda opt: None,
                 "_coordinate_checkpoint_request": lambda *args: True,
                 "_active_backend_for_exit": lambda: None, "log": Mock(), "warn": Mock()}
        exec(compile(ast.Module(body=[function], type_ignores=[]), "role", "exec"), scope)
        fluidcr = types.ModuleType("fluidcr")
        for name in ("begin_coordinated_checkpoint", "cancel_checkpoint_watchdog", "_start_watchdog_thread",
                     "clear_migration_flags", "perform_checkpoint_and_exit"):
            setattr(fluidcr, name, Mock())
        fluidcr.distributed_checkpoint_enabled = lambda: True
        fluidcr.checkpoint_requested = lambda: True
        dist = types.ModuleType("torch.distributed")
        dist.is_available = dist.is_initialized = lambda: True
        dist.get_world_size = lambda: 2
        dist.get_rank = lambda: 0
        torch = types.ModuleType("torch")
        torch.distributed = dist
        self.d.survivor_pause_and_rebuild = Mock()
        def gather(out, local):
            out[:] = [local, local]
        dist.all_gather_object = gather
        modules = {"fluidcr": fluidcr, "fluidcr.distributed": self.d,
                   "torch": torch, "torch.distributed": dist}
        with patch.dict(sys.modules, modules):
            self.d.write_manifest("all", checkpoint_id="periodic")
            scope["_maybe_coordinated_checkpoint"](None)
            fluidcr.perform_checkpoint_and_exit.assert_called_once()
            fluidcr.perform_checkpoint_and_exit.reset_mock()
            fluidcr._start_watchdog_thread.reset_mock()
            self.d.write_manifest([1], checkpoint_id="partial", restore_owned_resume=True, generation=2)
            scope["_maybe_coordinated_checkpoint"](None)
            fluidcr.perform_checkpoint_and_exit.assert_not_called()
            fluidcr._start_watchdog_thread.assert_not_called()
            self.d.survivor_pause_and_rebuild.assert_called_once()
            dist.get_rank = lambda: 1
            scope["_maybe_coordinated_checkpoint"](None)
            fluidcr.perform_checkpoint_and_exit.assert_called_once()
            fluidcr._start_watchdog_thread.assert_called_once()


if __name__ == "__main__":
    unittest.main()
