import contextlib
import json
import os
from pathlib import Path
import runpy
import sys
import tempfile
import types
import unittest
from unittest.mock import patch


class ResumeInPlaceTests(unittest.TestCase):
    def test_scoped_release(self):
        for case in ("success", "partial", "owned", "no-periodic", "wrong-id",
                     "wrong-local-id", "not-ready", "missing-manifest", "retry", "last-rank"):
            with self.subTest(case=case), tempfile.TemporaryDirectory() as temp:
                root = Path(temp)
                rank = root / "trainer-0"
                rank.mkdir()
                lock = rank / "lock"
                lock.write_text("1")
                (rank / "latest.pt").write_text("preserve")
                other = root / "trainer-1"
                other.mkdir()
                (other / "lock").write_text("1")
                if case == "last-rank": (other / "lock").unlink()
                manifest_path = root / "migration-manifest.json"
                manifest = {"checkpointID": "round", "targets": "all"}
                status = {"checkpointID": "round", "state": "CheckpointReady"}
                if case == "partial": manifest["targets"] = [0]
                if case == "owned": manifest["restoreOwnedResume"] = True
                if case == "no-periodic": manifest["noPeriodicResume"] = True
                if case == "wrong-id": manifest["checkpointID"] = "old"
                if case == "wrong-local-id": status["checkpointID"] = "old"
                if case == "not-ready": status["state"] = "Running"
                if case in ("missing-manifest", "retry"): manifest = {}
                if case == "retry": lock.unlink()
                manifest_path.write_text(json.dumps(manifest))
                module = types.ModuleType("fluidcr")
                module.ctrl = types.SimpleNamespace(_read_status=lambda _: status)
                module.distributed = types.SimpleNamespace(
                    read_manifest=lambda: manifest, _base_dir=lambda: str(root),
                    manifest_path=lambda: str(manifest_path))
                module.group_restore = types.SimpleNamespace(legacy_control=contextlib.nullcontext)
                with patch.dict(sys.modules, {"fluidcr": module}), patch.dict(
                        os.environ, {"FLUIDCR_CHECKPOINT_PATH": str(rank / "latest.pt")}), patch.object(
                        sys, "argv", ["resume", "round"]):
                    if case in ("success", "retry", "last-rank"):
                        runpy.run_path(str(Path(__file__).with_name("resume_in_place.py")))
                        self.assertFalse(lock.exists())
                    else:
                        with self.assertRaises(RuntimeError):
                            runpy.run_path(str(Path(__file__).with_name("resume_in_place.py")))
                        self.assertTrue(lock.exists())
                    self.assertEqual((other / "lock").exists(), case != "last-rank")
                    self.assertEqual(manifest_path.exists(), case != "last-rank")
                    self.assertEqual((rank / "latest.pt").read_text(), "preserve")


if __name__ == "__main__":
    unittest.main()
