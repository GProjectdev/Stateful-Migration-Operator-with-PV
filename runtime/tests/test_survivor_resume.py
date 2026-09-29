import importlib.util
import json
import os
from pathlib import Path
import tempfile
import types
import sys
import unittest
from unittest.mock import Mock, patch

from test_ctrl import ctrl

class SurvivorResumeTests(unittest.TestCase):
    def test_rebuild_receipt_requires_authorization_and_success(self):
        source = Path(__file__).resolve().parents[1] / "fluidcr" / "distributed.py"
        spec = importlib.util.spec_from_file_location("distributed_test", source)
        d = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(d)
        for mode in ("success", "unauthorized", "rebuild-failed"):
            with self.subTest(mode=mode), tempfile.TemporaryDirectory() as temp:
                ckpt = str(Path(temp) / "latest.pt")
                proof = {"pid": os.getpid(), "podUID": "uid", "restoreOwnedResume": True}
                if mode != "unauthorized": proof["releaseAuthorizedAt"] = "approved"
                package = types.ModuleType("fluidcr")
                package.log = Mock()
                package.clear_migration_flags = Mock()
                with patch.dict(sys.modules, {"fluidcr": package, "fluidcr.ctrl": ctrl}), patch.dict(os.environ, {"FLUIDCR_CHECKPOINT_PATH": ckpt, "FLUIDCR_POD_UID": "uid"}), patch.object(d, "_destroy_pg_safely"), patch.object(d, "_write_pause_lock"), patch.object(d, "_wait_for_pause_lock_removal"), patch.object(d, "_reinit_process_group"), patch.object(d, "read_survivor_proof", return_value=proof), patch.object(d, "rebuild_tracked_ddp", side_effect=RuntimeError("failed") if mode == "rebuild-failed" else None):
                    ctrl._write_status(ckpt, {"checkpointID": "old-loaded-round"})
                    if mode == "success":
                        d.survivor_pause_and_rebuild()
                        result = json.loads((Path(temp) / ".fluidcr-survivor.json").read_text())
                        self.assertEqual(result["state"], "SurvivorResumed")
                        self.assertEqual(result["loadedCheckpointID"], "old-loaded-round")
                    else:
                        with self.assertRaises(RuntimeError): d.survivor_pause_and_rebuild()
                        self.assertFalse((Path(temp) / ".fluidcr-survivor.json").exists())

    def test_runtime_preserves_loaded_id_and_rejects_missing_pause_proof(self):
        for mode in ("resumed", "missing-lock", "wrong-pid", "new-checkpoint", "parked", "no-gpu"):
            with self.subTest(mode=mode), tempfile.TemporaryDirectory() as temp:
                ckpt = str(Path(temp) / "latest.pt")
                proof = {"state": "SurvivorResumed", "pid": 101, "podUID": "uid", "rank": 0, "nodeName": "node", "checkpointID": "partial-round", "loadedCheckpointID": "old-round", "generation": 7, "pauseLockPath": str(Path(temp)/"pause-lock"), "releaseAuthorizedAt": "approved", "resumedAt": "2026-09-29T16:40:00Z"}
                if mode in ("missing-lock", "parked"): proof["state"] = "SurvivorParked"
                if mode == "parked": Path(proof["pauseLockPath"]).write_text("101")
                if mode == "wrong-pid": proof["pid"] = 999
                d = types.ModuleType("fluidcr.distributed")
                d.read_survivor_proof = Mock(return_value=proof)
                with patch.dict(sys.modules, {"fluidcr.distributed": d}), patch.dict(os.environ, {"FLUIDCR_CHECKPOINT_PATH": ckpt, "FLUIDCR_POD_UID": "uid", "FLUIDCR_POD_NAME": "trainer-0", "FLUIDCR_NODE_NAME": "node", "RANK": "0", "WORLD_SIZE": "2"}), patch.object(ctrl, "registered_worker_pids", return_value={1:101}), patch.object(ctrl, "_pid_uses_gpu", return_value=mode != "no-gpu"):
                    loaded = "new-round" if mode == "new-checkpoint" else "old-round"
                    ctrl._write_status(ckpt, {"state":"Running", "globalStep": 42, "checkpointID": loaded})
                    handler = ctrl._CtrlRequestHandler.__new__(ctrl._CtrlRequestHandler)
                    handler.path = "/runtime"
                    handler._send_json = Mock()
                    handler.do_GET()
                    code, body = handler._send_json.call_args.args
                    if mode in ("missing-lock", "wrong-pid"):
                        self.assertEqual(code, 503)
                    else:
                        self.assertEqual(code, 200)
                        self.assertEqual(body["checkpointID"], "partial-round" if mode == "parked" else loaded)
                        self.assertEqual("survivorResume" in body, mode in ("resumed", "no-gpu"))
                        self.assertEqual(body["checkpointReady"], mode not in ("parked", "no-gpu"))
