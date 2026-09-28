import importlib.util
import json
from pathlib import Path
import tempfile
import unittest

spec = importlib.util.spec_from_file_location("staged_probe", Path(__file__).with_name("staged_probe.py"))
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)


class StagedProbeTest(unittest.TestCase):
    def test_read_only_proof_and_rejections(self):
        for mode in ("valid", "zero-rank", "uid", "rank", "checkpoint", "generation", "targets", "full", "lock", "state", "pause", "launcher", "not-owned"):
            with self.subTest(mode=mode), tempfile.TemporaryDirectory() as tmp:
                root = Path(tmp)
                proc = root / "proc"
                proc.mkdir()
                rank = root / "trainer-1"
                rank.mkdir()
                (rank / "latest.pt").write_bytes(b"checkpoint")
                (rank / "lock").write_text("1")
                expected = dict(checkpointID="round", sourcePodUID="old", sourceNode="old-node", podName="trainer-1",
                                workloadUID="world", rank=1, generation=7, targets=[1])
                env = dict(FLUIDCR_POD_UID="old", FLUIDCR_NODE_NAME="old-node", FLUIDCR_POD_NAME="trainer-1",
                           FLUIDCR_SOURCE_WORLD_UID="world", RANK="1", FLUIDCR_CHECKPOINT_PATH=str(rank / "latest.pt"),
                           FLUIDCR_CHECKPOINT_DIR=str(root))
                manifest = dict(checkpointID="round", generation=7, targets=[1], restoreOwnedResume=True, noPeriodicResume=True)
                status = dict(checkpointID="round", state="CheckpointReady")
                if mode == "zero-rank":
                    expected.update(rank=0, targets=[0])
                    env["RANK"] = "0"
                    manifest["targets"] = [0]
                if mode == "not-owned": manifest["restoreOwnedResume"] = False
                if mode == "uid": env["FLUIDCR_POD_UID"] = "other"
                if mode == "rank": env["RANK"] = "0"
                if mode == "checkpoint": status["checkpointID"] = "other"
                if mode == "generation": manifest["generation"] = 8
                if mode == "targets": manifest["targets"] = [0]
                if mode == "full": manifest["targets"] = "all"
                if mode == "lock": (rank / "lock").write_text("99")
                if mode == "state": status["state"] = "Running"
                if mode == "pause": (rank / "pause-lock").write_text("12")
                (proc / "environ").write_bytes(b"\0".join((k + "=" + v).encode() for k, v in env.items()))
                (proc / "cmdline").write_bytes(b"python\0/opt/fluidcr/bin/fluidcr-launcher\0" if mode != "launcher" else b"python\0other.py\0")
                (root / "migration-manifest.json").write_text(json.dumps(manifest))
                (rank / ".fluidcr-runtime.json").write_text(json.dumps(status))
                before = {str(p): p.read_bytes() for p in root.rglob("*") if p.is_file()}
                if mode in ("valid", "zero-rank"): module.probe(expected, proc)
                else:
                    with self.assertRaises(RuntimeError): module.probe(expected, proc)
                after = {str(p): p.read_bytes() for p in root.rglob("*") if p.is_file()}
                self.assertEqual(before, after)


if __name__ == "__main__":
    unittest.main()
