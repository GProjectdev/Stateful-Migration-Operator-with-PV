"""CPU-only publication races and checkpoint failure regressions."""
import ast
import importlib.util
import os
from pathlib import Path
import shutil
import sys
import tempfile
import threading
import types
import unittest
from unittest.mock import Mock, patch

ROOT = Path(__file__).resolve().parents[1] / "fluidcr"


def load(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def extract(path, name, scope):
    tree = ast.parse(path.read_text(encoding="utf-8"))
    nodes = [n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == name]
    exec(compile(ast.Module(body=nodes, type_ignores=[]), str(path), "exec"), scope)
    return scope[name]


class PublicationTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        env = patch.dict(os.environ, FLUIDCR_CHECKPOINT_DIR=self.tmp.name,
                         FLUIDCR_SOURCE_WORLD_UID="world", RANK="0", WORLD_SIZE="2")
        env.start()
        self.addCleanup(env.stop)
        self.d = load("publication_distributed", ROOT / "distributed.py")
        package = types.ModuleType("fluidcr")
        package.distributed = self.d
        with patch.dict(sys.modules, fluidcr=package):
            self.g = load("publication_group", ROOT / "group_restore.py")
        self.d.bump_generation()
        self.d.write_manifest("all", checkpoint_id="first")
        self.live = Path(self.tmp.name) / "rank0" / "latest.pt"
        self.live.parent.mkdir()
        self.live.write_bytes(b"old checkpoint")
        self.context = self.g.capture_round(str(self.live))
        self.marker = Path(self.g._path("rounds/first/ranks/0.json"))
        self.artifact = Path(self.g._path(self.context["artifactPath"]))

    def publish(self):
        self.g.publish_round(str(self.live), self.context)

    def assert_clean_failure(self):
        self.assertFalse(self.marker.exists())
        self.assertFalse(self.artifact.exists())
        self.assertFalse(Path(self.g._path(".fluidcr-group-control.lock")).exists())
        self.assertEqual(list(self.live.parent.rglob(".artifact-*")), [])

    def test_copy_hash_fsync_outside_lock_and_idempotent_retry(self):
        original_copy, original_digest, original_sync = shutil.copyfileobj, self.g._digest, os.fsync
        def unlocked(func):
            def run(*args, **kwargs):
                self.assertFalse(Path(self.g._path(".fluidcr-group-control.lock")).exists())
                return func(*args, **kwargs)
            return run
        with patch.object(self.g.shutil, "copyfileobj", unlocked(original_copy)), \
             patch.object(self.g, "_digest", unlocked(original_digest)):
            self.publish()
            before = self.marker.read_bytes()
            # Force atime changes while hashing the existing artifact.
            def digest(path):
                result = unlocked(original_digest)(path)
                if Path(path) == self.artifact:
                    stat = os.stat(path)
                    os.utime(path, ns=(stat.st_atime_ns + 1000000000, stat.st_mtime_ns))
                return result
            with patch.object(self.g, "_digest", digest):
                self.publish()
            self.assertEqual(before, self.marker.read_bytes())
        # Metadata writes may fsync under lock; only artifact fsync is expensive.
        with patch.object(self.g.os, "fsync", unlocked(original_sync)), \
             patch.object(self.g, "_write"):
            self.publish()

    def test_round_or_generation_change_during_copy_rejects_commit(self):
        original = shutil.copyfileobj
        for change in ("round", "generation"):
            with self.subTest(change=change):
                def copy(inp, out):
                    original(inp, out)
                    with self.g.control_lock():
                        if change == "round":
                            self.d.write_manifest("all", checkpoint_id="second")
                        else:
                            self.d.bump_generation()
                with patch.object(self.g.shutil, "copyfileobj", copy):
                    with self.assertRaises(self.g.GroupConflict):
                        self.publish()
                self.assert_clean_failure()
                self.d.write_manifest("all", checkpoint_id="first")
                self.context = self.g.capture_round(str(self.live))

    def test_copy_failure_has_no_success_marker(self):
        with patch.object(self.g.shutil, "copyfileobj", side_effect=OSError("disk full")):
            with self.assertRaises(OSError):
                self.publish()
        self.assert_clean_failure()

    def test_two_ranks_copy_concurrently_and_publish_both_receipts(self):
        other = Path(self.tmp.name) / "rank1" / "latest.pt"
        other.parent.mkdir()
        other.write_bytes(b"rank one checkpoint")
        with patch.dict(os.environ, RANK="1"):
            other_context = self.g.capture_round(str(other))
        barrier = threading.Barrier(2, timeout=3)
        original = shutil.copyfileobj
        errors = []
        def copy(inp, out):
            barrier.wait()
            original(inp, out)
        def publish(path, context):
            try:
                self.g.publish_round(str(path), context)
            except BaseException as exc:
                errors.append(exc)
        threads = [
            threading.Thread(target=publish, args=(self.live, self.context), daemon=True),
            threading.Thread(target=publish, args=(other, other_context), daemon=True),
        ]
        with patch.object(self.g.shutil, "copyfileobj", copy):
            for thread in threads:
                thread.start()
            for thread in threads:
                thread.join(timeout=10)
        self.assertTrue(all(not thread.is_alive() for thread in threads))
        self.assertEqual(errors, [])
        for rank, context, data in (
                (0, self.context, self.live.read_bytes()),
                (1, other_context, other.read_bytes())):
            receipt = self.g._read(self.g._path("rounds/first/ranks/{}.json".format(rank)))
            self.assertEqual(receipt["rank"], rank)
            artifact = self.g._path(context["artifactPath"])
            self.assertEqual(Path(artifact).read_bytes(), data)
            self.assertEqual(receipt["sha256"], self.g._digest(artifact))
        self.assertFalse(Path(self.g._path(".fluidcr-group-control.lock")).exists())

    def test_fsync_failure_has_no_success_marker(self):
        with patch.object(self.g.os, "fsync", side_effect=OSError("fsync failed")):
            with self.assertRaises(OSError):
                self.publish()
        self.assert_clean_failure()

    def test_prepared_operation_during_copy_rejects_commit(self):
        original = shutil.copyfileobj
        def copy(inp, out):
            original(inp, out)
            with self.g.control_lock():
                self.g._write(self.g._active_path(), {"state": "prepared"})
        with patch.object(self.g.shutil, "copyfileobj", copy):
            with self.assertRaises(self.g.GroupConflict):
                self.publish()
        self.assert_clean_failure()

    def test_previous_round_survives_new_round_and_conflicting_retry(self):
        self.publish()
        old_marker, old_artifact = self.marker.read_bytes(), self.artifact.read_bytes()
        self.live.write_bytes(b"new checkpoint")
        with self.assertRaises(self.g.GroupConflict):
            self.publish()
        self.d.bump_generation()
        self.d.write_manifest("all", checkpoint_id="second")
        self.context = self.g.capture_round(str(self.live))
        self.publish()
        self.assertEqual(self.marker.read_bytes(), old_marker)
        self.assertEqual(self.artifact.read_bytes(), old_artifact)
        self.assertEqual(Path(self.g._path(self.context["artifactPath"])).read_bytes(), b"new checkpoint")


class FailureTests(unittest.TestCase):
    def test_save_exception_never_reports_saved_or_success_exit(self):
        statuses, exits = [], []
        ctrl = types.ModuleType("fluidcr.ctrl")
        ctrl.record_runtime_status = lambda **kwargs: statuses.append(kwargs["state"])
        backend = types.SimpleNamespace(name="test", save_checkpoint=Mock(side_effect=OSError("disk full")))
        scope = dict(Any=object, _coordinated_checkpoint_active=threading.Event(),
                     _lifecycle_diagnostic=Mock(), log=Mock(), warn=Mock(),
                     global_step=lambda: 1, CHECKPOINT_PATH="unused", EXIT_CODE=99,
                     CHECKPOINT_FAILURE_EXIT_CODE=98, os=types.SimpleNamespace(_exit=exits.append))
        perform = extract(ROOT / "__init__.py", "perform_checkpoint_and_exit", scope)
        with patch.dict(sys.modules, {"fluidcr.ctrl": ctrl}):
            perform(backend)
        self.assertEqual(exits, [98])
        self.assertEqual(statuses, ["CheckpointSaving"])

    def test_launcher_failure_never_creates_ready_lock(self):
        statuses = []
        create_lock = Mock()
        scope = dict(List=list, ensure_api_server_started=Mock(), _log=Mock(),
                     _lifecycle_log=Mock(), _build_env=lambda: {},
                     _spawn_worker=lambda *args: 98, SNAPSHOT_EXIT_CODE=99,
                     record_runtime_status=lambda **kw: statuses.append(kw["state"]),
                     _create_lock=create_lock)
        main = extract(ROOT / "launcher.py", "main", scope)
        self.assertEqual(main(["train"]), 98)
        self.assertEqual(statuses, ["Failed"])
        create_lock.assert_not_called()


if __name__ == "__main__":
    unittest.main()
