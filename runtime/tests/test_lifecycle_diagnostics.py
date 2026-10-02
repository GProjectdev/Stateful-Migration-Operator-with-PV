"""Regression tests for quiet-mode lifecycle diagnostics.

The tests AST-extract only the relevant functions so they do not import
FluidCR, install signal handlers, or require GPU libraries.
"""

import ast
import io
import threading
import unittest
from pathlib import Path
from types import SimpleNamespace


def _repo_roots():
    root = Path(__file__).resolve().parents[1]
    if root.name == "runtime":
        sibling = root.parents[1] / "My_FluidCR-work"
    else:
        sibling = root.parent / "Stateful-Migration-System" / "runtime"
    return [path for path in (root, sibling) if path.exists()]


def _functions(path, names):
    tree = ast.parse(path.read_text(encoding="utf-8"))
    found = [
        node for node in tree.body
        if isinstance(node, ast.FunctionDef) and node.name in names
    ]
    assert len(found) == len(names), (path, names)
    return ast.Module(body=found, type_ignores=[])


class LifecycleDiagnosticTests(unittest.TestCase):
    def test_watchdog_diagnostics_are_visible_and_cancellable(self):
        names = {
            "_lifecycle_diagnostic",
            "_start_watchdog_thread",
            "cancel_checkpoint_watchdog",
        }
        for root in _repo_roots():
            path = root / "fluidcr" / "__init__.py"
            with self.subTest(path=str(path)):
                callbacks = []
                exits = []

                class DeferredThread:
                    def __init__(self, target, daemon):
                        self.target = target

                    def start(self):
                        callbacks.append(self.target)

                stderr = io.StringIO()
                scope = {
                    "sys": SimpleNamespace(stderr=stderr),
                    "threading": SimpleNamespace(
                        Event=threading.Event,
                        Thread=DeferredThread,
                    ),
                    "_active_watchdog_cancel": None,
                    "_SIGUSR1_WATCHDOG_TIMEOUT": 0,
                    "warn": lambda message: None,
                    "os": SimpleNamespace(_exit=exits.append),
                    "EXIT_CODE": 99,
                }
                exec(compile(_functions(path, names), str(path), "exec"), scope)

                scope["_start_watchdog_thread"]()
                scope["_start_watchdog_thread"]()
                scope["cancel_checkpoint_watchdog"]()
                callbacks[0]()
                self.assertEqual(exits, [])

                scope["_start_watchdog_thread"]()
                callbacks[1]()
                self.assertEqual(exits, [99])

                output = stderr.getvalue()
                self.assertIn("checkpoint watchdog armed", output)
                self.assertIn("checkpoint watchdog already armed", output)
                self.assertIn("checkpoint watchdog cancelled", output)
                self.assertIn("checkpoint watchdog fired", output)

    def test_duplicate_sigusr1_during_coordinated_phase_is_ignored(self):
        names = {
            "_lifecycle_diagnostic",
            "cancel_checkpoint_watchdog",
            "begin_coordinated_checkpoint",
            "clear_migration_flags",
            "_sigusr1_handler",
        }
        for root in _repo_roots():
            path = root / "fluidcr" / "__init__.py"
            with self.subTest(path=str(path)):
                stderr = io.StringIO()
                phase = threading.Event()
                arms = []

                class CancelEvent:
                    cancelled = False

                    def set(self):
                        self.cancelled = True
                        self.assert_phase_was_active()

                    def assert_phase_was_active(self):
                        assert phase.is_set()

                cancel = CancelEvent()
                scope = {
                    "sys": SimpleNamespace(stderr=stderr),
                    "Any": object,
                    "_active_watchdog_cancel": cancel,
                    "_coordinated_checkpoint_active": phase,
                    "_checkpoint_requested": threading.Event(),
                    "_start_watchdog_thread": lambda: arms.append("armed"),
                }
                exec(compile(_functions(path, names), str(path), "exec"), scope)

                scope["begin_coordinated_checkpoint"]()
                self.assertTrue(phase.is_set())
                self.assertTrue(cancel.cancelled)

                scope["_sigusr1_handler"](None, None)
                self.assertEqual(arms, [])
                self.assertIn(
                    "checkpoint signal ignored phase=coordinated-active",
                    stderr.getvalue(),
                )

                scope["clear_migration_flags"]()
                self.assertFalse(phase.is_set())

    def test_target_save_exit_rearms_watchdog_during_coordinated_phase(self):
        for root in _repo_roots():
            path = root / "fluidcr" / "__init__.py"
            with self.subTest(path=str(path)):
                tree = ast.parse(path.read_text(encoding="utf-8"))
                funcs = {
                    node.name: node for node in tree.body
                    if isinstance(node, ast.FunctionDef)
                }
                perform = funcs["perform_checkpoint_and_exit"]
                calls = [
                    node for node in ast.walk(perform)
                    if isinstance(node, ast.Call)
                    and isinstance(node.func, ast.Name)
                    and node.func.id == "_start_watchdog_thread"
                ]
                self.assertTrue(calls)
    def test_launcher_child_exit_diagnostic_ignores_debug_flag(self):
        names = {"_lifecycle_log", "_spawn_worker"}
        for root in _repo_roots():
            path = root / "fluidcr" / "launcher.py"
            with self.subTest(path=str(path)):
                class FakeProcess:
                    pid = 4321
                    returncode = None

                    def wait(self):
                        self.returncode = 99

                events = []
                stderr = io.StringIO()
                scope = {
                    "sys": SimpleNamespace(stderr=stderr),
                    "List": list,
                    "Dict": dict,
                    "subprocess": SimpleNamespace(
                        Popen=lambda command, env: FakeProcess()
                    ),
                    "os": SimpleNamespace(getpid=lambda: 1234),
                    "record_runtime_status": lambda **kwargs: events.append(kwargs),
                    "register_worker_pid": lambda launcher, worker: events.append(
                        ("register", launcher, worker)
                    ),
                    "unregister_worker_pid": lambda launcher: events.append(
                        ("unregister", launcher)
                    ),
                    "_log": lambda message: None,
                }
                exec(compile(_functions(path, names), str(path), "exec"), scope)

                self.assertEqual(scope["_spawn_worker"](["train"], {}), 99)
                self.assertIn(
                    "worker child exited pid=4321 exit_code=99",
                    stderr.getvalue(),
                )


if __name__ == "__main__":
    unittest.main()