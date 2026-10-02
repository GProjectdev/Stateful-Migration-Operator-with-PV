"""Test watchdog functions without importing GPU and Linux signal hooks."""
import ast
from pathlib import Path
from types import SimpleNamespace
import threading
import unittest


class WatchdogTests(unittest.TestCase):
    def test_repeated_signal_then_survivor_cancel(self):
        overlay = Path(__file__).resolve().parents[1] / "fluidcr" / "__init__.py"
        base = overlay.parents[3] / "My_FluidCR-work" / "fluidcr" / "__init__.py"
        for path in [overlay] + ([base] if base.exists() else []):
            with self.subTest(path=str(path)):
                tree = ast.parse(path.read_text(encoding="utf-8"))
                names = {"_start_watchdog_thread", "cancel_checkpoint_watchdog"}
                functions = [n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name in names]
                self.assertEqual(len(functions), 2)
                callbacks, exits = [], []
                class DeferredThread:
                    def __init__(self, target, daemon):
                        self.target = target
                    def start(self):
                        callbacks.append(self.target)
                scope = dict(threading=SimpleNamespace(Event=threading.Event, Thread=DeferredThread),
                             _active_watchdog_cancel=None, _SIGUSR1_WATCHDOG_TIMEOUT=0,
                             warn=lambda message: None, _lifecycle_diagnostic=lambda message: None, os=SimpleNamespace(_exit=exits.append), EXIT_CODE=99)
                exec(compile(ast.Module(body=functions, type_ignores=[]), str(path), "exec"), scope)
                scope["_start_watchdog_thread"]()
                scope["_start_watchdog_thread"]()
                scope["cancel_checkpoint_watchdog"]()
                for callback in callbacks:
                    callback()
                self.assertEqual(exits, [], "orphan watchdog killed the survivor")
                self.assertEqual(len(callbacks), 1)
                scope["_start_watchdog_thread"]()
                self.assertEqual(len(callbacks), 2)
                callbacks[-1]()
                self.assertEqual(exits, [99])


if __name__ == "__main__":
    unittest.main()
