"""PyTorch backend tracking regressions without importing torch."""
import importlib.util
from pathlib import Path
import sys
import tempfile
import types
import unittest
import uuid
import weakref
from unittest.mock import Mock, patch


def load_pytorch_backend(checkpoint_path):
    package = types.ModuleType("fluidcr")
    package.__path__ = [str(Path(__file__).resolve().parents[1] / "fluidcr")]
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
        "pytorch_tracking_" + uuid.uuid4().hex, source
    )
    module = importlib.util.module_from_spec(spec)
    with patch.dict(sys.modules, {
        "fluidcr": package,
        "fluidcr._config": config,
        "fluidcr.backends": backends,
    }):
        spec.loader.exec_module(module)
    return module


class FakeParam:
    pass


class FakeModel:
    def __init__(self, name="model"):
        self.name = name
        self.param = FakeParam()
        self.loaded = None

    def parameters(self):
        return iter([self.param])

    def modules(self):
        return iter([self])

    def state_dict(self):
        return {"weight": self.name}

    def load_state_dict(self, state):
        self.loaded = state


class FakeDDP:
    def __init__(self, module):
        self.module = module

    def parameters(self):
        return self.module.parameters()

    def modules(self):
        return iter([self, self.module])


class FakeOptimizer:
    def __init__(self):
        self._fluidcr_step = 0
        self.param_groups = []
        self.state = {}
        self.loaded = None

    def state_dict(self):
        return {"state": "optimizer"}

    def load_state_dict(self, state):
        self.loaded = state


class PytorchTrackingTests(unittest.TestCase):
    def test_root_model_dedup_uses_canonical_module_identity(self):
        backend = load_pytorch_backend("unused.pt")
        first = FakeDDP(FakeModel("same-keys"))
        duplicate_ref = first
        second = FakeDDP(FakeModel("same-keys"))
        backend._tracked_models[:] = [
            weakref.ref(first),
            weakref.ref(first.module),
            weakref.ref(duplicate_ref),
            weakref.ref(second),
            weakref.ref(second.module),
        ]

        roots = backend._root_models()

        self.assertEqual(roots, [first, second])
        self.assertIs(backend._unwrap_model(roots[0]), first.module)
        self.assertIs(backend._unwrap_model(roots[1]), second.module)
        self.assertEqual(roots[0].module.state_dict(), roots[1].module.state_dict())

    def test_sequential_survivor_rebuild_checkpoint_restores_fresh_rank(self):
        with tempfile.TemporaryDirectory() as temp:
            checkpoint = Path(temp) / "latest.pt"
            survivor = load_pytorch_backend(checkpoint)
            model = FakeDDP(FakeModel("survivor"))
            optimizer = FakeOptimizer()
            survivor._tracked_models[:] = [weakref.ref(model), weakref.ref(model)]
            survivor._tracked_optimizers[:] = [weakref.ref(optimizer)]
            payloads = []
            torch = types.ModuleType("torch")
            torch.save = lambda payload, path: payloads.append(payload) or Path(path).write_text("ckpt")

            with patch.dict(sys.modules, {"torch": torch}), \
                 patch.object(survivor.group_restore, "capture_round", return_value=None), \
                 patch.object(survivor.group_restore, "publish_round"), \
                 patch.object(survivor, "_collect_rng_states", return_value={"torch_cpu": "rng"}):
                survivor._save_checkpoint(str(checkpoint))

            self.assertEqual(len(payloads[0]["models"]), 1)
            self.assertEqual(len(payloads[0]["optimizers"]), 1)

            fresh = load_pytorch_backend(checkpoint)
            fresh_model = FakeDDP(FakeModel("fresh"))
            fresh_optimizer = FakeOptimizer()
            fresh._tracked_models[:] = [weakref.ref(fresh_model)]
            fresh._tracked_optimizers[:] = [weakref.ref(fresh_optimizer)]
            torch.load = Mock(return_value=payloads[0])
            torch.Tensor = object

            with patch.dict(sys.modules, {"torch": torch}), \
                 patch.object(fresh, "_checkpoint_load_path", return_value=str(checkpoint)), \
                 patch.object(fresh, "_restore_rng_states"):
                fresh._try_load_checkpoint()

            self.assertEqual(fresh_model.module.loaded, {"weight": "survivor"})
            self.assertEqual(fresh_optimizer.loaded, {"state": "optimizer"})


if __name__ == "__main__":
    unittest.main()
