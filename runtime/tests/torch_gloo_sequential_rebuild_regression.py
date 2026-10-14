"""Optional real-torch CPU/Gloo regression for trainer environments.

Run from the repository root with a Python environment that has torch installed:

    python runtime/tests/torch_gloo_sequential_rebuild_regression.py

This is intentionally not part of unittest discovery. The required runtime image
owns the torch dependency; the local default Python used by lightweight runtime
tests may not have torch.

The survivor half installs the FluidCR torch patches and re-runs DDP.__init__ on
the same wrapper to exercise the duplicate tracking path. The fresh-rank half
loads that checkpoint through an explicitly seeded registry in the same Python
process; use a real multi-process trainer test for full two-rank/GPU coverage.
"""
import importlib.util
from pathlib import Path
import sys
import tempfile
import types
import uuid
from unittest.mock import Mock, patch


def load_backend(checkpoint_path):
    package = types.ModuleType("fluidcr")
    package.__path__ = [str(Path(__file__).resolve().parents[1] / "fluidcr")]
    package.distributed_checkpoint_enabled = lambda: False
    package.checkpoint_requested = lambda: False
    package.log = Mock()
    package.warn = Mock()
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
    spec = importlib.util.spec_from_file_location("real_torch_" + uuid.uuid4().hex, source)
    module = importlib.util.module_from_spec(spec)
    sys.modules.update({
        "fluidcr": package,
        "fluidcr._config": config,
        "fluidcr.backends": backends,
    })
    spec.loader.exec_module(module)
    return module


def main():
    import torch
    import torch.distributed as dist
    from torch import nn
    from torch.nn.parallel import DistributedDataParallel

    with tempfile.TemporaryDirectory() as temp:
        init_file = Path(temp) / "init"
        checkpoint = Path(temp) / "latest.pt"
        dist.init_process_group(
            backend="gloo",
            init_method=f"file://{init_file}",
            rank=0,
            world_size=1,
        )
        try:
            survivor = load_backend(checkpoint)
            survivor._apply_patches(torch)
            model = DistributedDataParallel(nn.Linear(2, 2))
            optimizer = torch.optim.SGD(model.parameters(), lr=0.1)
            DistributedDataParallel.__init__(model, model.module)
            with patch.object(survivor.group_restore, "capture_round", return_value=None), \
                 patch.object(survivor.group_restore, "publish_round"):
                survivor._save_checkpoint(str(checkpoint))
            payload = torch.load(checkpoint, map_location="cpu", weights_only=False)
            assert len(payload["models"]) == 1, payload
            assert len(payload["optimizers"]) == 1, payload

            fresh = load_backend(checkpoint)
            fresh_model = DistributedDataParallel(nn.Linear(2, 2))
            fresh_optimizer = torch.optim.SGD(fresh_model.parameters(), lr=0.1)
            fresh._tracked_models[:] = survivor._tracked_models[-1:]
            fresh._tracked_optimizers[:] = survivor._tracked_optimizers[-1:]
            with patch.object(fresh, "_checkpoint_load_path", return_value=str(checkpoint)):
                fresh._try_load_checkpoint()
            assert fresh_optimizer._fluidcr_step == optimizer._fluidcr_step
        finally:
            dist.destroy_process_group()


if __name__ == "__main__":
    main()
