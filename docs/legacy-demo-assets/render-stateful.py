"""Render explicit controller modes from pinned Old-Version manifests."""
import argparse
from pathlib import Path
import yaml

p = argparse.ArgumentParser()
p.add_argument("repo", type=Path)
p.add_argument("output", type=Path)
p.add_argument("--account", required=True)
p.add_argument("--tag", required=True)
a = p.parse_args()
a.output.mkdir(parents=True, exist_ok=True)
def read(name):
    return [x for x in yaml.safe_load_all((a.repo / name).read_text(encoding="utf-8")) if x]
def write(name, docs):
    (a.output / name).write_text(yaml.safe_dump_all(docs, sort_keys=False), encoding="utf-8")
docs = read("deploy/all-in-one.yaml")
backup = next(d for d in docs if d["kind"] == "Deployment")
c = backup["spec"]["template"]["spec"]["containers"][0]
c["image"] = f"{a.account}/stateful-migration-operator:migrationBackup_{a.tag}"
c["args"] += ["--enable-checkpoint-backup-controller=false",
              "--enable-migration-backup-controller=true",
              "--enable-migration-restore-controller=false"]
write("backup.yaml", docs)
docs = read("config/rbac/checkpoint_backup_rbac.yaml") + read("config/checkpoint-backup/daemonset.yaml")
ds = next(d for d in docs if d["kind"] == "DaemonSet")
ds["spec"]["template"]["spec"]["containers"][0]["image"] = f"{a.account}/stateful-migration-operator:checkpointBackup_{a.tag}"
write("checkpoint.yaml", docs)
docs = []
for name in ("sa.yaml", "clusterrole.yaml", "clusterrolebinding.yaml", "deployment.yaml"):
    docs += read("suspension/deploy/" + name)
deployment = next(d for d in docs if d["kind"] == "Deployment")
deployment["spec"]["replicas"] = 0
deployment["spec"]["template"]["spec"]["containers"][0]["image"] = f"{a.account}/karmada-suspension:{a.tag}"
write("suspension.yaml", docs)
print("Rendered backup.yaml, checkpoint.yaml, suspension.yaml")
