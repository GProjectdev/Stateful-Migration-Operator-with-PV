"""Render from saved live JSON; never contact a cluster."""
import argparse
import json
from pathlib import Path

p = argparse.ArgumentParser()
p.add_argument("input")
p.add_argument("output")
p.add_argument("--image", required=True)
p.add_argument("--pause-pv-migration", action="store_true")
a = p.parse_args()
d = json.loads(Path(a.input).read_text(encoding="utf-8"))
assert d["kind"] == "Deployment"
assert d["metadata"]["name"] == "karmada-controller-manager"
matches = [c for c in d["spec"]["template"]["spec"]["containers"]
           if "karmada-controller-manager" in c.get("image", "")]
assert len(matches) == 1, "Inspect actual manager container manually"
c = matches[0]
assert c.get("command"), "Legacy image has no ENTRYPOINT; inspect existing command"
c["image"] = a.image
controllers = None
for field in ("command", "args"):
    src, out, i = c.get(field, []), [], 0
    while i < len(src):
        if src[i] == "--controllers":
            assert controllers is None and i + 1 < len(src), "Duplicate/invalid controllers flag"
            controllers, i = src[i + 1], i + 2
        elif src[i].startswith("--controllers="):
            assert controllers is None, "Duplicate controllers flag"
            controllers, i = src[i].split("=", 1)[1], i + 1
        else:
            out.append(src[i])
            i += 1
    if field in c:
        c[field] = out
names = [n for n in (controllers or "*").split(",")
         if n.lstrip("-") not in {"pvCleanup", "pvMigration", "pvSync"}]
names += ["pvSync", "-pvMigration" if a.pause_pv_migration else "pvMigration", "-pvCleanup"]
c.setdefault("args", []).append("--controllers=" + ",".join(names))
d["spec"]["strategy"] = {"type": "Recreate"}
d.pop("status", None)
for key in ("uid", "resourceVersion", "generation", "creationTimestamp", "managedFields"):
    d["metadata"].pop(key, None)
d["metadata"].get("annotations", {}).pop("kubectl.kubernetes.io/last-applied-configuration", None)
Path(a.output).write_text(json.dumps(d, indent=2) + "\n", encoding="utf-8")
print("Rendered", a.output, "with", c["args"][-1])

patch = [{"op": "replace", "path": "/spec/template", "value": d["spec"]["template"]},
         {"op": "replace", "path": "/spec/strategy", "value": d["spec"]["strategy"]}]
Path(a.output + ".patch.json").write_text(json.dumps(patch, indent=2) + "\n", encoding="utf-8")