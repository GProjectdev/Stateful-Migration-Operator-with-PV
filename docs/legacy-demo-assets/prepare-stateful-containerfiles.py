"""Extract pinned legacy image recipes without executing its Docker-based script."""
import argparse
import json
import re
from pathlib import Path

p = argparse.ArgumentParser()
p.add_argument("repo", type=Path)
p.add_argument("output", type=Path)
a = p.parse_args()
source = (a.repo / "build-and-push.sh").read_text(encoding="utf-8")
templates = re.findall(r"cat > \$\{dockerfile_name\} <<EOF\n(.*?)\nEOF", source, re.S)
if len(templates) != 2:
    raise SystemExit("Expected two legacy Dockerfile templates; inspect the source revision.")
a.output.mkdir(parents=True, exist_ok=True)
placeholder = "$" + "{flag_array}"
for mode, index in (("checkpoint", 0), ("migration", 1), ("restore", 1)):
    match = re.search(r'\["' + mode + r'"\]="(--enable-checkpoint-backup-controller=[^"\n]+)"', source)
    if not match:
        raise SystemExit("Missing controller flags: " + mode)
    flags = ", ".join(json.dumps(flag) for flag in match.group(1).split())
    recipe = templates[index].replace(placeholder, flags)
    # Unquoted shell heredocs originally remove dollar/backslash escaping.
    recipe = recipe.replace("\\$", "$").replace("\\\\", "\\")
    recipe = re.sub(r"(?m)^FROM (golang|alpine):", r"FROM docker.io/library/\1:", recipe)
    if placeholder in recipe:
        raise SystemExit("Unresolved controller mode")
    (a.output / ("Containerfile." + mode)).write_text(recipe + "\n", encoding="utf-8")
recipe = (a.repo / "suspension/Dockerfile").read_text(encoding="utf-8")
recipe = re.sub(r"(?m)^FROM golang:", "FROM docker.io/library/golang:", recipe)
(a.output / "Containerfile.suspension").write_text(recipe, encoding="utf-8")
print("Prepared checkpoint/migration/restore/suspension Containerfiles")
