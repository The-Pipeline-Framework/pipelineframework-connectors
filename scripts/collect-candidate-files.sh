#!/usr/bin/env bash
set -euo pipefail

output_dir=${1:?usage: collect-candidate-files.sh OUTPUT_DIR}
[[ ! -e "$output_dir" ]] || { echo "output directory already exists: $output_dir" >&2; exit 2; }
repo_root=$(cd "$(dirname "$0")/.." && pwd)
python3 - "$repo_root" "$output_dir" <<'PY'
import json
import pathlib
import shutil
import subprocess
import sys
import xml.etree.ElementTree as ET

root = pathlib.Path(sys.argv[1]).resolve()
output = pathlib.Path(sys.argv[2]).resolve()
version = ET.parse(root / "pom.xml").getroot().findtext("{http://maven.apache.org/POM/4.0.0}version", "")
coordinates = json.loads(subprocess.check_output(["python3", str(root / "scripts/candidate-coordinates.py")], text=True))
local_repo = root / ".m2/repository"
repository = output / "repository"
for coordinate in coordinates:
    group, artifact, packaging = coordinate["groupId"], coordinate["artifactId"], coordinate["packaging"]
    source = local_repo.joinpath(*group.split("."), artifact, version)
    destination = repository.joinpath(*group.split("."), artifact, version)
    destination.mkdir(parents=True, exist_ok=True)
    expected = [f"{artifact}-{version}.pom"]
    if packaging != "pom":
        expected.append(f"{artifact}-{version}.jar")
    for name in expected:
        file = source / name
        if not file.is_file() or file.is_symlink():
            raise SystemExit(f"missing installed candidate artifact: {file}")
        shutil.copyfile(file, destination / name)
PY
