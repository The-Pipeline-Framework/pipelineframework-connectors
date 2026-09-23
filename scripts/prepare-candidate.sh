#!/usr/bin/env bash
set -euo pipefail

mode=${1:?usage: prepare-candidate.sh pull_request|push PR_NUMBER SHA}
pr_number=${2:--}
sha=${3:?}
repo_root=$(cd "$(dirname "$0")/.." && pwd)
base_version=$(python3 - "$repo_root/pom.xml" <<'PY'
import sys
import xml.etree.ElementTree as ET
version = ET.parse(sys.argv[1]).getroot().findtext("{http://maven.apache.org/POM/4.0.0}version", "")
if not version.endswith("-SNAPSHOT"):
    raise SystemExit("root project version must end in -SNAPSHOT")
print(version.removesuffix("-SNAPSHOT"))
PY
)
case "$mode" in
  pull_request) version_mode=pr ;;
  push) version_mode=main ;;
  *) echo "event must be pull_request or push" >&2; exit 2 ;;
esac
candidate=$(bash "$repo_root/scripts/candidate-version.sh" "$version_mode" "$base_version" "$pr_number" "$sha")

cd "$repo_root"
"$repo_root/mvnw" -B -N -Dmaven.repo.local="$repo_root/.m2/repository" \
  -Dmaven.deploy.skip=true -Dgpg.skip=true -Dtpf.flatten.skip=true \
  org.codehaus.mojo:versions-maven-plugin:2.22.0:set \
  -DnewVersion="$candidate" -DprocessAllModules=true -DgenerateBackupPoms=false
python3 "$repo_root/scripts/rewrite-reactor-dependencies.py" "$repo_root" "$candidate"
echo "candidate=$candidate" >> "${GITHUB_OUTPUT:-/dev/null}"
