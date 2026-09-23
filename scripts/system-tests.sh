#!/usr/bin/env bash
set -euo pipefail
read -r -a maven_args <<< "${MAVEN_ARGS:-}" || true

case "${1:-}" in
  verify)
    ./mvnw -B "${maven_args[@]}" \
      -Dmaven.deploy.skip=true -Dgpg.skip=true -Dtpf.flatten.skip=true verify
    ;;
  live-providers)
    cat >&2 <<'MESSAGE'
The connectors repository does not yet own a credentialed live-provider test lane.
Module verification exercises deterministic connector tests only and must not be
reported as live-provider coverage. Keep the coordinator full train disabled until
an owner-repository live-provider workflow and trusted relay are available.
MESSAGE
    exit 2
    ;;
  candidate-version)
    event=${2:?usage: system-tests.sh candidate-version pull_request|push PR_NUMBER SHA}
    number=${3:--}
    sha=${4:?}
    case "$event" in
      pull_request) mode=pr ;;
      push) mode=main ;;
      *) echo "unsupported event: $event" >&2; exit 2 ;;
    esac
    version=$(python3 - <<'PY'
import xml.etree.ElementTree as ET
import re
root = ET.parse("pom.xml").getroot()
version = root.findtext("{http://maven.apache.org/POM/4.0.0}version", "")
if version.endswith("-SNAPSHOT"):
    print(version.removesuffix("-SNAPSHOT"))
else:
    print(re.sub(r"-(?:pr\.\d+|main)\.[0-9a-f]{12}$", "", version))
PY
)
    actual=$(bash scripts/candidate-version.sh "$mode" "$version" "$number" "$sha")
    if [[ "$mode" == pr ]]; then expected="${version}-pr.${number}.${sha:0:12}"; else expected="${version}-main.${sha:0:12}"; fi
    [[ "$actual" == "$expected" ]] || { echo "candidate version mismatch" >&2; exit 1; }
    ;;
  *) echo "usage: system-tests.sh verify|live-providers|candidate-version pull_request|push PR_NUMBER SHA" >&2; exit 2 ;;
esac
