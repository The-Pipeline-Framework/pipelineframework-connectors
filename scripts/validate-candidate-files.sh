#!/usr/bin/env bash
set -euo pipefail

root=${1:?usage: validate-candidate-files.sh BUILD_ROOT CURRENT_PR_JSON}
current_pr=${2:?}
python3 - "$root" "$current_pr" <<'PY'
import hashlib
import json
import os
import pathlib
import re
import subprocess
import sys
import xml.etree.ElementTree as ET

root = pathlib.Path(sys.argv[1])
pr_path = pathlib.Path(sys.argv[2])
base_version = ET.parse("pom.xml").getroot().findtext("{http://maven.apache.org/POM/4.0.0}version", "")
assert base_version.endswith("-SNAPSHOT") and re.fullmatch(r"\d+\.\d+\.\d+-SNAPSHOT", base_version)
base_version = base_version.removesuffix("-SNAPSHOT")
repo = "The-Pipeline-Framework/pipelineframework-connectors"
coordinates = json.loads(subprocess.check_output(["python3", "scripts/candidate-coordinates.py"], text=True))
metadata = json.loads((root / "build-metadata.json").read_text())
required = {"schemaVersion", "repository", "sourceRepository", "component", "sourceSha", "pullRequestNumber", "candidateVersion", "provenance", "mavenArtifacts"}
assert set(metadata) == required, "unexpected or missing build metadata fields"
assert metadata["schemaVersion"] == 1 and metadata["repository"] == repo and metadata["component"] == "connectors"
source_sha = metadata["sourceSha"]
assert re.fullmatch(r"[0-9a-f]{40}", source_sha), "invalid source SHA"
build = metadata["provenance"]["build"]
assert build == {"repository": repo, "runId": int(os.environ["BUILD_RUN_ID"]),
                 "runAttempt": int(os.environ["BUILD_RUN_ATTEMPT"]),
                 "workflowPath": ".github/workflows/tpf-candidate-build.yml",
                 "event": os.environ["BUILD_RUN_EVENT"]}, "build provenance does not match workflow_run"
assert os.environ["BUILD_RUN_PATH"] == ".github/workflows/tpf-candidate-build.yml"
assert os.environ["BUILD_RUN_REPOSITORY"] == repo
version = metadata["candidateVersion"]
event = os.environ["BUILD_RUN_EVENT"]
if event == "pull_request":
    number = metadata["pullRequestNumber"]
    assert isinstance(number, int) and number > 0
    associated = json.loads(os.environ.get("BUILD_ASSOCIATED_PR_NUMBERS", "[]"))
    assert associated and number in associated, "PR is not associated with triggering workflow run"
    assert re.fullmatch(rf"{re.escape(base_version)}-pr\.{number}\.{source_sha[:12]}", version)
    pr = json.loads(pr_path.read_text())
    assert pr["state"] == "open" and pr["number"] == number, "candidate PR is no longer open"
    head = pr["head"]
    assert head["sha"] == source_sha, "PR head changed since build"
    assert head["repo"]["full_name"].lower() == metadata["sourceRepository"].lower()
    assert os.environ["BUILD_RUN_HEAD_BRANCH"] == head["ref"]
    if metadata["sourceRepository"].lower() != repo.lower():
        assert "safe-to-system-test" in {label["name"] for label in pr.get("labels", [])}, "fork PR lacks safe-to-system-test label"
elif event == "push":
    assert metadata["pullRequestNumber"] is None and metadata["sourceRepository"] == repo
    assert os.environ["BUILD_RUN_HEAD_BRANCH"] == "main"
    assert source_sha == os.environ["BUILD_RUN_HEAD_SHA"]
    assert re.fullmatch(rf"{re.escape(base_version)}-main\.{source_sha[:12]}", version)
else:
    raise AssertionError(f"unsupported build event: {event}")

artifacts = metadata["mavenArtifacts"]
assert len(artifacts) == len(coordinates), "unexpected coordinate count"
expected_paths = set()
for artifact, coordinate in zip(artifacts, coordinates, strict=True):
    group, name, packaging = coordinate["groupId"], coordinate["artifactId"], coordinate["packaging"]
    assert set(artifact) == {"groupId", "artifactId", "version", "packaging", "files"}
    assert (artifact["groupId"], artifact["artifactId"], artifact["packaging"]) == (group, name, packaging)
    assert artifact["version"] == version
    directory = root / "repository" / pathlib.Path(*group.split(".")) / name / version
    names = [f"{name}-{version}.pom"]
    if packaging != "pom":
        names.append(f"{name}-{version}.jar")
    assert [item["name"] for item in artifact["files"]] == names, f"bad files for {name}"
    for item in artifact["files"]:
        path = directory / item["name"]
        assert path.is_file() and not path.is_symlink(), f"missing or unsafe candidate file: {path}"
        assert hashlib.sha256(path.read_bytes()).hexdigest() == item["sha256"], f"checksum mismatch: {path}"
        expected_paths.add(path.relative_to(root / "repository").as_posix())
    pom = ET.parse(directory / f"{name}-{version}.pom").getroot()
    assert pom.findtext("{http://maven.apache.org/POM/4.0.0}groupId") == group
    assert pom.findtext("{http://maven.apache.org/POM/4.0.0}artifactId") == name
    assert pom.findtext("{http://maven.apache.org/POM/4.0.0}version") == version
actual_paths = {path.relative_to(root / "repository").as_posix() for path in (root / "repository").rglob("*") if path.is_file()}
assert actual_paths == expected_paths, f"unexpected candidate repository files: {actual_paths ^ expected_paths}"
assert not any(path.is_symlink() for path in (root / "repository").rglob("*")), "symlink in candidate repository"
PY
