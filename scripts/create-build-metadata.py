#!/usr/bin/env python3
import hashlib
import json
import os
import pathlib
import subprocess
import sys
import xml.etree.ElementTree as ET

root = pathlib.Path(sys.argv[1])
repo_root = pathlib.Path(__file__).resolve().parents[1]
repository = os.environ["GITHUB_REPOSITORY"]
source_repository = os.environ["SOURCE_REPOSITORY"]
source_sha = os.environ["SOURCE_SHA"].lower()
event = os.environ["GITHUB_EVENT_NAME"]
candidate_version = os.environ.get("CANDIDATE_VERSION", "")
if not candidate_version or "-SNAPSHOT" in candidate_version:
    raise SystemExit("reactor root does not contain an immutable candidate version")
if event == "pull_request":
    pull_request = os.environ.get("PULL_REQUEST_NUMBER", "")
    if not pull_request.isdigit():
        raise SystemExit("pull request number is required")
    pull_request_number = int(pull_request)
    expected_suffix = f"-pr.{pull_request_number}.{source_sha[:12]}"
elif event == "push":
    pull_request_number = None
    expected_suffix = f"-main.{source_sha[:12]}"
else:
    raise SystemExit(f"unsupported candidate build event: {event}")
if not source_sha or len(source_sha) != 40 or not candidate_version.endswith(expected_suffix):
    raise SystemExit("candidate version does not match source event and full SHA")

coordinates = json.loads(subprocess.check_output(["python3", str(repo_root / "scripts/candidate-coordinates.py")], text=True))
artifacts = []
for coordinate in coordinates:
    group_id, artifact_id, packaging = coordinate["groupId"], coordinate["artifactId"], coordinate["packaging"]
    directory = root / "repository" / pathlib.Path(*group_id.split("."), artifact_id, candidate_version)
    names = [f"{artifact_id}-{candidate_version}.pom"]
    if packaging != "pom":
        names.append(f"{artifact_id}-{candidate_version}.jar")
    files = []
    for name in names:
        path = directory / name
        if not path.is_file() or path.is_symlink():
            raise SystemExit(f"missing candidate file: {path}")
        if name.endswith(".pom"):
            pom = ET.parse(path).getroot()
            ns = "{http://maven.apache.org/POM/4.0.0}"
            parent = pom.find(ns + "parent")
            actual = (
                pom.findtext(ns + "groupId") or (parent.findtext(ns + "groupId") if parent is not None else ""),
                pom.findtext(ns + "artifactId", ""),
                pom.findtext(ns + "version") or (parent.findtext(ns + "version") if parent is not None else ""),
            )
            if actual != (group_id, artifact_id, candidate_version):
                raise SystemExit(f"candidate-version output does not match installed POM for {group_id}:{artifact_id}")
        files.append({"name": name, "sha256": hashlib.sha256(path.read_bytes()).hexdigest()})
    artifacts.append({"groupId": group_id, "artifactId": artifact_id, "version": candidate_version,
                      "packaging": packaging, "files": files})

metadata = {
    "schemaVersion": 1,
    "repository": repository,
    "sourceRepository": source_repository,
    "component": "connectors",
    "sourceSha": source_sha,
    "pullRequestNumber": pull_request_number,
    "candidateVersion": candidate_version,
    "provenance": {"build": {
        "repository": repository,
        "runId": int(os.environ["GITHUB_RUN_ID"]),
        "runAttempt": int(os.environ["GITHUB_RUN_ATTEMPT"]),
        "workflowPath": ".github/workflows/tpf-candidate-build.yml",
        "event": event,
    }},
    "mavenArtifacts": artifacts,
}
(root / "build-metadata.json").write_text(json.dumps(metadata, indent=2, sort_keys=True) + "\n")
