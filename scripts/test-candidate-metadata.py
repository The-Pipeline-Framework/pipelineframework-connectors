#!/usr/bin/env python3
import hashlib
import json
import os
import pathlib
import subprocess
import tempfile

ROOT = pathlib.Path(__file__).resolve().parents[1]
COORDS = json.loads(subprocess.check_output(["python3", str(ROOT / "scripts/candidate-coordinates.py")], text=True))
VERSION = "26.9.4"

for event, number in (("pull_request", "42"), ("push", "")):
    sha = "0123456789abcdef0123456789abcdef01234567"
    candidate = f"{VERSION}-pr.{number}.{sha[:12]}" if number else f"{VERSION}-main.{sha[:12]}"
    with tempfile.TemporaryDirectory(prefix="tpf-connectors-metadata-") as temporary:
        root = pathlib.Path(temporary)
        repository = root / "repository"
        for coordinate in COORDS:
            group, artifact, packaging = coordinate["groupId"], coordinate["artifactId"], coordinate["packaging"]
            directory = repository.joinpath(*group.split("."), artifact, candidate)
            directory.mkdir(parents=True)
            (directory / f"{artifact}-{candidate}.pom").write_text(
                f'<project xmlns="http://maven.apache.org/POM/4.0.0"><groupId>{group}</groupId><artifactId>{artifact}</artifactId><version>{candidate}</version><packaging>{packaging}</packaging></project>\n')
            if packaging != "pom":
                (directory / f"{artifact}-{candidate}.jar").write_bytes(f"fixture:{artifact}".encode())
        env = os.environ | {
            "GITHUB_REPOSITORY": "The-Pipeline-Framework/pipelineframework-connectors",
            "SOURCE_REPOSITORY": "Contributor/pipelineframework-connectors" if number else "The-Pipeline-Framework/pipelineframework-connectors",
            "SOURCE_SHA": sha, "GITHUB_EVENT_NAME": event, "PULL_REQUEST_NUMBER": number,
            "GITHUB_RUN_ID": "1234", "GITHUB_RUN_ATTEMPT": "2",
        }
        subprocess.run(["python3", str(ROOT / "scripts/create-build-metadata.py"), str(root)], check=True,
                       cwd=ROOT, env=env | {"CANDIDATE_VERSION": candidate})
        metadata = json.loads((root / "build-metadata.json").read_text())
        assert metadata["candidateVersion"] == candidate
        assert metadata["pullRequestNumber"] == (42 if number else None)
        assert len(metadata["mavenArtifacts"]) == len(COORDS)
        assert metadata["provenance"]["build"]["event"] == event
        for artifact in metadata["mavenArtifacts"]:
            for item in artifact["files"]:
                path = repository.joinpath(*artifact["groupId"].split("."), artifact["artifactId"], candidate, item["name"])
                assert hashlib.sha256(path.read_bytes()).hexdigest() == item["sha256"]
        final_env = env | {"GITHUB_RUN_ID": "5678", "GITHUB_RUN_ATTEMPT": "1"}
        subprocess.run(["python3", str(ROOT / "scripts/finalize-candidate-manifest.py"), str(root)], check=True,
                       cwd=ROOT, env=final_env)
        manifest_bytes = (root / "candidate-manifest/candidate-manifest.json").read_bytes()
        manifest = json.loads(manifest_bytes)
        assert manifest["provenance"]["publication"]["workflowPath"] == ".github/workflows/tpf-candidate-publish.yml"
        assert manifest["provenance"]["build"]["repository"] == env["GITHUB_REPOSITORY"]
        event_doc = json.loads((root / "candidate-event/event.json").read_text())
        assert event_doc["manifest_sha256"] == hashlib.sha256(manifest_bytes).hexdigest()
        assert event_doc["publication_run_id"] == 5678
        pr_json = root / "current-pr.json"
        pr_json.write_text(json.dumps({"state": "open", "number": 42,
            "head": {"sha": sha, "ref": "fixture-branch", "repo": {"full_name": env["SOURCE_REPOSITORY"]}},
            "labels": [{"name": "safe-to-system-test"}]}))
        validate_env = env | {
            "BUILD_RUN_ID": "1234", "BUILD_RUN_ATTEMPT": "2", "BUILD_RUN_EVENT": event,
            "BUILD_RUN_HEAD_SHA": sha, "BUILD_RUN_HEAD_BRANCH": "fixture-branch" if number else "main",
            "BUILD_RUN_PATH": ".github/workflows/tpf-candidate-build.yml",
            "BUILD_RUN_REPOSITORY": "The-Pipeline-Framework/pipelineframework-connectors",
            "BUILD_ASSOCIATED_PR_NUMBERS": "[42]" if number else "[]",
        }
        if number:
            validate_env["BUILD_RUN_HEAD_SHA"] = "fedcba9876543210fedcba9876543210fedcba98"
        subprocess.run(["bash", str(ROOT / "scripts/validate-candidate-files.sh"), str(root), str(pr_json)],
                       check=True, cwd=ROOT, env=validate_env)
        if number:
            missing = validate_env | {"BUILD_ASSOCIATED_PR_NUMBERS": "[]"}
            assert subprocess.run(["bash", str(ROOT / "scripts/validate-candidate-files.sh"), str(root), str(pr_json)],
                                  cwd=ROOT, env=missing, capture_output=True).returncode != 0
print(f"synthetic PR/main metadata and checksum fixtures passed for {len(COORDS)} coordinates")
