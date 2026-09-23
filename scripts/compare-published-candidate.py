#!/usr/bin/env python3
"""Require an existing immutable candidate version to match POM and primary bytes."""
import pathlib
import sys

packaging, candidate_pom_arg, candidate_artifact_arg, published_pom_arg, published_artifact_arg = sys.argv[1:]
candidate_pom = pathlib.Path(candidate_pom_arg)
published_pom = pathlib.Path(published_pom_arg)
if not published_pom.is_file():
    raise SystemExit("published POM is missing; refusing incomplete candidate version")
if candidate_pom.read_bytes() != published_pom.read_bytes():
    raise SystemExit("published POM differs from candidate; refusing immutable-version drift")
if packaging == "pom":
    pass
elif packaging in {"jar", "maven-plugin"}:
    candidate_artifact = pathlib.Path(candidate_artifact_arg)
    published_artifact = pathlib.Path(published_artifact_arg)
    if not published_artifact.is_file():
        raise SystemExit("published primary JAR is missing; refusing immutable-version drift")
    if candidate_artifact.read_bytes() != published_artifact.read_bytes():
        raise SystemExit("published JAR differs from candidate; refusing immutable-version drift")
else:
    raise SystemExit(f"unsupported packaging: {packaging}")
print("published candidate matches exactly")
