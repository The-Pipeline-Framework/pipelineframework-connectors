#!/usr/bin/env python3
import pathlib
import subprocess
import tempfile

script = pathlib.Path(__file__).with_name("compare-published-candidate.py")
with tempfile.TemporaryDirectory(prefix="tpf-connectors-published-") as temporary:
    root = pathlib.Path(temporary)
    candidate_pom, published_pom = root / "candidate.pom", root / "published.pom"
    candidate_jar, published_jar = root / "candidate.jar", root / "published.jar"
    candidate_pom.write_bytes(b"same pom")
    published_pom.write_bytes(b"same pom")
    candidate_jar.write_bytes(b"same jar")
    published_jar.write_bytes(b"same jar")
    args = ["python3", str(script), "jar", str(candidate_pom), str(candidate_jar), str(published_pom), str(published_jar)]
    subprocess.run(args, check=True)
    published_jar.write_bytes(b"drift")
    assert subprocess.run(args, capture_output=True).returncode != 0
    published_jar.write_bytes(b"same jar")
    published_pom.write_bytes(b"drift")
    assert subprocess.run(args, capture_output=True).returncode != 0
    published_pom.unlink()
    assert subprocess.run(args, capture_output=True).returncode != 0
    subprocess.run(["python3", str(script), "pom", str(candidate_pom), "", str(candidate_pom), ""], check=True)
    assert subprocess.run(["python3", str(script), "maven-plugin", str(candidate_pom), str(candidate_jar), str(candidate_pom), str(candidate_jar)], check=True).returncode == 0
print("published-candidate idempotency and drift fixtures passed")
