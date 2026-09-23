#!/usr/bin/env python3
"""Synthetic proof that only reactor dependencies move to the candidate version."""
import pathlib
import subprocess
import tempfile
import xml.etree.ElementTree as ET

ROOT = pathlib.Path(__file__).resolve().parents[1]
with tempfile.TemporaryDirectory(prefix="tpf-connectors-reactor-") as temporary:
    root = pathlib.Path(temporary)
    ns = "http://maven.apache.org/POM/4.0.0"
    (root / "pom.xml").write_text(f"""<project xmlns=\"{ns}\"><groupId>org.pipelineframework</groupId><artifactId>root</artifactId><version>26.9.4-SNAPSHOT</version><modules><module>framework/connectors</module></modules><properties><pipelineframework.runtime.version>26.9.4-SNAPSHOT</pipelineframework.runtime.version><pipelineframework.contracts.version>26.9.4-SNAPSHOT</pipelineframework.contracts.version><pipelineframework.compiler.version>26.9.4-SNAPSHOT</pipelineframework.compiler.version></properties></project>""")
    nested = root / "framework/connectors/nested"
    nested.mkdir(parents=True)
    (root / "framework/connectors/pom.xml").write_text(f"""<project xmlns=\"{ns}\"><groupId>org.pipelineframework</groupId><artifactId>connectors-parent</artifactId><version>26.9.4-SNAPSHOT</version><modules><module>nested</module></modules></project>""")
    consumer = nested / "pom.xml"
    consumer.write_text(f"""<project xmlns=\"{ns}\"><groupId>org.pipelineframework</groupId><artifactId>connector</artifactId><dependencies><dependency><groupId>org.pipelineframework</groupId><artifactId>connectors-parent</artifactId><version>26.9.4-SNAPSHOT</version></dependency><dependency><groupId>org.pipelineframework</groupId><artifactId>pipelineframework-runtime-core</artifactId><version>${{pipelineframework.contracts.version}}</version></dependency></dependencies></project>""")
    candidate = "26.9.4-pr.42.0123456789ab"
    subprocess.run(["python3", str(ROOT / "scripts/rewrite-reactor-dependencies.py"), str(root), candidate], check=True)
    xml = ET.parse(consumer).getroot()
    dependencies = xml.find("{http://maven.apache.org/POM/4.0.0}dependencies")
    assert dependencies[0].findtext(f"{{{ns}}}version") == candidate
    assert dependencies[1].findtext(f"{{{ns}}}version") == "${pipelineframework.contracts.version}"
    root_text = (root / "pom.xml").read_text()
    for prop in ("pipelineframework.runtime.version", "pipelineframework.contracts.version", "pipelineframework.compiler.version"):
        assert f"<{prop}>26.9.4-SNAPSHOT</{prop}>" in root_text
print("nested reactor rewrite preserved all external version properties")
