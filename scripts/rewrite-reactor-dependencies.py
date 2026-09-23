#!/usr/bin/env python3
"""Align literal dependency versions that point at projects in this Maven reactor."""
import pathlib
import re
import sys
import xml.etree.ElementTree as ET

root_dir = pathlib.Path(sys.argv[1]).resolve()
candidate_version = sys.argv[2]
namespace = "{http://maven.apache.org/POM/4.0.0}"
poms = [root_dir / "pom.xml"]
visited = set()


def child_text(element, name):
    return element.findtext(namespace + name) if element is not None else None


def visit(pom):
    pom = pom.resolve()
    if pom in visited:
        return
    visited.add(pom)
    poms.append(pom) if pom != (root_dir / "pom.xml").resolve() else None
    project = ET.parse(pom).getroot()
    for module in project.findall(f"{namespace}modules/{namespace}module"):
        path = (pom.parent / (module.text or "").strip()).resolve()
        visit(path if path.is_file() else path / "pom.xml")


root_pom = root_dir / "pom.xml"
visited.add(root_pom.resolve())
root_project = ET.parse(root_pom).getroot()
for module in root_project.findall(f"{namespace}modules/{namespace}module"):
    path = (root_pom.parent / (module.text or "").strip()).resolve()
    visit(path if path.is_file() else path / "pom.xml")
reactor = set()
for pom in poms:
    project = ET.parse(pom).getroot()
    parent = project.find(namespace + "parent")
    group = child_text(project, "groupId") or child_text(parent, "groupId")
    artifact = child_text(project, "artifactId")
    if group and artifact:
        reactor.add((group, artifact))

for pom in poms:
    source = pom.read_text()

    def update_dependency(match):
        block = match.group(0)
        group_match = re.search(r"<groupId>\s*([^<]+)\s*</groupId>", block)
        artifact_match = re.search(r"<artifactId>\s*([^<]+)\s*</artifactId>", block)
        if not group_match or not artifact_match or (group_match.group(1).strip(), artifact_match.group(1).strip()) not in reactor:
            return block
        return re.sub(r"(<version>\s*)[^<]+(\s*</version>)", rf"\g<1>{candidate_version}\2", block, count=1)

    updated = re.sub(r"<dependency\b[^>]*>.*?</dependency>", update_dependency, source, flags=re.DOTALL)
    if updated != source:
        pom.write_text(updated)
