#!/usr/bin/env python3
"""Derive the exact connector-owned Maven coordinate set from the publication contract."""
import json
import pathlib
import sys
import xml.etree.ElementTree as ET

NS = "{http://maven.apache.org/POM/4.0.0}"
ROOT = pathlib.Path(__file__).resolve().parents[1]


def child_text(element, name):
    if element is None:
        return ""
    return element.findtext(NS + name, "")


def reactor_projects():
    projects = {}
    visited = set()

    def visit(pom):
        pom = pom.resolve()
        if pom in visited:
            return
        visited.add(pom)
        project = ET.parse(pom).getroot()
        parent = project.find(NS + "parent")
        group = child_text(project, "groupId") or child_text(parent, "groupId")
        artifact = child_text(project, "artifactId")
        packaging = child_text(project, "packaging") or "jar"
        if group and artifact:
            coordinate = (group, artifact)
            if coordinate in projects:
                raise ValueError(f"duplicate reactor coordinate: {group}:{artifact}")
            projects[coordinate] = packaging
        for module in project.findall(f"{NS}modules/{NS}module"):
            path = (pom.parent / (module.text or "").strip()).resolve()
            module_pom = path if path.is_file() else path / "pom.xml"
            if not module_pom.is_file():
                raise ValueError(f"reactor module POM is missing: {module_pom}")
            visit(module_pom)

    visit(ROOT / "pom.xml")
    return projects


def owned_coordinates():
    manifest = json.loads((ROOT / "public-artifacts.json").read_text())
    group = manifest["groupId"]
    public = manifest.get("publicArtifacts", [])
    internal = manifest.get("internalArtifacts", [])
    external = manifest.get("externalArtifacts", [])
    declared = {}
    for item in public:
        coordinate = (item.get("groupId", group), item["artifactId"])
        if coordinate in declared:
            raise ValueError(f"duplicate publication coordinate: {':'.join(coordinate)}")
        declared[coordinate] = item["packaging"]
    projects = reactor_projects()
    for artifact in internal:
        coordinate = (group, artifact)
        if coordinate in declared:
            raise ValueError(f"internal artifact overlaps public coordinate: {':'.join(coordinate)}")
        if coordinate not in projects:
            raise ValueError(f"internal artifact is not a reactor project: {':'.join(coordinate)}")
        declared[coordinate] = projects[coordinate]
    reactor_owned = set(projects)
    declared_owned = set(declared)
    if declared_owned != reactor_owned:
        missing = sorted(": ".join(x) for x in reactor_owned - declared_owned)
        extra = sorted(": ".join(x) for x in declared_owned - reactor_owned)
        raise ValueError(f"publication contract/reactor drift; missing={missing}, extra={extra}")
    for coordinate, packaging in declared.items():
        if projects[coordinate] != packaging:
            raise ValueError(f"packaging drift for {':'.join(coordinate)}: manifest={packaging}, reactor={projects[coordinate]}")
    external_coordinates = {(item.get("groupId", group), item["artifactId"]) for item in external}
    if external_coordinates & declared_owned:
        raise ValueError("external artifacts must not be included in candidate coordinates")
    return [
        {"groupId": g, "artifactId": a, "packaging": p}
        for (g, a), p in sorted(declared.items())
    ]


if __name__ == "__main__":
    try:
        coordinates = owned_coordinates()
        if sys.argv[1:] == ["--check"]:
            print(f"verified {len(coordinates)} owned coordinates; external artifacts excluded")
        elif not sys.argv[1:]:
            print(json.dumps(coordinates))
        else:
            raise ValueError("usage: candidate-coordinates.py [--check]")
    except Exception as exc:
        print(str(exc), file=sys.stderr)
        raise SystemExit(1)
