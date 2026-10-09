#!/usr/bin/env python3
"""Require evidence for the service integration suites inherited from the monorepo."""
import sys
from pathlib import Path
import xml.etree.ElementTree as ET

REQUIRED_SUITES = {
    "framework/connectors/query-hibernate-reactive":
        "org.pipelineframework.connector.query.jpa.HibernateReactiveQueryConnectorIT",
    "framework/connectors/vector-store-pgvector":
        "org.pipelineframework.connector.vector.pgvector.PgVectorConnectorIT",
    "framework/host-oidc-quarkus":
        "org.pipelineframework.host.oidc.ConnectionRestartIT",
}


def verify(root):
    for module, name in REQUIRED_SUITES.items():
        report = root / module / "target/failsafe-reports" / f"TEST-{name}.xml"
        suite = ET.parse(report).getroot()
        if suite.tag != "testsuite" or suite.get("name") != name:
            raise ValueError(f"Unexpected integration suite in {report}")
        counts = {key: int(suite.attrib[key]) for key in
                  ("tests", "failures", "errors", "skipped")}
        cases = suite.findall("testcase")
        observed = {
            "tests": len(cases),
            "failures": sum(case.find("failure") is not None for case in cases),
            "errors": sum(case.find("error") is not None for case in cases),
            "skipped": sum(case.find("skipped") is not None for case in cases),
        }
        if counts != observed:
            raise ValueError(f"Integration counters disagree with testcase evidence: {report}: "
                             f"declared={counts}, observed={observed}")
        passing = sum(all(case.find(outcome) is None for outcome in
                          ("failure", "error", "skipped")) for case in cases)
        if passing < 1 or any(counts[key] != 0 for key in
                                      ("failures", "errors", "skipped")):
            raise ValueError(f"Missing passing, unskipped integration coverage: {report}: {counts}")
        print(f"Service integration evidence: {name}: {counts['tests']} passed")


if __name__ == "__main__":
    try:
        verify(Path.cwd())
    except (OSError, ValueError, KeyError, ET.ParseError) as failure:
        sys.exit(f"Connector service integration evidence failed: {failure}")
