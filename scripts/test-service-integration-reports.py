import importlib.util
from pathlib import Path
import tempfile
import shutil
import subprocess
import sys
import unittest

sys.dont_write_bytecode = True
spec = importlib.util.spec_from_file_location("evidence", Path(__file__).with_name("verify-service-integration-reports.py"))
evidence = importlib.util.module_from_spec(spec)
spec.loader.exec_module(evidence)


class IntegrationEvidenceTest(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)
        self.reports = []
        for module, name in evidence.REQUIRED_SUITES.items():
            report = self.root / module / "target/failsafe-reports" / f"TEST-{name}.xml"
            report.parent.mkdir(parents=True)
            report.write_text(f'<testsuite name="{name}" tests="2" failures="0" errors="0" skipped="0"/>')
            self.reports.append(report)

    def test_all_suites_pass(self):
        evidence.verify(self.root)

    def test_missing_suite_fails(self):
        for report in self.reports:
            with self.subTest(report=report):
                original = report.read_text()
                report.unlink()
                with self.assertRaises(OSError):
                    evidence.verify(self.root)
                report.write_text(original)

    def test_empty_skipped_failed_and_invalid_reports_fail(self):
        report = self.reports[0]
        original = report.read_text()
        for key in ("tests", "skipped", "failures", "errors"):
            with self.subTest(key=key):
                report.write_text(original.replace(f'{key}="2"' if key == "tests" else f'{key}="0"',
                                                   f'{key}="0"' if key == "tests" else f'{key}="1"'))
                with self.assertRaises(ValueError):
                    evidence.verify(self.root)
        for text in ("not XML", '<testsuite/>'):
            report.write_text(text)
            with self.assertRaises((ValueError, evidence.ET.ParseError)):
                evidence.verify(self.root)

    def test_owner_entrypoint_requires_evidence_after_clean_verify(self):
        scripts = self.root / "scripts"
        scripts.mkdir()
        for name in ("system-tests.sh", "verify-service-integration-reports.py"):
            shutil.copyfile(Path(__file__).with_name(name), scripts / name)
        wrapper = self.root / "mvnw"
        wrapper.write_text('#!/bin/sh\nprintf "%s\\n" "$@" > maven-arguments\n')
        wrapper.chmod(0o755)
        result = subprocess.run(["bash", "scripts/system-tests.sh", "verify"],
                                cwd=self.root, capture_output=True, text=True)
        self.assertEqual(0, result.returncode, result.stderr)
        arguments = (self.root / "maven-arguments").read_text().splitlines()
        self.assertEqual(["clean", "verify"], arguments[-2:])
        self.assertFalse(any(argument.startswith(("-DskipITs", "-DskipTests")) for argument in arguments))
        self.reports[0].unlink()
        result = subprocess.run(["bash", "scripts/system-tests.sh", "verify"],
                                cwd=self.root, capture_output=True, text=True)
        self.assertNotEqual(0, result.returncode, 'Maven success alone is not integration evidence')
        wrapper.write_text('#!/bin/sh\nexit 7\n')
        result = subprocess.run(["bash", "scripts/system-tests.sh", "verify"],
                                cwd=self.root, capture_output=True, text=True)
        self.assertEqual(7, result.returncode, 'preserve the original Maven failure')


if __name__ == "__main__":
    unittest.main()
