"""Exercise suite argument forwarding without resolving Maven dependencies."""
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest


class SystemTestArguments(unittest.TestCase):
    def invoke(self, arguments):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory).resolve()
            shutil.copyfile(Path(__file__).with_name('system-tests.sh'), root / 'system-tests.sh')
            (root / 'mvnw').write_text('#!/bin/bash\nprintf "%s\\n" "$@" > maven-arguments.txt\n')
            (root / 'mvnw').chmod(0o755)
            # Report verification is outside this argument test's boundary.
            (root / 'python3').write_text('#!/bin/bash\nexit 0\n')
            (root / 'python3').chmod(0o755)
            environment = dict(os.environ, MAVEN_ARGS=arguments,
                               PATH=str(root) + os.pathsep + os.environ['PATH'])
            subprocess.run(['bash', 'system-tests.sh', 'verify'], cwd=root,
                           env=environment, check=True, capture_output=True, text=True)
            actual = (root / 'maven-arguments.txt').read_text().splitlines()
            return actual, str(root / '.m2' / 'repository')

    def repositories(self, actual):
        return [value for value in actual if value.startswith('-Dmaven.repo.local=')]

    def test_coordinator_repository_and_candidate_pins_survive(self):
        pins = ['-Dpipelineframework.contracts.version=26.10.1-main.7bd043af309b',
                '-Dpipelineframework.runtime.version=26.10.1-main.0d9a84655de4']
        actual, unused = self.invoke(' '.join(pins + ['-Dmaven.repo.local=/tmp/hydrated-candidates']))
        self.assertEqual(self.repositories(actual), ['-Dmaven.repo.local=/tmp/hydrated-candidates'])
        for pin in pins:
            self.assertIn(pin, actual)
        self.assertEqual(actual[-2:], ['clean', 'verify'])
        for required in ['-Dmaven.deploy.skip=true', '-Dgpg.skip=true', '-Dtpf.flatten.skip=true']:
            self.assertIn(required, actual)

    def test_default_repository_with_candidate_pin(self):
        pin = '-Dpipelineframework.contracts.version=26.10.1-main.7bd043af309b'
        actual, default = self.invoke(pin)
        self.assertIn(pin, actual)
        self.assertEqual(self.repositories(actual), ['-Dmaven.repo.local=' + default])

    def test_default_repository_without_arguments(self):
        actual, default = self.invoke('')
        self.assertEqual(self.repositories(actual), ['-Dmaven.repo.local=' + default])


if __name__ == '__main__':
    unittest.main()
