import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import test from 'node:test';

const workflow = await readFile(new URL('../.github/workflows/verify.yml', import.meta.url), 'utf8');

test('PR verification runs Maven unit tests and defers Failsafe integration tests', () => {
  const verifyCommand = workflow.match(/run: (\.\/mvnw[^\n]*clean verify[^\n]*)/u)?.[1];

  assert.ok(verifyCommand, 'the Verify job should run Maven clean verify');
  assert.match(verifyCommand, /-DskipITs\b/u);
  assert.doesNotMatch(verifyCommand, /-DskipTests\b/u);
});

test('external integration tests require a maintainer manual dispatch opt-in', () => {
  assert.match(workflow, /workflow_dispatch:[\s\S]*?run_external_it:[\s\S]*?type: boolean/u);
  assert.match(workflow, /if: github\.event_name == 'workflow_dispatch' && inputs\.run_external_it/u);
  assert.match(workflow, /name: Run external-service integration tests[\s\S]*?clean verify -Dgpg\.skip/u);
});

test('PR verification retains publication-contract checks', () => {
  assert.match(workflow, /node --test scripts\/verify-ci-workflow\.test\.mjs scripts\/verify-connector-publication\.test\.mjs/u);
  assert.match(workflow, /node scripts\/verify-connector-publication\.mjs target\/connectors-effective-pom\.xml/u);
});
