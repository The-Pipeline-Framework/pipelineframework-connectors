import assert from 'node:assert/strict';
import test from 'node:test';

import { expectedDeployability, expectedDeployabilityFromManifest } from './verify-connector-publication.mjs';

test('externally consumed file and QuickBooks providers remain publishable', () => {
  for (const artifactId of ['representation-provider-file', 'host-quickbooks-mcp']) {
    assert.equal(expectedDeployability.get(artifactId), true, `${artifactId} must be published for application consumers`);
  }
  for (const artifactId of ['framework-connectors', 'host-gmail', 'host-microsoft-graph', 'host-oidc-quarkus', 'representation-provider-fixture']) {
    assert.equal(expectedDeployability.get(artifactId), false, `${artifactId} remains internal`);
  }
});

test('derives deployability from the publication manifest', () => {
  assert.deepEqual(
    [...expectedDeployabilityFromManifest({
      publicArtifacts: [{ artifactId: 'public-one', packaging: 'jar' }],
      internalArtifacts: ['internal-one'],
    })],
    [['public-one', true], ['internal-one', false]],
  );
});

test('rejects an artifact classified as both public and internal', () => {
  assert.throws(
    () => expectedDeployabilityFromManifest({
      publicArtifacts: [{ artifactId: 'duplicate', packaging: 'jar' }],
      internalArtifacts: ['duplicate'],
    }),
    /duplicate publication classification: duplicate/,
  );
});
