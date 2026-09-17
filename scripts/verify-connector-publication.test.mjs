import assert from 'node:assert/strict';
import test from 'node:test';

import { expectedDeployabilityFromManifest } from './verify-connector-publication.mjs';

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
