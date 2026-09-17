import test from 'node:test';
import assert from 'node:assert/strict';
import { assertMonorepoPublishersStopped, publishedArtifacts } from './require-monorepo-connector-publishers-stop.mjs';

const mirror = '<properties><maven.deploy.skip>true</maven.deploy.skip></properties>';
const frameworkPom = `<profile><id>central-publishing</id><build><plugins><plugin><artifactId>central-publishing-maven-plugin</artifactId><configuration><excludeArtifacts>${publishedArtifacts.map(({ artifactId }) => `<excludeArtifact>${artifactId}</excludeArtifact>`).join('')}</excludeArtifacts></configuration></plugin></plugins></build></profile>`;

test('refuses while monorepo still declares a connector artifact public', () => {
  assert.throws(() => assertMonorepoPublishersStopped({ publicArtifacts: [{ artifactId: 'http-contract' }], externalArtifacts: [] }, {}, ''), /still declares http-contract/);
});

test('accepts an externally owned, non-deployable monorepo mirror', () => {
  const manifest = {
    publicArtifacts: [],
    externalArtifacts: publishedArtifacts.map(({ artifactId }) => ({ artifactId, ownership: 'external', reactorSourceMirror: true })),
  };
  const mirrors = Object.fromEntries(publishedArtifacts.map(({ artifactId }) => [artifactId, mirror]));
  assert.doesNotThrow(() => assertMonorepoPublishersStopped(manifest, mirrors, frameworkPom));
});
