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

test('rejects a deploy skip that exists only in an inactive profile', () => {
  const manifest = {
    publicArtifacts: [],
    externalArtifacts: publishedArtifacts.map(({ artifactId }) => ({ artifactId, ownership: 'external', reactorSourceMirror: true })),
  };
  const mirrors = Object.fromEntries(publishedArtifacts.map(({ artifactId }) => [artifactId, mirror]));
  mirrors['http-contract'] = '<project><profiles><profile><build><plugins><plugin><artifactId>maven-deploy-plugin</artifactId><configuration><skip>true</skip></configuration></plugin></plugins></build></profile></profiles></project>';
  assert.throws(() => assertMonorepoPublishersStopped(manifest, mirrors, frameworkPom), /http-contract source mirror remains deployable/);
});

test('rejects a missing mirror while its owning reactor module remains active', () => {
  const manifest = {
    publicArtifacts: [],
    externalArtifacts: publishedArtifacts.map(({ artifactId }) => ({ artifactId, ownership: 'external' })),
  };
  const mirrors = Object.fromEntries(publishedArtifacts.map(({ artifactId }) => [artifactId, mirror]));
  delete mirrors['http-contract'];
  const activeFrameworkPom = `${frameworkPom}<modules><module>connectors</module></modules>`;
  assert.throws(() => assertMonorepoPublishersStopped(manifest, mirrors, activeFrameworkPom), /reactor module connectors remains active/);
});

test('accepts a removed mirror after its owning reactor module is gone', () => {
  const manifest = {
    publicArtifacts: [],
    externalArtifacts: publishedArtifacts.map(({ artifactId }) => ({ artifactId, ownership: 'external' })),
  };
  assert.doesNotThrow(() => assertMonorepoPublishersStopped(manifest, {}, frameworkPom));
});
