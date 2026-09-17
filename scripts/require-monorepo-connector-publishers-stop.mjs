#!/usr/bin/env node
import path from 'node:path';
import { fileURLToPath } from 'node:url';

export const publishedArtifacts = [
  { artifactId: 'http-contract', mirrorPath: 'framework/connectors/http-contract/pom.xml', reactorParent: 'connectors', reactorModule: 'http-contract' },
  { artifactId: 'representation-provider-opencsv', mirrorPath: 'framework/representation-provider-opencsv/pom.xml', reactorParent: 'framework', reactorModule: 'representation-provider-opencsv' },
  { artifactId: 'object-ingest-connector', mirrorPath: 'framework/connectors/object-ingest/pom.xml', reactorParent: 'connectors', reactorModule: 'object-ingest' },
  { artifactId: 'query-jpa-connector', mirrorPath: 'framework/connectors/query-jpa/pom.xml', reactorParent: 'connectors', reactorModule: 'query-jpa' },
  { artifactId: 'query-hibernate-common', mirrorPath: 'framework/connectors/query-hibernate-common/pom.xml', reactorParent: 'connectors', reactorModule: 'query-hibernate-common' },
  { artifactId: 'connector-import-tooling', mirrorPath: 'framework/connector-import-tooling/pom.xml', reactorParent: 'framework', reactorModule: 'connector-import-tooling' },
  { artifactId: 'connector-maven-plugin', mirrorPath: 'framework/connector-maven-plugin/pom.xml', reactorParent: 'framework', reactorModule: 'connector-maven-plugin' },
  { artifactId: 'connector-mcp-maven-plugin', mirrorPath: 'framework/connector-mcp-maven-plugin/pom.xml', reactorParent: 'framework', reactorModule: 'connector-mcp-maven-plugin' },
  { artifactId: 'connector-openapi-maven-plugin', mirrorPath: 'framework/connector-openapi-maven-plugin/pom.xml', reactorParent: 'framework', reactorModule: 'connector-openapi-maven-plugin' },
  { artifactId: 'mcp-contract', mirrorPath: 'framework/connectors/mcp-contract/pom.xml', reactorParent: 'connectors', reactorModule: 'mcp-contract' },
];

function externalArtifact(manifest, artifactId) {
  return manifest.externalArtifacts.find((entry) => entry?.artifactId === artifactId);
}

function centralPublishingPlugin(frameworkPom) {
  const profile = [...(frameworkPom ?? '').matchAll(/<profile>([\s\S]*?)<\/profile>/g)]
    .map((match) => match[1]).find((candidate) => /<id>\s*central-publishing\s*<\/id>/.test(candidate));
  return [...(profile ?? '').matchAll(/<plugin>([\s\S]*?)<\/plugin>/g)]
    .map((match) => match[1]).find((plugin) => /<artifactId>\s*central-publishing-maven-plugin\s*<\/artifactId>/.test(plugin));
}

function centralExcludes(frameworkPom, artifactId) {
  const plugin = centralPublishingPlugin(frameworkPom);
  const exclusions = plugin?.match(/<excludeArtifacts>([\s\S]*?)<\/excludeArtifacts>/)?.[1] ?? '';
  return [...exclusions.matchAll(/<excludeArtifact>\s*([^<]+?)\s*<\/excludeArtifact>/g)].some((match) => match[1] === artifactId);
}

function isNonDeployable(mirrorPom) {
  const activeProject = mirrorPom
    .replace(/<profiles>[\s\S]*?<\/profiles>/g, '')
    .replace(/<pluginManagement>[\s\S]*?<\/pluginManagement>/g, '');
  const properties = activeProject.match(/<properties>([\s\S]*?)<\/properties>/)?.[1] ?? '';
  if (/<maven\.deploy\.skip>\s*true\s*<\/maven\.deploy\.skip>/.test(properties)) return true;
  const build = activeProject.match(/<build>([\s\S]*?)<\/build>/)?.[1] ?? '';
  const plugins = build.match(/<plugins>([\s\S]*?)<\/plugins>/)?.[1] ?? '';
  const deployPlugin = [...plugins.matchAll(/<plugin>([\s\S]*?)<\/plugin>/g)]
    .map((match) => match[1]).find((plugin) => /<artifactId>\s*maven-deploy-plugin\s*<\/artifactId>/.test(plugin));
  return /<skip>\s*true\s*<\/skip>/.test(deployPlugin ?? '');
}

function includesReactorModule(frameworkPom, modulePath) {
  const escaped = modulePath.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
  return new RegExp(`<module>\\s*${escaped}\\s*</module>`).test(frameworkPom);
}

export function assertMonorepoPublishersStopped(manifest, mirrors, frameworkPom, connectorsPom = '', artifacts = publishedArtifacts) {
  if (!Array.isArray(manifest?.publicArtifacts) || !Array.isArray(manifest?.internalArtifacts) || !Array.isArray(manifest?.externalArtifacts)) {
    throw new Error('Monorepo publication manifest is missing public, internal, or external artifacts');
  }
  for (const { artifactId, reactorParent, reactorModule } of artifacts) {
    if (manifest.publicArtifacts.some((entry) => entry?.artifactId === artifactId)) throw new Error(`Monorepo still declares ${artifactId} as public`);
    const external = externalArtifact(manifest, artifactId);
    const internal = manifest.internalArtifacts.includes(artifactId);
    if (external?.ownership !== 'external' && !internal) throw new Error(`Monorepo does not classify ${artifactId} as internal or externally owned`);
    const mirrorPom = mirrors?.[artifactId];
    if (mirrorPom === undefined) {
      if (internal) throw new Error(`Monorepo still declares ${artifactId} internal but its POM is missing`);
      if (external.reactorSourceMirror === true) throw new Error(`Monorepo ${artifactId} source mirror is declared but its POM is missing`);
      const reactorPom = reactorParent === 'connectors' ? connectorsPom : frameworkPom;
      if (includesReactorModule(reactorPom, reactorModule)) throw new Error(`Monorepo ${artifactId} POM is missing while reactor module ${reactorModule} remains active`);
      continue;
    }
    if (external && external.reactorSourceMirror !== true) throw new Error(`Monorepo ${artifactId} source mirror is not marked reactorSourceMirror`);
    const inheritedNonDeployable = reactorParent === 'connectors' && isNonDeployable(connectorsPom);
    if (!isNonDeployable(mirrorPom) && !inheritedNonDeployable) throw new Error(`Monorepo ${artifactId} source mirror remains deployable`);
    if (!centralExcludes(frameworkPom, artifactId)) throw new Error(`Monorepo Central bundle does not exclude ${artifactId}`);
  }
}

const repository = 'The-Pipeline-Framework/pipelineframework';
const apiRoot = `https://api.github.com/repos/${repository}`;
async function githubFile(filePath, commitSha, optional = false) {
  const headers = { Accept: 'application/vnd.github+json', 'X-GitHub-Api-Version': '2022-11-28' };
  if (process.env.GITHUB_TOKEN) headers.Authorization = `Bearer ${process.env.GITHUB_TOKEN}`;
  const response = await fetch(`${apiRoot}/contents/${filePath}?ref=${commitSha}`, { headers });
  if (optional && response.status === 404) return undefined;
  if (!response.ok) throw new Error(`GitHub publication preflight failed: HTTP ${response.status} for ${filePath}`);
  const file = await response.json();
  if (file.type !== 'file' || file.encoding !== 'base64' || typeof file.content !== 'string') throw new Error(`Invalid GitHub content for ${filePath}`);
  return Buffer.from(file.content, 'base64').toString('utf8');
}

async function main() {
  const headers = { Accept: 'application/vnd.github+json', 'X-GitHub-Api-Version': '2022-11-28' };
  if (process.env.GITHUB_TOKEN) headers.Authorization = `Bearer ${process.env.GITHUB_TOKEN}`;
  const refResponse = await fetch(`${apiRoot}/git/ref/heads/main`, { headers });
  if (!refResponse.ok) throw new Error(`GitHub publication preflight failed: HTTP ${refResponse.status} for main`);
  const commitSha = (await refResponse.json()).object?.sha;
  if (typeof commitSha !== 'string' || !/^[0-9a-f]{40}$/.test(commitSha)) throw new Error('No monorepo main commit returned');
  const [manifestJson, frameworkPom, connectorsPom, ...mirrorPoms] = await Promise.all([
    githubFile('framework/public-artifacts.json', commitSha), githubFile('framework/pom.xml', commitSha), githubFile('framework/connectors/pom.xml', commitSha),
    ...publishedArtifacts.map(({ mirrorPath }) => githubFile(mirrorPath, commitSha, true)),
  ]);
  const mirrors = Object.fromEntries(publishedArtifacts.map(({ artifactId }, index) => [artifactId, mirrorPoms[index]]));
  assertMonorepoPublishersStopped(JSON.parse(manifestJson), mirrors, frameworkPom, connectorsPom);
  console.log(`Monorepo ${commitSha} no longer publishes staged connector artifacts; connectors is the sole publisher`);
}

if (process.argv[1] && path.resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  try { await main(); } catch (error) { console.error(error.message); process.exitCode = 1; }
}
