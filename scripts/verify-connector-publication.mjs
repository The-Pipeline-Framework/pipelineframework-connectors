#!/usr/bin/env node
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

export const expectedDeployability = new Map([
  ['pipelineframework-connectors-parent', true],
  ['http-contract', true],
  ['representation-provider-opencsv', true],
  ['object-ingest-connector', true],
  ['query-jpa-connector', true],
  ['query-hibernate-common', true],
  ['connector-import-tooling', false], ['connector-maven-plugin', false], ['connector-mcp-maven-plugin', false],
  ['connector-openapi-maven-plugin', false], ['embedding-query-connector', false],
  ['embedding-query-langchain4j-connector', false], ['framework-connectors', false], ['gmail-query-connector', false],
  ['graphql-connector', false], ['graphql-smallrye-connector', false], ['host-gmail', false],
  ['host-microsoft-graph', false], ['host-oidc-quarkus', false], ['host-quickbooks-mcp', false],
  ['http-connector', false], ['llm-query-connector', false], ['llm-query-langchain4j-connector', false],
  ['mcp-connector', false], ['mcp-contract', false], ['query-hibernate-reactive-connector', false],
  ['representation-provider-file', false], ['representation-provider-fixture', false],
  ['representation-provider-http', false], ['vector-store-connector', false], ['vector-store-pgvector-connector', false],
]);

function elementValue(xml, element) {
  return xml.match(new RegExp(`<${element}>([^<]+)</${element}>`))?.[1]?.trim() ?? '';
}

export function assertEffectivePublication(effectivePom, expected = expectedDeployability) {
  const projects = new Map();
  for (const match of effectivePom.matchAll(/<project(?:\s[^>]*)?>([\s\S]*?)<\/project>/g)) {
    const project = match[1].replace(/<parent>[\s\S]*?<\/parent>/, '');
    if (elementValue(project, 'groupId') !== 'org.pipelineframework') continue;
    const artifactId = elementValue(project, 'artifactId');
    if (!artifactId) continue;
    if (projects.has(artifactId)) throw new Error(`effective POM contains duplicate project: ${artifactId}`);
    const properties = project.match(/<properties>([\s\S]*?)<\/properties>/)?.[1] ?? '';
    const deploySkip = elementValue(properties, 'maven.deploy.skip');
    if (!['true', 'false'].includes(deploySkip)) throw new Error(`effective maven.deploy.skip for ${artifactId} is not boolean`);
    const central = project.match(/<artifactId>central-publishing-maven-plugin<\/artifactId>([\s\S]*?)(?=<plugin>|<\/plugins>)/g) ?? [];
    const centralExcluded = central.some((plugin) => new RegExp(`<excludeArtifact>\\s*${artifactId}\\s*</excludeArtifact>`).test(plugin));
    projects.set(artifactId, {
      deployable: deploySkip === 'false',
      centralPresent: central.length > 0,
      centralSkipped: central.some((plugin) => /<skipPublishing>\s*true\s*<\/skipPublishing>/.test(plugin)) || centralExcluded,
    });
  }
  for (const [artifactId, deployable] of expected) {
    const project = projects.get(artifactId);
    if (!project) throw new Error(`expected reactor project is missing: ${artifactId}`);
    if (project.deployable !== deployable) throw new Error(`deployability drift for ${artifactId}`);
    if (!project.centralPresent) throw new Error(`Central plugin is missing for ${artifactId}`);
    if (!deployable && !project.centralSkipped) throw new Error(`internal artifact is not Central-skipped: ${artifactId}`);
    if (deployable && project.centralSkipped) throw new Error(`public artifact is Central-skipped: ${artifactId}`);
  }
  for (const artifactId of projects.keys()) if (!expected.has(artifactId)) throw new Error(`unexpected reactor project: ${artifactId}`);
  return projects;
}

function main() {
  const file = process.argv[2];
  if (!file) throw new Error('Usage: verify-connector-publication.mjs <effective-pom.xml>');
  const projects = assertEffectivePublication(fs.readFileSync(path.resolve(file), 'utf8'));
  console.log(`verified connector publication flags for ${projects.size} reactor projects`);
}

if (process.argv[1] && path.resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  try { main(); } catch (error) { console.error(error.message); process.exitCode = 1; }
}
