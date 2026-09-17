#!/usr/bin/env node
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

export function expectedDeployabilityFromManifest(manifest) {
  const expected = new Map();
  for (const artifact of manifest.publicArtifacts ?? []) {
    if (expected.has(artifact.artifactId)) throw new Error(`duplicate publication classification: ${artifact.artifactId}`);
    expected.set(artifact.artifactId, true);
  }
  for (const artifactId of manifest.internalArtifacts ?? []) {
    if (expected.has(artifactId)) throw new Error(`duplicate publication classification: ${artifactId}`);
    expected.set(artifactId, false);
  }
  return expected;
}

const manifestPath = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..', 'public-artifacts.json');
export const expectedDeployability = expectedDeployabilityFromManifest(
  JSON.parse(fs.readFileSync(manifestPath, 'utf8')),
);

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
