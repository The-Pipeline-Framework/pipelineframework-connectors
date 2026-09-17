# Connectors repository boundary

This repository owns the TPF connector surface: every module under `framework/connectors`, connector import/plugin tooling, representation-provider implementations, and the TPF-owned OIDC, Gmail, Microsoft Graph, and QuickBooks MCP host integrations.

The repository does not own runtime, deployment, Spring, plugins, compiler, framework-neutral contracts, Blocks, examples, expansions, documentation, or transport-completeness tests. Those remain in their respective repositories or in the monorepo conformance surface.

`pipelineframework.contracts.version`, `pipelineframework.compiler.version`, and `pipelineframework.runtime.version` identify external released snapshot dependencies. All projects are staging-only and must keep deployment skipped.
