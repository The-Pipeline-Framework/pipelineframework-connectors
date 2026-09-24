# The Pipeline Framework Connectors

This repository publishes TPF-owned typed I/O capabilities:

- Connector contracts and implementations for external observations and effects;
- import tooling and Maven plugins for pinned external capabilities;
- representation-provider implementations;
- authorized OIDC, Gmail, Microsoft Graph, and QuickBooks MCP host integrations.

Connectors consume released contract, compiler, and runtime artifacts. They do not own compiler semantics, runtime
implementations, reusable Block composition, or application credentials and Command authority.

Build with an isolated Maven repository:

```sh
./mvnw clean verify -Dmaven.repo.local="$PWD/.m2/repository"
```

Use the `central-publishing` profile only to sign and deploy the canonical reactor. For authoring guidance, see
[Connectors](https://pipelineframework.org/develop/connectors/); for the component boundary, see
[TPF Components and Repositories](https://pipelineframework.org/architecture/components-and-repositories).

## System-test candidates

`TPF Candidate Build` runs at the exact pull-request or `main` SHA with read-only permissions and no secrets. It
builds a commit-specific Connector candidate and uploads only allowlisted Maven files and preliminary metadata. The
trusted `TPF Candidate Publish` workflow validates the build and current head, publishes those files to this
repository's GitHub Packages registry, then dispatches `tpf-candidate-v1` to the coordination repository without
executing project or fork code.

Fork pull requests require `safe-to-system-test`. Candidate publication uses the coordination GitHub App and the
workflow package token; it uses no Maven Central credentials or GPG key.
