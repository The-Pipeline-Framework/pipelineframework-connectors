# Connectors Repository Instructions

This repository owns TPF Connector implementations, import tooling, representation-provider implementations, and
TPF-owned authorized external-service hosts. A Connector models typed admission, publication, external observation,
or external effect; it is not a generic plugin or a second Pipeline language.

## Boundary

- Consume semantic, compiler, and runtime seams as released artifacts. Do not copy their source or redefine their
  validation rules here.
- Keep Quarkus/Spring runtime implementation, foundational plugins, Blocks, Expansions, examples, applications, and
  canonical documentation in their owning repositories.
- Keep application bindings, credentials, endpoint policy, and Command authority with the application or host.
- Imported capabilities must preserve pinned external contract identity and deterministic generated metadata.

## Cross-repository changes

Update canonical documentation or an ADR in `pipelineframework` when a change alters Connector meaning, authority,
or a shared contract. Use the GitNexus `tpf` group for cross-repository impact and verify findings in the owning
worktree. Do not introduce source fallbacks for another component release.

## Build and publication

Owner-local verification is the first gate. `TPF Candidate Build` and the trusted publisher create an immutable,
commit-specific Connector candidate for the coordination repository; `tpf/system-tests` records downstream
evidence on that exact source SHA. Use a compatibility set for coordinated repository changes, and require a green
full train for formal BOM or release promotion. Keep the stable owner suite command in
`.github/tpf-system-tests.json`.

Always use the repository-local Maven cache:

```sh
./mvnw <goals> -Dmaven.repo.local="$PWD/.m2/repository"
```

Do not introduce Maven profiles except `central-publishing`. It may attach, sign, and deploy artifacts but must not
select another source universe, module graph, or build topology.

Do not commit, push, publish, or change another repository unless explicitly requested.
