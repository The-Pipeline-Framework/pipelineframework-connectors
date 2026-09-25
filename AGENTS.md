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

Owner-local verification is the first gate. `.github/tpf-system-tests.json` owns the stable Connector suite
commands. `TPF Candidate Build` and the trusted publisher create an immutable, commit-specific Connector candidate;
`tpf/system-tests` records downstream evidence on that exact source SHA.

For an ordinary single-repository pull request, use the candidate publisher and singleton system-test path above.
For a coordinated change, do **not** wait for participating candidate publishers and do not merge or publish
snapshots one repository at a time. Manually run
[`TPF System Tests — Compatibility Set`](https://github.com/The-Pipeline-Framework/pipelineframework/actions/workflows/system-test-compatibility-set.yml)
with one stable set ID and 2–10 pull-request URLs, one per line. The coordinator pins each PR head and tested merge
commit, builds participating Maven reactors in dependency order into one isolated repository, and runs one product
test over the resulting set. A new commit invalidates that PR's result: rerun the same set ID with the current URLs.
Require the same `tpf/system-tests` success on every participating SHA. Do not substitute snapshots, branch heads,
source checkouts or a composite Maven reactor. See the canonical
[cross-repository system-test runbook](https://github.com/The-Pipeline-Framework/pipelineframework/blob/main/docs/evolve/cross-repository-system-tests.md).

Repository setup requires repository-scoped dispatch credentials. If the workflow exposes them as
`SYSTEM_TEST_APP_ID` and `SYSTEM_TEST_APP_PRIVATE_KEY`, they must belong to a dispatch-only App installed solely on
`pipelineframework`, never the coordinator App. The trusted publisher uses the repository `GITHUB_TOKEN` with
`packages: write`; fork publication additionally requires the
`safe-to-system-test` label. Never expose publication, dispatch or status credentials to owner-suite jobs.

Require a green full train for formal BOM or release promotion.

Always use the repository-local Maven cache:

```sh
./mvnw <goals> -Dmaven.repo.local="$PWD/.m2/repository"
```

Do not introduce Maven profiles except `central-publishing`. It may attach, sign, and deploy artifacts but must not
select another source universe, module graph, or build topology.

Do not commit, push, publish, or change another repository unless explicitly requested.
