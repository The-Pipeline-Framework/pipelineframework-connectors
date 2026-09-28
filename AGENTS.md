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
For a coordinated change, give every participating pull request the same head-branch name under the same GitHub
owner. Candidate intake discovers those open component pull requests and dispatches one deterministic compatibility
set automatically; later intake events for the same set cancel the older in-flight run. Do **not** wait for
participating candidate publishers and do not merge or publish snapshots one repository at a time.

Use the manual
[`TPF System Tests — Compatibility Set`](https://github.com/The-Pipeline-Framework/pipelineframework/actions/workflows/system-test-compatibility-set.yml)
entry point only when a coordinated set intentionally uses different branch names. Supply one stable set ID and
2–10 pull-request URLs, one per line. Both paths pin each exact PR head and current base, build the participating
Maven reactors and derived downstream closure in dependency order into one isolated repository, and report one
`tpf/system-tests` result to every participating SHA. Do not manually add unchanged downstream repositories or
publish intermediate snapshots; the coordinator derives current-main and downstream closure targets. Do not
substitute snapshots, floating branch heads, source checkouts or a composite Maven reactor. See the canonical
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
