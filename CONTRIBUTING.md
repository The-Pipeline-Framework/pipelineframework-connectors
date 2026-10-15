# Contributing

This repository owns Connector implementations, import tooling, representation providers, and authorized external-service hosts. It does not own applications, compiler/runtime implementations, reusable Blocks, or the canonical meaning of shared TPF contracts. The root [AGENTS.md](AGENTS.md) is authoritative for dependency, publication, cross-repository, and credential rules; this page is a practical route into the code.

## Canonical documentation and related repositories

This page covers contribution mechanics, not a second connector manual. User-facing usage, protocol meaning, configuration and guarantees belong on [the canonical documentation site](https://pipelineframework.org/develop/connectors/), currently sourced from [`pipelineframework/docs/develop/connectors`](https://github.com/The-Pipeline-Framework/pipelineframework/tree/main/docs/develop/connectors). Update those pages when a public connector capability changes; extend architecture/deploy/operate documentation where its audience requires it. Use the canonical `tpf-docs-writer` guidance when available, while keeping this public contribution path independent of private foundry skills. Preserve published routes and frozen release documentation.

Verify the affected component seam against the exact dependency artifacts used by this checkout:

| Owner | Evidence needed when that seam changes |
| --- | --- |
| [Contracts](https://github.com/The-Pipeline-Framework/pipelineframework-contracts) | Public Connector SPI, shared protocol and descriptor identity; distinguish framework-neutral contracts from connector-contributed vocabulary. |
| [Compiler](https://github.com/The-Pipeline-Framework/pipelineframework-compiler) | Contributed-type discovery/validation, generated Java/protobuf/adapters and import metadata in a compiling consumer. |
| [Runtime](https://github.com/The-Pipeline-Framework/pipelineframework-runtime) | Actual invocation/context/lifecycle and applicable Query capture, Command or Await guarantees. |
| [Canonical docs and coordination](https://github.com/The-Pipeline-Framework/pipelineframework) | Author-facing contract, architectural ownership and the existing cross-repository compatibility process. |

Use bounded GitNexus structural discovery where needed, then verify in the exact owning source/artifacts; a main-branch index alone does not establish release support. Search a close precedent before inventing an adapter. If a framework change is required, coordinate it in its owning repository using the compatibility-set process in [AGENTS.md](AGENTS.md); do not copy its implementation here, use a source fallback, or publish intermediate snapshots. Unchanged dependencies do not need ceremonial PRs or a repeated full graph investigation.

## Start with a narrow contract

Before adding a module or provider, state the consumer capability and its portable input/output contract in the issue or pull request. Separate shared vocabulary from a particular provider adapter, and keep the first slice small enough to review independently. Issues [#41](https://github.com/The-Pipeline-Framework/pipelineframework-connectors/issues/41) and [#42](https://github.com/The-Pipeline-Framework/pipelineframework-connectors/issues/42) illustrate this: #41 sets the ecosystem boundary and sequencing; #42 asks for portable conversation/context types, not a conversation controller or application implementation.

The application or authorized host owns scope, identity, credentials, endpoint policy, disclosure, retention, provider selection, and approval authority. Connectors may perform provider SDK and storage I/O. Preserve native Query capture/replay for observations and Command authority/idempotency for effects. Data returned from external systems is evidence, not authority. Pure deterministic utilities do not need to be exposed as external Queries. Keep authored application transformations free of hidden network or storage I/O.

## Ownership and code map

- `framework/connectors/<name>/` contains connector modules; `framework/connectors/pom.xml` aggregates them. Provider implementations commonly live under `src/main/java`, provider registration under `src/main/resources/META-INF/services/`, and tests/resources under `src/test/`.
- Reuse the existing provider-neutral contract seam where appropriate (for example `framework/connectors/http-contract/`). Create a separate contract module only for a demonstrated shared ownership/release need; avoid provider SDK types in portable contracts.
- `framework/representation-provider-*` contains representation providers. `framework/connector-import-tooling/` and the connector Maven plugins contain import/generation tooling.
- `framework/host-*` contains TPF-owned authorized service hosts. Application binding and credentials remain application/host configuration.
- `pom.xml` records this checkout's active TPF component snapshot line. Consume released artifacts; do not copy compiler/runtime source or invent a stable baseline.

Useful precedents:

- `framework/connectors/http/` provides an existing Connector SPI discovery resource at `src/main/resources/META-INF/services/org.pipelineframework.connector.ConnectorProvider`; use its actual public contract and tests rather than assuming every connector has the same registration path.
- `framework/connectors/vector-store/` provides a contributed protocol-type discovery precedent at `src/main/resources/META-INF/services/org.pipelineframework.protocol.ProtocolTypeContributor`. Check the matching contracts/compiler machinery before adding vocabulary.
- `framework/connectors/query-jpa/` provides a generated-query consumer fixture: `src/test/resources/jpa-provider-query-pipeline.yaml` and `src/test/java/org/pipelineframework/connector/query/jpa/JpaGeneratedQueryJourneyTest.java`.
- `framework/connectors/vector-store-pgvector/` pairs focused unit tests with a PostgreSQL/pgvector service integration test.

## Tests and verification

Derive expected behavior from the original issue, canonical TPF contract and pinned provider specification before implementation. Record or reuse lightweight obligations and an adequacy owner independent of the implementation author in the existing issue/plan. A worker may challenge an obligation with evidence, but may not silently weaken it. No private foundry skill or separate verification agent is required.

Select failure hypotheses from the boundary actually changing: for example pagination and valid large result sets, unknown/nullable variants, ordering/cursors, ambiguous effects and retries, changed live state during Query replay, cancellation/resource cleanup, or cross-tenant identifiers. Use the cheapest reliable unit, contract, component or service-integration test with an observable independent oracle. Not every hypothesis applies. Inspect authoritative results/state when relevant; do not treat coverage, test count or mock invocations as the acceptance criterion.

Add focused tests beside the changed module. Cover contract validation, provider registration/discovery, typed binding and output where applicable, and error/edge behavior. If a public provider contract changes, include a minimal compiled consumer fixture showing the public provider can be discovered, its types resolve, and the binding reaches it; use the JPA fixture above as a pattern rather than copying its domain.

Run a focused module build with the repository-local Maven cache:

```sh
./mvnw -pl framework/connectors/<module> -am test -Dmaven.repo.local="$PWD/.m2/repository"
```

For ordinary PR CI's canonical reactor verification, the workflow runs `clean verify -DskipITs`; the repository-root README and `.github/workflows/verify.yml` describe its opt-in external-service lane. PostgreSQL/pgvector and local OIDC fixtures are service integration tests, not tests against commercial provider accounts. The stable downstream Connector suite is defined by `.github/tpf-system-tests.json` and runs through `scripts/system-tests.sh verify` with the same isolated cache. Follow [AGENTS.md](AGENTS.md) for candidate, compatibility-set, publication, and release gates; coordinated changes must use exact immutable candidates and the prescribed compatibility-set path.

Do not add a new module, portable type family, provider choice, CI job, or cross-repository contract based only on a plausible design. Keep the change within its owning repository, and update canonical `pipelineframework` docs/ADR when semantics or a shared contract actually changes, as required by [AGENTS.md](AGENTS.md).

Report the original requirement, semantic owner, implementation path and actual executable evidence together, including exact source/dependency versions, commands/results, substituted boundaries and unresolved gaps. Preserve failed/inconclusive probes honestly. Resolve acceptance blockers within scope, record unrelated defects/enhancements, and stop when the agreed obligations have sufficient evidence. Unexpected cross-repository, infrastructure or material cost expansion needs owner disposition before continuing. A connector fixture does not establish the consuming application's end-to-end acceptance.
