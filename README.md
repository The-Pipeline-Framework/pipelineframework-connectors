# The Pipeline Framework Connectors

Standalone connector modules for The Pipeline Framework: typed connectors, representation providers, import tooling, and TPF-owned host integrations.

Build and run unit tests with Java 21 using `./mvnw -Dmaven.repo.local="$PWD/.m2/repository" clean verify -DskipITs`.

Pull-request CI runs compilation, unit tests, and publication-contract verification. It deliberately defers the Failsafe `*IT` suites for the Hibernate Reactive query connector, pgvector connector, and OIDC host connection: these are the slower external-service integration lane. A maintainer can run that lane from the Actions `Verify` workflow by selecting `Run external-service integration tests`; the opt-in run executes the full reactor verification, including those integration tests.

This staging checkout is intentionally non-deployable. Contracts, compiler, and runtime artifacts are resolved from their released snapshot repositories.
