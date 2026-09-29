# Unit-test coverage

The Maven reactor instruments unit tests with JaCoCo during the standard `verify`
lifecycle. Each module writes an XML report to
`target/site/jacoco/jacoco.xml`. Pull-request verification adds the combined
line-coverage figure to the GitHub Actions run summary and uploads the module
reports as the `jacoco-unit-coverage` artifact.

The reports exclude MapStruct-generated `*MapperImpl` classes because their
generated mapping instructions are not maintained as source code. Other
production classes remain in the report. The coverage figure is informational:
there is no minimum percentage or build-breaking coverage threshold.

Unit-test coverage runs with the normal pull-request verification. External
service integration tests remain in their separately dispatched workflow lane
and are not required to produce the unit-test coverage reports.
