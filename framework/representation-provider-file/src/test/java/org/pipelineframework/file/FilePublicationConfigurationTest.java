package org.pipelineframework.file;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import io.smallrye.mutiny.Uni;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.pipelineframework.config.boundary.PipelineObjectPayloadConfig;
import org.pipelineframework.config.boundary.PipelineObjectSourceConfig;
import org.pipelineframework.connector.ConnectorBindingDefinition;
import org.pipelineframework.connector.ConnectorBindingName;
import org.pipelineframework.connector.ConnectorBindingRegistry;
import org.pipelineframework.connector.ConnectorConfigurationDocument;
import org.pipelineframework.connector.ConnectorRuntimeContext;
import org.pipelineframework.connector.objectingest.FilesystemObjectConnector;
import org.pipelineframework.connector.objectingest.FilesystemObjectSourceProvider;

/** Exercises the production constructor and actual file publication from an installed-app working directory. */
class FilePublicationConfigurationTest {
    @TempDir
    Path temporary;

    @Test
    void honorsEnvironmentPathOutsideWorkingDirectory() throws Exception {
        Path config = pipeline("Application Support/custom-production.yaml");
        succeeds(run(Optional.empty(), Optional.of(config.toString()), workingDirectory()));
    }

    @Test
    void propertyTakesPrecedenceOverEnvironmentAndDiscoveredPipeline() throws Exception {
        Path config = pipeline("Application Support/property.yaml");
        Path working = workingDirectory();
        Files.writeString(working.resolve("pipeline.yaml"), "invalid: [");
        succeeds(run(Optional.of(config.toString()), Optional.of(temporary.resolve("missing.yaml").toString()), working));
    }

    @Test
    void blankPropertyFallsBackToEnvironment() throws Exception {
        succeeds(run(Optional.of("  "), Optional.of(pipeline("env.yaml").toString()), workingDirectory()));
    }

    @Test
    void discoversPipelineWhenExplicitSettingsAreBlank() throws Exception {
        Path config = pipeline("working/config/pipeline.yaml");
        succeeds(run(Optional.of("  "), Optional.of("  "), config.getParent().getParent()));
    }

    @Test
    void resolvesRelativeExplicitPath() throws Exception {
        Path config = pipeline("working/config/custom.yaml");
        succeeds(run(Optional.empty(), Optional.of("config/custom.yaml"), config.getParent().getParent()));
    }

    @Test
    void missingExplicitPathFailsInsteadOfUsingDiscoveredPipeline() throws Exception {
        Path config = pipeline("working/config/pipeline.yaml");
        Path missing = temporary.resolve("missing.yaml");
        Result result = run(Optional.empty(), Optional.of(missing.toString()), config.getParent().getParent());
        assertTrue(result.exitCode() != 0, result.output());
        assertTrue(result.output().contains("missing.yaml"), result.output());
    }

    @Test
    void malformedExplicitFileFailsInsteadOfUsingDiscoveredPipeline() throws Exception {
        Path config = pipeline("working/config/pipeline.yaml");
        Path invalid = temporary.resolve("invalid.yaml");
        Files.writeString(invalid, "invalid: [");
        Result result = run(Optional.of(invalid.toString()), Optional.of(config.toString()), config.getParent().getParent());
        assertTrue(result.exitCode() != 0, result.output());
    }

    private Path pipeline(String relative) throws Exception {
        Path config = temporary.resolve(relative);
        Files.createDirectories(config.getParent());
        Path output = Files.createDirectories(temporary.resolve("published"));
        Files.writeString(config, """
            basePackage: example
            transport: LOCAL
            publish:
              rendered:
                kind: object
                provider: filesystem
                binding: documents
                location:
                  root: '%s'
            """.formatted(output));
        return config;
    }

    private Path workingDirectory() throws Exception {
        return Files.createDirectories(temporary.resolve("unrelated working directory"));
    }

    private Result run(Optional<String> property, Optional<String> environment, Path working) throws Exception {
        Path input = Files.createDirectories(temporary.resolve("input"));
        Files.writeString(input.resolve("invoice.txt"), "invoice evidence");
        List<String> command = new ArrayList<>();
        command.add(Path.of(System.getProperty("java.home"), "bin", "java").toString());
        command.add("--enable-preview");
        property.ifPresent(value -> command.add("-Dpipeline.config=" + value));
        command.add("-cp");
        command.add(System.getProperty("surefire.test.class.path", System.getProperty("java.class.path")));
        command.add(PublicationProcess.class.getName());
        command.add(input.toString());
        Path log = temporary.resolve("process.log");
        ProcessBuilder builder = new ProcessBuilder(command).directory(working.toFile())
            .redirectErrorStream(true).redirectOutput(log.toFile());
        builder.environment().remove("PIPELINE_CONFIG");
        builder.environment().remove("JAVA_TOOL_OPTIONS");
        builder.environment().remove("JDK_JAVA_OPTIONS");
        environment.ifPresent(value -> builder.environment().put("PIPELINE_CONFIG", value));
        Process process = builder.start();
        if (!process.waitFor(30, TimeUnit.SECONDS)) {
            process.destroyForcibly();
            throw new AssertionError("Publication process timed out: " + Files.readString(log));
        }
        return new Result(process.exitValue(), Files.readString(log));
    }

    private void succeeds(Result result) throws Exception {
        assertEquals(0, result.exitCode(), result.output());
        assertEquals("INVOICE EVIDENCE", Files.readString(temporary.resolve("published/rendered.txt")));
    }

    private record Result(int exitCode, String output) { }

    public static final class PublicationProcess {
        public static void main(String[] args) throws Exception {
            try {
                publish(args);
            } catch (Exception failure) {
                failure.printStackTrace();
                System.exit(1);
            }
            System.exit(0);
        }

        private static void publish(String[] args) throws Exception {
            Path inputRoot = Path.of(args[0]);
            PipelineObjectSourceConfig source = new PipelineObjectSourceConfig(
                "documents", "object", "filesystem", Optional.of("documents"), Map.of("root", inputRoot.toString()),
                null, null, null, PipelineObjectPayloadConfig.reference());
            FilesystemObjectSourceProvider provider = new FilesystemObjectSourceProvider();
            var raw = provider.list(source, 10).getFirst().contentRef();
            FilesystemObjectConnector connector = new FilesystemObjectConnector();
            ConnectorBindingRegistry bindings = ConnectorBindingRegistry.fromProviders(
                List.of(new ConnectorBindingDefinition(ConnectorBindingName.of("documents"), connector.id(), 1,
                    new ConnectorConfigurationDocument(Map.of()))), List.of(connector));
            try {
                var owned = bindings.ownPayloadReference(ConnectorBindingName.of("documents"),
                    provider.id(), provider.majorVersion(), raw);
                FileRepresentationRuntime runtime = new FileRepresentationRuntime(bindings, ConnectorRuntimeContext.empty());
                runtime.oneToOne(owned, 1024, "rendered", 1024, Optional.empty(), input -> {
                    try {
                        Path output = input.getParent().getParent().resolve("output/rendered.txt");
                        return Uni.createFrom().item(Files.writeString(output, Files.readString(input).toUpperCase(java.util.Locale.ROOT)));
                    } catch (java.io.IOException failure) {
                        return Uni.createFrom().failure(failure);
                    }
                }).await().atMost(Duration.ofSeconds(10));
            } finally {
                bindings.stop(ConnectorRuntimeContext.empty()).toCompletableFuture().join();
            }
        }
    }
}
