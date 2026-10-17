package org.pipelineframework.connector.conversation;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import javax.tools.DiagnosticCollector;
import javax.tools.JavaFileObject;
import javax.tools.ToolProvider;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.pipelineframework.proto.PipelineJavaDomainGenerator;
import org.pipelineframework.proto.PipelineProtoGenerator;

/** Compiles a consumer's application-owned state through the released annotation processor. */
class ConversationGeneratedContractTest {
    @TempDir Path temporary;

    @Test
    void resolvesContributedConversationTypesIntoCanonicalMetadata() throws Exception {
        Path source = temporary.resolve("Consumer.java");
        Path stateSource = temporary.resolve("TurnState.java");
        Files.writeString(stateSource, """
            package example;
            public record TurnState(
                org.pipelineframework.connector.conversation.ConversationScope scope,
                org.pipelineframework.connector.conversation.ConversationTurn lastTurn,
                org.pipelineframework.connector.conversation.HistoryPage history) {}
            """);
        Files.writeString(source, """
            package example;
            public class Consumer {
                public io.smallrye.mutiny.Uni<TurnState> process(TurnState state) {
                    return io.smallrye.mutiny.Uni.createFrom().item(state);
                }
            }
            """);
        Path classes = temporary.resolve("classes");
        Path generated = temporary.resolve("generated");
        Files.createDirectories(classes);
        Files.createDirectories(generated);
        var diagnostics = new DiagnosticCollector<JavaFileObject>();
        var compiler = ToolProvider.getSystemJavaCompiler();
        try (var files = compiler.getStandardFileManager(diagnostics, null, null)) {
            var options = List.of("-proc:only", "-d", classes.toString(), "-s", generated.toString(),
                "-classpath", System.getProperty("java.class.path"),
                "-processor", "org.pipelineframework.processor.PipelineStepProcessor",
                "-Apipeline.config=" + Path.of("src/test/resources/conversation-type-proof.yaml").toAbsolutePath());
            boolean success = Boolean.TRUE.equals(compiler.getTask(null, files, diagnostics, options, null,
                files.getJavaFileObjects(source, stateSource)).call());
            assertTrue(success, diagnostics.getDiagnostics().toString());
        }
        Path contract = classes.resolve("META-INF/pipeline/pipeline-contract.json");
        assertTrue(Files.exists(contract), "compiler must emit canonical contract metadata; files="
            + Files.walk(temporary).map(Path::toString).toList());
        String metadata = Files.readString(contract);
        assertTrue(metadata.contains("tpf.conversation.ConversationTurn"), metadata);
        assertTrue(metadata.contains("TurnState"), metadata);
    }

    @Test
    void rendersApplicationOwnedJavaAndProtobufFromContributedTypes() throws Exception {
        Path fixture = temporary.resolve("conversation.yaml");
        Files.copy(Path.of("src/test/resources/conversation-type-proof.yaml"), fixture);
        Path java = temporary.resolve("generated-domain");
        Path protobuf = temporary.resolve("generated-proto");
        System.setProperty("pipeline.idl.bootstrap", "true");
        try {
            new PipelineProtoGenerator().generate(temporary, fixture, protobuf);
            new PipelineJavaDomainGenerator().generate(temporary, Optional.of(fixture), Optional.of(java));
        } finally {
            System.clearProperty("pipeline.idl.bootstrap");
        }
        var javaSources = Files.walk(java).filter(path -> path.toString().endsWith(".java")).toList();
        Path state = javaSources.stream().filter(path -> path.getFileName().toString().equals("TurnState.java"))
            .findFirst().orElseThrow();
        String stateSource = Files.readString(state);
        assertTrue(stateSource.contains("ConversationTurn"), stateSource);
        assertTrue(stateSource.contains("HistoryPage"), stateSource);
        String types = Files.readString(protobuf.resolve("pipeline-types.proto"));
        assertTrue(types.contains("ConversationTurn"), types);
        assertTrue(types.contains("TurnState"), types);
    }
}
