package org.pipelineframework.connector.objectingest;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.nio.charset.StandardCharsets;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CancellationException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.pipelineframework.config.boundary.PipelineObjectSourceConfig;
import org.pipelineframework.config.boundary.PipelineHttpPayloadBoundaryConfig;
import org.pipelineframework.config.boundary.PipelineObjectFilterConfig;
import org.pipelineframework.config.boundary.PipelineObjectIdentityConfig;
import org.pipelineframework.config.boundary.PipelineObjectNamingConfig;
import org.pipelineframework.config.boundary.PipelineObjectPayloadConfig;
import org.pipelineframework.config.boundary.PipelineObjectPollConfig;
import org.pipelineframework.config.boundary.PipelineObjectPublishGroupingConfig;
import org.pipelineframework.config.boundary.PipelineObjectPublishPayloadConfig;
import org.pipelineframework.config.boundary.PipelineObjectPublishConfig;
import org.pipelineframework.connector.ConnectorBindingDefinition;
import org.pipelineframework.connector.ConnectorBindingName;
import org.pipelineframework.connector.ConnectorBindingRegistry;
import org.pipelineframework.connector.ConnectorConfigurationDocument;
import org.pipelineframework.connector.ConnectorOperationKind;
import org.pipelineframework.connector.ConnectorPayloadOrigin;
import org.pipelineframework.connector.ConnectorProviderId;
import org.pipelineframework.connector.ConnectorRuntimeContext;
import org.pipelineframework.connector.MaterializedPayload;
import org.pipelineframework.connector.ObjectSourceOperation;
import org.pipelineframework.connector.ObjectReadSession;
import org.pipelineframework.connector.OwnedPayloadTransfer;
import org.pipelineframework.connector.PayloadBoundaryOwner;
import org.pipelineframework.connector.PayloadMaterializer;
import org.pipelineframework.objectingest.ObjectSourceItem;
import org.pipelineframework.objectingest.ObjectSourceProvider;
import org.pipelineframework.objectpublish.ObjectTargetProvider;
import org.pipelineframework.objectpublish.ObjectWriteOpenRequest;
import org.pipelineframework.objectpublish.ObjectWriteCloseRequest;
import org.pipelineframework.repository.PayloadReference;
import org.pipelineframework.step.NonRetryableException;

class FilesystemPayloadMaterializationTest {
    @TempDir
    Path tempDir;

    @Test
    void ownedUploadCanBeDownloadedButCrossTenantReferenceFailsBeforeRead() throws Exception {
        ConnectorBindingName bindingName = ConnectorBindingName.of("local-documents");
        ConnectorRuntimeContext context = ConnectorRuntimeContext.empty();
        ConnectorBindingRegistry bindings = ConnectorBindingRegistry.fromProviders(
            List.of(new ConnectorBindingDefinition(bindingName, ConnectorProviderId.of("filesystem.objects"), 1,
                ConnectorConfigurationDocument.empty())), List.of(new FilesystemObjectConnector()));
        PipelineObjectPublishConfig target = new PipelineObjectPublishConfig("documents", "object", "filesystem",
            java.util.Optional.of(bindingName.value()), Map.of("root", tempDir.toString()),
            PipelineObjectNamingConfig.defaults(), PipelineObjectPublishPayloadConfig.defaults(),
            PipelineObjectPublishGroupingConfig.defaults());
        PipelineObjectSourceConfig source = new PipelineObjectSourceConfig("documents", "object", "filesystem",
            java.util.Optional.of(bindingName.value()), Map.of("root", tempDir.toString()),
            PipelineObjectFilterConfig.defaults(), PipelineObjectPollConfig.defaults(),
            PipelineObjectIdentityConfig.defaults(), PipelineObjectPayloadConfig.reference());
        var upload = new PipelineHttpPayloadBoundaryConfig("upload", PipelineHttpPayloadBoundaryConfig.Direction.UPLOAD,
            "documents", "Document", "payload_ref", List.of("text/plain"), 1024, "document.write");
        var download = new PipelineHttpPayloadBoundaryConfig("download", PipelineHttpPayloadBoundaryConfig.Direction.DOWNLOAD,
            "documents", "Document", "payload_ref", List.of("text/plain"), 0, "document.read");
        OwnedPayloadTransfer engine = new OwnedPayloadTransfer(bindings, context,
            request -> new PayloadBoundaryOwner(request.requestedTenantId(), request.requestedScopeId()));
        byte[] bytes = "owned payload".getBytes(StandardCharsets.UTF_8);
        try {
            PayloadReference reference = engine.upload(upload, target, "alice", "tenant-a", "invoice-1",
                "text/plain", new ByteArrayInputStream(bytes));
            assertEquals(bytes.length, reference.sizeBytes());
            assertEquals("tenant-a", reference.metadata().get(OwnedPayloadTransfer.OWNER_TENANT));
            assertEquals(bindingName.value(), reference.metadata().get(OwnedPayloadTransfer.OWNER_BINDING));
            assertNotNull(reference.metadata().get(OwnedPayloadTransfer.OWNER_ORIGIN));
            assertNotNull(reference.metadata().get(FilesystemReferenceAuthority.CAPABILITY_METADATA));
            ByteArrayOutputStream output = new ByteArrayOutputStream();
            engine.download(download, source, "alice", "tenant-a", "invoice-1", reference, output);
            assertArrayEquals(bytes, output.toByteArray());
            assertThrows(SecurityException.class, () -> engine.openDownload(download, source,
                "mallory", "tenant-b", "invoice-1", reference));
            ConnectorPayloadOrigin origin = reference.connectorOrigin().orElseThrow();
            PayloadReference changedOrigin = reference.withConnectorOrigin(new ConnectorPayloadOrigin(
                origin.bindingName(), origin.operation(), origin.providerMajorVersion() + 1,
                origin.configuration()));
            assertThrows(SecurityException.class, () -> engine.openDownload(download, source,
                "alice", "tenant-a", "invoice-1", changedOrigin));
        } finally {
            bindings.stop(context).toCompletableFuture().join();
        }
    }

    @Test
    void oversizedOwnedUploadAbortsWithoutCompletedObject() throws Exception {
        ConnectorBindingName bindingName = ConnectorBindingName.of("local-documents");
        ConnectorRuntimeContext context = ConnectorRuntimeContext.empty();
        ConnectorBindingRegistry bindings = ConnectorBindingRegistry.fromProviders(
            List.of(new ConnectorBindingDefinition(bindingName, ConnectorProviderId.of("filesystem.objects"), 1,
                ConnectorConfigurationDocument.empty())), List.of(new FilesystemObjectConnector()));
        PipelineObjectPublishConfig target = new PipelineObjectPublishConfig("documents", "object", "filesystem",
            java.util.Optional.of(bindingName.value()), Map.of("root", tempDir.toString()),
            PipelineObjectNamingConfig.defaults(), PipelineObjectPublishPayloadConfig.defaults(),
            PipelineObjectPublishGroupingConfig.defaults());
        var upload = new PipelineHttpPayloadBoundaryConfig("upload", PipelineHttpPayloadBoundaryConfig.Direction.UPLOAD,
            "documents", "Document", "payload_ref", List.of("text/plain"), 2, "document.write");
        OwnedPayloadTransfer engine = new OwnedPayloadTransfer(bindings, context,
            request -> new PayloadBoundaryOwner("tenant-a", "invoice-1"));
        try {
            assertThrows(IllegalArgumentException.class, () -> engine.upload(upload, target, "alice", "tenant-a",
                "invoice-1", "text/plain", new ByteArrayInputStream("too large".getBytes(StandardCharsets.UTF_8))));
            try (var paths = Files.walk(tempDir)) {
                assertFalse(paths.anyMatch(Files::isRegularFile));
            }
        } finally {
            bindings.stop(context).toCompletableFuture().join();
        }
    }

    @Test
    void cancelledOwnedUploadAbortsItsProviderSession() throws Exception {
        ConnectorBindingName bindingName = ConnectorBindingName.of("local-documents");
        ConnectorRuntimeContext context = ConnectorRuntimeContext.empty();
        ConnectorBindingRegistry bindings = ConnectorBindingRegistry.fromProviders(
            List.of(new ConnectorBindingDefinition(bindingName, ConnectorProviderId.of("filesystem.objects"), 1,
                ConnectorConfigurationDocument.empty())), List.of(new FilesystemObjectConnector()));
        PipelineObjectPublishConfig target = new PipelineObjectPublishConfig("documents", "object", "filesystem",
            java.util.Optional.of(bindingName.value()), Map.of("root", tempDir.toString()),
            PipelineObjectNamingConfig.defaults(), PipelineObjectPublishPayloadConfig.defaults(),
            PipelineObjectPublishGroupingConfig.defaults());
        var upload = new PipelineHttpPayloadBoundaryConfig("upload", PipelineHttpPayloadBoundaryConfig.Direction.UPLOAD,
            "documents", "Document", "payload_ref", List.of("text/plain"), 1024, "document.write");
        OwnedPayloadTransfer engine = new OwnedPayloadTransfer(bindings, context,
            request -> new PayloadBoundaryOwner("tenant-a", "invoice-1"));
        AtomicBoolean cancelled = new AtomicBoolean();
        ByteArrayInputStream body = new ByteArrayInputStream("content".getBytes(StandardCharsets.UTF_8)) {
            @Override
            public synchronized int read(byte[] buffer, int offset, int length) {
                int count = super.read(buffer, offset, length);
                cancelled.set(true);
                return count;
            }
        };
        try {
            assertThrows(CancellationException.class, () -> engine.upload(upload, target, "alice", "tenant-a",
                "invoice-1", "text/plain", body, cancelled::get));
            try (var paths = Files.walk(tempDir)) {
                assertFalse(paths.anyMatch(Files::isRegularFile));
            }
        } finally {
            bindings.stop(context).toCompletableFuture().join();
        }
    }

    @Test
    void connectorTargetIssuesReferenceReadableByItsSourceOperation() throws Exception {
        byte[] bytes = "published".getBytes(StandardCharsets.UTF_8);
        String checksum = HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes));
        FilesystemObjectConnector connector = new FilesystemObjectConnector();
        ObjectTargetProvider target = connector.operations().stream()
            .filter(ObjectTargetProvider.class::isInstance).map(ObjectTargetProvider.class::cast)
            .findFirst().orElseThrow();
        ObjectSourceProvider source = connector.operations().stream()
            .filter(ObjectSourceProvider.class::isInstance).map(ObjectSourceProvider.class::cast)
            .findFirst().orElseThrow();
        PipelineObjectPublishConfig configuration = new PipelineObjectPublishConfig(
            "outputs", "object", "filesystem", Map.of("root", tempDir.toString()), null, null);
        var session = target.open(new ObjectWriteOpenRequest(
            "outputs", configuration, "result.txt", "text/plain", Map.of(), "upload-1"))
            .toCompletableFuture().join();
        session.write(ByteBuffer.wrap(bytes)).toCompletableFuture().join();
        PayloadReference issued = session.close(new ObjectWriteCloseRequest(bytes.length, checksum, Map.of()))
            .toCompletableFuture().join().reference();

        assertArrayEquals(bytes, source.materialize(issued, 1024).toCompletableFuture().join().bytes());
        try (ObjectReadSession read = source.openRead(issued).toCompletableFuture().join()) {
            byte[] streamed = new byte[bytes.length];
            read.read(bytes.length).toCompletableFuture().join().orElseThrow().get(streamed);
            assertArrayEquals(bytes, streamed);
        }
    }

    @Test
    void bindingOwnedFilesystemReferenceFeedsNeutralConsumerWithoutMapperGlue() throws Exception {
        byte[] expected = "portable payload".getBytes(StandardCharsets.UTF_8);
        Files.write(tempDir.resolve("document.txt"), expected);
        ConnectorBindingName bindingName = ConnectorBindingName.of("local-documents");
        ConnectorBindingRegistry bindings = ConnectorBindingRegistry.fromProviders(
            List.of(new ConnectorBindingDefinition(
                bindingName, ConnectorProviderId.of("filesystem.objects"), 1, ConnectorConfigurationDocument.empty())),
            List.of(new FilesystemObjectConnector()));
        bindings.activate(bindingName, ConnectorRuntimeContext.empty()).toCompletableFuture().join();
        try {
            ObjectSourceProvider sourceOperation = (ObjectSourceProvider) bindings.requireOperation(
                bindingName, "filesystem", ConnectorOperationKind.OBJECT_SOURCE, 1);
            ObjectSourceItem item = sourceOperation.list(source(), 1).getFirst();
            PayloadReference reference = item.contentRef().withConnectorOrigin(
                bindings.objectSourceOrigin(bindingName, "filesystem", 1));
            NeutralConsumer consumer = new NeutralConsumer(bindings::materialize);

            MaterializedPayload payload = consumer.consume(reference, 1024);

            assertArrayEquals(expected, payload.bytes());
            assertEquals("raw", payload.codec());
            assertEquals(reference.checksum(), payload.checksum());

            Files.writeString(tempDir.resolve("document.txt"), "changed");
            CompletionException failure = assertThrows(
                CompletionException.class, () -> consumer.consume(reference, 1024));
            assertEquals("Filesystem payload checksum mismatch: document.txt", failure.getCause().getMessage());
        } finally {
            bindings.stop(ConnectorRuntimeContext.empty()).toCompletableFuture().join();
        }
    }

    @Test
    void enforcesDeclaredMaximumBeforeOpeningFilesystemContent() throws Exception {
        Files.writeString(tempDir.resolve("large.txt"), "1234567890");
        FilesystemObjectSourceProvider provider = new FilesystemObjectSourceProvider();
        PayloadReference reference = provider.list(source(), 1).getFirst().contentRef();

        CompletionException failure = assertThrows(CompletionException.class, () ->
            provider.materialize(reference, 3).toCompletableFuture().join());

        assertInstanceOf(NonRetryableException.class, failure.getCause());
        assertEquals("Object exceeds configured maxBytes: large.txt", failure.getCause().getMessage());
    }

    @Test
    void rejectsModifiedFilesystemLocatorProvenance() throws Exception {
        byte[] expected = "portable payload".getBytes(StandardCharsets.UTF_8);
        Files.write(tempDir.resolve("document.txt"), expected);
        FilesystemObjectSourceProvider provider = new FilesystemObjectSourceProvider();
        PayloadReference reference = provider.list(source(), 1).getFirst().contentRef();

        Files.write(tempDir.resolve("other.txt"), expected);
        PayloadReference modifiedKey = copyWithLocator(reference, reference.container(), "other.txt");

        Path otherRoot = Files.createDirectory(tempDir.resolve("other-root"));
        Files.write(otherRoot.resolve("document.txt"), expected);
        PayloadReference modifiedContainer = copyWithLocator(reference, otherRoot.toRealPath().toString(), reference.key());

        CompletionException keyFailure = assertThrows(CompletionException.class, () ->
            provider.materialize(modifiedKey, 1024).toCompletableFuture().join());
        CompletionException containerFailure = assertThrows(CompletionException.class, () ->
            provider.materialize(modifiedContainer, 1024).toCompletableFuture().join());

        assertEquals(
            "Filesystem payload locator provenance mismatch: other.txt", keyFailure.getCause().getMessage());
        assertEquals(
            "Filesystem payload locator provenance mismatch: document.txt", containerFailure.getCause().getMessage());
    }

    @Test
    void rejectsRecomputedLegacyDigestForUnlistedFile() throws Exception {
        Files.writeString(tempDir.resolve("listed.txt"), "listed");
        Files.writeString(tempDir.resolve("private.txt"), "private");
        FilesystemObjectSourceProvider provider = new FilesystemObjectSourceProvider();
        PayloadReference issued = provider.list(source(), 1).getFirst().contentRef();
        Path root = tempDir.toRealPath();
        String key = "private.txt";
        byte[] legacyLocator = (root + "\n" + key + "\n" + root.resolve(key))
            .getBytes(StandardCharsets.UTF_8);
        Map<String, String> metadata = new java.util.HashMap<>(issued.metadata());
        metadata.put("tpf.filesystem.locator.sha256",
            HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(legacyLocator)));
        PayloadReference forged = new PayloadReference(
            issued.provider(), issued.container(), key, issued.contentType(), issued.codec(),
            issued.checksum(), issued.sizeBytes(), issued.version(), metadata, issued.connectorOrigin());

        CompletionException failure = assertThrows(CompletionException.class, () ->
            provider.materialize(forged, 1024).toCompletableFuture().join());
        assertEquals("Filesystem payload locator provenance mismatch: private.txt", failure.getCause().getMessage());
    }

    @Test
    void fallbackAuthoritySurvivesProviderRecreationWithinProcess() throws Exception {
        Files.writeString(tempDir.resolve("document.txt"), "content");
        PayloadReference issued = new FilesystemObjectSourceProvider().list(source(), 1).getFirst().contentRef();

        byte[] bytes = new FilesystemObjectSourceProvider().materialize(issued, 1024)
            .toCompletableFuture().join().bytes();
        assertArrayEquals("content".getBytes(StandardCharsets.UTF_8), bytes);
    }

    @Test
    void configuredAuthoritySurvivesProviderRecreation() throws Exception {
        String property = "tpf.object.reference.hmac-key";
        String previous = System.getProperty(property);
        System.setProperty(property, java.util.Base64.getEncoder().encodeToString(new byte[32]));
        try {
            Files.writeString(tempDir.resolve("document.txt"), "content");
            PayloadReference issued = new FilesystemObjectSourceProvider().list(source(), 1).getFirst().contentRef();
            byte[] bytes = new FilesystemObjectSourceProvider().materialize(issued, 1024)
                .toCompletableFuture().join().bytes();
            assertArrayEquals("content".getBytes(StandardCharsets.UTF_8), bytes);
        } finally {
            if (previous == null) {
                System.clearProperty(property);
            } else {
                System.setProperty(property, previous);
            }
        }
    }

    @Test
    void streamingReadOnlyAdvancesOnDemandAndClosesOnCancellation() throws Exception {
        Files.writeString(tempDir.resolve("document.txt"), "abcdef");
        FilesystemObjectSourceProvider provider = new FilesystemObjectSourceProvider(Runnable::run);
        PayloadReference issued = provider.list(source(), 1).getFirst().contentRef();

        ObjectReadSession session = provider.openRead(issued).toCompletableFuture().join();
        byte[] first = new byte[2];
        session.read(2).toCompletableFuture().join().orElseThrow().get(first);
        assertArrayEquals("ab".getBytes(StandardCharsets.UTF_8), first);
        session.close();
        CompletionException closed = assertThrows(CompletionException.class, () ->
            session.read(2).toCompletableFuture().join());
        assertEquals("filesystem read session is closed", closed.getCause().getMessage());

        try (ObjectReadSession complete = provider.openRead(issued).toCompletableFuture().join()) {
            byte[] all = new byte[6];
            complete.read(6).toCompletableFuture().join().orElseThrow().get(all);
            assertArrayEquals("abcdef".getBytes(StandardCharsets.UTF_8), all);
            assertFalse(complete.read(6).toCompletableFuture().join().isPresent());
        }
    }

    @Test
    void rejectsInRootSymlinkThatEscapesCanonicalSourceRoot() throws Exception {
        byte[] expected = "portable payload".getBytes(StandardCharsets.UTF_8);
        Path sourceFile = tempDir.resolve("document.txt");
        Files.write(sourceFile, expected);
        FilesystemObjectSourceProvider provider = new FilesystemObjectSourceProvider();
        PayloadReference reference = provider.list(source(), 1).getFirst().contentRef();
        Path outside = Files.createTempFile("tpf-outside-", ".txt");
        try {
            Files.write(outside, expected);
            Files.delete(sourceFile);
            Files.createSymbolicLink(sourceFile, outside);

            CompletionException failure = assertThrows(CompletionException.class, () ->
                provider.materialize(reference, 1024).toCompletableFuture().join());

            assertEquals(
                "Filesystem object path escapes canonical root: document.txt", failure.getCause().getMessage());
        } finally {
            Files.deleteIfExists(outside);
        }
    }

    @Test
    void materializesOnProviderManagedExecutor() throws Exception {
        byte[] expected = "worker payload".getBytes(StandardCharsets.UTF_8);
        Files.write(tempDir.resolve("document.txt"), expected);
        AtomicReference<Runnable> scheduled = new AtomicReference<>();
        FilesystemObjectSourceProvider provider = new FilesystemObjectSourceProvider(scheduled::set);
        PayloadReference reference = provider.list(source(), 1).getFirst().contentRef();

        CompletionStage<MaterializedPayload> pending = provider.materialize(reference, 1024);

        assertFalse(pending.toCompletableFuture().isDone());
        assertNotNull(scheduled.get());
        scheduled.get().run();
        assertArrayEquals(expected, pending.toCompletableFuture().join().bytes());

    }

    private PipelineObjectSourceConfig source() {
        return new PipelineObjectSourceConfig(
            "documents", "object", "filesystem", Map.of("root", tempDir.toString()), null, null, null, null);
    }

    private PayloadReference copyWithLocator(PayloadReference reference, String container, String key) {
        return new PayloadReference(
            reference.provider(),
            container,
            key,
            reference.contentType(),
            reference.codec(),
            reference.checksum(),
            reference.sizeBytes(),
            reference.version(),
            reference.metadata(),
            reference.connectorOrigin());
    }

    private record NeutralConsumer(PayloadMaterializer materializer) {
        private MaterializedPayload consume(PayloadReference reference, long maxBytes) {
            return materializer.materialize(reference, maxBytes).toCompletableFuture().join();
        }
    }
}
