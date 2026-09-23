package org.pipelineframework.connector.objectingest;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.CompletionException;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.pipelineframework.config.boundary.PipelineObjectPublishConfig;
import org.pipelineframework.objectpublish.ObjectWriteCloseRequest;
import org.pipelineframework.objectpublish.ObjectWriteOpenRequest;
import org.pipelineframework.objectpublish.ObjectWriteResult;
import org.pipelineframework.objectpublish.ObjectWriteSession;
import org.pipelineframework.objectpublish.PagedObjectCompositionRequest;
import org.pipelineframework.objectpublish.PagedObjectPartQuery;

class FilesystemObjectTargetProviderTest {

    @TempDir
    Path tempDir;

    @Test
    void streamsChunksAndMovesTempFileOnClose() throws Exception {
        FilesystemObjectTargetProvider provider = new FilesystemObjectTargetProvider(Runnable::run);
        ObjectWriteSession session = provider.open(openRequest(target(tempDir), "results/payments.csv"))
            .toCompletableFuture().join();

        session.write(ByteBuffer.wrap("id,amount\n".getBytes(StandardCharsets.UTF_8))).toCompletableFuture().join();
        session.write(ByteBuffer.wrap("1,10\n".getBytes(StandardCharsets.UTF_8))).toCompletableFuture().join();
        ObjectWriteResult result = session.close(new ObjectWriteCloseRequest(
            "id,amount\n1,10\n".getBytes(StandardCharsets.UTF_8).length,
            "checksum",
            Map.of("recordCount", "1"))).toCompletableFuture().join();

        assertEquals("id,amount\n1,10\n", Files.readString(tempDir.resolve("results/payments.csv")));
        assertEquals("filesystem", result.reference().provider());
        assertEquals(tempDir.toRealPath().toString(), result.reference().container());
        assertEquals("results/payments.csv", result.reference().key());
        assertEquals("text/csv", result.reference().contentType());
        assertEquals("checksum", result.checksum());
        assertEquals("1", result.reference().metadata().get("recordCount"));
        assertTrue(Files.list(tempDir.resolve("results"))
            .noneMatch(path -> path.getFileName().toString().contains(".tpf-publish-")));
    }

    @Test
    void abortDeletesTempFile() throws Exception {
        FilesystemObjectTargetProvider provider = new FilesystemObjectTargetProvider(Runnable::run);
        ObjectWriteSession session = provider.open(openRequest(target(tempDir), "results/payments.csv"))
            .toCompletableFuture().join();

        session.write(ByteBuffer.wrap("partial".getBytes(StandardCharsets.UTF_8))).toCompletableFuture().join();
        session.abort(new RuntimeException("failed")).toCompletableFuture().join();

        assertFalse(Files.exists(tempDir.resolve("results/payments.csv")));
        assertTrue(Files.list(tempDir.resolve("results"))
            .noneMatch(path -> path.getFileName().toString().contains(".tpf-publish-")));
    }

    @Test
    void atomicallyReplacesAnExistingTarget() throws Exception {
        FilesystemObjectTargetProvider provider = new FilesystemObjectTargetProvider(Runnable::run);
        write(provider, "first");

        write(provider, "replacement");

        assertEquals("replacement", Files.readString(tempDir.resolve("results/payments.csv")));
    }

    @Test
    void rejectsEscapedOutputPath() {
        PipelineObjectPublishConfig target = target(tempDir);

        CompletionException exception = assertThrows(CompletionException.class, () ->
            new FilesystemObjectTargetProvider(Runnable::run).open(openRequest(target, "../outside.csv"))
                .toCompletableFuture().join());
        assertTrue(exception.getCause() instanceof SecurityException);
    }

    @Test
    void composesCommittedPagePartsInOrderWithOnePrefixAndSuffix() throws Exception {
        FilesystemObjectTargetProvider provider = new FilesystemObjectTargetProvider(Runnable::run);
        PipelineObjectPublishConfig target = target(tempDir);
        writePagePart(provider, target, ".tpf-pages/run/group/page-000.part", 0, "first,\"María\nGarcía\"\n");
        writePagePart(provider, target, ".tpf-pages/run/group/page-001.part", 1, "second,Zoë\n");

        var parts = provider.listParts(new PagedObjectPartQuery(
            target.name(), target, ".tpf-pages/run/")).toCompletableFuture().join();
        ObjectWriteResult result = provider.compose(new PagedObjectCompositionRequest(
            target.name(), target, "results/payments.csv", "text/csv", Map.of("recordCount", "2"),
            "compose-run", "header\n".getBytes(StandardCharsets.UTF_8),
            parts.stream().map(part -> part.objectKey()).toList(),
            "footer\n".getBytes(StandardCharsets.UTF_8))).toCompletableFuture().join();

        assertEquals(2, parts.size());
        assertEquals("header\nfirst,\"María\nGarcía\"\nsecond,Zoë\nfooter\n",
            Files.readString(tempDir.resolve("results/payments.csv")));
        assertEquals(Files.size(tempDir.resolve("results/payments.csv")), result.bytes());
    }

    private PipelineObjectPublishConfig target(Path root) {
        return new PipelineObjectPublishConfig(
            "results",
            "object",
            "filesystem",
            Map.of("root", root.toString()),
            null,
            null);
    }

    private ObjectWriteOpenRequest openRequest(PipelineObjectPublishConfig target, String key) {
        return new ObjectWriteOpenRequest(
            target.name(),
            target,
            key,
            "text/csv",
            Map.of("kind", "test"),
            "idempotency");
    }

    private void write(FilesystemObjectTargetProvider provider, String payload) {
        ObjectWriteSession session = provider.open(openRequest(target(tempDir), "results/payments.csv"))
            .toCompletableFuture().join();
        session.write(ByteBuffer.wrap(payload.getBytes(StandardCharsets.UTF_8))).toCompletableFuture().join();
        session.close(new ObjectWriteCloseRequest(payload.length(), "checksum", Map.of())).toCompletableFuture().join();
    }

    private void writePagePart(
        FilesystemObjectTargetProvider provider,
        PipelineObjectPublishConfig target,
        String key,
        int pageIndex,
        String payload) {
        Map<String, String> metadata = Map.of(
            "tpf.page.index", String.valueOf(pageIndex),
            "tpf.page.group", "payments",
            "tpf.page.finalKey", "results/payments.csv",
            "tpf.page.contentType", "text/csv",
            "tpf.page.partKey", key,
            "recordCount", "1");
        ObjectWriteSession session = provider.open(new ObjectWriteOpenRequest(
            target.name(), target, key, "text/csv", metadata, "page-" + pageIndex))
            .toCompletableFuture().join();
        byte[] bytes = payload.getBytes(StandardCharsets.UTF_8);
        session.write(ByteBuffer.wrap(bytes)).toCompletableFuture().join();
        session.close(new ObjectWriteCloseRequest(bytes.length, "checksum-" + pageIndex, metadata))
            .toCompletableFuture().join();
    }
}
