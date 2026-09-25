package org.pipelineframework.connector.objectingest;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.charset.StandardCharsets;
import java.nio.channels.FileChannel;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.HexFormat;
import java.util.List;
import java.util.ArrayList;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.concurrent.ForkJoinPool;

import org.pipelineframework.objectpublish.ObjectTargetProvider;
import org.pipelineframework.objectpublish.PagedObjectTargetProvider;
import org.pipelineframework.objectpublish.PagedObjectPart;
import org.pipelineframework.objectpublish.PagedObjectPartQuery;
import org.pipelineframework.objectpublish.PagedObjectCompositionRequest;
import org.pipelineframework.objectpublish.ObjectWriteCloseRequest;
import org.pipelineframework.objectpublish.ObjectWriteOpenRequest;
import org.pipelineframework.objectpublish.ObjectWriteResult;
import org.pipelineframework.objectpublish.ObjectWriteSession;
import org.pipelineframework.repository.PayloadReference;

/**
 * Filesystem object target provider for Object Publish.
 */
public class FilesystemObjectTargetProvider implements PagedObjectTargetProvider {
    private static final String LOCATOR_DIGEST_METADATA = "tpf.filesystem.locator.sha256";
    private final Executor executor;

    public FilesystemObjectTargetProvider() {
        this(ForkJoinPool.commonPool());
    }

    FilesystemObjectTargetProvider(Executor executor) {
        this.executor = executor;
    }

    @Override
    public String providerName() {
        return "filesystem";
    }

    @Override
    public CompletionStage<ObjectWriteSession> open(ObjectWriteOpenRequest request) {
        return CompletableFuture.supplyAsync(() -> openBlocking(request), executor);
    }

    @Override
    public CompletionStage<List<PagedObjectPart>> listParts(PagedObjectPartQuery query) {
        return CompletableFuture.supplyAsync(() -> listPartsBlocking(query), executor);
    }

    @Override
    public CompletionStage<ObjectWriteResult> compose(PagedObjectCompositionRequest request) {
        return CompletableFuture.supplyAsync(() -> composeBlocking(request), executor);
    }

    private List<PagedObjectPart> listPartsBlocking(PagedObjectPartQuery query) {
        Path root = root(query.targetName(), query.target());
        Path staged = requireUnderRoot(root, root.resolve(query.stagePrefix()).normalize());
        if (!Files.exists(staged)) {
            return List.of();
        }
        try (var paths = Files.walk(staged)) {
            List<PagedObjectPart> parts = new ArrayList<>();
            for (Path manifest : paths.filter(path -> path.getFileName().toString().endsWith(".part.manifest")).toList()) {
                Properties values = new Properties();
                try (InputStream input = Files.newInputStream(manifest)) {
                    values.load(input);
                }
                Map<String, String> metadata = new LinkedHashMap<>();
                values.stringPropertyNames().stream()
                    .filter(name -> name.startsWith("metadata."))
                    .forEach(name -> metadata.put(name.substring("metadata.".length()), values.getProperty(name)));
                parts.add(new PagedObjectPart(
                    values.getProperty("objectKey"), values.getProperty("groupKey"),
                    values.getProperty("finalObjectKey"), values.getProperty("contentType"),
                    Integer.parseInt(values.getProperty("pageIndex")), metadata));
            }
            return parts.stream()
                .sorted(java.util.Comparator.comparing(PagedObjectPart::groupKey)
                    .thenComparingInt(PagedObjectPart::pageIndex))
                .toList();
        } catch (IOException failure) {
            throw new CompletionException(failure);
        }
    }

    private ObjectWriteResult composeBlocking(PagedObjectCompositionRequest request) {
        Path root = root(request.targetName(), request.target());
        Path finalPath = requireUnderRoot(root, root.resolve(request.objectKey()).normalize());
        Path parent = finalPath.getParent();
        Optional<Path> temp = Optional.empty();
        try {
            if (parent != null) {
                Files.createDirectories(parent);
            }
            temp = Optional.of(Files.createTempFile(parent == null ? root : parent, ".tpf-compose-", ".tmp"));
            Path temporary = temp.orElseThrow();
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            long bytes = 0;
            try (OutputStream output = new BufferedOutputStream(Files.newOutputStream(temporary))) {
                bytes += copyAndDigest(new java.io.ByteArrayInputStream(request.prefix()), output, digest);
                byte[] buffer = new byte[64 * 1024];
                for (String partKey : request.orderedPartKeys()) {
                    Path part = requireUnderRoot(root, root.resolve(partKey).normalize());
                    if (!Files.isRegularFile(part)) {
                        throw new IllegalStateException("Paged object part is missing: " + partKey);
                    }
                    try (InputStream input = Files.newInputStream(part)) {
                        int read;
                        while ((read = input.read(buffer)) >= 0) {
                            if (read == 0) continue;
                            output.write(buffer, 0, read);
                            digest.update(buffer, 0, read);
                            bytes += read;
                        }
                    }
                }
                bytes += copyAndDigest(new java.io.ByteArrayInputStream(request.suffix()), output, digest);
            }
            forceFile(temporary);
            try {
                Files.move(temporary, finalPath, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
            } catch (AtomicMoveNotSupportedException | FileAlreadyExistsException failure) {
                Files.deleteIfExists(temporary);
                throw new IllegalStateException("Configured filesystem does not support atomic paged composition", failure);
            }
            forceDirectory(parent == null ? root : parent);
            String checksum = HexFormat.of().formatHex(digest.digest());
            Map<String, String> metadata = new LinkedHashMap<>(request.metadata());
            metadata.put("target", request.targetName());
            Path canonicalRoot = root.toRealPath();
            Path canonicalPath = finalPath.toRealPath();
            metadata.put(
                LOCATOR_DIGEST_METADATA,
                sha256((canonicalRoot + "\n" + request.objectKey() + "\n" + canonicalPath)
                    .getBytes(StandardCharsets.UTF_8)));
            PayloadReference reference = new PayloadReference(
                "filesystem", canonicalRoot.toString(), request.objectKey(), request.contentType(),
                "raw", checksum, bytes, null, metadata, Optional.empty());
            return new ObjectWriteResult(reference, bytes, checksum, Instant.now());
        } catch (IOException | NoSuchAlgorithmException | RuntimeException failure) {
            deleteWithSuppressedFailure(temp, failure);
            if (failure instanceof RuntimeException runtimeFailure) {
                throw runtimeFailure;
            }
            throw new CompletionException(failure);
        }
    }

    private static void deleteWithSuppressedFailure(Optional<Path> path, Throwable originalFailure) {
        if (path.isEmpty()) {
            return;
        }
        try {
            Files.deleteIfExists(path.orElseThrow());
        } catch (IOException cleanupFailure) {
            originalFailure.addSuppressed(cleanupFailure);
        }
    }

    private static void forceFile(Path path) throws IOException {
        try (FileChannel channel = FileChannel.open(path, java.nio.file.StandardOpenOption.WRITE)) {
            channel.force(true);
        }
    }

    private static void forceDirectory(Path path) throws IOException {
        try (FileChannel channel = FileChannel.open(path, java.nio.file.StandardOpenOption.READ)) {
            channel.force(true);
        }
    }

    private static String sha256(byte[] bytes) {
        try {
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes));
        } catch (NoSuchAlgorithmException failure) {
            throw new IllegalStateException("SHA-256 is unavailable", failure);
        }
    }

    private static long copyAndDigest(InputStream input, OutputStream output, MessageDigest digest) throws IOException {
        byte[] buffer = new byte[64 * 1024];
        long bytes = 0;
        int read;
        while ((read = input.read(buffer)) >= 0) {
            if (read == 0) continue;
            output.write(buffer, 0, read);
            digest.update(buffer, 0, read);
            bytes += read;
        }
        return bytes;
    }

    private ObjectWriteSession openBlocking(ObjectWriteOpenRequest request) {
        try {
            Path root = root(request);
            Path finalPath = requireUnderRoot(root, root.resolve(request.objectKey()).normalize());
            Path parent = finalPath.getParent();
            if (parent != null) {
                Files.createDirectories(parent);
            }
            Path tempPath = Files.createTempFile(parent == null ? root : parent, ".tpf-publish-", ".tmp");
            OutputStream output = new BufferedOutputStream(Files.newOutputStream(tempPath));
            return new FilesystemWriteSession(request, root, finalPath, tempPath, output, executor);
        } catch (IOException e) {
            throw new CompletionException(e);
        }
    }

    private Path root(ObjectWriteOpenRequest request) {
        return root(request.targetName(), request.target());
    }

    private Path root(String targetName, org.pipelineframework.config.boundary.PipelineObjectPublishConfig target) {
        Object root = target.location().get("root");
        if (root == null || root.toString().isBlank()) {
            throw new IllegalArgumentException("filesystem publish target '" + targetName + "' requires location.root");
        }
        return Path.of(root.toString()).toAbsolutePath().normalize();
    }

    private Path requireUnderRoot(Path root, Path path) {
        if (!path.startsWith(root)) {
            throw new SecurityException("Filesystem object publish path escapes configured root: " + path);
        }
        return path;
    }

    private static final class FilesystemWriteSession implements ObjectWriteSession {
        private final ObjectWriteOpenRequest request;
        private final Path root;
        private final Path finalPath;
        private final Path tempPath;
        private final OutputStream output;
        private final Executor executor;
        private boolean closed;

        private FilesystemWriteSession(
            ObjectWriteOpenRequest request,
            Path root,
            Path finalPath,
            Path tempPath,
            OutputStream output,
            Executor executor
        ) {
            this.request = request;
            this.root = root;
            this.finalPath = finalPath;
            this.tempPath = tempPath;
            this.output = output;
            this.executor = executor;
        }

        @Override
        public CompletionStage<Void> write(ByteBuffer chunk) {
            byte[] bytes = copy(chunk);
            return CompletableFuture.runAsync(() -> {
                synchronized (this) {
                    if (closed) {
                        throw new IllegalStateException("filesystem write session is closed");
                    }
                    try {
                        output.write(bytes);
                    } catch (IOException e) {
                        throw new CompletionException(e);
                    }
                }
            }, executor);
        }

        @Override
        public CompletionStage<ObjectWriteResult> close(ObjectWriteCloseRequest closeRequest) {
            return CompletableFuture.supplyAsync(() -> {
                synchronized (this) {
                    try {
                        if (!closed) {
                            output.flush();
                            output.close();
                            closed = true;
                        }
                        forceFile(tempPath);
                        Map<String, String> metadata = metadata(closeRequest);
                        validatePagedMetadata(metadata);
                        moveAtomicallyReplacingExistingTarget();
                        writePageManifestIfRequired(metadata);
                        metadata.put("target", request.targetName());
                        Path canonicalRoot = root.toRealPath();
                        Path canonicalPath = finalPath.toRealPath();
                        metadata.put(
                            LOCATOR_DIGEST_METADATA,
                            sha256((canonicalRoot + "\n" + request.objectKey() + "\n" + canonicalPath)
                                .getBytes(StandardCharsets.UTF_8)));
                        PayloadReference reference = new PayloadReference(
                            "filesystem",
                            canonicalRoot.toString(),
                            request.objectKey(),
                            request.contentType(),
                            "raw",
                            closeRequest.checksum(),
                            closeRequest.bytes(),
                            null,
                            metadata,
                            Optional.empty());
                        return new ObjectWriteResult(reference, closeRequest.bytes(), closeRequest.checksum(), Instant.now());
                    } catch (IOException | RuntimeException e) {
                        cleanupTemporaryFile(e);
                        throw new CompletionException(e);
                    }
                }
            }, executor);
        }

        private Map<String, String> metadata(ObjectWriteCloseRequest closeRequest) {
            Map<String, String> metadata = new LinkedHashMap<>(request.metadata());
            metadata.putAll(closeRequest.metadata());
            return metadata;
        }

        private static void validatePagedMetadata(Map<String, String> metadata) {
            if (!metadata.containsKey("tpf.page.index")) {
                return;
            }
            String pageIndex = requirePagedMetadata(metadata, "tpf.page.index");
            try {
                if (Integer.parseInt(pageIndex) < 0) {
                    throw new IllegalArgumentException(
                        "Paged filesystem part has negative metadata: tpf.page.index=" + pageIndex);
                }
            } catch (NumberFormatException failure) {
                throw new IllegalArgumentException(
                    "Paged filesystem part has invalid metadata: tpf.page.index=" + pageIndex, failure);
            }
            requirePagedMetadata(metadata, "tpf.page.group");
            requirePagedMetadata(metadata, "tpf.page.finalKey");
        }

        private static String requirePagedMetadata(Map<String, String> metadata, String key) {
            String value = metadata.get(key);
            if (value == null || value.isBlank()) {
                throw new IllegalArgumentException("Paged filesystem part is missing metadata: " + key);
            }
            return value;
        }

        private void writePageManifestIfRequired(Map<String, String> metadata) throws IOException {
            if (!metadata.containsKey("tpf.page.index")) {
                return;
            }
            Properties values = new Properties();
            values.setProperty("objectKey", request.objectKey());
            values.setProperty("groupKey", metadata.get("tpf.page.group"));
            values.setProperty("finalObjectKey", metadata.get("tpf.page.finalKey"));
            values.setProperty("contentType", request.contentType());
            values.setProperty("pageIndex", metadata.get("tpf.page.index"));
            metadata.forEach((key, value) -> values.setProperty("metadata." + key, value));
            Path manifest = finalPath.resolveSibling(finalPath.getFileName() + ".manifest");
            Path tempManifest = Files.createTempFile(manifest.getParent(), ".tpf-manifest-", ".tmp");
            try {
                try (OutputStream output = Files.newOutputStream(tempManifest)) {
                    values.store(output, "TPF paged object manifest");
                }
                forceFile(tempManifest);
                Files.move(tempManifest, manifest, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
                forceDirectory(manifest.getParent());
            } catch (IOException | RuntimeException failure) {
                deleteWithSuppressedFailure(Optional.of(tempManifest), failure);
                throw failure;
            }
        }

        @Override
        public CompletionStage<Void> abort(Throwable cause) {
            return CompletableFuture.runAsync(() -> {
                synchronized (this) {
                    try {
                        if (!closed) {
                            output.close();
                            closed = true;
                        }
                        Files.deleteIfExists(tempPath);
                    } catch (IOException e) {
                        throw new CompletionException(e);
                    }
                }
            }, executor);
        }

        private static byte[] copy(ByteBuffer chunk) {
            if (chunk == null) {
                return new byte[0];
            }
            ByteBuffer duplicate = chunk.slice();
            byte[] bytes = new byte[duplicate.remaining()];
            duplicate.get(bytes);
            return bytes;
        }

        private static String sha256(byte[] bytes) {
            try {
                return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes));
            } catch (NoSuchAlgorithmException failure) {
                throw new IllegalStateException("SHA-256 is unavailable", failure);
            }
        }

        private void moveAtomicallyReplacingExistingTarget() throws IOException {
            try {
                Files.move(tempPath, finalPath, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
                forceDirectory(finalPath.getParent());
            } catch (AtomicMoveNotSupportedException | FileAlreadyExistsException e) {
                throw new IllegalStateException(
                    "Configured filesystem does not support atomic replacement for " + finalPath, e);
            }
        }

        private void cleanupTemporaryFile(Throwable publishFailure) {
            try {
                Files.deleteIfExists(tempPath);
            } catch (IOException cleanupFailure) {
                publishFailure.addSuppressed(cleanupFailure);
            }
        }
    }
}
