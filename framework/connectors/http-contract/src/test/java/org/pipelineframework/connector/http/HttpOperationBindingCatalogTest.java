package org.pipelineframework.connector.http;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class HttpOperationBindingCatalogTest {
    @Test
    void roundTripsResolvedBindingsDeterministically() {
        var direct = new HttpOperationRepresentationBinding("http.direct", HttpRepresentationMode.DIRECT,
            Optional.empty(), Optional.empty(), "1".repeat(64));
        var curated = new HttpOperationRepresentationBinding("http.curated", HttpRepresentationMode.CURATED,
            Optional.of("com.example.WireValue"), Optional.of("com.example.ValueMapper"), "2".repeat(64));

        HttpOperationBindingCatalog restored = HttpOperationBindingCatalog.read(
            new HttpOperationBindingCatalog(List.of(direct, curated)).json());

        assertEquals(List.of(curated, direct), restored.bindings());
        assertEquals(Optional.of(curated), restored.find("http.curated"));
    }

    @Test
    void requiresMapperForGeneratedAndCuratedBindings() {
        IllegalArgumentException generated = assertThrows(IllegalArgumentException.class,
            () -> new HttpOperationRepresentationBinding("http.generated", HttpRepresentationMode.GENERATED,
                Optional.empty(), Optional.empty(), "1".repeat(64)));
        IllegalArgumentException curated = assertThrows(IllegalArgumentException.class,
            () -> new HttpOperationRepresentationBinding("http.curated", HttpRepresentationMode.CURATED,
                Optional.empty(), Optional.empty(), "1".repeat(64)));

        assertTrue(generated.getMessage().contains("require representation"));
        assertTrue(curated.getMessage().contains("require representation"));
    }

    @Test
    void deduplicatesEquivalentBindingsExposedByMultipleApplicationArchives(@TempDir Path directory)
            throws Exception {
        var binding = direct("1".repeat(64));
        String json = new HttpOperationBindingCatalog(List.of(binding)).json();
        ClassLoader duplicated = resources(directory, json, json);

        assertEquals(List.of(binding), HttpOperationBindingCatalog.load(duplicated).bindings());
    }

    @Test
    void rejectsConflictingBindingsExposedByMultipleApplicationArchives(@TempDir Path directory)
            throws Exception {
        String first = new HttpOperationBindingCatalog(List.of(direct("1".repeat(64)))).json();
        String second = new HttpOperationBindingCatalog(List.of(direct("2".repeat(64)))).json();
        ClassLoader conflicting = resources(directory, first, second);

        IllegalArgumentException failure = assertThrows(IllegalArgumentException.class,
            () -> HttpOperationBindingCatalog.load(conflicting));

        assertEquals("conflicting HTTP representation mapping key: http.direct", failure.getMessage());
    }

    private ClassLoader resources(Path directory, String firstJson, String secondJson) throws Exception {
        Path first = Files.writeString(directory.resolve("first.json"), firstJson);
        Path second = Files.writeString(directory.resolve("second.json"), secondJson);
        return new ClassLoader(getClass().getClassLoader()) {
            @Override
            public java.util.Enumeration<URL> getResources(String name) throws java.io.IOException {
                if (HttpOperationBindingCatalog.RESOURCE_PATH.equals(name)) {
                    return Collections.enumeration(List.of(first.toUri().toURL(), second.toUri().toURL()));
                }
                return super.getResources(name);
            }
        };
    }

    private static HttpOperationRepresentationBinding direct(String fingerprint) {
        return new HttpOperationRepresentationBinding("http.direct", HttpRepresentationMode.DIRECT,
            Optional.empty(), Optional.empty(), fingerprint);
    }
}
