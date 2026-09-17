package org.pipelineframework.connector.query.jpa;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.Set;
import org.junit.jupiter.api.Test;

class HibernateQueryPublicApiTest {
    @Test
    void reactiveArtifactSurfaceIsPublic() {
        assertTrue(Modifier.isPublic(JpaQueryPlan.class.getModifiers()));
        assertPublic(JpaQueryPlan.class, Set.of(
            "from", "fromMany", "queryId", "entityType", "projection", "toHql", "bindings",
            "maxResults", "firstResultOnly", "streamingLimit", "orderingGuard"));
        assertTrue(Modifier.isPublic(JpaQueryPlan.OrderingGuard.class.getModifiers()));
        assertPublic(JpaQueryPlan.OrderingGuard.class, Set.of("validateNext"));

        assertTrue(Modifier.isPublic(HibernateQueryResult.class.getModifiers()));
        assertPublic(HibernateQueryResult.class, Set.of("projectSingle"));
        assertTrue(Modifier.isPublic(HibernateQueryResult.NotFoundException.class.getModifiers()));
        assertTrue(Modifier.isPublic(HibernateQueryResult.MultipleResultsException.class.getModifiers()));

        assertTrue(Modifier.isPublic(JpaQueryProjection.class.getModifiers()));
        assertPublic(JpaQueryProjection.class, Set.of("project"));
    }

    private static void assertPublic(Class<?> type, Set<String> names) {
        for (String name : names) {
            Method[] methods = Arrays.stream(type.getDeclaredMethods())
                .filter(method -> method.getName().equals(name))
                .toArray(Method[]::new);
            assertTrue(methods.length > 0 && Arrays.stream(methods)
                    .allMatch(method -> Modifier.isPublic(method.getModifiers())),
                () -> type.getSimpleName() + "." + name + " must be public");
        }
    }
}
