package org.pipelineframework.connector.decision;

import java.lang.reflect.RecordComponent;
import java.util.LinkedHashSet;
import java.util.Objects;
import java.util.Set;

import com.fasterxml.jackson.databind.ObjectMapper;

final class DecisionProjection {
    private final ObjectMapper json;
    private final Class<?> outputType;
    private final DecisionCompletionConfiguration configuration;
    private final String resultComponent;

    DecisionProjection(ObjectMapper json, Class<?> outputType, DecisionCompletionConfiguration configuration) {
        this.json = Objects.requireNonNull(json);
        this.outputType = Objects.requireNonNull(outputType);
        this.configuration = Objects.requireNonNull(configuration);
        if (!outputType.isRecord()) throw new IllegalStateException("decision completion requires a record output");
        this.resultComponent = component(outputType, configuration.field()).getName();
        Set<String> expected = new LinkedHashSet<>();
        for (RecordComponent component : outputType.getRecordComponents()) {
            if (!component.getName().equals(configuration.field())) expected.add(component.getName());
        }
        if (!expected.equals(configuration.carry().keySet())) {
            throw new IllegalStateException("decision carry fields must exactly cover output fields " + expected);
        }
    }

    Object materialize(Object input, DecisionResult result) {
        RecordComponent[] components = outputType.getRecordComponents();
        Object[] arguments = new Object[components.length];
        Class<?>[] parameterTypes = new Class<?>[components.length];
        for (int index = 0; index < components.length; index++) {
            RecordComponent component = components[index];
            parameterTypes[index] = component.getType();
            arguments[index] = component.getName().equals(resultComponent)
                ? json.convertValue(result, component.getType())
                : readPath(input, configuration.carry().get(component.getName()));
        }
        try {
            return outputType.getDeclaredConstructor(parameterTypes).newInstance(arguments);
        } catch (Exception failure) {
            throw new IllegalArgumentException("decision output cannot be materialized", failure);
        }
    }

    static Object readPath(Object root, String path) {
        Object value = root;
        for (String segment : path.split("\\.")) {
            if (value == null || !value.getClass().isRecord()) {
                throw new IllegalArgumentException("decision input path is not a record path: " + path);
            }
            RecordComponent component = component(value.getClass(), segment);
            try {
                value = component.getAccessor().invoke(value);
            } catch (ReflectiveOperationException failure) {
                throw new IllegalArgumentException("decision input path cannot be read: " + path, failure);
            }
        }
        return value;
    }

    private static RecordComponent component(Class<?> type, String name) {
        for (RecordComponent component : type.getRecordComponents()) {
            if (component.getName().equals(name)) return component;
        }
        throw new IllegalArgumentException("record " + type.getSimpleName() + " has no component " + name);
    }
}
