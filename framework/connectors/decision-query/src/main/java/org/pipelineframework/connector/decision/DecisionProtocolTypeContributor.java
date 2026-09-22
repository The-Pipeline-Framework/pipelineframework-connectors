package org.pipelineframework.connector.decision;

import java.util.Collection;
import java.util.List;

import org.pipelineframework.config.template.PipelineTemplateTypeDefinition;
import org.pipelineframework.config.template.PipelineTemplateTypeReference;
import org.pipelineframework.connector.ConnectorProviderId;
import org.pipelineframework.protocol.ProtocolTypeContributor;
import org.pipelineframework.protocol.ProtocolTypeDescriptor;
import org.pipelineframework.protocol.ProtocolTypeIdentity;

/** Canonical vocabulary shared by bounded-decision providers and applications. */
public final class DecisionProtocolTypeContributor implements ProtocolTypeContributor {
    private static final ConnectorProviderId OWNER = ConnectorProviderId.of("tpf.decision");
    public static final ProtocolTypeIdentity CRITERION = new ProtocolTypeIdentity(OWNER, "DecisionCriterion");
    public static final ProtocolTypeIdentity QUESTION = new ProtocolTypeIdentity(OWNER, "DecisionQuestion");
    public static final ProtocolTypeIdentity REQUEST = new ProtocolTypeIdentity(OWNER, "DecisionRequest");
    public static final ProtocolTypeIdentity PROBABILITY = new ProtocolTypeIdentity(OWNER, "DecisionProbability");
    public static final ProtocolTypeIdentity ANSWER = new ProtocolTypeIdentity(OWNER, "DecisionAnswer");
    public static final ProtocolTypeIdentity RESULT = new ProtocolTypeIdentity(OWNER, "DecisionResult");

    @Override
    public Collection<ProtocolTypeDescriptor> protocolTypes() {
        var string = new PipelineTemplateTypeReference.Scalar("string");
        var decimal = new PipelineTemplateTypeReference.Scalar("decimal");
        var criterion = new PipelineTemplateTypeReference.Contributed(CRITERION.qualifiedName());
        var question = new PipelineTemplateTypeReference.Contributed(QUESTION.qualifiedName());
        var probability = new PipelineTemplateTypeReference.Contributed(PROBABILITY.qualifiedName());
        var answer = new PipelineTemplateTypeReference.Contributed(ANSWER.qualifiedName());
        return List.of(
            descriptor(CRITERION, "DecisionCriterion", List.of(field("label", string), field("description", string))),
            descriptor(QUESTION, "DecisionQuestion", List.of(field("name", string), field("type", string),
                field("instructions", string), repeated("criteria", criterion))),
            descriptor(REQUEST, "DecisionRequest", List.of(field("stateJson", string),
                repeated("questions", question))),
            descriptor(PROBABILITY, "DecisionProbability", List.of(field("label", string),
                field("probability", decimal))),
            descriptor(ANSWER, "DecisionAnswer", List.of(field("name", string), field("type", string),
                field("selected", string), field("value", decimal), field("confidence", decimal),
                repeated("probabilities", probability))),
            descriptor(RESULT, "DecisionResult", List.of(repeated("answers", answer))));
    }

    private static ProtocolTypeDescriptor descriptor(ProtocolTypeIdentity identity, String name,
        List<PipelineTemplateTypeDefinition.Field> fields) {
        return new ProtocolTypeDescriptor(identity, new PipelineTemplateTypeDefinition.RecordType(name, fields));
    }

    private static PipelineTemplateTypeDefinition.Field field(String name, PipelineTemplateTypeReference type) {
        return new PipelineTemplateTypeDefinition.Field(name, type);
    }

    private static PipelineTemplateTypeDefinition.Field repeated(String name, PipelineTemplateTypeReference type) {
        return new PipelineTemplateTypeDefinition.Field(name, type, true);
    }
}
