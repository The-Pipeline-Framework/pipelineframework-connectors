package org.pipelineframework.connector.conversation;

import java.util.Collection;
import java.util.List;

import org.pipelineframework.config.template.PipelineFieldNullability;
import org.pipelineframework.config.template.PipelineFieldPresence;
import org.pipelineframework.config.template.PipelineTemplateTypeDefinition;
import org.pipelineframework.config.template.PipelineTemplateTypeReference;
import org.pipelineframework.connector.ConnectorProviderId;
import org.pipelineframework.protocol.ProtocolTypeContributor;
import org.pipelineframework.protocol.ProtocolTypeDescriptor;
import org.pipelineframework.protocol.ProtocolTypeIdentity;

/** Text-only v1 conversation vocabulary for the existing canonical v3 type system. */
public final class ConversationProtocolTypeContributor implements ProtocolTypeContributor {
    private static final ConnectorProviderId NAMESPACE = ConnectorProviderId.of("tpf.conversation");
    public static final ProtocolTypeIdentity SCOPE = identity("ConversationScope");
    public static final ProtocolTypeIdentity TURN = identity("ConversationTurn");
    public static final ProtocolTypeIdentity READ_REQUEST = identity("HistoryReadRequest");
    public static final ProtocolTypeIdentity PAGE = identity("HistoryPage");
    public static final ProtocolTypeIdentity TAIL_REQUEST = identity("HistoryTailRequest");
    public static final ProtocolTypeIdentity TAIL_PAGE = identity("HistoryTailPage");
    public static final ProtocolTypeIdentity CANDIDATE = identity("ContextCandidate");
    public static final ProtocolTypeIdentity ASSEMBLY_REQUEST = identity("ContextAssemblyRequest");
    public static final ProtocolTypeIdentity SELECTED_ENTRY = identity("SelectedContextEntry");
    public static final ProtocolTypeIdentity SKIPPED_ENTRY = identity("SkippedContextEntry");
    public static final ProtocolTypeIdentity ASSEMBLY_RESULT = identity("ContextAssemblyResult");

    @Override
    public Collection<ProtocolTypeDescriptor> protocolTypes() {
        var string = scalar("string");
        var int32 = scalar("int32");
        var int64 = scalar("int64");
        var bool = scalar("bool");
        return List.of(
            record(SCOPE, field("tenantId", string), field("ownerId", string), field("conversationId", string)),
            record(TURN, field("scope", ref(SCOPE)), field("eventId", string), field("cursor", int64),
                field("occurredAt", string), field("speaker", string), field("source", string), field("text", string),
                optional("payloadRef", string), optional("replyToEventId", string)),
            record(READ_REQUEST, field("scope", ref(SCOPE)), field("afterCursor", int64), field("limit", int32)),
            record(PAGE, field("scope", ref(SCOPE)), repeated("turns", ref(TURN)),
                field("nextCursor", int64), field("hasMore", bool)),
            record(TAIL_REQUEST, field("scope", ref(SCOPE)), field("beforeCursor", int64), field("limit", int32)),
            record(TAIL_PAGE, field("scope", ref(SCOPE)), repeated("turns", ref(TURN)),
                field("previousCursor", int64), field("hasEarlier", bool)),
            record(CANDIDATE, field("scope", ref(SCOPE)), field("entryId", string), field("provenance", string),
                field("text", string), optional("payloadRef", string), field("required", bool)),
            record(ASSEMBLY_REQUEST, field("scope", ref(SCOPE)), repeated("candidates", ref(CANDIDATE)),
                field("contextLimitTokens", int32), field("instructionTokens", int32),
                field("callableSchemaTokens", int32), field("reservedOutputTokens", int32), field("maxEntryBytes", int32)),
            record(SELECTED_ENTRY, field("entryId", string), field("provenance", string), field("text", string),
                optional("payloadRef", string), field("estimatedTokens", int32), field("abbreviated", bool)),
            record(SKIPPED_ENTRY, field("entryId", string), field("provenance", string),
                field("sourceBytes", int32), field("measuredTokens", int32), field("reason", string)),
            record(ASSEMBLY_RESULT, field("fits", bool), repeated("selected", ref(SELECTED_ENTRY)),
                repeated("skipped", ref(SKIPPED_ENTRY)), field("estimatedInputTokens", int32),
                field("reservedOutputTokens", int32), field("measurement", string),
                optional("overflowReason", string), optional("observedInputTokens", int32)));
    }

    private static ProtocolTypeIdentity identity(String name) {
        return new ProtocolTypeIdentity(NAMESPACE, name);
    }

    private static PipelineTemplateTypeReference scalar(String name) {
        return new PipelineTemplateTypeReference.Scalar(name);
    }

    private static PipelineTemplateTypeReference ref(ProtocolTypeIdentity identity) {
        return new PipelineTemplateTypeReference.Contributed(identity.qualifiedName());
    }

    private static ProtocolTypeDescriptor record(ProtocolTypeIdentity identity, PipelineTemplateTypeDefinition.Field... fields) {
        return new ProtocolTypeDescriptor(identity,
            new PipelineTemplateTypeDefinition.RecordType(identity.typeName(), List.of(fields)));
    }

    private static PipelineTemplateTypeDefinition.Field field(String name, PipelineTemplateTypeReference type) {
        return new PipelineTemplateTypeDefinition.Field(name, type);
    }

    private static PipelineTemplateTypeDefinition.Field repeated(String name, PipelineTemplateTypeReference type) {
        return new PipelineTemplateTypeDefinition.Field(name, type, true);
    }

    private static PipelineTemplateTypeDefinition.Field optional(String name, PipelineTemplateTypeReference type) {
        return new PipelineTemplateTypeDefinition.Field(
            name, type, false, PipelineFieldPresence.OPTIONAL, PipelineFieldNullability.NON_NULL);
    }
}
