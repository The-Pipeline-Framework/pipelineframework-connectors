package org.pipelineframework.connector.conversation;

/** Host-authorised scope. Model output cannot grant these identifiers. */
public record ConversationScope(String tenantId, String ownerId, String conversationId) {
    public ConversationScope {
        tenantId = ConversationValues.identifier(tenantId, "tenant ID");
        ownerId = ConversationValues.identifier(ownerId, "owner ID");
        conversationId = ConversationValues.identifier(conversationId, "conversation ID");
    }
}
