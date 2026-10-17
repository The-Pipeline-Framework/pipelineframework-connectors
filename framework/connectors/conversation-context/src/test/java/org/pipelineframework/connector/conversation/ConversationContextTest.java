package org.pipelineframework.connector.conversation;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Optional;
import java.util.ServiceLoader;
import java.util.stream.IntStream;

import org.junit.jupiter.api.Test;
import org.pipelineframework.config.template.PipelineTemplateTypeDefinition;
import org.pipelineframework.protocol.ProtocolTypeContributor;

class ConversationContextTest {
    private final ConversationScope scope = new ConversationScope("tenant-a", "owner-a", "dialogue-a");
    private final ContextAssembler assembler = new ContextAssembler();

    @Test
    void rejectsMissingScopeOversizedContentAndForeignOrMisorderedPages() {
        assertThrows(IllegalArgumentException.class, () -> new ConversationScope(" ", "owner", "conversation"));
        assertThrows(IllegalArgumentException.class, () -> new ConversationScope("tenant\nother", "owner", "conversation"));
        assertThrows(NullPointerException.class, () -> turn(null, "one", 1));
        assertThrows(IllegalArgumentException.class, () -> new HistoryReadRequest(scope, -1L, 10));
        assertThrows(IllegalArgumentException.class, () -> new HistoryReadRequest(scope, 0L, 201));
        assertThrows(IllegalArgumentException.class, () -> new HistoryTailRequest(scope, -1L, 10));
        assertThrows(IllegalArgumentException.class, () -> new ConversationTurn(scope, "oversize", 1L,
            "2026-10-16T00:00:00Z", "owner", "approved-ui", "x".repeat(16_385), Optional.empty(), Optional.empty()));
        assertThrows(IllegalArgumentException.class, () -> new ConversationTurn(scope, "invalid-time", 1L,
            "yesterday", "owner", "approved-ui", "hello", Optional.empty(), Optional.empty()));
        assertThrows(IllegalArgumentException.class,
            () -> new HistoryPage(scope, List.of(turn(scope, "two", 2), turn(scope, "one", 1)), 2L, false));
        var foreign = new ConversationScope("tenant-b", "owner-b", "dialogue-a");
        assertThrows(IllegalArgumentException.class,
            () -> new HistoryPage(scope, List.of(turn(foreign, "one", 1)), 1L, false));
    }

    @Test
    void preservesProvenanceAndRejectsForeignCandidates() {
        var ui = candidate(scope, "one", "approved-ui", "hello", false, Optional.empty());
        var model = candidate(scope, "two", "model-output", "hello", false, Optional.empty());
        assertFalse(ui.equals(model));
        var foreign = new ConversationScope("tenant-b", "owner-b", "dialogue-a");
        assertThrows(IllegalArgumentException.class,
            () -> request(List.of(candidate(foreign, "one", "approved-ui", "hello", false, Optional.empty())), 100));
    }

    @Test
    void selectsDeterministicallyForFiveFiftyAndFiveHundredTurns() {
        for (int count : List.of(5, 50, 500)) {
            var candidates = IntStream.range(0, count).mapToObj(index ->
                candidate(scope, "id-" + index, "approved-ui", "small " + index, false, Optional.empty())).toList();
            var input = request(candidates, 180);
            var first = assembler.assemble(input);
            assertEquals(first, assembler.assemble(input));
            assertTrue(first.fits());
            assertTrue(first.estimatedInputTokens() + first.reservedOutputTokens() <= input.contextLimitTokens());
            assertEquals(count, first.selected().size() + first.skipped().size());
        }
    }

    @Test
    void reservesRequiredContentAndReportsCannotFit() {
        var optional = candidate(scope, "optional", "history", "x".repeat(70), false, Optional.empty());
        var required = candidate(scope, "required", "approved-policy", "must remain", true, Optional.empty());
        var result = assembler.assemble(request(List.of(optional, required), 150));
        assertTrue(result.fits());
        assertEquals(List.of("required"), result.selected().stream().map(SelectedContextEntry::entryId).toList());
        assertEquals(List.of("optional"), result.skipped().stream().map(SkippedContextEntry::entryId).toList());
        assertEquals("input-budget-exhausted", result.skipped().getFirst().reason());
        assertEquals(70, result.skipped().getFirst().sourceBytes());
        var cannotFit = assembler.assemble(request(List.of(required), 40));
        assertFalse(cannotFit.fits());
        assertEquals(Optional.of("required-content-exceeds-window"), cannotFit.overflowReason());
        assertTrue(cannotFit.selected().isEmpty());
    }

    @Test
    void abbreviatesToolOutputOnlyWithOwnedReference() {
        var giant = candidate(scope, "tool", "tool-observation", "🙂".repeat(300), false,
            Optional.of("owned-artifact-1"));
        var input = new ContextAssemblyRequest(scope, List.of(giant), 300, 5, 5, 20, 40);
        var result = assembler.assemble(input);
        assertTrue(result.fits());
        assertEquals(1, result.selected().size());
        assertTrue(result.selected().getFirst().abbreviated());
        assertEquals(Optional.of("owned-artifact-1"), result.selected().getFirst().payloadRef());
        assertTrue(result.selected().getFirst().text().getBytes(java.nio.charset.StandardCharsets.UTF_8).length <= 40);
        assertFalse(result.selected().getFirst().text().contains("�"));
        var withoutReference = assembler.assemble(new ContextAssemblyRequest(scope,
            List.of(candidate(scope, "tool", "tool-observation", "x".repeat(300), false, Optional.empty())),
            300, 5, 5, 20, 40));
        assertTrue(withoutReference.fits());
        assertEquals(List.of("tool"), withoutReference.skipped().stream().map(SkippedContextEntry::entryId).toList());
        assertEquals("oversized-without-owned-reference", withoutReference.skipped().getFirst().reason());
    }

    @Test
    void labelsExactCounterSeparatelyFromConservativeEstimate() {
        var input = request(List.of(candidate(scope, "entry", "approved-ui", "hello", true, Optional.empty())), 100);
        var exact = assembler.assemble(input, (id, provenance, text, reference) -> 7);
        assertTrue(exact.fits());
        assertEquals("EXACT_TOKENIZER", exact.measurement());
        assertEquals(17, exact.estimatedInputTokens());
        assertEquals("CONSERVATIVE_ESTIMATE", assembler.assemble(input).measurement());
        assertEquals(Optional.of(6), exact.withObservedInputTokens(6).observedInputTokens());
    }

    @Test
    void discoversContributedV3TypesAndOptionalField() {
        ProtocolTypeContributor contributor = ServiceLoader.load(ProtocolTypeContributor.class).stream()
            .map(ServiceLoader.Provider::get)
            .filter(ConversationProtocolTypeContributor.class::isInstance).findFirst().orElseThrow();
        var types = contributor.protocolTypes();
        assertEquals(11, types.size());
        var turn = types.stream().filter(type -> type.identity().equals(ConversationProtocolTypeContributor.TURN))
            .findFirst().orElseThrow();
        var fields = ((PipelineTemplateTypeDefinition.RecordType) turn.definition()).fields();
        assertEquals("tpf.conversation.ConversationScope", fields.getFirst().type().name());
        assertEquals("payloadRef", fields.get(7).name());
    }

    private ConversationTurn turn(ConversationScope currentScope, String id, long cursor) {
        return new ConversationTurn(currentScope, id, cursor, "2026-10-16T00:00:00Z", "owner", "approved-ui",
            "hello", Optional.empty(), Optional.empty());
    }

    private static ContextCandidate candidate(ConversationScope currentScope, String id, String provenance,
            String text, boolean required, Optional<String> reference) {
        return new ContextCandidate(currentScope, id, provenance, text, reference, required);
    }

    private ContextAssemblyRequest request(List<ContextCandidate> candidates, int limit) {
        return new ContextAssemblyRequest(scope, candidates, limit, 5, 5, 20, 100);
    }
}
