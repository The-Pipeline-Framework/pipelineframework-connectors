package org.pipelineframework.connector.decision;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

final class DecisionQueryOperationTest {
    @Test
    void choiceMustBeOneOfTheDeclaredAlternatives() {
        DecisionRequest request = new DecisionRequest("{}", List.of(new DecisionQuestion(
            "property", DecisionQuestionType.CHOICE, "", List.of(
                new DecisionCriterion("P1", "First"), new DecisionCriterion("P2", "Second")))));
        DecisionResult result = new DecisionResult(List.of(new DecisionAnswer(
            "property", DecisionQuestionType.CHOICE, "INVENTED", BigDecimal.ZERO, BigDecimal.ONE,
            List.of(new DecisionProbability("P1", BigDecimal.ZERO),
                new DecisionProbability("P2", BigDecimal.ONE)))));

        assertThrows(DecisionProviderFailureException.class, () -> DecisionQueryOperation.validate(request, result));
    }

    @Test
    void everyQuestionMustHaveExactlyOneAnswer() {
        DecisionRequest request = new DecisionRequest("{}", List.of(
            new DecisionQuestion("supplier", DecisionQuestionType.CHOICE, "",
                List.of(new DecisionCriterion("ENOUGH", "Enough"))),
            new DecisionQuestion("property", DecisionQuestionType.CHOICE, "",
                List.of(new DecisionCriterion("P1", "First")))));
        DecisionResult result = new DecisionResult(List.of(new DecisionAnswer(
            "supplier", DecisionQuestionType.CHOICE, "ENOUGH", BigDecimal.ZERO, BigDecimal.ONE,
            List.of(new DecisionProbability("ENOUGH", BigDecimal.ONE)))));

        assertThrows(DecisionProviderFailureException.class, () -> DecisionQueryOperation.validate(request, result));
    }

    @Test
    void completionCarriesInputFieldsAndPlacesTheDecisionResult() {
        DecisionResult result = new DecisionResult(List.of());
        var projection = new DecisionProjection(
            new com.fasterxml.jackson.databind.ObjectMapper(),
            Output.class,
            new DecisionCompletionConfiguration("result", Map.of("id", "id")));

        Output output = (Output) projection.materialize(new Input("invoice-1"), result);

        assertEquals(result, output.result());
        assertEquals("invoice-1", output.id());
    }

    record Input(String id) {}
    record Output(String id, DecisionResult result) {}
}
