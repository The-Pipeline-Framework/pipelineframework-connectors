package org.pipelineframework.connector.decision;

public final class DecisionProviderFailureException extends RuntimeException {
    public enum Kind { AUTHENTICATION_REQUIRED, TEMPORARILY_UNAVAILABLE, TERMINAL }

    private final Kind kind;
    private final String outcomeCode;

    public DecisionProviderFailureException(Kind kind, String outcomeCode, String message) {
        super(message);
        this.kind = java.util.Objects.requireNonNull(kind);
        this.outcomeCode = DecisionValues.text(outcomeCode, "decision failure code");
    }

    public DecisionProviderFailureException(Kind kind, String outcomeCode, String message, Throwable cause) {
        super(message, cause);
        this.kind = java.util.Objects.requireNonNull(kind);
        this.outcomeCode = DecisionValues.text(outcomeCode, "decision failure code");
    }

    public Kind kind() { return kind; }
    public String outcomeCode() { return outcomeCode; }
}
