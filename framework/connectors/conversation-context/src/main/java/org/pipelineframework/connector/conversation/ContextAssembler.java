package org.pipelineframework.connector.conversation;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/** Pure, deterministic assembler. It never reads history, calls a model, or changes memory. */
public final class ContextAssembler {
    public ContextAssemblyResult assemble(ContextAssemblyRequest request) {
        return assemble(request, ContextAssembler::estimatedEntryTokens, false);
    }

    /** Exact mode requires the host counter to measure the representation passed to its model. */
    public ContextAssemblyResult assemble(ContextAssemblyRequest request, ContextTokenCounter counter) {
        return assemble(request, counter, true);
    }

    private ContextAssemblyResult assemble(ContextAssemblyRequest request, ContextTokenCounter counter, boolean exact) {
        java.util.Objects.requireNonNull(counter, "token counter must not be null");
        String measurement = exact ? "EXACT_TOKENIZER" : "CONSERVATIVE_ESTIMATE";
        long overhead = (long) request.instructionTokens() + request.callableSchemaTokens();
        long available = (long) request.contextLimitTokens() - request.reservedOutputTokens();
        if (available < overhead) {
            return cannotFit(request, counter, (int) Math.min(overhead, Integer.MAX_VALUE),
                measurement, "fixed-overhead-exceeds-window");
        }

        long remainingRequired = 0;
        for (ContextCandidate candidate : request.candidates()) {
            if (!candidate.required()) {
                continue;
            }
            int bytes = candidate.text().getBytes(StandardCharsets.UTF_8).length;
            if (bytes > request.maxEntryBytes()) {
                return cannotFit(request, counter, (int) Math.min(overhead, Integer.MAX_VALUE),
                    measurement, "required-entry-exceeds-byte-limit");
            }
            remainingRequired += counted(counter, candidate, candidate.text());
        }
        if (overhead + remainingRequired > available) {
            return cannotFit(request, counter, (int) Math.min(overhead + remainingRequired, Integer.MAX_VALUE),
                measurement, "required-content-exceeds-window");
        }

        var entries = new ArrayList<SelectedContextEntry>();
        var skipped = new ArrayList<SkippedContextEntry>();
        long used = overhead;
        for (ContextCandidate candidate : request.candidates()) {
            String text = candidate.text();
            int bytes = text.getBytes(StandardCharsets.UTF_8).length;
            boolean abbreviated = bytes > request.maxEntryBytes();
            if (abbreviated) {
                if (candidate.required()) {
                    return cannotFit(request, counter, (int) Math.min(used, Integer.MAX_VALUE),
                        measurement, "required-entry-exceeds-byte-limit");
                }
                if (candidate.payloadRef().isEmpty()) {
                    skipped.add(skipped(candidate, bytes, counted(counter, candidate, text),
                        "oversized-without-owned-reference"));
                    continue;
                }
                text = excerpt(text, request.maxEntryBytes());
                bytes = text.getBytes(StandardCharsets.UTF_8).length;
            }
            long cost = counted(counter, candidate, text);
            if (candidate.required()) {
                remainingRequired -= cost;
            }
            if (used + cost + remainingRequired > available) {
                if (candidate.required()) {
                    return cannotFit(request, counter, (int) Math.min(used, Integer.MAX_VALUE),
                        measurement, "required-content-exceeds-window");
                }
                skipped.add(skipped(candidate, candidate.text().getBytes(StandardCharsets.UTF_8).length,
                    (int) cost, "input-budget-exhausted"));
                continue;
            }
            used += cost;
            entries.add(new SelectedContextEntry(candidate.entryId(), candidate.provenance(), text,
                candidate.payloadRef(), (int) cost, abbreviated));
        }
        return new ContextAssemblyResult(true, entries, skipped, (int) used,
            request.reservedOutputTokens(), measurement, Optional.empty(), Optional.empty());
    }

    private static int counted(ContextTokenCounter counter, ContextCandidate candidate, String text) {
        int count = counter.count(candidate.entryId(), candidate.provenance(), text, candidate.payloadRef());
        if (count <= 0) {
            throw new IllegalArgumentException("token counter must return a positive entry count");
        }
        return count;
    }

    private static int estimatedEntryTokens(String entryId, String provenance, String text,
            java.util.Optional<String> payloadRef) {
        // Account for JSON escaping and field labels as a byte-based upper estimate for selected entries.
        return 64 + jsonBytes(entryId) + jsonBytes(provenance) + jsonBytes(text)
            + payloadRef.map(ContextAssembler::jsonBytes).orElse(0);
    }

    private static int jsonBytes(String value) {
        int bytes = 2;
        for (int offset = 0; offset < value.length();) {
            int point = value.codePointAt(offset);
            bytes += point < 0x20 ? 6 : point == '"' || point == '\\' ? 2
                : new String(Character.toChars(point)).getBytes(StandardCharsets.UTF_8).length;
            offset += Character.charCount(point);
        }
        return bytes;
    }

    private static SkippedContextEntry skipped(ContextCandidate candidate, int sourceBytes, int measuredTokens,
            String reason) {
        return new SkippedContextEntry(candidate.entryId(), candidate.provenance(), sourceBytes,
            measuredTokens, reason);
    }

    private static ContextAssemblyResult cannotFit(ContextAssemblyRequest request, ContextTokenCounter counter,
            int estimate, String measurement, String reason) {
        var omitted = request.candidates().stream().map(candidate -> skipped(candidate,
            candidate.text().getBytes(StandardCharsets.UTF_8).length,
            counted(counter, candidate, candidate.text()), "assembly-cannot-fit")).toList();
        return new ContextAssemblyResult(false, List.of(), omitted,
            estimate, request.reservedOutputTokens(), measurement, Optional.of(reason), Optional.empty());
    }

    private static String excerpt(String value, int maxBytes) {
        int bytes = 0;
        int offset = 0;
        while (offset < value.length()) {
            int point = value.codePointAt(offset);
            int width = new String(Character.toChars(point)).getBytes(StandardCharsets.UTF_8).length;
            if (bytes + width > maxBytes) {
                break;
            }
            bytes += width;
            offset += Character.charCount(point);
        }
        return value.substring(0, offset);
    }
}
