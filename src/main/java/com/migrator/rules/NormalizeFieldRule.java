package com.migrator.rules;

import com.migrator.model.Record;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

/**
 * Trims whitespace from every string field.
 * Applies title-case to fields in the configured set.
 */
public class NormalizeFieldRule implements TransformationRule {

    private final Set<String> titleCaseFields;

    /** Only trims whitespace; no title-casing. */
    public NormalizeFieldRule() {
        this(Set.of());
    }

    /**
     * @param titleCaseFields field names whose string values should be converted to title case
     */
    public NormalizeFieldRule(Set<String> titleCaseFields) {
        this.titleCaseFields = Set.copyOf(titleCaseFields);
    }

    @Override
    public RuleResult apply(Record record) {
        Map<String, Object> newFields = new LinkedHashMap<>(record.getFields());

        for (Map.Entry<String, Object> entry : newFields.entrySet()) {
            if (entry.getValue() instanceof String s) {
                String trimmed = s.trim();
                if (titleCaseFields.contains(entry.getKey())) {
                    trimmed = toTitleCase(trimmed);
                }
                newFields.put(entry.getKey(), trimmed);
            }
        }

        return RuleResult.accept(new Record(newFields, record.getSourceFile()));
    }

    private static String toTitleCase(String input) {
        if (input == null || input.isEmpty()) {
            return input;
        }
        String[] words = input.split("\\s+");
        StringBuilder sb = new StringBuilder();
        for (String word : words) {
            if (word.isEmpty()) continue;
            if (sb.length() > 0) sb.append(' ');
            sb.append(Character.toUpperCase(word.charAt(0)));
            if (word.length() > 1) {
                sb.append(word.substring(1).toLowerCase());
            }
        }
        return sb.toString();
    }
}
