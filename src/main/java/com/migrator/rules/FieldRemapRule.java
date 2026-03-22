package com.migrator.rules;

import com.migrator.model.Record;

import java.util.Map;

/**
 * Renames legacy field names to their canonical counterparts.
 * Only fields present in the record are renamed; others are left untouched.
 */
public class FieldRemapRule implements TransformationRule {

    /** Maps oldName → newName. */
    private final Map<String, String> remappings;

    public FieldRemapRule(Map<String, String> remappings) {
        this.remappings = Map.copyOf(remappings);
    }

    @Override
    public RuleResult apply(Record record) {
        Record current = record;
        for (Map.Entry<String, String> entry : remappings.entrySet()) {
            current = current.withRenamedField(entry.getKey(), entry.getValue());
        }
        return RuleResult.accept(current);
    }
}
