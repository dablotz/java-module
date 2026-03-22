package com.migrator.rules;

import com.migrator.model.Record;

import java.util.List;

/**
 * Rejects any record that is missing one or more required fields (null or blank after trim).
 */
public class RequiredFieldRule implements TransformationRule {

    private final List<String> requiredFields;

    public RequiredFieldRule(List<String> requiredFields) {
        this.requiredFields = List.copyOf(requiredFields);
    }

    @Override
    public RuleResult apply(Record record) {
        for (String field : requiredFields) {
            String value = record.getStringField(field);
            if (value == null || value.trim().isEmpty()) {
                return RuleResult.reject(record,
                        "Missing required field: '" + field + "'");
            }
        }
        return RuleResult.accept(record);
    }
}
