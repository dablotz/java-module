package com.migrator.rules;

import com.migrator.model.Record;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class RequiredFieldRuleTest {

    private final RequiredFieldRule rule = new RequiredFieldRule(List.of("id", "email"));

    @Test
    void acceptsRecordWithAllRequiredFields() {
        Record rec = record("id", "123", "email", "user@example.com");
        RuleResult result = rule.apply(rec);
        assertTrue(result.isAccepted());
        assertSame(rec, result.getRecord());
    }

    @Test
    void rejectsRecordMissingRequiredField() {
        Record rec = record("id", "123");  // no email
        RuleResult result = rule.apply(rec);
        assertFalse(result.isAccepted());
        assertTrue(result.getRejectionReason().contains("email"));
    }

    @Test
    void rejectsRecordWithNullFieldValue() {
        Record rec = new Record(mapOf("id", "123", "email", null), "test.csv");
        RuleResult result = rule.apply(rec);
        assertFalse(result.isAccepted());
        assertTrue(result.getRejectionReason().contains("email"));
    }

    @Test
    void rejectsRecordWithBlankFieldValue() {
        Record rec = record("id", "  ", "email", "user@example.com");
        RuleResult result = rule.apply(rec);
        assertFalse(result.isAccepted());
        assertTrue(result.getRejectionReason().contains("id"));
    }

    @Test
    void rejectsRecordWithEmptyStringField() {
        Record rec = record("id", "", "email", "user@example.com");
        RuleResult result = rule.apply(rec);
        assertFalse(result.isAccepted());
    }

    @Test
    void noRequiredFieldsAlwaysAccepts() {
        RequiredFieldRule emptyRule = new RequiredFieldRule(List.of());
        Record rec = new Record(Map.of(), "test.csv");
        assertTrue(emptyRule.apply(rec).isAccepted());
    }

    // ── helpers ──────────────────────────────────────────────────────────────

    private static Record record(String... keyValues) {
        java.util.LinkedHashMap<String, Object> m = new java.util.LinkedHashMap<>();
        for (int i = 0; i < keyValues.length; i += 2) {
            m.put(keyValues[i], keyValues[i + 1]);
        }
        return new Record(m, "test.csv");
    }

    private static java.util.LinkedHashMap<String, Object> mapOf(String k1, Object v1,
                                                                  String k2, Object v2) {
        java.util.LinkedHashMap<String, Object> m = new java.util.LinkedHashMap<>();
        m.put(k1, v1);
        m.put(k2, v2);
        return m;
    }
}
