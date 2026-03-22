package com.migrator.rules;

import com.migrator.model.Record;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

class NormalizeFieldRuleTest {

    @Test
    void trimsLeadingAndTrailingWhitespaceFromAllStringFields() {
        NormalizeFieldRule rule = new NormalizeFieldRule();
        Record rec = record("name", "  Alice  ", "city", "\tLondon\n");
        Record out = rule.apply(rec).getRecord();
        assertEquals("Alice", out.getStringField("name"));
        assertEquals("London", out.getStringField("city"));
    }

    @Test
    void appliesTitleCaseToConfiguredFields() {
        NormalizeFieldRule rule = new NormalizeFieldRule(Set.of("customerName"));
        Record rec = record("customerName", "jane SMITH", "id", "ORD-001");
        Record out = rule.apply(rec).getRecord();
        assertEquals("Jane Smith", out.getStringField("customerName"));
        // field not in title-case set is only trimmed, not cased
        assertEquals("ORD-001", out.getStringField("id"));
    }

    @Test
    void titleCaseHandlesMultipleWords() {
        NormalizeFieldRule rule = new NormalizeFieldRule(Set.of("fullName"));
        Record rec = record("fullName", "JOHN PAUL JONES");
        Record out = rule.apply(rec).getRecord();
        assertEquals("John Paul Jones", out.getStringField("fullName"));
    }

    @Test
    void titleCaseHandlesSingleWord() {
        NormalizeFieldRule rule = new NormalizeFieldRule(Set.of("city"));
        Record rec = record("city", "BERLIN");
        assertEquals("Berlin", rule.apply(rec).getRecord().getStringField("city"));
    }

    @Test
    void doesNotModifyNonStringFields() {
        NormalizeFieldRule rule = new NormalizeFieldRule();
        LinkedHashMap<String, Object> fields = new LinkedHashMap<>();
        fields.put("amount", 199.99);
        fields.put("count", 5L);
        Record rec = new Record(fields, "test.csv");
        Record out = rule.apply(rec).getRecord();
        assertEquals(199.99, out.getField("amount"));
        assertEquals(5L, out.getField("count"));
    }

    @Test
    void alwaysReturnsAccepted() {
        NormalizeFieldRule rule = new NormalizeFieldRule();
        assertTrue(rule.apply(record("x", "y")).isAccepted());
    }

    // ── helpers ──────────────────────────────────────────────────────────────

    private static Record record(String... keyValues) {
        LinkedHashMap<String, Object> m = new LinkedHashMap<>();
        for (int i = 0; i < keyValues.length; i += 2) {
            m.put(keyValues[i], keyValues[i + 1]);
        }
        return new Record(m, "test.csv");
    }
}
