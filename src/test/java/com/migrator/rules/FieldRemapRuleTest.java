package com.migrator.rules;

import com.migrator.model.Record;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class FieldRemapRuleTest {

    private final FieldRemapRule rule =
            new FieldRemapRule(Map.of("cust_nm", "customerName", "ord_dt", "orderDate"));

    @Test
    void renamesLegacyFieldToCanonicalName() {
        Record rec = record("cust_nm", "Alice", "id", "1");
        Record out = rule.apply(rec).getRecord();
        assertEquals("Alice", out.getStringField("customerName"));
        assertNull(out.getStringField("cust_nm"));
    }

    @Test
    void renamesMultipleLegacyFields() {
        Record rec = record("cust_nm", "Bob", "ord_dt", "2024-01-01", "id", "2");
        Record out = rule.apply(rec).getRecord();
        assertEquals("Bob", out.getStringField("customerName"));
        assertEquals("2024-01-01", out.getStringField("orderDate"));
        assertNull(out.getStringField("cust_nm"));
        assertNull(out.getStringField("ord_dt"));
    }

    @Test
    void leavesUnknownFieldsUntouched() {
        Record rec = record("id", "99", "status", "active");
        Record out = rule.apply(rec).getRecord();
        assertEquals("99", out.getStringField("id"));
        assertEquals("active", out.getStringField("status"));
    }

    @Test
    void preservesFieldValueAfterRename() {
        Record rec = record("cust_nm", "  Jane Doe  ", "id", "3");
        Record out = rule.apply(rec).getRecord();
        // Value should be preserved exactly (trimming is NormalizeFieldRule's job)
        assertEquals("  Jane Doe  ", out.getStringField("customerName"));
    }

    @Test
    void alwaysReturnsAccepted() {
        assertTrue(rule.apply(record("x", "y")).isAccepted());
    }

    @Test
    void emptyRemappingsIsNoOp() {
        FieldRemapRule empty = new FieldRemapRule(Map.of());
        Record rec = record("cust_nm", "Alice");
        Record out = empty.apply(rec).getRecord();
        assertEquals("Alice", out.getStringField("cust_nm"));
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
