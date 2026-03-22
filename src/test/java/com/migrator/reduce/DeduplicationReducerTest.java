package com.migrator.reduce;

import com.migrator.model.Record;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class DeduplicationReducerTest {

    private final DeduplicationReducer reducer = new DeduplicationReducer();

    @Test
    void keepsExistingRecordOnConflict() {
        Record existing = record("id", "1", "value", "first");
        Record incoming = record("id", "1", "value", "second");
        Record kept = reducer.merge(existing, incoming);
        assertSame(existing, kept, "DeduplicationReducer must return the existing record");
    }

    @Test
    void returnsExactReferenceNotCopy() {
        Record existing = record("id", "A");
        Record incoming = record("id", "A");
        assertSame(existing, reducer.merge(existing, incoming));
    }

    @Test
    void simulatesFirstSeenWinsAcrossMultipleConflicts() {
        // Simulate processing three records with the same key
        Record r1 = record("id", "X", "seq", "1");
        Record r2 = record("id", "X", "seq", "2");
        Record r3 = record("id", "X", "seq", "3");

        Record after12 = reducer.merge(r1, r2);
        Record after123 = reducer.merge(after12, r3);

        assertSame(r1, after12);
        assertSame(r1, after123);
        assertEquals("1", after123.getStringField("seq"));
    }

    @Test
    void doesNotModifyEitherRecord() {
        Record existing = record("id", "1", "x", "original");
        Record incoming = record("id", "1", "x", "new");
        reducer.merge(existing, incoming);
        assertEquals("original", existing.getStringField("x"));
        assertEquals("new", incoming.getStringField("x"));
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
