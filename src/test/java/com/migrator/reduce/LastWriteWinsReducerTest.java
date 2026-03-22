package com.migrator.reduce;

import com.migrator.model.Record;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;

import static org.junit.jupiter.api.Assertions.*;

class LastWriteWinsReducerTest {

    private final LastWriteWinsReducer reducer = new LastWriteWinsReducer();

    @Test
    void keepsIncomingRecordOnConflict() {
        Record existing = record("id", "1", "value", "old");
        Record incoming = record("id", "1", "value", "new");
        Record kept = reducer.merge(existing, incoming);
        assertSame(incoming, kept, "LastWriteWinsReducer must return the incoming record");
    }

    @Test
    void returnsExactReferenceNotCopy() {
        Record existing = record("id", "A");
        Record incoming = record("id", "A");
        assertSame(incoming, reducer.merge(existing, incoming));
    }

    @Test
    void simulatesLastSeenWinsAcrossMultipleConflicts() {
        Record r1 = record("id", "X", "seq", "1");
        Record r2 = record("id", "X", "seq", "2");
        Record r3 = record("id", "X", "seq", "3");

        Record after12 = reducer.merge(r1, r2);
        Record after123 = reducer.merge(after12, r3);

        assertSame(r2, after12);
        assertSame(r3, after123);
        assertEquals("3", after123.getStringField("seq"));
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
