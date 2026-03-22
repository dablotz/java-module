package com.migrator.model;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Immutable value object representing a single data row throughout the pipeline.
 * Fields are stored as Object to support type coercion during transformation.
 */
public final class Record {

    private final Map<String, Object> fields;
    private final String sourceFile;

    public Record(Map<String, Object> fields, String sourceFile) {
        this.fields = Collections.unmodifiableMap(new LinkedHashMap<>(fields));
        this.sourceFile = sourceFile;
    }

    public Object getField(String key) {
        return fields.get(key);
    }

    public String getStringField(String key) {
        Object val = fields.get(key);
        return val != null ? val.toString() : null;
    }

    public boolean hasField(String key) {
        return fields.containsKey(key);
    }

    public Map<String, Object> getFields() {
        return fields;
    }

    public String getSourceFile() {
        return sourceFile;
    }

    /** Returns a new Record with the given field added or replaced. */
    public Record withField(String key, Object value) {
        Map<String, Object> newFields = new LinkedHashMap<>(fields);
        newFields.put(key, value);
        return new Record(newFields, sourceFile);
    }

    /** Returns a new Record with all entries from the given map merged in. */
    public Record withFields(Map<String, Object> overrides) {
        Map<String, Object> newFields = new LinkedHashMap<>(fields);
        newFields.putAll(overrides);
        return new Record(newFields, sourceFile);
    }

    /** Returns a new Record with oldKey removed and its value stored under newKey. */
    public Record withRenamedField(String oldKey, String newKey) {
        if (!fields.containsKey(oldKey)) {
            return this;
        }
        Map<String, Object> newFields = new LinkedHashMap<>();
        for (Map.Entry<String, Object> entry : fields.entrySet()) {
            if (entry.getKey().equals(oldKey)) {
                newFields.put(newKey, entry.getValue());
            } else {
                newFields.put(entry.getKey(), entry.getValue());
            }
        }
        return new Record(newFields, sourceFile);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Record other)) return false;
        return Objects.equals(fields, other.fields);
    }

    @Override
    public int hashCode() {
        return Objects.hash(fields);
    }

    @Override
    public String toString() {
        return "Record{source='" + sourceFile + "', fields=" + fields + "}";
    }
}
