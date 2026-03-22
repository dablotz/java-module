package com.migrator.reduce;

import com.migrator.model.Record;

/**
 * Conflict strategy: the first-seen record wins; any subsequent record with the
 * same key is treated as a duplicate and will be written to the rejected output.
 */
public class DeduplicationReducer implements Reducer {

    @Override
    public Record merge(Record existing, Record incoming) {
        return existing;
    }
}
