package com.migrator.reduce;

import com.migrator.model.Record;

/**
 * Strategy for resolving duplicate-key conflicts during the reduce phase.
 * Implementations must return one of the two input record references unchanged
 * so that the pipeline can determine which record was displaced.
 */
public interface Reducer {

    /**
     * Choose which record to keep when two records share the same key.
     *
     * @param existing the record already in the accumulator
     * @param incoming the newly-arrived record
     * @return the record to retain (must be either {@code existing} or {@code incoming})
     */
    Record merge(Record existing, Record incoming);
}
