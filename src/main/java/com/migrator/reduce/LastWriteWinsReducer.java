package com.migrator.reduce;

import com.migrator.model.Record;

/**
 * Conflict strategy: the incoming (later) record replaces the existing one.
 * When multiple input files are processed in order, the last file's value wins.
 */
public class LastWriteWinsReducer implements Reducer {

    @Override
    public Record merge(Record existing, Record incoming) {
        return incoming;
    }
}
