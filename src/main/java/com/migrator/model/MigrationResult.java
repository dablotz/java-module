package com.migrator.model;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Accumulates counts and rejected records over the course of a pipeline run.
 * The final accepted count is set explicitly after the reduce phase completes.
 */
public class MigrationResult {

    public record RejectedEntry(Record record, String reason) {}

    private final List<String> sourceFiles = new ArrayList<>();
    private int totalRecordsRead = 0;
    private int finalAcceptedCount = 0;
    private final List<RejectedEntry> rejectedEntries = new ArrayList<>();

    public void addSourceFile(String fileName) {
        sourceFiles.add(fileName);
    }

    public void incrementRead(int count) {
        totalRecordsRead += count;
    }

    /** Called after the reduce phase to lock in the final accepted record count. */
    public void setFinalAcceptedCount(int count) {
        finalAcceptedCount = count;
    }

    public void addRejectedEntry(Record record, String reason) {
        rejectedEntries.add(new RejectedEntry(record, reason));
    }

    public List<String> getSourceFiles() {
        return Collections.unmodifiableList(sourceFiles);
    }

    public int getTotalRecordsRead() {
        return totalRecordsRead;
    }

    public int getTotalRecordsAccepted() {
        return finalAcceptedCount;
    }

    public int getTotalRecordsRejected() {
        return totalRecordsRead - finalAcceptedCount;
    }

    public List<RejectedEntry> getRejectedEntries() {
        return Collections.unmodifiableList(rejectedEntries);
    }
}
