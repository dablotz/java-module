package com.migrator.server;

/**
 * Mutable state holder for a single migration job.
 *
 * Thread-safety contract: all non-status fields are written exactly once from the
 * executor thread, strictly before the terminal {@code status} write. Because
 * {@code status} is {@code volatile}, that write acts as a happens-before barrier —
 * any thread that observes {@code COMPLETE} or {@code FAILED} is guaranteed to see
 * the values written to the other fields beforehand.
 */
public class JobRecord {

    public final String jobId;
    public volatile JobStatus status;

    // Populated on COMPLETE
    public String outputFile;
    public int totalRecordsRead;
    public int totalRecordsAccepted;
    public int totalRecordsRejected;

    // Populated on FAILED
    public String errorMessage;

    public JobRecord(String jobId) {
        this.jobId = jobId;
        this.status = JobStatus.PENDING;
    }
}
