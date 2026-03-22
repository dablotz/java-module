package com.migrator.server;

import com.migrator.model.MigrationResult;

import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Live counters aggregated across all jobs since server startup.
 * All fields are {@link AtomicLong} — safe to increment from multiple executor threads.
 */
public class MetricsStore {

    private final AtomicLong totalJobsSubmitted   = new AtomicLong();
    private final AtomicLong totalJobsCompleted   = new AtomicLong();
    private final AtomicLong totalJobsFailed      = new AtomicLong();
    private final AtomicLong totalRecordsRead     = new AtomicLong();
    private final AtomicLong totalRecordsAccepted = new AtomicLong();
    private final AtomicLong totalRecordsRejected = new AtomicLong();

    /** Called in the HTTP thread when a job is accepted (before async dispatch). */
    public void recordSubmission() {
        totalJobsSubmitted.incrementAndGet();
    }

    /** Called from the executor thread when a job finishes successfully. */
    public void recordCompletion(MigrationResult result) {
        totalJobsCompleted.incrementAndGet();
        totalRecordsRead.addAndGet(result.getTotalRecordsRead());
        totalRecordsAccepted.addAndGet(result.getTotalRecordsAccepted());
        totalRecordsRejected.addAndGet(result.getTotalRecordsRejected());
    }

    /** Called from the executor thread when a job fails. */
    public void recordFailure() {
        totalJobsFailed.incrementAndGet();
    }

    /** Returns an immutable point-in-time snapshot suitable for JSON serialization. */
    public Map<String, Long> snapshot() {
        return Map.of(
                "totalJobsSubmitted",   totalJobsSubmitted.get(),
                "totalJobsCompleted",   totalJobsCompleted.get(),
                "totalJobsFailed",      totalJobsFailed.get(),
                "totalRecordsRead",     totalRecordsRead.get(),
                "totalRecordsAccepted", totalRecordsAccepted.get(),
                "totalRecordsRejected", totalRecordsRejected.get()
        );
    }
}
