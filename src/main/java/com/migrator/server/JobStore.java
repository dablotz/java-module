package com.migrator.server;

import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Thread-safe registry of all jobs submitted since server startup.
 * Backed by a {@link ConcurrentHashMap} — reads are lock-free.
 */
public class JobStore {

    private final ConcurrentHashMap<String, JobRecord> store = new ConcurrentHashMap<>();

    public void register(JobRecord job) {
        store.put(job.jobId, job);
    }

    public Optional<JobRecord> get(String jobId) {
        return Optional.ofNullable(store.get(jobId));
    }
}
