package com.migrator.server;

import com.migrator.model.MigrationResult;
import com.migrator.pipeline.PipelineBuilder;
import com.migrator.reduce.LastWriteWinsReducer;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

/**
 * Dispatches migration jobs to a fixed thread pool and manages their lifecycle:
 * status transitions, metrics updates, and input temp-directory cleanup.
 */
public class MigrationJobRunner {

    private final ExecutorService executor;
    private final JobStore jobStore;
    private final MetricsStore metricsStore;

    public MigrationJobRunner(JobStore jobStore, MetricsStore metricsStore) {
        this.executor     = Executors.newFixedThreadPool(4);
        this.jobStore     = jobStore;
        this.metricsStore = metricsStore;
    }

    /**
     * Enqueues {@code job} for async execution.
     *
     * @param job            the registered job record to update
     * @param inputFiles     uploaded CSV files already written to a temp directory
     * @param outputDir      temp directory for the pipeline output; not deleted on completion
     * @param outputFileName filename for the JSON output (e.g. {@code "result.json"})
     */
    public void submit(JobRecord job, List<Path> inputFiles, Path outputDir, String outputFileName) {
        executor.submit(() -> {
            job.status = JobStatus.RUNNING;
            Path inputDir = inputFiles.isEmpty() ? null : inputFiles.get(0).getParent();
            try {
                MigrationResult result = new PipelineBuilder()
                        .inputFiles(inputFiles)
                        .outputFile(outputDir.resolve(outputFileName))
                        .keyField("id")
                        .requiredFields(List.of("id"))
                        .fieldRemappings(Map.of("cust_nm", "customerName"))
                        .titleCaseFields(Set.of("customerName", "name", "fullName"))
                        .reducer(new LastWriteWinsReducer())
                        .build()
                        .execute();

                // Populate result fields before the terminal volatile write so that
                // any thread observing COMPLETE is guaranteed to see them.
                job.outputFile           = outputDir.resolve(outputFileName).toAbsolutePath().toString();
                job.totalRecordsRead     = result.getTotalRecordsRead();
                job.totalRecordsAccepted = result.getTotalRecordsAccepted();
                job.totalRecordsRejected = result.getTotalRecordsRejected();
                job.status               = JobStatus.COMPLETE;

                metricsStore.recordCompletion(result);

            } catch (Exception e) {
                job.errorMessage = e.getMessage();
                job.status       = JobStatus.FAILED;
                metricsStore.recordFailure();
            } finally {
                deleteDirectory(inputDir);
            }
        });
    }

    /**
     * Initiates an orderly shutdown: waits up to 30 seconds for in-flight jobs,
     * then forces termination.
     */
    public void shutdown() {
        executor.shutdown();
        try {
            if (!executor.awaitTermination(30, TimeUnit.SECONDS)) {
                executor.shutdownNow();
            }
        } catch (InterruptedException e) {
            executor.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }

    private static void deleteDirectory(Path dir) {
        if (dir == null || !Files.exists(dir)) return;
        try (Stream<Path> walk = Files.walk(dir)) {
            walk.sorted(Comparator.reverseOrder())
                .forEach(p -> {
                    try { Files.delete(p); } catch (IOException ignored) {}
                });
        } catch (IOException ignored) {}
    }
}
