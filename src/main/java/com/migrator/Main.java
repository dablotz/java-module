package com.migrator;

import com.migrator.server.JobRecord;
import com.migrator.server.JobStatus;
import com.migrator.server.JobStore;
import com.migrator.server.MetricsStore;
import com.migrator.server.MigrationJobRunner;
import io.javalin.Javalin;
import io.javalin.http.UploadedFile;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * HTTP server entry point. Starts a Javalin server on port 7000 and registers
 * four endpoints: POST /migrate, GET /jobs/{jobId}, GET /metrics, GET /health.
 */
public class Main {

    private static final int PORT = 7000;

    public static void main(String[] args) {
        JobStore           jobStore     = new JobStore();
        MetricsStore       metricsStore = new MetricsStore();
        MigrationJobRunner runner       = new MigrationJobRunner(jobStore, metricsStore);

        Javalin app = Javalin.create().start(PORT);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            app.stop();
            runner.shutdown();
        }));

        // ── POST /migrate ─────────────────────────────────────────────────────
        app.post("/migrate", ctx -> {
            List<UploadedFile> uploads = ctx.uploadedFiles("files");
            if (uploads.isEmpty()) {
                ctx.status(400).json(Map.of("error",
                        "At least one CSV file must be uploaded under the 'files' field"));
                return;
            }

            String jobId = UUID.randomUUID().toString();
            String rawOutputName = ctx.formParam("outputFileName");
            String outputFileName = (rawOutputName != null && !rawOutputName.isBlank())
                    ? rawOutputName
                    : jobId + ".json";

            // Write uploaded files to a per-job temp input directory
            Path inputDir = Files.createTempDirectory("migrator-" + jobId + "-in");
            List<Path> inputPaths = new ArrayList<>(uploads.size());
            for (UploadedFile upload : uploads) {
                Path dest = inputDir.resolve(upload.filename());
                try (InputStream in = upload.content()) {
                    Files.copy(in, dest, StandardCopyOption.REPLACE_EXISTING);
                }
                inputPaths.add(dest);
            }

            // Separate per-job output directory — not deleted after the job finishes
            Path outputDir = Files.createTempDirectory("migrator-" + jobId + "-out");

            JobRecord job = new JobRecord(jobId);
            jobStore.register(job);
            metricsStore.recordSubmission();
            runner.submit(job, inputPaths, outputDir, outputFileName);

            ctx.status(202).json(Map.of("jobId", jobId, "status", "PENDING"));
        });

        // ── GET /jobs/{jobId} ─────────────────────────────────────────────────
        app.get("/jobs/{jobId}", ctx -> {
            String jobId = ctx.pathParam("jobId");
            jobStore.get(jobId).ifPresentOrElse(job -> {
                JobStatus status = job.status; // single volatile read
                switch (status) {
                    case COMPLETE -> ctx.json(Map.of(
                            "jobId",                job.jobId,
                            "status",               status.name(),
                            "totalRecordsRead",     job.totalRecordsRead,
                            "totalRecordsAccepted", job.totalRecordsAccepted,
                            "totalRecordsRejected", job.totalRecordsRejected,
                            "outputFile",           job.outputFile
                    ));
                    case FAILED -> ctx.json(Map.of(
                            "jobId",        job.jobId,
                            "status",       status.name(),
                            "errorMessage", job.errorMessage
                    ));
                    default -> ctx.json(Map.of(
                            "jobId",  job.jobId,
                            "status", status.name()
                    ));
                }
            }, () -> ctx.status(404).json(Map.of("error", "Job not found: " + jobId)));
        });

        // ── GET /metrics ──────────────────────────────────────────────────────
        app.get("/metrics", ctx -> ctx.json(metricsStore.snapshot()));

        // ── GET /health ───────────────────────────────────────────────────────
        app.get("/health", ctx -> ctx.json(Map.of("status", "UP")));
    }
}
