package com.migrator.pipeline;

import com.migrator.io.RejectedRecordWriter;
import com.migrator.logging.MigrationLogger;
import com.migrator.model.MigrationResult;
import com.migrator.model.Record;
import com.migrator.reduce.Reducer;
import com.migrator.rules.RuleResult;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.UUID;

/**
 * Orchestrates the four pipeline stages in order:
 * <ol>
 *   <li><b>Extract</b>   – reads each CSV file into raw records</li>
 *   <li><b>Transform</b> – runs the rule chain: Required → Remap → Normalize</li>
 *   <li><b>Reduce</b>    – merges records by key using the configured strategy</li>
 *   <li><b>Load</b>      – writes the manifest JSON and rejected-records CSV</li>
 * </ol>
 */
public class Pipeline {

    private final List<Path> inputFiles;
    private final Path outputFile;
    private final Extractor extractor;
    private final Transformer transformer;
    private final Reducer reducer;
    private final Loader loader;
    private final RejectedRecordWriter rejectedWriter;
    private final String keyField;
    private final MigrationLogger logger;

    Pipeline(List<Path> inputFiles,
             Path outputFile,
             Extractor extractor,
             Transformer transformer,
             Reducer reducer,
             Loader loader,
             RejectedRecordWriter rejectedWriter,
             String keyField) {
        this.inputFiles = List.copyOf(inputFiles);
        this.outputFile = outputFile;
        this.extractor = extractor;
        this.transformer = transformer;
        this.reducer = reducer;
        this.loader = loader;
        this.rejectedWriter = rejectedWriter;
        this.keyField = keyField;
        this.logger = new MigrationLogger(Pipeline.class);
    }

    /**
     * Runs all stages and returns a populated {@link MigrationResult}.
     *
     * @throws IOException  if the output file cannot be written (non-recoverable)
     */
    public MigrationResult execute() throws IOException {
        MigrationResult result = new MigrationResult();
        LinkedHashMap<String, Record> accumulator = new LinkedHashMap<>();

        logger.info("=== Pipeline starting: {} input file(s) ===", inputFiles.size());

        // ── EXTRACT + TRANSFORM (map phase) ──────────────────────────────────
        for (Path file : inputFiles) {
            MigrationLogger fileLog = logger.withStage("Extract:" + file.getFileName());
            fileLog.info("Reading {}", file);

            List<Record> extracted;
            try {
                extracted = extractor.extract(file);
            } catch (IOException e) {
                logger.error("Cannot read {}, skipping: {}", file, e.getMessage());
                continue;
            }

            result.addSourceFile(file.getFileName().toString());
            result.incrementRead(extracted.size());
            fileLog.info("{} record(s) extracted", extracted.size());

            MigrationLogger txLog = logger.withStage("Transform:" + file.getFileName());
            int accepted = 0;
            int rejected = 0;
            List<Record> transformPassed = new ArrayList<>(extracted.size());

            for (Record rec : extracted) {
                RuleResult ruleResult = transformer.transform(rec);
                if (ruleResult.isAccepted()) {
                    transformPassed.add(ruleResult.getRecord());
                    accepted++;
                } else {
                    result.addRejectedEntry(rec, ruleResult.getRejectionReason());
                    rejected++;
                    txLog.debug("Rejected: {} — {}", rec, ruleResult.getRejectionReason());
                }
            }
            txLog.info("accepted={}, rejected={}", accepted, rejected);

            // ── REDUCE (merge into accumulator) ──────────────────────────────
            MigrationLogger rdLog = logger.withStage("Reduce:" + file.getFileName());
            for (Record rec : transformPassed) {
                String key = rec.getStringField(keyField);
                if (key == null || key.isBlank()) {
                    key = "_nokey_" + UUID.randomUUID();
                }

                if (accumulator.containsKey(key)) {
                    Record existing = accumulator.get(key);
                    Record kept = reducer.merge(existing, rec);
                    accumulator.put(key, kept);

                    // The record that was NOT kept is displaced / rejected
                    Record displaced = (kept == rec) ? existing : rec;
                    String reason = (kept == rec)
                            ? "Duplicate key='" + key + "': replaced by later record"
                            : "Duplicate key='" + key + "': first-seen wins";
                    result.addRejectedEntry(displaced, reason);
                    rdLog.debug("Conflict on key='{}': {}", key, reason);
                } else {
                    accumulator.put(key, rec);
                }
            }
        }

        // ── FINALISE counts ──────────────────────────────────────────────────
        result.setFinalAcceptedCount(accumulator.size());
        logger.info("=== Reduce complete: {} unique record(s) in accumulator ===",
                accumulator.size());

        // ── LOAD ─────────────────────────────────────────────────────────────
        logger.info("=== Stage: Load ===");
        loader.load(new ArrayList<>(accumulator.values()), result, outputFile);

        // ── WRITE REJECTED ────────────────────────────────────────────────────
        rejectedWriter.write(result.getRejectedEntries(), outputFile);

        logger.info("=== Pipeline complete — read={}, accepted={}, rejected={} ===",
                result.getTotalRecordsRead(),
                result.getTotalRecordsAccepted(),
                result.getTotalRecordsRejected());

        return result;
    }
}
