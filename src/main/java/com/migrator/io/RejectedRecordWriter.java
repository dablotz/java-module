package com.migrator.io;

import com.migrator.logging.MigrationLogger;
import com.migrator.model.MigrationResult.RejectedEntry;
import com.migrator.model.Record;
import com.opencsv.CSVWriter;

import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * Writes rejected records to a _rejected.csv file alongside the main output.
 * An extra {@code rejectionReason} column is appended to each row.
 */
public class RejectedRecordWriter {

    private static final String REASON_COLUMN = "rejectionReason";

    private final MigrationLogger logger;

    public RejectedRecordWriter() {
        this.logger = new MigrationLogger(RejectedRecordWriter.class);
    }

    /**
     * Writes all rejected entries to a sibling file of {@code outputFile}.
     * The sibling is named {@code <basename>_rejected.csv}.
     * Does nothing if the list is empty.
     */
    public void write(List<RejectedEntry> rejectedEntries, Path outputFile) {
        if (rejectedEntries.isEmpty()) {
            logger.debug("No rejected records to write.");
            return;
        }

        Path rejectedPath = resolveRejectedPath(outputFile);
        logger.info("Writing {} rejected record(s) to {}", rejectedEntries.size(), rejectedPath);

        // Collect a stable union of all field names, preserving insertion order
        Set<String> headers = new LinkedHashSet<>();
        for (RejectedEntry entry : rejectedEntries) {
            headers.addAll(entry.record().getFields().keySet());
        }
        headers.add(REASON_COLUMN);

        try (CSVWriter writer = new CSVWriter(new FileWriter(rejectedPath.toFile()))) {
            writer.writeNext(headers.toArray(new String[0]));

            for (RejectedEntry entry : rejectedEntries) {
                Record rec = entry.record();
                List<String> row = new ArrayList<>(headers.size());
                for (String header : headers) {
                    if (REASON_COLUMN.equals(header)) {
                        row.add(entry.reason());
                    } else {
                        Object val = rec.getField(header);
                        row.add(val != null ? val.toString() : "");
                    }
                }
                writer.writeNext(row.toArray(new String[0]));
            }
        } catch (IOException e) {
            logger.error("Failed to write rejected records to {}: {}", rejectedPath, e.getMessage());
        }
    }

    private static Path resolveRejectedPath(Path outputFile) {
        String fileName = outputFile.getFileName().toString();
        int dot = fileName.lastIndexOf('.');
        String base = dot > 0 ? fileName.substring(0, dot) : fileName;
        String rejectedName = base + "_rejected.csv";
        Path parent = outputFile.getParent();
        return parent != null ? parent.resolve(rejectedName) : Path.of(rejectedName);
    }
}
