package com.migrator.pipeline;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.migrator.logging.MigrationLogger;
import com.migrator.model.MigrationResult;
import com.migrator.model.OutputManifest;
import com.migrator.model.Record;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Serializes the final record collection plus run metadata to a JSON file.
 * String values that parse cleanly as long or double are stored as their
 * numeric type so the JSON output reflects proper types.
 */
public class Loader {

    private final ObjectMapper mapper;
    private final MigrationLogger logger;

    public Loader() {
        this.mapper = new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT);
        this.logger = new MigrationLogger(Loader.class);
    }

    /**
     * @throws IOException if the output file cannot be written
     */
    public void load(List<Record> records, MigrationResult result, Path outputFile)
            throws IOException {
        if (outputFile.getParent() != null) {
            Files.createDirectories(outputFile.getParent());
        }

        List<Map<String, Object>> coerced = new ArrayList<>(records.size());
        for (Record rec : records) {
            coerced.add(coerceTypes(rec.getFields()));
        }

        OutputManifest manifest = new OutputManifest();
        manifest.setExportedAt(Instant.now().toString());
        manifest.setSourceFiles(result.getSourceFiles());
        manifest.setTotalRecordsRead(result.getTotalRecordsRead());
        manifest.setTotalRecordsAccepted(result.getTotalRecordsAccepted());
        manifest.setTotalRecordsRejected(result.getTotalRecordsRejected());
        manifest.setRecords(coerced);

        mapper.writeValue(outputFile.toFile(), manifest);
        logger.info("Wrote {} record(s) to {}", records.size(), outputFile);
    }

    /**
     * Attempts to parse each string value as a number so the JSON contains
     * numeric types where appropriate (e.g. {@code "199.99"} → {@code 199.99}).
     */
    private static Map<String, Object> coerceTypes(Map<String, Object> fields) {
        Map<String, Object> out = new LinkedHashMap<>(fields.size());
        for (Map.Entry<String, Object> entry : fields.entrySet()) {
            Object val = entry.getValue();
            if (val instanceof String s && !s.isEmpty()) {
                val = tryParseNumber(s);
            }
            out.put(entry.getKey(), val);
        }
        return out;
    }

    private static Object tryParseNumber(String s) {
        try {
            return Long.parseLong(s);
        } catch (NumberFormatException ignored) {
            // fall through
        }
        try {
            return Double.parseDouble(s);
        } catch (NumberFormatException ignored) {
            // fall through
        }
        return s;
    }
}
