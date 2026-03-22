package com.migrator.pipeline;

import com.migrator.logging.MigrationLogger;
import com.migrator.model.Record;
import com.opencsv.CSVReaderHeaderAware;
import com.opencsv.exceptions.CsvValidationException;

import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Reads a single CSV file and returns one {@link Record} per data row.
 * The first row is treated as a header; all values are read as strings.
 */
public class Extractor {

    private final MigrationLogger logger;

    public Extractor() {
        this.logger = new MigrationLogger(Extractor.class);
    }

    /**
     * @param file path to the CSV file to read
     * @return list of records, one per row (excluding the header)
     * @throws IOException if the file cannot be opened or is not valid CSV
     */
    public List<Record> extract(Path file) throws IOException {
        String fileName = file.getFileName().toString();
        List<Record> records = new ArrayList<>();

        try (CSVReaderHeaderAware reader =
                     new CSVReaderHeaderAware(new FileReader(file.toFile()))) {
            Map<String, String> row;
            while ((row = reader.readMap()) != null) {
                Map<String, Object> fields = new LinkedHashMap<>(row);
                records.add(new Record(fields, fileName));
            }
        } catch (CsvValidationException e) {
            throw new IOException("Invalid CSV format in " + file + ": " + e.getMessage(), e);
        }

        logger.debug("Extracted {} records from {}", records.size(), fileName);
        return records;
    }
}
