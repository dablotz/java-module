package com.migrator.io;

import com.migrator.logging.MigrationLogger;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

/**
 * Validates a list of raw file-path strings and returns only the readable ones.
 * Missing or unreadable files are logged and skipped so the pipeline can continue.
 */
public class FileCollector {

    private final MigrationLogger logger;

    public FileCollector() {
        this.logger = new MigrationLogger(FileCollector.class);
    }

    /**
     * @param rawPaths file path strings supplied by the caller
     * @return paths that exist and can be read, in the same order
     */
    public List<Path> collect(List<String> rawPaths) {
        List<Path> valid = new ArrayList<>();
        for (String raw : rawPaths) {
            Path path = Path.of(raw);
            if (!Files.exists(path)) {
                logger.warn("Input file not found, skipping: {}", raw);
            } else if (!Files.isReadable(path)) {
                logger.warn("Input file not readable, skipping: {}", raw);
            } else {
                valid.add(path);
                logger.debug("Collected input file: {}", path);
            }
        }
        return valid;
    }
}
