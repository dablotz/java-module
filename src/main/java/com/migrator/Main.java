package com.migrator;

import com.migrator.io.FileCollector;
import com.migrator.logging.MigrationLogger;
import com.migrator.model.MigrationResult;
import com.migrator.pipeline.Pipeline;
import com.migrator.pipeline.PipelineBuilder;
import com.migrator.reduce.DeduplicationReducer;
import com.migrator.reduce.LastWriteWinsReducer;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * CLI entry point.
 *
 * <pre>
 *   java -jar migrator.jar --input dir/region_a.csv dir/region_b.csv --output result.json
 * </pre>
 *
 * Optional flags (append after {@code --output}):
 * <ul>
 *   <li>{@code --strategy dedup} — use first-seen wins instead of last-write wins</li>
 *   <li>{@code --key <field>}    — key field for deduplication (default: {@code id})</li>
 * </ul>
 */
public class Main {

    private static final MigrationLogger LOG = new MigrationLogger(Main.class);

    public static void main(String[] args) {
        if (args.length == 0) {
            printUsage();
            System.exit(1);
        }

        List<String> inputPaths = new ArrayList<>();
        String outputPath = null;
        String strategy = "last-write-wins";
        String keyField = "id";

        int i = 0;
        while (i < args.length) {
            switch (args[i]) {
                case "--input" -> {
                    i++;
                    while (i < args.length && !args[i].startsWith("--")) {
                        inputPaths.add(args[i++]);
                    }
                }
                case "--output" -> {
                    if (i + 1 >= args.length) {
                        System.err.println("Error: --output requires a file path");
                        printUsage();
                        System.exit(1);
                    }
                    outputPath = args[++i];
                    i++;
                }
                case "--strategy" -> {
                    if (i + 1 >= args.length) {
                        System.err.println("Error: --strategy requires a value (dedup|last-write-wins)");
                        printUsage();
                        System.exit(1);
                    }
                    strategy = args[++i];
                    i++;
                }
                case "--key" -> {
                    if (i + 1 >= args.length) {
                        System.err.println("Error: --key requires a field name");
                        printUsage();
                        System.exit(1);
                    }
                    keyField = args[++i];
                    i++;
                }
                default -> {
                    System.err.println("Error: unknown argument '" + args[i] + "'");
                    printUsage();
                    System.exit(1);
                }
            }
        }

        if (inputPaths.isEmpty()) {
            System.err.println("Error: --input requires at least one file path");
            printUsage();
            System.exit(1);
        }
        if (outputPath == null) {
            System.err.println("Error: --output is required");
            printUsage();
            System.exit(1);
        }

        FileCollector collector = new FileCollector();
        List<Path> validInputs = collector.collect(inputPaths);
        if (validInputs.isEmpty()) {
            System.err.println("Error: none of the supplied input files could be read");
            System.exit(1);
        }

        try {
            Pipeline pipeline = new PipelineBuilder()
                    .inputFiles(validInputs)
                    .outputFile(Path.of(outputPath))
                    .keyField(keyField)
                    .requiredFields(List.of(keyField))
                    .fieldRemappings(Map.of("cust_nm", "customerName"))
                    .titleCaseFields(Set.of("customerName", "name", "fullName"))
                    .reducer("dedup".equalsIgnoreCase(strategy)
                            ? new DeduplicationReducer()
                            : new LastWriteWinsReducer())
                    .build();

            MigrationResult result = pipeline.execute();

            LOG.info("Migration complete — read={}, accepted={}, rejected={}",
                    result.getTotalRecordsRead(),
                    result.getTotalRecordsAccepted(),
                    result.getTotalRecordsRejected());

        } catch (Exception e) {
            LOG.error("Fatal error during migration", e);
            System.err.println("Fatal error: " + e.getMessage());
            System.exit(2);
        }
    }

    private static void printUsage() {
        System.err.println("""
                Usage:
                  java -jar migrator.jar --input <file1> [file2 ...] --output <output.json> [options]

                Options:
                  --key <field>          Key field for deduplication (default: id)
                  --strategy <name>      Conflict strategy: last-write-wins (default) | dedup

                Example:
                  java -jar migrator.jar --input region_a.csv region_b.csv --output result.json
                """);
    }
}
