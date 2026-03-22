package com.migrator.pipeline;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.migrator.model.MigrationResult;
import com.migrator.model.OutputManifest;
import com.migrator.reduce.DeduplicationReducer;
import com.migrator.reduce.LastWriteWinsReducer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

class PipelineIntegrationTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    // ── end-to-end: last-write-wins across two files ──────────────────────────

    @Test
    void lastWriteWins_mergesFilesAndPicksLatestOnConflict(@TempDir Path dir)
            throws IOException {
        Path csv1 = dir.resolve("region_a.csv");
        Files.writeString(csv1, """
                id,cust_nm,orderTotal
                ORD-001,  jane smith  ,199.99
                ORD-002,john doe,50.00
                """);

        Path csv2 = dir.resolve("region_b.csv");
        Files.writeString(csv2, """
                id,cust_nm,orderTotal
                ORD-001,JANE SMITH,249.99
                ORD-003,bob jones,75.00
                """);

        Path output = dir.resolve("result.json");

        Pipeline pipeline = new PipelineBuilder()
                .inputFiles(List.of(csv1, csv2))
                .outputFile(output)
                .keyField("id")
                .requiredFields(List.of("id"))
                .fieldRemappings(Map.of("cust_nm", "customerName"))
                .titleCaseFields(Set.of("customerName"))
                .reducer(new LastWriteWinsReducer())
                .build();

        MigrationResult result = pipeline.execute();

        // 4 records read (2 per file), 1 conflict → 3 accepted, 1 rejected
        assertEquals(4, result.getTotalRecordsRead());
        assertEquals(3, result.getTotalRecordsAccepted());
        assertEquals(1, result.getTotalRecordsRejected());

        // Output file must exist and be valid JSON
        assertTrue(Files.exists(output));
        OutputManifest manifest = MAPPER.readValue(output.toFile(), OutputManifest.class);

        assertEquals(4, manifest.getTotalRecordsRead());
        assertEquals(3, manifest.getTotalRecordsAccepted());
        assertEquals(1, manifest.getTotalRecordsRejected());
        assertNotNull(manifest.getExportedAt());
        assertTrue(manifest.getSourceFiles().contains("region_a.csv"));
        assertTrue(manifest.getSourceFiles().contains("region_b.csv"));
        assertEquals(3, manifest.getRecords().size());

        // ORD-001 should have the value from region_b (last write wins = 249.99)
        Map<String, Object> ord001 = manifest.getRecords().stream()
                .filter(r -> "ORD-001".equals(r.get("id")))
                .findFirst()
                .orElseThrow();
        assertEquals(249.99, ((Number) ord001.get("orderTotal")).doubleValue(), 0.001);

        // customerName should be title-cased
        assertEquals("Jane Smith", ord001.get("customerName"));
    }

    // ── deduplication reducer keeps first-seen ─────────────────────────────

    @Test
    void dedup_keepsFirstSeenRecordOnConflict(@TempDir Path dir) throws IOException {
        Path csv1 = dir.resolve("first.csv");
        Files.writeString(csv1, """
                id,value
                A,from-first
                B,only-in-first
                """);

        Path csv2 = dir.resolve("second.csv");
        Files.writeString(csv2, """
                id,value
                A,from-second
                C,only-in-second
                """);

        Path output = dir.resolve("out.json");

        Pipeline pipeline = new PipelineBuilder()
                .inputFiles(List.of(csv1, csv2))
                .outputFile(output)
                .keyField("id")
                .requiredFields(List.of("id"))
                .reducer(new DeduplicationReducer())
                .build();

        MigrationResult result = pipeline.execute();

        assertEquals(4, result.getTotalRecordsRead());
        assertEquals(3, result.getTotalRecordsAccepted());  // A, B, C
        assertEquals(1, result.getTotalRecordsRejected());  // A from second file

        OutputManifest manifest = MAPPER.readValue(output.toFile(), OutputManifest.class);
        Map<String, Object> recA = manifest.getRecords().stream()
                .filter(r -> "A".equals(r.get("id")))
                .findFirst()
                .orElseThrow();
        assertEquals("from-first", recA.get("value"));
    }

    // ── transform rejections ────────────────────────────────────────────────

    @Test
    void recordsMissingRequiredFieldAreRejected(@TempDir Path dir) throws IOException {
        Path csv = dir.resolve("data.csv");
        Files.writeString(csv, """
                id,name
                ,no-id-here
                VALID-1,has-id
                """);

        Path output = dir.resolve("out.json");

        Pipeline pipeline = new PipelineBuilder()
                .inputFiles(List.of(csv))
                .outputFile(output)
                .keyField("id")
                .requiredFields(List.of("id"))
                .build();

        MigrationResult result = pipeline.execute();

        assertEquals(2, result.getTotalRecordsRead());
        assertEquals(1, result.getTotalRecordsAccepted());
        assertEquals(1, result.getTotalRecordsRejected());
        assertEquals(1, result.getRejectedEntries().size());
        assertTrue(result.getRejectedEntries().get(0).reason().contains("id"));
    }

    // ── rejected CSV is written alongside output ────────────────────────────

    @Test
    void rejectedCsvIsWrittenWhenRecordsAreRejected(@TempDir Path dir) throws IOException {
        Path csv = dir.resolve("mixed.csv");
        Files.writeString(csv, """
                id,value
                ,missing-id
                GOOD,present
                """);

        Path output = dir.resolve("result.json");

        new PipelineBuilder()
                .inputFiles(List.of(csv))
                .outputFile(output)
                .keyField("id")
                .requiredFields(List.of("id"))
                .build()
                .execute();

        Path rejected = dir.resolve("result_rejected.csv");
        assertTrue(Files.exists(rejected), "rejected CSV should be written");

        String content = Files.readString(rejected);
        assertTrue(content.contains("rejectionReason"), "header must include rejectionReason column");
        assertTrue(content.contains("missing-id"), "rejected row data should appear");
    }

    // ── field remapping ──────────────────────────────────────────────────────

    @Test
    void fieldRemapRenamesLegacyColumns(@TempDir Path dir) throws IOException {
        Path csv = dir.resolve("legacy.csv");
        Files.writeString(csv, """
                id,cust_nm,orderTotal
                ORD-1,Alice,10.00
                """);

        Path output = dir.resolve("out.json");

        new PipelineBuilder()
                .inputFiles(List.of(csv))
                .outputFile(output)
                .keyField("id")
                .requiredFields(List.of("id"))
                .fieldRemappings(Map.of("cust_nm", "customerName"))
                .build()
                .execute();

        OutputManifest manifest = MAPPER.readValue(output.toFile(), OutputManifest.class);
        Map<String, Object> rec = manifest.getRecords().get(0);

        assertTrue(rec.containsKey("customerName"), "canonical name must be present");
        assertFalse(rec.containsKey("cust_nm"), "legacy name must be absent");
        assertEquals("Alice", rec.get("customerName"));
    }
}
