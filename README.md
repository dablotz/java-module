# Migrator

A map-reduce style data migration tool that reads multiple CSV files with identical schemas, applies business rule transformations to each record, coalesces all records into a single unified collection, and writes the result to a structured JSON file.

## How it works

The tool runs each input file through a four-stage pipeline:

1. **Extract** — reads each CSV file into an in-memory record collection
2. **Transform** — applies a chain of rules to every record in order:
   - `RequiredFieldRule` — rejects records missing configured key fields
   - `FieldRemapRule` — renames legacy column names to canonical ones (e.g. `cust_nm` → `customerName`)
   - `NormalizeFieldRule` — trims whitespace from all fields; applies title case to configured fields
3. **Reduce** — merges records across files by a key field, resolving duplicates with a configurable strategy
4. **Load** — writes a JSON manifest and a rejected-records CSV alongside it

Records that fail any transformation rule, or that lose a duplicate conflict, are written to a `<output>_rejected.csv` file with a `rejectionReason` column appended.

## Output format

```json
{
  "exportedAt": "2024-11-01T17:51:00Z",
  "sourceFiles": ["region_a.csv", "region_b.csv"],
  "totalRecordsRead": 10,
  "totalRecordsAccepted": 8,
  "totalRecordsRejected": 2,
  "records": [
    {
      "id": "ORD-001",
      "customerName": "Jane Smith",
      "orderTotal": 249.99
    }
  ]
}
```

## Building

Requires Java 17 and Maven 3.9+.

```bash
mvn clean package
```

This produces `target/migrator.jar`.

## Running

```bash
java -jar target/migrator.jar --input <file1> [file2 ...] --output <output.json> [options]
```

### Options

| Flag | Default | Description |
|---|---|---|
| `--input` | _(required)_ | One or more paths to input CSV files |
| `--output` | _(required)_ | Destination path for the JSON output |
| `--key` | `id` | Field name used to identify and deduplicate records |
| `--strategy` | `last-write-wins` | Duplicate resolution strategy: `last-write-wins` or `dedup` |

### Duplicate strategies

- **`last-write-wins`** — when the same key appears in multiple files, the record from the latest file in the input list wins
- **`dedup`** — the first occurrence of a key is kept; all subsequent duplicates are rejected

### Example

```bash
java -jar target/migrator.jar \
  --input data/region_a.csv data/region_b.csv \
  --output data/result.json
```

## Running with Docker

Build the image:

```bash
docker build -t migrator .
```

Run with a bind-mounted data directory:

```bash
docker run --rm -v "$(pwd)/data:/data" migrator \
  --input /data/region_a.csv /data/region_b.csv \
  --output /data/result.json
```

Output files (`result.json`, `result_rejected.csv`) are written into the mounted directory and remain on the host after the container exits.

To open a shell inside the container for debugging:

```bash
docker run --rm -it --entrypoint /bin/sh -v "$(pwd)/data:/data" migrator
```

## Project structure

```
src/main/java/com/migrator/
├── Main.java                    Entry point, CLI argument parsing
├── pipeline/
│   ├── Pipeline.java            Orchestrates the four stages
│   ├── Extractor.java           Reads a CSV file into records
│   ├── Transformer.java         Applies the rule chain to a record
│   ├── Loader.java              Serializes the output manifest to JSON
│   └── PipelineBuilder.java     Fluent builder for pipeline configuration
├── rules/
│   ├── TransformationRule.java  Interface for transformation steps
│   ├── RequiredFieldRule.java   Rejects records with missing fields
│   ├── NormalizeFieldRule.java  Trims whitespace; applies title case
│   └── FieldRemapRule.java      Renames legacy field names
├── reduce/
│   ├── Reducer.java               Interface for duplicate resolution
│   ├── LastWriteWinsReducer.java  Later record replaces earlier
│   └── DeduplicationReducer.java  First record seen is kept
├── model/
│   ├── Record.java              Immutable row value object
│   ├── MigrationResult.java     Accumulates run statistics
│   └── OutputManifest.java      JSON output envelope
├── io/
│   ├── FileCollector.java         Validates input file paths
│   └── RejectedRecordWriter.java  Writes failed records to CSV
└── logging/
    └── MigrationLogger.java     Structured SLF4J wrapper
```

## Running tests

```bash
mvn test
```
