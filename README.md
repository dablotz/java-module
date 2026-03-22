# Migrator

A map-reduce style data migration service that accepts CSV files over HTTP, applies business rule transformations to each record, coalesces all records into a single unified collection, and returns the result as structured JSON.

## How it works

Each migration request runs through a four-stage pipeline:

1. **Extract** — reads each uploaded CSV file into an in-memory record collection
2. **Transform** — applies a chain of rules to every record in order:
   - `RequiredFieldRule` — rejects records missing configured key fields
   - `FieldRemapRule` — renames legacy column names to canonical ones (e.g. `cust_nm` → `customerName`)
   - `NormalizeFieldRule` — trims whitespace from all fields; applies title case to configured fields
3. **Reduce** — merges records across files by a key field, resolving duplicates with a configurable strategy
4. **Load** — writes a JSON manifest and a rejected-records CSV to the job output directory

Records that fail any transformation rule, or that lose a duplicate conflict, are written to a `<output>_rejected.csv` file with a `rejectionReason` column appended.

## API

The service listens on port **7000** and exposes four endpoints.

### `POST /migrate`

Submit a migration job. Upload one or more CSV files as a multipart form.

| Form field | Required | Description |
|---|---|---|
| `files` | yes | One or more CSV files (multipart upload) |
| `outputFileName` | no | Output filename (default: `<jobId>.json`) |

**Response `202 Accepted`:**
```json
{ "jobId": "e3b0c442-...", "status": "PENDING" }
```

**Response `400 Bad Request`** if no files are provided.

---

### `GET /jobs/{jobId}`

Poll the status of a submitted job.

**While running (`PENDING` or `RUNNING`):**
```json
{ "jobId": "e3b0c442-...", "status": "RUNNING" }
```

**On success (`COMPLETE`):**
```json
{
  "jobId": "e3b0c442-...",
  "status": "COMPLETE",
  "totalRecordsRead": 10,
  "totalRecordsAccepted": 8,
  "totalRecordsRejected": 2,
  "outputFile": "/tmp/migrator-e3b0c442-out/result.json"
}
```

**On failure (`FAILED`):**
```json
{ "jobId": "e3b0c442-...", "status": "FAILED", "errorMessage": "..." }
```

**Response `404`** if the job ID is unknown.

---

### `GET /metrics`

Returns aggregate counters across all jobs since server startup.

```json
{
  "totalJobsSubmitted": 12,
  "totalJobsCompleted": 11,
  "totalJobsFailed": 1,
  "totalRecordsRead": 340,
  "totalRecordsAccepted": 318,
  "totalRecordsRejected": 22
}
```

---

### `GET /health`

Liveness check.

```json
{ "status": "UP" }
```

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
java -jar target/migrator.jar
```

The server starts on port 7000.

## Running with Docker

Build the image:

```bash
docker build -t migrator .
```

Run the server:

```bash
docker run --rm -p 7000:7000 migrator
```

The Dockerfile uses a multi-stage build with a separate `deps` stage to pre-fetch Maven dependencies. This allows offline builds in environments without internet access:

```bash
# One-time setup (requires internet access):
docker build --target deps -t migrator-deps .
docker save migrator-deps | gzip > migrator-deps.tar.gz

# Offline build:
docker load < migrator-deps.tar.gz
DOCKER_BUILDKIT=1 docker build --cache-from migrator-deps -t migrator .
```

### Example: submit a job with curl

```bash
curl -X POST http://localhost:7000/migrate \
  -F "files=@data/region_a.csv" \
  -F "files=@data/region_b.csv" \
  -F "outputFileName=result.json"
```

Then poll until complete:

```bash
curl http://localhost:7000/jobs/<jobId>
```

## Project structure

```
src/main/java/com/migrator/
├── Main.java                    HTTP server entry point (Javalin, port 7000)
├── server/
│   ├── MigrationJobRunner.java  Dispatches jobs to a 4-thread executor pool
│   ├── JobStore.java            In-memory registry of active/completed jobs
│   ├── JobRecord.java           Mutable job state (status, result fields)
│   ├── JobStatus.java           Enum: PENDING, RUNNING, COMPLETE, FAILED
│   └── MetricsStore.java        Atomic aggregate counters across all jobs
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
