package com.migrator.model;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;
import java.util.Map;

/**
 * JSON envelope written to the output file.
 * Jackson serializes each field using its camelCase name directly.
 */
public class OutputManifest {

    @JsonProperty("exportedAt")
    private String exportedAt;

    @JsonProperty("sourceFiles")
    private List<String> sourceFiles;

    @JsonProperty("totalRecordsRead")
    private int totalRecordsRead;

    @JsonProperty("totalRecordsAccepted")
    private int totalRecordsAccepted;

    @JsonProperty("totalRecordsRejected")
    private int totalRecordsRejected;

    @JsonProperty("records")
    private List<Map<String, Object>> records;

    public String getExportedAt() { return exportedAt; }
    public void setExportedAt(String exportedAt) { this.exportedAt = exportedAt; }

    public List<String> getSourceFiles() { return sourceFiles; }
    public void setSourceFiles(List<String> sourceFiles) { this.sourceFiles = sourceFiles; }

    public int getTotalRecordsRead() { return totalRecordsRead; }
    public void setTotalRecordsRead(int totalRecordsRead) { this.totalRecordsRead = totalRecordsRead; }

    public int getTotalRecordsAccepted() { return totalRecordsAccepted; }
    public void setTotalRecordsAccepted(int totalRecordsAccepted) { this.totalRecordsAccepted = totalRecordsAccepted; }

    public int getTotalRecordsRejected() { return totalRecordsRejected; }
    public void setTotalRecordsRejected(int totalRecordsRejected) { this.totalRecordsRejected = totalRecordsRejected; }

    public List<Map<String, Object>> getRecords() { return records; }
    public void setRecords(List<Map<String, Object>> records) { this.records = records; }
}
