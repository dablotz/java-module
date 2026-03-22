package com.migrator.pipeline;

import com.migrator.io.RejectedRecordWriter;
import com.migrator.reduce.LastWriteWinsReducer;
import com.migrator.reduce.Reducer;
import com.migrator.rules.FieldRemapRule;
import com.migrator.rules.NormalizeFieldRule;
import com.migrator.rules.RequiredFieldRule;
import com.migrator.rules.TransformationRule;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Fluent builder for constructing a fully configured {@link Pipeline}.
 *
 * <p>Default settings:
 * <ul>
 *   <li>Key field: {@code "id"}</li>
 *   <li>Reducer: {@link LastWriteWinsReducer}</li>
 *   <li>No required fields enforced beyond what the caller specifies</li>
 * </ul>
 */
public class PipelineBuilder {

    private List<Path> inputFiles = List.of();
    private Path outputFile;
    private String keyField = "id";
    private List<String> requiredFields = List.of();
    private Map<String, String> fieldRemappings = Map.of();
    private Set<String> titleCaseFields = Set.of();
    private Reducer reducer = new LastWriteWinsReducer();

    public PipelineBuilder inputFiles(List<Path> inputFiles) {
        this.inputFiles = List.copyOf(inputFiles);
        return this;
    }

    public PipelineBuilder outputFile(Path outputFile) {
        this.outputFile = outputFile;
        return this;
    }

    public PipelineBuilder keyField(String keyField) {
        this.keyField = keyField;
        return this;
    }

    public PipelineBuilder requiredFields(List<String> requiredFields) {
        this.requiredFields = List.copyOf(requiredFields);
        return this;
    }

    public PipelineBuilder fieldRemappings(Map<String, String> fieldRemappings) {
        this.fieldRemappings = Map.copyOf(fieldRemappings);
        return this;
    }

    public PipelineBuilder titleCaseFields(Set<String> titleCaseFields) {
        this.titleCaseFields = Set.copyOf(titleCaseFields);
        return this;
    }

    public PipelineBuilder reducer(Reducer reducer) {
        this.reducer = reducer;
        return this;
    }

    public Pipeline build() {
        if (inputFiles.isEmpty()) {
            throw new IllegalStateException("At least one input file must be specified");
        }
        if (outputFile == null) {
            throw new IllegalStateException("outputFile must be specified");
        }

        // Rule order: validate required fields first (using legacy names), remap to canonical
        // names next, then normalize — so title-case applies to the canonical field names.
        List<TransformationRule> rules = new ArrayList<>();
        rules.add(new RequiredFieldRule(requiredFields));
        rules.add(new FieldRemapRule(fieldRemappings));
        rules.add(new NormalizeFieldRule(titleCaseFields));

        return new Pipeline(
                inputFiles,
                outputFile,
                new Extractor(),
                new Transformer(rules),
                reducer,
                new Loader(),
                new RejectedRecordWriter(),
                keyField
        );
    }
}
