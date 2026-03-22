package com.migrator.pipeline;

import com.migrator.logging.MigrationLogger;
import com.migrator.model.Record;
import com.migrator.rules.RuleResult;
import com.migrator.rules.TransformationRule;

import java.util.List;

/**
 * Applies an ordered chain of {@link TransformationRule}s to a single record.
 * Processing stops at the first rejection.
 */
public class Transformer {

    private final List<TransformationRule> rules;
    private final MigrationLogger logger;

    public Transformer(List<TransformationRule> rules) {
        this.rules = List.copyOf(rules);
        this.logger = new MigrationLogger(Transformer.class);
    }

    /**
     * Runs every rule in sequence. Returns the first rejection or the final
     * accepted (and possibly modified) record.
     */
    public RuleResult transform(Record record) {
        Record current = record;
        for (TransformationRule rule : rules) {
            RuleResult result = rule.apply(current);
            if (!result.isAccepted()) {
                logger.debug("Record rejected by {}: {}",
                        rule.getClass().getSimpleName(), result.getRejectionReason());
                return result;
            }
            current = result.getRecord();
        }
        return RuleResult.accept(current);
    }
}
