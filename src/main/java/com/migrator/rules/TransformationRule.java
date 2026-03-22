package com.migrator.rules;

import com.migrator.model.Record;

/**
 * Single step in the transformation chain of responsibility.
 * Implementations either pass the record through (possibly modified) or reject it.
 */
public interface TransformationRule {

    RuleResult apply(Record record);
}
