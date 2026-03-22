package com.migrator.rules;

import com.migrator.model.Record;

/**
 * Outcome of applying a single TransformationRule.
 * Carries either the (possibly transformed) record or a rejection reason.
 */
public final class RuleResult {

    private final Record record;
    private final boolean accepted;
    private final String rejectionReason;

    private RuleResult(Record record, boolean accepted, String rejectionReason) {
        this.record = record;
        this.accepted = accepted;
        this.rejectionReason = rejectionReason;
    }

    public static RuleResult accept(Record record) {
        return new RuleResult(record, true, null);
    }

    public static RuleResult reject(Record record, String reason) {
        return new RuleResult(record, false, reason);
    }

    public boolean isAccepted() {
        return accepted;
    }

    public Record getRecord() {
        return record;
    }

    public String getRejectionReason() {
        return rejectionReason;
    }
}
