package com.migrator.logging;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Thin structured wrapper around SLF4J.
 * Adds a pipeline-stage prefix to every message for consistent log output.
 */
public class MigrationLogger {

    private final Logger logger;
    private String stage = "";

    public MigrationLogger(Class<?> clazz) {
        this.logger = LoggerFactory.getLogger(clazz);
    }

    public MigrationLogger(Logger logger) {
        this.logger = logger;
    }

    /** Returns a new logger that prefixes every message with the given stage label. */
    public MigrationLogger withStage(String stageName) {
        MigrationLogger child = new MigrationLogger(logger);
        child.stage = "[" + stageName + "] ";
        return child;
    }

    public void info(String msg, Object... args) {
        logger.info(stage + msg, args);
    }

    public void debug(String msg, Object... args) {
        logger.debug(stage + msg, args);
    }

    public void warn(String msg, Object... args) {
        logger.warn(stage + msg, args);
    }

    public void error(String msg, Object... args) {
        logger.error(stage + msg, args);
    }

    public void error(String msg, Throwable t) {
        logger.error(stage + msg, t);
    }
}
