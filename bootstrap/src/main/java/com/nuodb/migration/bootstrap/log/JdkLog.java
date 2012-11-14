package com.nuodb.migration.bootstrap.log;

import java.util.logging.Level;
import java.util.logging.Logger;

public class JdkLog implements Log {

    private final Logger logger;

    public JdkLog(String name) {
        this.logger = Logger.getLogger(name);
    }

    public final boolean isErrorEnabled() {
        return logger.isLoggable(Level.SEVERE);
    }

    public final boolean isWarnEnabled() {
        return logger.isLoggable(Level.WARNING);
    }

    public final boolean isInfoEnabled() {
        return logger.isLoggable(Level.INFO);
    }

    public final boolean isDebugEnabled() {
        return logger.isLoggable(Level.FINE);
    }

    public final boolean isFatalEnabled() {
        return logger.isLoggable(Level.SEVERE);
    }

    public final boolean isTraceEnabled() {
        return logger.isLoggable(Level.FINER);
    }

    public final void debug(Object message) {
        log(Level.FINE, String.valueOf(message), null);
    }

    public final void debug(Object message, Throwable t) {
        log(Level.FINE, String.valueOf(message), t);
    }

    public final void trace(Object message) {
        log(Level.FINER, String.valueOf(message), null);
    }

    public final void trace(Object message, Throwable t) {
        log(Level.FINER, String.valueOf(message), t);
    }

    public final void info(Object message) {
        log(Level.INFO, String.valueOf(message), null);
    }

    public final void info(Object message, Throwable t) {
        log(Level.INFO, String.valueOf(message), t);
    }

    public final void warn(Object message) {
        log(Level.WARNING, String.valueOf(message), null);
    }

    public final void warn(Object message, Throwable t) {
        log(Level.WARNING, String.valueOf(message), t);
    }

    public final void error(Object message) {
        log(Level.SEVERE, String.valueOf(message), null);
    }

    public final void error(Object message, Throwable t) {
        log(Level.SEVERE, String.valueOf(message), t);
    }

    public final void fatal(Object message) {
        log(Level.SEVERE, String.valueOf(message), null);
    }

    public final void fatal(Object message, Throwable t) {
        log(Level.SEVERE, String.valueOf(message), t);
    }

    private void log(Level level, String message, Throwable exception) {
        if (logger.isLoggable(level)) {
            Throwable dummyException = new Throwable();
            StackTraceElement locations[] = dummyException.getStackTrace();
            String sourceClass = "unknown";
            String sourceMethod = "unknown";
            if (locations != null && locations.length > 2) {
                StackTraceElement caller = locations[2];
                sourceClass = caller.getClassName();
                sourceMethod = caller.getMethodName();
            }
            if (exception == null) {
                logger.logp(level, sourceClass, sourceMethod, message);
            } else {
                logger.logp(level, sourceClass, sourceMethod, message, exception);
            }
        }
    }
}
