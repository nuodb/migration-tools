package com.nuodb.migration.bootstrap.log;

public interface Log {
    abstract boolean isDebugEnabled();

    abstract boolean isErrorEnabled();

    abstract boolean isFatalEnabled();

    abstract boolean isInfoEnabled();

    abstract boolean isTraceEnabled();

    abstract boolean isWarnEnabled();

    abstract void trace(Object obj);

    abstract void trace(Object obj, Throwable throwable);

    abstract void debug(Object obj);

    abstract void debug(Object obj, Throwable throwable);

    abstract void info(Object obj);

    abstract void info(Object obj, Throwable throwable);

    abstract void warn(Object obj);

    abstract void warn(Object obj, Throwable throwable);

    abstract void error(Object obj);

    abstract void error(Object obj, Throwable throwable);

    abstract void fatal(Object obj);

    abstract void fatal(Object obj, Throwable throwable);
}
