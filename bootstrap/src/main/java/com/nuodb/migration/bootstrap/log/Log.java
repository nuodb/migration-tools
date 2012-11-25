package com.nuodb.migration.bootstrap.log;

public interface Log {
    boolean isDebugEnabled();

    boolean isErrorEnabled();

    boolean isFatalEnabled();

    boolean isInfoEnabled();

    boolean isTraceEnabled();

    boolean isWarnEnabled();

    void trace(Object obj);

    void trace(Object obj, Throwable throwable);

    void debug(Object obj);

    void debug(Object obj, Throwable throwable);

    void info(Object obj);

    void info(Object obj, Throwable throwable);

    void warn(Object obj);

    void warn(Object obj, Throwable throwable);

    void error(Object obj);

    void error(Object obj, Throwable throwable);

    void fatal(Object obj);

    void fatal(Object obj, Throwable throwable);
}
