package com.nuodb.migration.bootstrap.log;

public class LogFactory {

    public static Log getLog(Class type) {
        return getLog(type.getName());
    }

    public static Log getLog(String name) {
        return new JdkLog(name);
    }
}
