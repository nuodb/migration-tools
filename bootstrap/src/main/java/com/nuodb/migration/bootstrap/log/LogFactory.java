package com.nuodb.migration.bootstrap.log;

public class LogFactory {

    public static Log getLogger(Class clazz) {
        return getLogger(clazz.getName());
    }

    public static Log getLogger(String name) {
        return new JdkLog(name);
    }
}
