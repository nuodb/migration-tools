package com.nuodb.migration.bootstrap.log;

public class LogFactory {

    public static Log getLog(Class clazz) {
        return getLog(clazz.getName());
    }

    public static Log getLog(String name) {
        return new JdkLog(name);
    }
}
