package com.nuodb.tools.migration.utils;

import com.nuodb.tools.migration.MigrationException;

@SuppressWarnings("unchecked")
public class ClassUtils {

    private ClassUtils() {
    }

    public static ClassLoader getClassLoader() {
        ClassLoader loader = Thread.currentThread().getContextClassLoader();
        if (loader == null) {
            loader = ClassUtils.class.getClassLoader();
        }
        return loader;
    }

    public static <T> T newInstance(String className) {
        try {
            return (T) newInstance(getClassLoader().loadClass(className));
        } catch (ClassNotFoundException e) {
            throw new MigrationException("Class is not found " + className);
        }
    }

    public static <T> T newInstance(Class<T> type) {
        try {
            return type.newInstance();
        } catch (InstantiationException e) {
            throw new MigrationException("Failed instantiating class " + type);
        } catch (IllegalAccessException e) {
            throw new MigrationException("Failed instantiating class " + type);
        }
    }
}
