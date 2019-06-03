/**
 * Copyright (c) 2015, NuoDB, Inc.
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *     * Redistributions of source code must retain the above copyright
 *       notice, this list of conditions and the following disclaimer.
 *     * Redistributions in binary form must reproduce the above copyright
 *       notice, this list of conditions and the following disclaimer in the
 *       documentation and/or other materials provided with the distribution.
 *     * Neither the name of NuoDB, Inc. nor the names of its contributors may
 *       be used to endorse or promote products derived from this software
 *       without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED. IN NO EVENT SHALL NUODB, INC. BE LIABLE FOR ANY DIRECT, INDIRECT,
 * INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA,
 * OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
 * LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE
 * OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF
 * ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */
package com.nuodb.migrator.utils;

import org.apache.commons.lang3.ArrayUtils;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Collection;

import static com.google.common.collect.Lists.newArrayList;
import static java.lang.String.format;
import static java.lang.reflect.Modifier.isStatic;
import static java.util.Arrays.asList;
import static org.apache.commons.lang3.ArrayUtils.EMPTY_OBJECT_ARRAY;
import static org.apache.commons.lang3.reflect.ConstructorUtils.invokeConstructor;

@SuppressWarnings("unchecked")
public class ReflectionUtils {

    private static final char PACKAGE_SEPARATOR = '.';

    private static final char INNER_CLASS_SEPARATOR = '$';

    public static final String CGLIB_CLASS_SEPARATOR = "$$";

    private ReflectionUtils() {
    }

    public static String getClassName(Class type) {
        return type.getName();
    }

    public static String getShortClassName(Class type) {
        String name = getClassName(type);
        int lastDotIndex = name.lastIndexOf(PACKAGE_SEPARATOR);
        int nameEndIndex = name.indexOf(CGLIB_CLASS_SEPARATOR);
        if (nameEndIndex == -1) {
            nameEndIndex = name.length();
        }
        String shortName = name.substring(lastDotIndex + 1, nameEndIndex);
        shortName = shortName.replace(INNER_CLASS_SEPARATOR, PACKAGE_SEPARATOR);
        return shortName;
    }

    public static ClassLoader getClassLoader() {
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        if (classLoader == null) {
            classLoader = ReflectionUtils.class.getClassLoader();
        }
        return classLoader;
    }

    public static <T> Class<T> loadClassNoWrap(String className) throws Throwable {
        return (Class<T>) getClassLoader().loadClass(className);
    }

    public static <T> Class<T> loadClass(String className) {
        try {
            return loadClassNoWrap(className);
        } catch (Throwable exception) {
            throw new ReflectionException(exception);
        }
    }

    public static <T> T newInstanceNoWrap(Class<T> type) throws Throwable {
        return type.newInstance();
    }

    public static <T> T newInstanceNoWrap(Class<T> type, Object[] arguments) throws Throwable {
        if (arguments == null) {
            arguments = ArrayUtils.EMPTY_OBJECT_ARRAY;
        }
        Class argumentTypes[] = new Class[arguments.length];
        for (int i = 0; i < arguments.length; i++) {
            argumentTypes[i] = arguments[i].getClass();
        }
        return newInstanceNoWrap(type, arguments, argumentTypes);
    }

    public static <T> T newInstanceNoWrap(Class<T> type, Object[] arguments, Class[] argumentTypes) throws Throwable {
        try {
            return invokeConstructor(type, arguments, argumentTypes);
        } catch (InvocationTargetException cause) {
            throw cause.getTargetException();
        }
    }

    public static <T> T newInstance(String className) {
        try {
            return (T) newInstance(getClassLoader().loadClass(className));
        } catch (ClassNotFoundException exception) {
            throw new ReflectionException("Class is not found " + className, exception);
        }
    }

    public static <T> T newInstance(Class<T> type) {
        try {
            return newInstanceNoWrap(type);
        } catch (Throwable exception) {
            throw new ReflectionException("Failed creating instance of " + type, exception);
        }
    }

    public static <T> T newInstance(Class<T> type, Object argument) {
        return newInstance(type, new Object[] { argument });
    }

    public static <T> T newInstance(Class<T> type, Object[] arguments) {
        if (arguments == null) {
            arguments = EMPTY_OBJECT_ARRAY;
        }
        Class argumentTypes[] = new Class[arguments.length];
        for (int i = 0; i < arguments.length; i++) {
            argumentTypes[i] = arguments[i].getClass();
        }
        return newInstance(type, arguments, argumentTypes);
    }

    public static <T> T newInstance(Class<T> type, Object[] arguments, Class[] argumentTypes) {
        try {
            return newInstanceNoWrap(type, arguments, argumentTypes);
        } catch (Throwable cause) {
            throw new ReflectionException(cause);
        }
    }

    public static <T> T invokeMethodNoWrap(Object object, Method method, Object... arguments) throws Throwable {
        try {
            if (arguments == null) {
                arguments = EMPTY_OBJECT_ARRAY;
            }
            method.setAccessible(true);
            return (T) method.invoke(object, arguments);
        } catch (InvocationTargetException cause) {
            throw cause.getCause();
        }
    }

    public static <T> T invokeMethod(Object object, Method method, Object... arguments) {
        try {
            return invokeMethodNoWrap(object, method, arguments);
        } catch (Throwable cause) {
            throw wrap(cause, method, object);
        }
    }

    private static ReflectionException wrap(Throwable cause, Method method, Object object) {
        return isStatic(method.getModifiers())
                ? new ReflectionException(format("Failed to invoke static %s method", method), cause)
                : new ReflectionException(
                        format("Failed to invoke %s method on object of %s class", method, object.getClass().getName()),
                        cause);
    }

    public static Method getMethodNoWrap(Class type, String name, Class[] argumentTypes) throws Exception {
        if (argumentTypes == null) {
            argumentTypes = ArrayUtils.EMPTY_CLASS_ARRAY;
        }
        return type.getMethod(name, argumentTypes);
    }

    public static Method getMethod(Class type, String name, Class... argumentTypes) {
        try {
            return getMethodNoWrap(type, name, argumentTypes);
        } catch (Throwable cause) {
            throw new ReflectionException(cause);
        }
    }

    public static Class[] getInterfaces(Class type) {
        Collection<Class> interfaces = newArrayList();
        while (type != null) {
            if (type.isInterface()) {
                interfaces.add(type);
            }
            interfaces.addAll(asList(type.getInterfaces()));
            type = type.getSuperclass();
        }
        return interfaces.toArray(new Class[interfaces.size()]);
    }
}
