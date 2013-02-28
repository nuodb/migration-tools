/**
 * Copyright (c) 2012, NuoDB, Inc.
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
import org.apache.commons.lang3.reflect.ConstructorUtils;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

@SuppressWarnings("unchecked")
public class ReflectionUtils {

    private ReflectionUtils() {
    }

    public static ClassLoader getClassLoader() {
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        if (classLoader == null) {
            classLoader = ReflectionUtils.class.getClassLoader();
        }
        return classLoader;
    }

    public static <T> Class<T> loadClass(String className) {
        try {
            return (Class<T>) getClassLoader().loadClass(className);
        } catch (ClassNotFoundException exception) {
            throw new ReflectionException(exception);
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
            return type.newInstance();
        } catch (InstantiationException exception) {
            throw new ReflectionException("Failed creating instance of " + type, exception);
        } catch (IllegalAccessException exception) {
            throw new ReflectionException("Failed creating instance of " + type, exception);
        }
    }

    public static <T> T newInstance(Class<T> type, Object argument) {
        return newInstance(type, new Object[]{argument});
    }

    public static <T> T newInstance(Class<T> type, Object[] arguments) {
        if (arguments == null) {
            arguments = ArrayUtils.EMPTY_OBJECT_ARRAY;
        }
        Class argumentTypes[] = new Class[arguments.length];
        for (int i = 0; i < arguments.length; i++) {
            argumentTypes[i] = arguments[i].getClass();
        }
        return newInstance(type, arguments, argumentTypes);
    }

    public static <T> T newInstance(Class<T> type, Object[] arguments, Class[] argumentTypes) {
        try {
            return ConstructorUtils.invokeConstructor(type, arguments, argumentTypes);
        } catch (NoSuchMethodException exception) {
            throw new ReflectionException(exception);
        } catch (IllegalAccessException exception) {
            throw new ReflectionException(exception);
        } catch (InvocationTargetException exception) {
            throw new ReflectionException(exception.getTargetException());
        } catch (InstantiationException exception) {
            throw new ReflectionException(exception);
        }
    }

    public static <T> T invokeMethod(Object object, Method method, Object... args) {
        try {
            if (args == null) {
                args = ArrayUtils.EMPTY_OBJECT_ARRAY;
            }
            method.setAccessible(true);
            return (T) method.invoke(object, args);
        } catch (IllegalAccessException exception) {
            throw new ReflectionException(exception);
        } catch (InvocationTargetException exception) {
            throw new ReflectionException(exception.getTargetException());
        }
    }
}
