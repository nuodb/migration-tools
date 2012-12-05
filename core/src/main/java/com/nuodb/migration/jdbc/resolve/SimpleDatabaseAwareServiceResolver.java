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
package com.nuodb.migration.jdbc.resolve;

import com.google.common.collect.Maps;
import com.nuodb.migration.utils.ReflectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.util.Map;

import static java.lang.String.format;

/**
 * @author Sergey Bushik
 */
public class SimpleDatabaseAwareServiceResolver<T> implements DatabaseAwareServiceResolver<T> {

    private final transient Logger logger = LoggerFactory.getLogger(getClass());

    private Map<DatabaseMatcher, Class<? extends T>> databaseMatchers = Maps.newHashMap();

    private Class<? extends T> targetServiceClass;
    private Class<? extends T> defaultServiceClass;

    public SimpleDatabaseAwareServiceResolver(Class<? extends T> targetServiceClass) {
        this.targetServiceClass = targetServiceClass;
    }

    public SimpleDatabaseAwareServiceResolver(Class<? extends T> targetServiceClass,
                                              Class<? extends T> defaultServiceClass) {
        this.targetServiceClass = targetServiceClass;
        this.defaultServiceClass = defaultServiceClass;
    }

    public void registerService(String productName, Class<? extends T> serviceClass) {
        registerService(new SimpleDatabaseMatcher(productName), serviceClass);
    }

    public void registerService(String productName, String productVersion, Class<? extends T> serviceClass) {
        registerService(new SimpleDatabaseMatcher(productName, productVersion), serviceClass);
    }

    public void registerService(String productName, String productVersion, int majorVersion,
                                Class<? extends T> serviceClass) {
        registerService(new SimpleDatabaseMatcher(productName, productVersion, majorVersion), serviceClass);
    }

    public void registerService(String productName, String productVersion, int majorVersion, int minorVersion,
                                Class<? extends T> serviceClass) {
        registerService(new SimpleDatabaseMatcher(productName, productVersion, majorVersion, minorVersion),
                serviceClass);
    }

    public void registerService(DatabaseMatcher databaseMatcher, Class<? extends T> serviceClass) {
        databaseMatchers.put(databaseMatcher, serviceClass);
    }

    @Override
    public T resolveService(DatabaseMetaData metaData) throws SQLException {
        String productName = metaData.getDatabaseProductName();
        String productVersion = metaData.getDatabaseProductVersion();
        int minorVersion = metaData.getDatabaseMinorVersion();
        int majorVersion = metaData.getDatabaseMajorVersion();
        Class<? extends T> serviceClass = null;
        for (Map.Entry<DatabaseMatcher, Class<? extends T>> databaseInfoMatcherEntry : databaseMatchers.entrySet()) {
            DatabaseMatcher databaseMatcher = databaseInfoMatcherEntry.getKey();
            if (databaseMatcher.matches(productName, productVersion, minorVersion, majorVersion)) {
                serviceClass = databaseInfoMatcherEntry.getValue();
                break;
            }
        }
        if (serviceClass != null) {
            if (logger.isDebugEnabled()) {
                logger.debug(format("Resolved %s to %s", targetServiceClass.getName(), serviceClass.getName()));
            }
        } else if (defaultServiceClass != null) {
            if (logger.isWarnEnabled()) {
                logger.warn(format("Defaulted %s to %s", targetServiceClass.getName(), defaultServiceClass.getName()));
            }
            serviceClass = defaultServiceClass;
        }
        return serviceClass != null ? createObject(serviceClass, metaData) : null;
    }

    protected T createObject(Class<? extends T> serviceClass, DatabaseMetaData metaData) {
        return ReflectionUtils.newInstance(serviceClass);
    }
}