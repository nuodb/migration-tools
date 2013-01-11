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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.util.Map;

import static com.google.common.collect.Maps.newHashMap;
import static com.nuodb.migration.utils.ReflectionUtils.newInstance;
import static java.lang.String.format;

/**
 * @author Sergey Bushik
 */
public class SimpleDatabaseServiceResolver<T> implements DatabaseServiceResolver<T> {

    private final transient Logger logger = LoggerFactory.getLogger(getClass());

    private Map<DatabaseMatcher, Class<? extends T>> serviceClassMap = newHashMap();
    private Map<DatabaseInfo, T> serviceMap = newHashMap();
    private Class<? extends T> serviceClass;
    private Class<? extends T> defaultServiceClass;

    public SimpleDatabaseServiceResolver(Class<? extends T> serviceClass) {
        this.serviceClass = serviceClass;
    }

    public SimpleDatabaseServiceResolver(Class<? extends T> serviceClass,
                                         Class<? extends T> defaultServiceClass) {
        this.serviceClass = serviceClass;
        this.defaultServiceClass = defaultServiceClass;
    }

    @Override
    public void register(DatabaseInfo databaseInfo, Class<? extends T> serviceClass) {
        register(new SimpleDatabaseMatcher(databaseInfo), serviceClass);
    }

    @Override
    public void register(DatabaseMatcher databaseMatcher, Class<? extends T> serviceClass) {
        serviceClassMap.put(databaseMatcher, serviceClass);
    }

    @Override
    public void register(String productName, String productVersion, Class<? extends T> serviceClass) {
        register(new DatabaseInfo(productName, productVersion), serviceClass);
    }

    @Override
    public void register(String productName, String productVersion, Integer majorVersion,
                            Class<? extends T> serviceClass) {
        register(new DatabaseInfo(productName, productVersion, majorVersion), serviceClass);
    }

    @Override
    public void register(String productName, String productVersion, Integer majorVersion, Integer minorVersion,
                            Class<? extends T> serviceClass) {
        register(new DatabaseInfo(productName, productVersion, majorVersion, minorVersion), serviceClass);
    }

    @Override
    public void register(String productName, Class<? extends T> serviceClass) {
        register(new DatabaseInfo(productName), serviceClass);
    }

    @Override
    public T resolve(DatabaseInfo databaseInfo) {
        T service = serviceMap.get(databaseInfo);
        if (service != null) {
            return service;
        }
        Class<? extends T> serviceClass = null;
        for (Map.Entry<DatabaseMatcher, Class<? extends T>> databaseInfoMatcherEntry : serviceClassMap.entrySet()) {
            DatabaseMatcher databaseMatcher = databaseInfoMatcherEntry.getKey();
            if (databaseMatcher.matches(databaseInfo)) {
                serviceClass = databaseInfoMatcherEntry.getValue();
                break;
            }
        }
        if (serviceClass != null) {
            if (logger.isDebugEnabled()) {
                logger.debug(format("Resolved %s to %s", this.serviceClass.getName(), serviceClass.getName()));
            }
        } else if (defaultServiceClass != null) {
            if (logger.isWarnEnabled()) {
                logger.warn(format("Defaulted %s to %s", this.serviceClass.getName(), defaultServiceClass.getName()));
            }
            serviceClass = defaultServiceClass;
        }
        service = serviceClass != null ? createService(serviceClass, databaseInfo) : null;
        if (service instanceof DatabaseServiceResolverAware) {
            ((DatabaseServiceResolverAware) service).setDatabaseServiceResolver(this);
        }
        serviceMap.put(databaseInfo, service);
        return service;
    }

    @Override
    public T resolve(DatabaseMetaData databaseMetaData) throws SQLException {
        return resolve(new DatabaseInfo(databaseMetaData));
    }

    protected T createService(Class<? extends T> serviceClass, DatabaseInfo databaseInfo) {
        return newInstance(serviceClass);
    }
}