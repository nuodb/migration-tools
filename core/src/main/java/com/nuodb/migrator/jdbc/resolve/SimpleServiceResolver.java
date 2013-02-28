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
package com.nuodb.migrator.jdbc.resolve;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.util.Map;
import java.util.WeakHashMap;

import static com.google.common.collect.Maps.newHashMap;
import static com.nuodb.migrator.utils.ReflectionUtils.newInstance;
import static java.lang.String.format;

/**
 * @author Sergey Bushik
 */
public class SimpleServiceResolver<T> implements ServiceResolver<T> {

    private final transient Logger logger = LoggerFactory.getLogger(getClass());

    private Map<DatabaseInfo, T> serviceCacheMap = new WeakHashMap<DatabaseInfo, T>();
    private T defaultService;
    private Map<DatabaseMatcher, T> serviceMap = newHashMap();
    private Class<? extends T> defaultServiceClass;
    private Map<DatabaseMatcher, Class<? extends T>> serviceClassMap = newHashMap();

    public SimpleServiceResolver() {
    }

    public SimpleServiceResolver(T defaultService) {
        this.defaultService = defaultService;
    }

    public SimpleServiceResolver(Class<? extends T> defaultServiceClass) {
        this.defaultServiceClass = defaultServiceClass;
    }

    @Override
    public void register(String productName, T service) {
        register(new DatabaseInfo(productName), service);
    }

    @Override
    public void register(DatabaseInfo databaseInfo, T service) {
        register(new SimpleDatabaseMatcher(databaseInfo), service);
    }

    @Override
    public void register(DatabaseMatcher databaseMatcher, T service) {
        serviceMap.put(databaseMatcher, service);
    }

    @Override
    public void register(String productName, Class<? extends T> serviceClass) {
        register(new DatabaseInfo(productName), serviceClass);
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
    public T resolve(Connection connection) throws SQLException {
        return resolve(connection.getMetaData());
    }

    @Override
    public T resolve(DatabaseInfo databaseInfo) {
        T service = serviceCacheMap.get(databaseInfo);
        if (service != null) {
            return service;
        }
        if ((service = resolveService(databaseInfo)) == null) {
            Class<? extends T> serviceClass = resolveServiceClass(databaseInfo);
            service = serviceClass != null ? createService(serviceClass, databaseInfo) : null;
        }
        if (service instanceof ServiceResolverAware) {
            ((ServiceResolverAware) service).setServiceResolver(this);
        }
        serviceCacheMap.put(databaseInfo, service);
        return service;
    }

    protected T resolveService(DatabaseInfo databaseInfo) {
        T service = null;
        for (Map.Entry<DatabaseMatcher, T> databaseInfoMatcherEntry : serviceMap.entrySet()) {
            DatabaseMatcher databaseMatcher = databaseInfoMatcherEntry.getKey();
            if (databaseMatcher.matches(databaseInfo)) {
                service = databaseInfoMatcherEntry.getValue();
                break;
            }
        }
        if (service != null) {
            if (logger.isDebugEnabled()) {
                logger.debug(format("Service instance resolved %s", getServiceName(service)));
            }
        } else if (defaultService != null) {
            if (logger.isDebugEnabled()) {
                logger.debug(format("Service instance defaulted to %s", getServiceName(defaultService)));
            }
            service = defaultService;
        }
        return service;
    }

    protected String getServiceName(T service) {
        return service.getClass().getName();
    }

    protected Class<? extends T> resolveServiceClass(DatabaseInfo databaseInfo) {
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
                logger.debug(format("Service class resolved %s", getServiceClassName(serviceClass)));
            }
        } else if (defaultServiceClass != null) {
            if (logger.isDebugEnabled()) {
                logger.debug(format("Service class defaulted to %s", getServiceClassName(defaultServiceClass)));
            }
            serviceClass = defaultServiceClass;
        }
        return serviceClass;
    }

    protected String getServiceClassName(Class<? extends T> serviceClass) {
        return serviceClass.getName();
    }

    @Override
    public T resolve(DatabaseMetaData databaseMetaData) throws SQLException {
        return resolve(new DatabaseInfo(databaseMetaData));
    }

    protected T createService(Class<? extends T> serviceClass, DatabaseInfo databaseInfo) {
        return newInstance(serviceClass);
    }
}