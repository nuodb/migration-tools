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
package com.nuodb.migrator.jdbc.metadata.resolver;

import com.google.common.collect.Maps;
import com.nuodb.migrator.jdbc.metadata.DatabaseInfo;

import java.util.Map;

import static com.google.common.collect.Maps.newConcurrentMap;

/**
 * @author Sergey Bushik
 */
public class SimpleCachingServiceResolver<T> extends SimpleServiceResolver<T> {

    private Map<DatabaseInfo, T> serviceMap;

    public SimpleCachingServiceResolver() {
        serviceMap = newConcurrentMap();
    }

    public SimpleCachingServiceResolver(Map<DatabaseInfo, T> serviceMap) {
        this.serviceMap = serviceMap;
    }

    public SimpleCachingServiceResolver(T defaultService) {
        this(defaultService, Maps.<DatabaseInfo, T>newConcurrentMap());
    }

    public SimpleCachingServiceResolver(T defaultService, Map<DatabaseInfo, T> serviceMap) {
        super(defaultService);
        this.serviceMap = serviceMap;
    }

    public SimpleCachingServiceResolver(Class<? extends T> defaultServiceClass) {
        this(defaultServiceClass, Maps.<DatabaseInfo, T>newConcurrentMap());
    }

    public SimpleCachingServiceResolver(Class<? extends T> defaultServiceClass, Map<DatabaseInfo, T> serviceMap) {
        super(defaultServiceClass);
        this.serviceMap = serviceMap;
    }

    @Override
    public T resolve(DatabaseInfo databaseInfo) {
        T service = serviceMap.get(databaseInfo);
        if (service != null) {
            return service;
        }
        service = super.resolve(databaseInfo);
        if (service != null) {
            serviceMap.put(databaseInfo, service);
        }
        return service;
    }

    public Map<DatabaseInfo, T> getServiceMap() {
        return serviceMap;
    }

    public void setServiceMap(Map<DatabaseInfo, T> serviceMap) {
        this.serviceMap = serviceMap;
    }
}
