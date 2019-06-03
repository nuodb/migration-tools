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
package com.nuodb.migrator.context;

import com.nuodb.migrator.utils.ReflectionException;

import java.util.Collection;

import static com.google.common.collect.Iterables.isEmpty;
import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Sets.newLinkedHashSet;
import static com.nuodb.migrator.utils.ReflectionUtils.loadClass;
import static com.nuodb.migrator.utils.ReflectionUtils.newInstance;
import static java.lang.String.format;

/**
 * @author Sergey Bushik
 */
@SuppressWarnings("unchecked")
public abstract class ContextBase implements Context {

    private Collection<Object> services = newLinkedHashSet();
    private Collection<Class<?>> serviceTypes = newLinkedHashSet();

    @Override
    public <T> T createService(Class<T> serviceType) {
        T candidate;
        Collection<T> candidates = newArrayList();
        for (Object service : services) {
            if (serviceType.isAssignableFrom(service.getClass())) {
                candidates.add((T) service);
            }
        }
        candidate = getService(serviceType, candidates);
        if (candidate == null) {
            Collection<Class<? extends T>> serviceTypes = newArrayList();
            for (Class<?> service : this.serviceTypes) {
                if (serviceType.isAssignableFrom(service)) {
                    serviceTypes.add((Class<? extends T>) service);
                }
            }
            candidate = createService(serviceType, serviceTypes);
        }
        if (candidate == null) {
            candidate = initiateService(serviceType);
        }
        if (candidate == null) {
            throw new ContextException(format("Service of %s type is not registered in this context", serviceType));
        }
        return candidate;
    }

    protected <T> T initiateService(Class<T> serviceType) {
        ServiceInitiator<T> serviceInitiator = getServiceInitiator(serviceType);
        return serviceInitiator != null ? serviceInitiator.initiateService(this) : null;
    }

    protected <T> ServiceInitiator<T> getServiceInitiator(Class<T> serviceType) {
        Class<T> serviceInitiatorType;
        try {
            serviceInitiatorType = loadClass(serviceType.getName() + "Initiator");
        } catch (ReflectionException exception) {
            serviceInitiatorType = null;
        }
        if (serviceInitiatorType != null && ServiceInitiator.class.isAssignableFrom(serviceInitiatorType)) {
            return (ServiceInitiator<T>) newInstance(serviceInitiatorType);
        } else {
            return null;
        }
    }

    @Override
    public void addService(Object service) {
        services.add(service);
    }

    @Override
    public void addServiceType(Class<?> serviceType) {
        serviceTypes.add(serviceType);
    }

    protected <T> T getService(Class<T> serviceType, Collection<T> services) {
        return !isEmpty(services) ? services.iterator().next() : null;
    }

    protected <T> T createService(Class<T> serviceType, Collection<Class<? extends T>> serviceTypes) {
        return !isEmpty(serviceTypes) ? newInstance(serviceTypes.iterator().next()) : null;
    }
}
