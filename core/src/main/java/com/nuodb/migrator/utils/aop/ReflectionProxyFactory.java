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
package com.nuodb.migrator.utils.aop;

import java.util.Collection;

import static com.google.common.collect.Lists.newArrayList;
import static com.nuodb.migrator.utils.ReflectionUtils.getClassLoader;
import static com.nuodb.migrator.utils.ReflectionUtils.getInterfaces;
import static java.lang.reflect.Proxy.newProxyInstance;

/**
 * @author Sergey Bushik
 */
public class ReflectionProxyFactory implements AopProxyFactory {

    private Object target;
    private ClassLoader classLoader;
    private Class[] interfaces;

    public ReflectionProxyFactory(Object target) {
        this(target, getInterfaces(target.getClass()));
    }

    public ReflectionProxyFactory(Object target, Class... interfaces) {
        this(target, getClassLoader(), interfaces);
    }

    public ReflectionProxyFactory(Object target, ClassLoader classLoader, Class... interfaces) {
        this.target = target;
        this.classLoader = classLoader;
        this.interfaces = interfaces;
    }

    @Override
    public AopProxy createAopProxy() {
        Collection<Class> interfaces = newArrayList(this.interfaces);
        interfaces.add(AopProxy.class);
        return (AopProxy) newProxyInstance(classLoader, interfaces.toArray(new Class[interfaces.size()]),
                new ReflectionProxy(target));
    }
}
