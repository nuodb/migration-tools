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

import org.aopalliance.intercept.MethodInterceptor;
import org.aopalliance.intercept.MethodInvocation;

import java.lang.reflect.Method;
import java.util.Collection;
import java.util.Map;

import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Maps.newHashMap;
import static com.nuodb.migrator.utils.ReflectionUtils.invokeMethodNoWrap;

/**
 * @author Sergey Bushik
 */
@SuppressWarnings("unchecked")
public class MethodInterceptors {

    /**
     * Wraps before method advice and creates method interceptor
     *
     * @param advice
     *            before method advice
     * @return before method advice adapter
     */
    public static MethodInterceptor newBeforeMethodInterceptor(final BeforeMethod advice) {
        return new MethodInterceptor() {
            @Override
            public Object invoke(MethodInvocation invocation) throws Throwable {
                advice.beforeMethod(invocation.getMethod(), invocation.getArguments(), invocation.getThis());
                return invocation.proceed();
            }
        };
    }

    /**
     * Wraps after method advice and creates method interceptor
     *
     * @param advice
     *            after method advice
     * @return after method advice adapter
     */
    public static MethodInterceptor newAfterMethodInterceptor(final AfterMethod advice) {
        return new MethodInterceptor() {
            @Override
            public Object invoke(MethodInvocation invocation) throws Throwable {
                Object returnValue = invocation.proceed();
                advice.afterMethod(returnValue, invocation.getMethod(), invocation.getArguments(),
                        invocation.getThis());
                return returnValue;
            }
        };
    }

    /**
     * Wraps after throws advice and creates method interceptor
     *
     * @param advice
     *            after throws advice
     * @return after method advice adapter
     */
    public static MethodInterceptor newAfterThrowsInterceptor(final AfterThrows advice) {
        return new MethodInterceptor() {
            @Override
            public Object invoke(MethodInvocation invocation) throws Throwable {
                try {
                    return invocation.proceed();
                } catch (Throwable exception) {
                    advice.afterThrows(invocation.getMethod(), invocation.getArguments(), invocation.getThis(),
                            exception);
                    throw exception;
                }
            }
        };
    }

    /**
     * Introduce requested interfaces by performing invocations on the delegate
     * object
     *
     * @param delegate
     *            target for invocations
     * @param interfaces
     *            array of interfaces to be introduced by returned advice
     * @return method interceptor, which introduces interfaces
     */
    public static MethodInterceptor newIntroduceInterfacesInterceptor(final Object delegate,
            final Class... interfaces) {
        return new MethodInterceptor() {

            private Collection<Class> introducedInterfaces = newArrayList(interfaces);
            private Map<Method, Boolean> introducedMethods = newHashMap();

            private boolean implementsInterface(Class sourceInterface) {
                for (Class introducedInterface : introducedInterfaces) {
                    if (sourceInterface.isInterface() && sourceInterface.isAssignableFrom(introducedInterface)) {
                        return true;
                    }
                }
                return false;
            }

            private boolean introducesMethod(MethodInvocation invocation) {
                Boolean introducesMethod = introducedMethods.get(invocation.getMethod());
                if (introducesMethod != null) {
                    return introducesMethod;
                } else {
                    boolean implementsInterface = implementsInterface(invocation.getMethod().getDeclaringClass());
                    introducedMethods.put(invocation.getMethod(), implementsInterface);
                    return implementsInterface;
                }
            }

            @Override
            public Object invoke(MethodInvocation invocation) throws Throwable {
                if (introducesMethod(invocation)) {
                    Object returnValue = invokeMethodNoWrap(delegate, invocation.getMethod(),
                            invocation.getArguments());
                    if (returnValue == delegate && invocation instanceof AopProxyMethodInvocation) {
                        AopProxy aopProxy = ((AopProxyMethodInvocation) invocation).getAopProxy();
                        if (invocation.getMethod().getReturnType().isInstance(aopProxy)) {
                            returnValue = aopProxy;
                        }
                    }
                    return returnValue;
                }
                return invocation.proceed();
            }
        };
    }
}
