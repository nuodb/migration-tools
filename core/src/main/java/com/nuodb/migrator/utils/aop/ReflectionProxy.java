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

import org.aopalliance.aop.Advice;
import org.aopalliance.intercept.MethodInterceptor;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;

import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Maps.newHashMap;
import static com.nuodb.migrator.utils.ReflectionUtils.invokeMethodNoWrap;
import static com.nuodb.migrator.utils.aop.MethodInterceptors.*;

/**
 * @author Sergey Bushik
 */
public class ReflectionProxy extends AopProxyBase implements InvocationHandler {

    private Map<Method, List<MethodInterceptor>> methodInterceptorMap = newHashMap();
    private MethodInterceptor introduceInterfacesInterceptor;

    public ReflectionProxy(Object target, Class... interfaces) {
        super(target, interfaces);
        this.introduceInterfacesInterceptor = newIntroduceInterfacesInterceptor(this, AopProxy.class);
    }

    @Override
    protected boolean supportsAdvice(Advice advice) {
        return advice instanceof BeforeMethod || advice instanceof AfterMethod || advice instanceof AfterThrows
                || advice instanceof MethodInterceptor;
    }

    @Override
    public Object invoke(Object proxy, final Method method, final Object[] arguments) throws Throwable {
        final Object target = getTarget();
        final List<MethodInterceptor> methodInterceptors = getMethodInterceptors(method, arguments);
        if (methodInterceptors.isEmpty()) {
            return invokeMethodNoWrap(target, method, arguments);
        } else {
            Object returnValue = new AopProxyMethodInvocationBase(this, method, arguments) {

                private int methodInterceptorIndex;

                @Override
                public Object proceed() throws Throwable {
                    if (methodInterceptorIndex == methodInterceptors.size()) {
                        return invokeMethodNoWrap(target, method, arguments);
                    } else {
                        return methodInterceptors.get(methodInterceptorIndex++).invoke(this);
                    }
                }
            }.proceed();
            Class<?> returnType = method.getReturnType();
            if (returnValue == null && returnType != Void.TYPE && returnType.isPrimitive()) {
                throw new AopProxyException("Null return value does not match primitive return type for: " + method);
            }
            return returnValue;
        }
    }

    @Override
    protected void updateAdvisors() {
        super.updateAdvisors();
        methodInterceptorMap.clear();
    }

    protected List<MethodInterceptor> getMethodInterceptors(Method method, Object[] arguments) {
        List<MethodInterceptor> methodInterceptors = methodInterceptorMap.get(method);
        if (methodInterceptors == null) {
            methodInterceptors = newArrayList();
            for (Advisor advisor : getAdvisors()) {
                if (!(advisor instanceof MethodAdvisor)
                        || ((MethodAdvisor) advisor).getMethodMatcher().matches(method, getTargetClass(), arguments)) {
                    Advice advice = advisor.getAdvice();
                    if (advice instanceof BeforeMethod) {
                        methodInterceptors.add(newBeforeMethodInterceptor((BeforeMethod) advice));
                    } else if (advice instanceof AfterMethod) {
                        methodInterceptors.add(newAfterMethodInterceptor((AfterMethod) advice));
                    } else if (advice instanceof AfterThrows) {
                        methodInterceptors.add(newAfterThrowsInterceptor((AfterThrows) advice));
                    } else if (advice instanceof MethodInterceptor) {
                        methodInterceptors.add((MethodInterceptor) advice);
                    }
                }
            }
            methodInterceptors.add(introduceInterfacesInterceptor);
            methodInterceptorMap.put(method, methodInterceptors);
        }
        return methodInterceptors;
    }
}
