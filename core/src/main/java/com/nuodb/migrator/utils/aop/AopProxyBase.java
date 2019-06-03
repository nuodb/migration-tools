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

import java.util.Iterator;
import java.util.List;

import static com.google.common.collect.Lists.newArrayList;
import static com.nuodb.migrator.utils.aop.MethodAdvisors.newMethodAdvisor;
import static java.lang.String.format;

/**
 * @author Sergey Bushik
 */
public abstract class AopProxyBase implements AopProxy {

    private static final Advisor[] EMPTY_ADVISORS = new Advisor[0];

    private Advisor[] advisors = EMPTY_ADVISORS;
    private List<Advisor> advisorsList = newArrayList();

    private final Object target;
    private final Class targetClass;
    private final Class[] interfaces;

    public AopProxyBase(Object target, Class[] interfaces) {
        this.target = target;
        this.targetClass = target != null ? target.getClass() : null;
        this.interfaces = interfaces;
    }

    @Override
    public Object getTarget() {
        return target;
    }

    @Override
    public Class getTargetClass() {
        return targetClass;
    }

    @Override
    public Class[] getInterfaces() {
        return interfaces;
    }

    @Override
    public void addAdvice(Advice advice) {
        addAdvice(advisorsList.size(), advice);
    }

    @Override
    public void addAdvice(int index, Advice advice) {
        addAdvisor(index, newMethodAdvisor(advice));
    }

    @Override
    public void addAdvisor(Advisor advisor) {
        addAdvisor(advisorsList.size(), advisor);
    }

    @Override
    public void addAdvisor(int index, Advisor advisor) {
        if (index > advisorsList.size()) {
            throw new IllegalArgumentException(
                    format("Illegal position %d in advises list with size %d", index, advisorsList.size()));
        }
        if (supportsAdvice(advisor.getAdvice())) {
            advisorsList.add(index, advisor);
        } else {
            throw new AopProxyException(format("Can't add advice to the list, advice is not supported %s", advisor));
        }
        updateAdvisors();
    }

    protected abstract boolean supportsAdvice(Advice advice);

    @Override
    public boolean removeAdvice(Advice advice) {
        boolean updated = false;
        for (Iterator<Advisor> iterator = advisorsList.iterator(); iterator.hasNext();) {
            Advisor advisor = iterator.next();
            if (advisor.getAdvice().equals(advice)) {
                iterator.remove();
                updated = true;
            }
        }
        if (updated) {
            updateAdvisors();
        }
        return updated;
    }

    protected void updateAdvisors() {
        advisors = advisorsList.isEmpty() ? EMPTY_ADVISORS : advisorsList.toArray(new Advisor[advisorsList.size()]);
    }

    @Override
    public boolean removeAdvisor(Advisor advisor) {
        boolean updated = advisorsList.remove(advisor);
        if (updated) {
            updateAdvisors();
        }
        return updated;
    }

    @Override
    public Advisor[] getAdvisors() {
        return advisors;
    }
}
