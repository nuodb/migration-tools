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
package com.nuodb.migrator.jdbc.session;

import com.google.common.collect.Maps;
import com.nuodb.migrator.MigratorException;
import org.slf4j.Logger;

import java.util.Collection;
import java.util.List;
import java.util.Map;

import static com.google.common.collect.Iterables.get;
import static com.google.common.collect.Lists.newCopyOnWriteArrayList;
import static com.nuodb.migrator.jdbc.JdbcUtils.closeQuietly;
import static com.nuodb.migrator.utils.Collections.isEmpty;
import static java.lang.String.format;
import static java.util.Collections.synchronizedMap;
import static org.slf4j.LoggerFactory.getLogger;

/**
 * @author Sergey Bushik
 */
@SuppressWarnings("ThrowableResultOfMethodCallIgnored")
public class SimpleWorkManager<L extends WorkListener> implements WorkManager<L> {

    public static final boolean THROW_FAILURE_ON_CLOSE = true;
    protected final transient Logger logger = getLogger(getClass());
    private boolean throwFailureOnClose = THROW_FAILURE_ON_CLOSE;
    private Map<Work, Throwable> failures = synchronizedMap(Maps.<Work, Throwable>newLinkedHashMap());
    private List<L> listeners = newCopyOnWriteArrayList();

    @Override
    public boolean hasListeners() {
        return !listeners.isEmpty();
    }

    @Override
    public Collection<L> getListeners() {
        return listeners;
    }

    @Override
    public void addListener(L listener) {
        listeners.add(listener);
    }

    @Override
    public void addListener(int index, L listener) {
        listeners.add(index, listener);
    }

    @Override
    public void removeListener(L listener) {
        listeners.remove(listener);
    }

    @Override
    public void execute(Work work, Session session) {
        try {
            init(work, session);
            execute(work);
        } catch (Exception exception) {
            failure(work, exception);
        } finally {
            try {
                close(work);
            } catch (Exception exception) {
                failure(work, exception);
            }
        }
    }

    @Override
    public void execute(Work work, SessionFactory sessionFactory) {
        Session session = null;
        try {
            session = sessionFactory.openSession();
            execute(work, session);
        } catch (Exception exception) {
            failure(work, exception);
        } finally {
            closeQuietly(session);
        }
    }

    protected void init(Work work, Session session) throws Exception {
        if (logger.isTraceEnabled()) {
            logger.trace(format("%s work is being initiated", work.getName()));
        }
        work.init(session);
    }

    protected void execute(Work work) throws Exception {
        if (logger.isTraceEnabled()) {
            logger.trace(format("%s work is being executed", work.getName()));
        }
        onExecuteStart(work);
        work.execute();
        onExecuteEnd(work);
    }

    protected void onExecuteStart(Work work) {
        if (hasListeners()) {
            WorkEvent event = createWorkEvent(work);
            for (L listener : getListeners()) {
                listener.onExecuteStart(event);
            }
        }
    }

    protected void onExecuteEnd(Work work) {
        if (hasListeners()) {
            WorkEvent event = createWorkEvent(work);
            for (L listener : getListeners()) {
                listener.onExecuteEnd(event);
            }
        }
    }

    protected WorkEvent createWorkEvent(Work work) {
        return new WorkEvent(work);
    }

    protected void failure(Work work, Throwable failure) {
        if (logger.isWarnEnabled()) {
            logger.warn(format("%s work failed with error %s", work.getName(), failure.getMessage()));
        }
        failures.put(work, failure);
        onFailure(work, failure);
    }

    protected void onFailure(Work work, Throwable failure) {
        if (!isEmpty(listeners)) {
            WorkEvent event = new WorkEvent(work, failure);
            for (L listener : listeners) {
                listener.onFailure(event);
            }
        }
    }

    protected void close(Work work) throws Exception {
        if (logger.isTraceEnabled()) {
            logger.trace(format("%s work is being closed", work.getName()));
        }
        work.close();
    }

    @Override
    public void close() throws Exception {
        Map<Work, Throwable> failures = getFailures();
        if (isThrowFailureOnClose() && !isEmpty(failures)) {
            final Throwable failure = get(failures.values(), 0);
            throw failure instanceof MigratorException ? (MigratorException) failure : new WorkException(failure);
        }
    }

    @Override
    public Map<Work, Throwable> getFailures() {
        return failures;
    }

    public boolean isThrowFailureOnClose() {
        return throwFailureOnClose;
    }

    public void setThrowFailureOnClose(boolean throwFailureOnClose) {
        this.throwFailureOnClose = throwFailureOnClose;
    }
}
