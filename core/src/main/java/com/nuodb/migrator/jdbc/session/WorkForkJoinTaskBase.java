/**
 * Copyright (c) 2014, NuoDB, Inc.
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

import com.nuodb.migrator.utils.concurrent.ForkJoinTask;

import static com.nuodb.migrator.jdbc.JdbcUtils.closeQuietly;

/**
 * @author Sergey Bushik
 */
public abstract class WorkForkJoinTaskBase<V> extends ForkJoinTask<V> implements Work {

    private WorkManager workManager;
    private SessionFactory sessionFactory;
    private Session session;
    private boolean closeSession;
    private V rawResult;

    public WorkForkJoinTaskBase(WorkManager workManager, SessionFactory sessionFactory) {
        this.workManager = workManager;
        this.sessionFactory = sessionFactory;
        this.closeSession = true;
    }

    public WorkForkJoinTaskBase(WorkManager workManager, Session session) {
        this.workManager = workManager;
        this.session = session;
        this.closeSession = false;
    }

    @Override
    public void init(Session session) throws Exception {
        this.session = session;
        init();
    }

    protected void init() throws Exception {
    }

    @Override
    protected boolean exec() {
        if (sessionFactory != null) {
            workManager.execute(this, sessionFactory);
        } else {
            workManager.execute(this, session);
        }
        Throwable failure = (Throwable) workManager.getFailures().get(this);
        return failure == null;
    }

    @Override
    public void close() throws Exception {
        if (closeSession) {
            closeQuietly(session);
        }
    }

    protected Session getSession() {
        return session;
    }

    @Override
    public V getRawResult() {
        return rawResult;
    }

    @Override
    protected void setRawResult(V rawResult) {
        this.rawResult = rawResult;
    }
}
