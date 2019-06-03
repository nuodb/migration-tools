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

import com.nuodb.migrator.jdbc.connection.ConnectionProxy;
import com.nuodb.migrator.jdbc.dialect.Dialect;
import com.nuodb.migrator.jdbc.metadata.DatabaseInfo;
import com.nuodb.migrator.spec.ConnectionSpec;
import org.slf4j.Logger;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Map;
import java.util.Set;

import static com.google.common.collect.Maps.newHashMap;
import static java.lang.String.format;
import static org.slf4j.LoggerFactory.getLogger;

/**
 * @author Sergey Bushik
 */
@SuppressWarnings({ "NullableProblems", "unchecked" })
public class SessionBase implements Session {

    private SessionFactoryBase sessionFactoryBase;
    private Connection connection;
    private Dialect dialect;
    private Map context;
    private boolean enforceTableLocksForDDL;

    public SessionBase(SessionFactoryBase sessionFactoryBase, Connection connection, Dialect dialect,
            boolean enforceTableLocksForDDL) {
        this(sessionFactoryBase, connection, dialect, null, enforceTableLocksForDDL);
    }

    public SessionBase(SessionFactoryBase sessionFactoryBase, Connection connection, Dialect dialect, Map context,
            boolean enforceTableLocksForDDL) {
        this.sessionFactoryBase = sessionFactoryBase;
        this.connection = connection;
        this.dialect = dialect;
        this.context = context == null ? newHashMap() : context;
        this.enforceTableLocksForDDL = enforceTableLocksForDDL;
    }

    @Override
    public int size() {
        return context.size();
    }

    @Override
    public boolean isEmpty() {
        return context.isEmpty();
    }

    @Override
    public boolean containsKey(Object key) {
        return context.containsKey(key);
    }

    @Override
    public boolean containsValue(Object value) {
        return context.containsValue(value);
    }

    @Override
    public Object get(Object key) {
        return context.get(key);
    }

    @Override
    public Object put(Object key, Object value) {
        return context.put(key, value);
    }

    @Override
    public Object remove(Object key) {
        return context.remove(key);
    }

    @Override
    public void putAll(Map m) {
        context.putAll(m);
    }

    @Override
    public void clear() {
        context.clear();
    }

    @Override
    public Set keySet() {
        return context.keySet();
    }

    @Override
    public Collection values() {
        return context.values();
    }

    @Override
    public Set<Entry> entrySet() {
        return context.entrySet();
    }

    @Override
    public ConnectionSpec getConnectionSpec() {
        return ((ConnectionProxy) getConnection()).getConnectionSpec();
    }

    @Override
    public DatabaseInfo getDatabaseInfo() {
        return dialect.getDatabaseInfo();
    }

    @Override
    public Connection getConnection() {
        return connection;
    }

    @Override
    public Dialect getDialect() {
        return dialect;
    }

    @Override
    public boolean shouldEnforceTableLocksForDDL() {
        return enforceTableLocksForDDL;
    }

    @Override
    public void execute(Work work, WorkManager workManager) throws Exception {
        workManager.execute(work, this);
    }

    @Override
    public void close() throws SQLException {
        sessionFactoryBase.closeSession(this);
    }
}
