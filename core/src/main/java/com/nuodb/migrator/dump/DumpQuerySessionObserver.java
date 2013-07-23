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
package com.nuodb.migrator.dump;

import com.nuodb.migrator.jdbc.dialect.Dialect;
import com.nuodb.migrator.jdbc.session.Session;
import com.nuodb.migrator.jdbc.session.SessionObserver;

import java.sql.Connection;
import java.sql.SQLException;

import static java.sql.Connection.TRANSACTION_READ_COMMITTED;
import static java.sql.Connection.TRANSACTION_REPEATABLE_READ;

/**
 * @author Sergey Bushik
 */
public class DumpQuerySessionObserver implements SessionObserver {

    private DumpQueryContext dumpQueryContext;

    public DumpQuerySessionObserver(DumpQueryContext dumpQueryContext) {
        this.dumpQueryContext = dumpQueryContext;
    }

    @Override
    public void afterOpen(Session session) throws SQLException {
        Connection connection = session.getConnection();
        Dialect dialect = session.getDialect();
        dialect.setTransactionIsolation(connection,
                new int[]{TRANSACTION_REPEATABLE_READ, TRANSACTION_READ_COMMITTED});
        if (dialect.supportsSessionTimeZone()) {
            dialect.setSessionTimeZone(connection, dumpQueryContext.getTimeZone());
        }
    }

    @Override
    public void beforeClose(Session session) throws SQLException {
        Dialect dialect = session.getDialect();
        Connection connection = session.getConnection();
        if (dialect.supportsSessionTimeZone()) {
            dialect.setSessionTimeZone(connection, null);
        }
    }
}