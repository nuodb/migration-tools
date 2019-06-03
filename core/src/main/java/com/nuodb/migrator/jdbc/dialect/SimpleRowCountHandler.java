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
package com.nuodb.migrator.jdbc.dialect;

import com.nuodb.migrator.jdbc.query.StatementTemplate;
import com.nuodb.migrator.jdbc.query.StatementCallback;
import com.nuodb.migrator.jdbc.query.StatementFactory;
import org.apache.commons.lang3.mutable.MutableObject;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * @author Sergey Bushik
 */
public class SimpleRowCountHandler implements RowCountHandler {

    private Dialect dialect;
    private RowCountType rowCountType;

    public SimpleRowCountHandler(Dialect dialect, RowCountType rowCountType) {
        this.dialect = dialect;
        this.rowCountType = rowCountType;
    }

    @Override
    public RowCountQuery getRowCountQuery() {
        return createRowCountQuery();
    }

    protected RowCountQuery createRowCountQuery() {
        RowCountQuery rowCountQuery = null;
        switch (getRowCountType()) {
        case APPROX:
            rowCountQuery = createApproxRowCountQuery();
            break;
        case EXACT:
            rowCountQuery = createExactRowCountQuery();
            break;
        }
        return rowCountQuery;
    }

    protected RowCountQuery createApproxRowCountQuery() {
        return null;
    }

    protected RowCountQuery createExactRowCountQuery() {
        return null;
    }

    @Override
    public long getRowCount(Connection connection) throws SQLException {
        return getRowCount(connection, getRowCountQuery());
    }

    protected long getRowCount(Connection connection, final RowCountQuery rowCountQuery) throws SQLException {
        final MutableObject<Long> rowCount = new MutableObject<Long>();
        new StatementTemplate(connection).executeStatement(new StatementFactory<Statement>() {
            @Override
            public Statement createStatement(Connection connection) throws SQLException {
                return connection.createStatement();
            }
        }, new StatementCallback<Statement>() {
            @Override
            public void executeStatement(Statement statement) throws SQLException {
                rowCount.setValue(getRowCount(statement, rowCountQuery));
            }
        });
        return rowCount.getValue();
    }

    protected Long getRowCount(Statement statement, RowCountQuery rowCountQuery) throws SQLException {
        return getRowCount(statement.executeQuery(rowCountQuery.getQuery().toString()), rowCountQuery);
    }

    protected Long getRowCount(ResultSet resultSet, RowCountQuery rowCountQuery) throws SQLException {
        return resultSet.next() ? resultSet.getLong(1) : null;
    }

    @Override
    public Dialect getDialect() {
        return dialect;
    }

    @Override
    public RowCountType getRowCountType() {
        return rowCountType;
    }
}
