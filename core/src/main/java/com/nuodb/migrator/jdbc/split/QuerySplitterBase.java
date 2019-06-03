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
package com.nuodb.migrator.jdbc.split;

import com.nuodb.migrator.jdbc.query.ParametersBinder;
import com.nuodb.migrator.jdbc.query.QueryLimit;
import com.nuodb.migrator.jdbc.query.Query;
import com.nuodb.migrator.jdbc.query.StatementCallback;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * @author Sergey Bushik
 */
@SuppressWarnings("unchecked")
public abstract class QuerySplitterBase<S extends Statement> implements QuerySplitter<S> {

    private Query query;
    private QueryLimit queryLimit;
    private ParametersBinder parametersBinder;

    private int splitIndex;

    protected QuerySplitterBase(Query query, QueryLimit queryLimit, ParametersBinder parametersBinder) {
        this.query = query;
        this.queryLimit = queryLimit;
        this.parametersBinder = parametersBinder;
    }

    @Override
    public Query getQuery() {
        return query;
    }

    @Override
    public QueryLimit getQueryLimit() {
        return queryLimit;
    }

    @Override
    public boolean hasNextQuerySplit(Connection connection) throws SQLException {
        return hasNextQuerySplit(connection, splitIndex);
    }

    protected abstract boolean hasNextQuerySplit(Connection connection, int splitIndex) throws SQLException;

    @Override
    public QuerySplit getNextQuerySplit(Connection connection) throws SQLException {
        return getNextQuerySplit(connection, null);
    }

    @Override
    public QuerySplit getNextQuerySplit(Connection connection, StatementCallback<S> callback) throws SQLException {
        return hasNextQuerySplit(connection) ? createNextQuerySplit(connection, callback) : null;
    }

    protected QuerySplit createNextQuerySplit(Connection connection, StatementCallback<S> callback)
            throws SQLException {
        return createQuerySplit(connection, callback, createQueryLimit(connection, splitIndex), splitIndex++);
    }

    protected abstract QueryLimit createQueryLimit(Connection connection, int splitIndex) throws SQLException;

    protected abstract S prepareStatement(Connection connection, QueryLimit queryLimit, int splitIndex)
            throws SQLException;

    protected abstract S createStatement(Connection connection, QueryLimit queryLimit, int splitIndex)
            throws SQLException;

    protected abstract ResultSet executeStatement(S statement, QueryLimit queryLimit, int splitIndex)
            throws SQLException;

    protected QuerySplit createQuerySplit(final Connection connection, final StatementCallback<S> callback,
            final QueryLimit queryLimit, final int splitIndex) throws SQLException {
        return new QuerySplit() {
            @Override
            public int getSplitIndex() {
                return splitIndex;
            }

            @Override
            public Query getQuery() {
                return query;
            }

            @Override
            public QueryLimit getQueryLimit() {
                return queryLimit;
            }

            @Override
            public ResultSet getResultSet() throws SQLException {
                return getResultSet(connection);
            }

            @Override
            public ResultSet getResultSet(Connection connection) throws SQLException {
                return getResultSet(connection, callback);
            }

            @Override
            public ResultSet getResultSet(Connection connection, StatementCallback callback) throws SQLException {
                final S statement = isParameterized() ? prepareStatement(connection, queryLimit, splitIndex)
                        : createStatement(connection, queryLimit, splitIndex);
                if (callback != null) {
                    callback.executeStatement(statement);
                }
                return executeStatement(statement, queryLimit, splitIndex);
            }
        };
    }

    public boolean isParameterized() {
        return parametersBinder != null;
    }

    public ParametersBinder getParametersBinder() {
        return parametersBinder;
    }
}
