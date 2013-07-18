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
package com.nuodb.migrator.jdbc.split;

import com.nuodb.migrator.jdbc.dialect.Dialect;
import com.nuodb.migrator.jdbc.dialect.LimitHandler;
import com.nuodb.migrator.jdbc.dialect.QueryLimit;
import com.nuodb.migrator.jdbc.query.Query;

import java.sql.*;

import static com.nuodb.migrator.jdbc.dialect.QueryLimitUtils.getLimit;
import static com.nuodb.migrator.jdbc.dialect.QueryLimitUtils.getOffset;
import static java.lang.Math.min;

/**
 * @author Sergey Bushik
 */
public class LimitQuerySplitter extends LazyQuerySplitterBase<Statement> {

    private Dialect dialect;
    private RowCountStrategy rowCountStrategy;

    protected LimitQuerySplitter(Dialect dialect, Query query, QueryLimit queryLimit,
                                 RowCountStrategy rowCountStrategy) {
        super(query, queryLimit);
        this.dialect = dialect;
        this.rowCountStrategy = rowCountStrategy;
    }

    @Override
    protected boolean hasNextQuerySplit(Connection connection, int splitIndex) throws SQLException {
        QueryLimit queryLimit = getQueryLimit();
        return splitIndex == 0 || splitIndex * getLimit(queryLimit) + getOffset(queryLimit) < getRowCount(connection);
    }

    @Override
    protected QueryLimit createQueryLimit(Connection connection, int splitIndex) throws SQLException {
        QueryLimit queryLimit = getQueryLimit();
        long offset = splitIndex * getLimit(queryLimit) + getOffset(queryLimit);
        // long limit = getLimit(queryLimit);
        long limit = min(getLimit(queryLimit), getRowCount(connection) - offset);
        return new QueryLimit(limit, offset);
    }

    protected long getRowCount(Connection connection) throws SQLException {
        return getRowCountStrategy().getRowCount(connection);
    }

    @Override
    protected Statement createStatement(Connection connection, QueryLimit queryLimit,
                                        int splitIndex) throws SQLException {
        Statement statement;
        if (getDialect().supportsLimitParameters()) {
            LimitHandler limitHandler = getDialect().createLimitHandler(getQuery().toString(), queryLimit);
            PreparedStatement preparedStatement = connection.prepareStatement(limitHandler.getLimitQuery());
            int column = 1;
            column += limitHandler.bindParametersAtStart(preparedStatement, column);
            column += bindParameters(preparedStatement, column);
            column += limitHandler.bindParametersAtEnd(preparedStatement, column);
            statement = preparedStatement;
        } else {
            statement = connection.createStatement();
        }
        return statement;
    }

    protected int bindParameters(PreparedStatement preparedStatement, int column) {
        return 0;
    }

    @Override
    protected ResultSet executeStatement(Statement statement, QueryLimit queryLimit,
                                         int splitIndex) throws SQLException {
        if (getDialect().supportsLimitParameters()) {
            return ((PreparedStatement) statement).executeQuery();
        } else {
            LimitHandler limitHandler = getDialect().createLimitHandler(getQuery().toString(), queryLimit);
            return statement.executeQuery(limitHandler.getLimitQuery());
        }

    }

    public Dialect getDialect() {
        return dialect;
    }

    public RowCountStrategy getRowCountStrategy() {
        return rowCountStrategy;
    }
}
