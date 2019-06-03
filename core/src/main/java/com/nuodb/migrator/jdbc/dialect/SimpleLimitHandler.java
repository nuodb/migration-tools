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

import com.nuodb.migrator.jdbc.query.QueryHelper;
import com.nuodb.migrator.jdbc.query.QueryLimit;
import com.nuodb.migrator.jdbc.query.QueryLimitUtils;

import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * @author Sergey Bushik
 */
public class SimpleLimitHandler extends QueryHelper implements LimitHandler {

    private String query;
    private QueryLimit queryLimit;

    public SimpleLimitHandler(Dialect dialect, String query, QueryLimit queryLimit) {
        super(dialect);
        this.query = query;
        this.queryLimit = queryLimit;
    }

    @Override
    public String getLimitQuery(boolean parameterized) {
        String limitQuery;
        if (hasOffset()) {
            limitQuery = parameterized ? createParameterizedLimitOffsetQuery(query)
                    : createLimitOffsetQuery(query, getCount(), getOffset());
        } else {
            limitQuery = parameterized ? createParameterizedLimitQuery(query) : createLimitQuery(query, getCount());
        }
        return limitQuery;
    }

    protected boolean hasOffset() {
        return getOffset() > 0;
    }

    protected String createLimitQuery(String query, long count) {
        throw new DialectException("Limit query syntax is not supported");
    }

    protected String createParameterizedLimitQuery(String query) {
        throw new DialectException("Limit query syntax with parameters is not supported");
    }

    protected String createLimitOffsetQuery(String query, long count, long offset) {
        throw new DialectException("Limit offset query syntax is not supported");
    }

    protected String createParameterizedLimitOffsetQuery(String query) {
        throw new DialectException("Limit offset query syntax with parameters is not supported");
    }

    protected long getCount() {
        return QueryLimitUtils.getCount(queryLimit);
    }

    protected long getOffset() {
        return QueryLimitUtils.getOffset(queryLimit);
    }

    @Override
    public String getQuery() {
        return query;
    }

    @Override
    public QueryLimit getQueryLimit() {
        return queryLimit;
    }

    @Override
    public int bindParametersAtStart(PreparedStatement statement, int index) throws SQLException {
        return supportsLimitParameters() && isBindParametersAtStart() ? bindParameters(statement, index) : 0;
    }

    @Override
    public int bindParametersAtEnd(PreparedStatement statement, int index) throws SQLException {
        return supportsLimitParameters() && !isBindParametersAtStart() ? bindParameters(statement, index) : 0;
    }

    protected boolean supportsLimitParameters() {
        return getDialect().supportsLimitParameters();
    }

    protected boolean isBindParametersAtStart() {
        return false;
    }

    protected int bindParameters(PreparedStatement statement, int index) throws SQLException {
        int parameters;
        if (!hasOffset()) {
            bindLimit(statement, index);
            parameters = 1;
        } else {
            bindLimitOffset(statement, index);
            parameters = 2;
        }
        return parameters;
    }

    protected void bindLimit(PreparedStatement statement, int index) throws SQLException {
        statement.setLong(index, getCount());
    }

    protected void bindLimitOffset(PreparedStatement statement, int index) throws SQLException {
        statement.setLong(index, getCount());
        statement.setLong(index + 1, getOffset());
    }
}
