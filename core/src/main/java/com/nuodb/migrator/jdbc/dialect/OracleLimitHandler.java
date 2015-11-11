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

import com.nuodb.migrator.jdbc.query.QueryLimit;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collection;

import static com.nuodb.migrator.jdbc.query.QueryUtils.AND;
import static com.nuodb.migrator.jdbc.query.QueryUtils.where;
import static java.util.Arrays.asList;
import static org.apache.commons.lang3.StringUtils.endsWithIgnoreCase;

/**
 * @author Sergey Bushik
 */
public class OracleLimitHandler extends SimpleLimitHandler {

    public OracleLimitHandler(Dialect dialect, String query, QueryLimit queryLimit) {
        super(dialect, query, queryLimit);
    }

    @Override
    protected String createLimitQuery(String query, long count) {
        return createLimitQuery(query, asList("ROWNUM_ <= " + count));
    }

    @Override
    protected String createParameterizedLimitQuery(String query) {
        return createLimitQuery(query, asList("ROWNUM_ <= ?"));
    }

    @Override
    protected String createLimitOffsetQuery(String query, long count, long offset) {
        return createLimitQuery(query, asList("ROWNUM_ > " + offset, "ROWNUM_ <= " + (count + offset)));
    }

    @Override
    protected String createParameterizedLimitOffsetQuery(String query) {
        return createLimitQuery(query, asList("ROWNUM_ > ?", "ROWNUM_ <= ?"));
    }

    protected String createLimitQuery(String query, Collection<String> filters) {
        boolean forUpdate = false;
        if (endsWithIgnoreCase(query, " FOR UPDATE")) {
            query = query.substring(0, query.length() - 11);
            forUpdate = true;
        }

        StringBuilder limitQuery = new StringBuilder(query.length());
        limitQuery.append("SELECT ").append(getColumns(query));
        limitQuery.append(" FROM ( SELECT ROW_.*, ROWNUM ROWNUM_ FROM (").append(query).append(") ROW_ )");

        where(limitQuery, filters, AND);

        if (forUpdate) {
            limitQuery.append(" FOR UPDATE");
        }
        return limitQuery.toString();
    }

    @Override
    protected void bindLimit(PreparedStatement statement, int index) throws SQLException {
        statement.setLong(index, getCount());
    }

    @Override
    protected void bindLimitOffset(PreparedStatement statement, int index) throws SQLException {
        statement.setLong(index, getOffset());
        statement.setLong(index + 1, getCount() + getOffset());
    }
}
