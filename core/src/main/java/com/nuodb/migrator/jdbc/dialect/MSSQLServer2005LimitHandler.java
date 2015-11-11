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

import static java.lang.String.valueOf;

/**
 * @author Sergey Bushik
 */
public class MSSQLServer2005LimitHandler extends MSSQLServerLimitHandler {

    public MSSQLServer2005LimitHandler(Dialect dialect, String query, QueryLimit queryLimit) {
        super(dialect, query, queryLimit);
    }

    @Override
    protected String createParameterizedLimitOffsetQuery(String query) {
        return createLimitOffsetQuery(query, "?", "?");
    }

    @Override
    protected String createLimitOffsetQuery(String query, long count, long offset) {
        return createLimitOffsetQuery(query, valueOf(offset), valueOf(offset + count));
    }

    protected String createLimitOffsetQuery(String query, String rowStart, String rowEnd) {
        StringBuilder limitQuery = new StringBuilder(query.length());
        limitQuery.append("SELECT ").append(getColumns(query)).append(" FROM (");
        limitQuery.append(addColumn(query, "ROW_NUMBER() OVER (ORDER BY CURRENT_TIMESTAMP) AS ROW_NUMBER__"));
        limitQuery.append(") QUERY__ WHERE ROW_NUMBER__ > " + rowStart + " AND ROW_NUMBER__ <= " + rowEnd);
        return limitQuery.toString();
    }

    @Override
    protected boolean isBindParametersAtStart() {
        return !hasOffset();
    }

    @Override
    protected void bindLimitOffset(PreparedStatement statement, int index) throws SQLException {
        statement.setLong(index, getOffset());
        statement.setLong(index + 1, getCount() + getOffset());
    }
}
