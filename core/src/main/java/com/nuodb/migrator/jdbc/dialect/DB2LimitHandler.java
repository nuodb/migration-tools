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

/**
 * @author Sergey Bushik
 */
public class DB2LimitHandler extends SimpleLimitHandler {

    public DB2LimitHandler(Dialect dialect, String query, QueryLimit queryLimit) {
        super(dialect, query, queryLimit);
    }

    @Override
    protected String createLimitQuery(String query, long count) {
        StringBuilder limitQuery = new StringBuilder(query.length());
        limitQuery.append(query);
        if (count > 0) {
            limitQuery.append(" FETCH FIRST ").append(count).append(" ROWS ONLY");
        }
        return limitQuery.toString();
    }

    @Override
    protected String createLimitOffsetQuery(String query, long count, long offset) {
        StringBuilder limitQuery = new StringBuilder(query.length());
        limitQuery.append("SELECT ").append(getColumns(query)).append(" FROM (");
        limitQuery.append("SELECT QUERY2__.*, ROWNUMBER() OVER (ORDER BY ORDER OF QUERY2__) AS ROW_NUM__ FROM (");
        limitQuery.append(createLimitQuery(query, offset + count)).append(") AS QUERY2__)");
        limitQuery.append(" AS QUERY1__ WHERE ROW_NUM__ > ").append(offset).append(" ORDER BY ROW_NUM__");
        return limitQuery.toString();
    }
}
