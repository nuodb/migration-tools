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

import static com.nuodb.migrator.utils.StringUtils.indexOf;
import static java.lang.String.valueOf;

/**
 * @author Sergey Bushik
 */
public class MSSQLServerLimitHandler extends SimpleLimitHandler {

    public MSSQLServerLimitHandler(Dialect dialect, String query, QueryLimit queryLimit) {
        super(dialect, query, queryLimit);
    }

    @Override
    protected String createParameterizedLimitQuery(String query) {
        return createLimitQuery(query, "?");
    }

    @Override
    protected String createLimitQuery(String query, long count) {
        return createLimitQuery(query, valueOf(count));
    }

    protected String createLimitQuery(String query, String limit) {
        StringBuilder limitQuery = new StringBuilder(query);
        final int distinct = indexOf(query, DISTINCT, 0);
        if (distinct != -1) {
            limitQuery.insert(distinct + DISTINCT.length(), " TOP(" + limit + ")");
        } else {
            int selectStart = indexOf(query, SELECT, 0, true);
            limitQuery.insert(selectStart + SELECT.length(), " TOP(" + limit + ")");
        }
        return limitQuery.toString();
    }

    @Override
    protected boolean isBindParametersAtStart() {
        return true;
    }
}
