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
package com.nuodb.tools.migration.dump.query;

import com.nuodb.tools.migration.jdbc.metamodel.Column;
import com.nuodb.tools.migration.jdbc.metamodel.Table;

/**
 * @author Sergey Bushik
 */
public class SelectQuery implements Query {

    private StringBuilder select = new StringBuilder();
    private StringBuilder from = new StringBuilder();
    private StringBuilder where = new StringBuilder();

    public void addColumn(String column) {
        if (select.length() > 0) {
            select.append(", ");
        }
        select.append(column);
    }

    public void addColumn(Column column) {
        addColumn(column.getName().value());
    }

    public void addTable(Table table) {
        addTable(table.getName().value());
    }

    public void addTable(String table) {
        if (from.length() > 0) {
            from.append(", ");
        }
        from.append(table);
    }

    public void addCondition(String condition) {
        if (where.length() > 0) {
            where.append(" and ").append(condition);
        } else {
            where.append(condition);
        }
    }

    @Override
    public String toQueryString() {
        StringBuilder query = new StringBuilder();
        query.append("select ");
        query.append(select);
        query.append(" from ");
        query.append(from);
        if (where.length() > 0) {
            query.append(" where ");
            query.append(where);
        }
        return query.toString();
    }

    @Override
    public String toString() {
        return toQueryString();
    }
}
