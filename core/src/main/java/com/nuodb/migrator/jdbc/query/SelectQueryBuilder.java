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
package com.nuodb.migrator.jdbc.query;

import com.nuodb.migrator.jdbc.dialect.Dialect;
import com.nuodb.migrator.jdbc.metadata.Column;
import com.nuodb.migrator.jdbc.metadata.Database;
import com.nuodb.migrator.jdbc.metadata.Table;

import java.util.Collection;

import static com.google.common.collect.Lists.newArrayList;
import static com.nuodb.migrator.utils.Collections.isEmpty;

/**
 * @author Sergey Bushik
 */
public class SelectQueryBuilder implements QueryBuilder<SelectQuery> {

    private static final boolean QUALIFY_NAMES = true;

    private Dialect dialect;
    private Table from;
    private boolean qualifyNames = QUALIFY_NAMES;
    private Collection<Object> columns = newArrayList();
    private Collection<String> filters = newArrayList();

    @Override
    public SelectQuery build() {
        SelectQuery query = new SelectQuery();
        query.setQualifyNames(qualifyNames);
        query.from(from);
        Database database = from.getDatabase();
        if (dialect != null) {
            query.setDialect(dialect);
        } else if (database != null) {
            query.setDialect(database.getDialect());
        }
        if (isEmpty(columns)) {
            for (Column column : from.getColumns()) {
                query.column(column);
            }
        } else {
            for (Object column : columns) {
                query.column(column);
            }
        }
        if (filters != null) {
            for (String filter : filters) {
                query.where(filter);
            }
        }
        return query;
    }

    public SelectQueryBuilder dialect(Dialect dialect) {
        this.dialect = dialect;
        return this;
    }

    public SelectQueryBuilder from(Table from) {
        this.from = from;
        return this;
    }

    public SelectQueryBuilder qualifyNames(boolean qualifyNames) {
        this.qualifyNames = qualifyNames;
        return this;
    }

    public SelectQueryBuilder column(String column) {
        this.columns.add(column);
        return this;
    }

    public SelectQueryBuilder column(Column column) {
        this.columns.add(column);
        return this;
    }

    public SelectQueryBuilder filter(String filter) {
        this.filters.add(filter);
        return this;
    }

    public SelectQueryBuilder filters(Collection<String> filters) {
        this.filters = filters;
        return this;
    }
}
