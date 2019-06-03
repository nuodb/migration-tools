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

import com.google.common.collect.Lists;
import com.nuodb.migrator.jdbc.dialect.Dialect;
import com.nuodb.migrator.jdbc.metadata.Column;
import com.nuodb.migrator.jdbc.metadata.Database;
import com.nuodb.migrator.jdbc.metadata.Table;

import java.util.Collection;

/**
 * @author Sergey Bushik
 */
public class InsertQueryBuilder implements QueryBuilder<InsertQuery> {

    private static final boolean QUALIFY_NAMES = true;

    private Dialect dialect;
    private InsertType insertType;
    private Table table;
    private boolean qualifyNames = QUALIFY_NAMES;
    private Collection<String> columns = Lists.newArrayList();

    @Override
    public InsertQuery build() {
        InsertQuery insertQuery = new InsertQuery();
        insertQuery.setInsertType(insertType);
        insertQuery.setQualifyNames(qualifyNames);
        insertQuery.setInto(table);
        Database database = table.getDatabase();
        if (dialect != null) {
            insertQuery.setDialect(dialect);
        } else if (database != null) {
            insertQuery.setDialect(database.getDialect());
        }
        if (columns != null && !columns.isEmpty()) {
            for (String column : columns) {
                insertQuery.addColumn(table.getColumn(column));
            }
        } else {
            for (Column column : table.getColumns()) {
                insertQuery.addColumn(column);
            }
        }
        return insertQuery;
    }

    public InsertQueryBuilder dialect(Dialect dialect) {
        this.dialect = dialect;
        return this;
    }

    public InsertQueryBuilder insertType(InsertType insertType) {
        this.insertType = insertType;
        return this;
    }

    public InsertQueryBuilder into(Table table) {
        this.table = table;
        return this;
    }

    public InsertQueryBuilder qualifyNames(boolean qualifyNames) {
        this.qualifyNames = qualifyNames;
        return this;
    }

    public InsertQueryBuilder columns(Collection<String> columns) {
        this.columns = columns;
        return this;
    }
}
