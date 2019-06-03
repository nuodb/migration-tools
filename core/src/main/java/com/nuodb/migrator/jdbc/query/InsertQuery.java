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

import com.google.common.collect.Maps;
import com.nuodb.migrator.jdbc.dialect.Dialect;
import com.nuodb.migrator.jdbc.metadata.Column;
import com.nuodb.migrator.jdbc.metadata.Table;

import java.util.Iterator;
import java.util.Map;

import static com.nuodb.migrator.jdbc.query.InsertType.INSERT;

/**
 * @author Sergey Bushik
 */
public class InsertQuery extends QueryBase {

    private InsertType insertType = INSERT;
    private Dialect dialect;
    private Table into;
    private Map<Column, String> columns = Maps.newLinkedHashMap();

    public InsertType getInsertType() {
        return insertType;
    }

    public void setInsertType(InsertType insertType) {
        this.insertType = insertType;
    }

    public Dialect getDialect() {
        return dialect;
    }

    public void setDialect(Dialect dialect) {
        this.dialect = dialect;
    }

    public Table getInto() {
        return into;
    }

    public void setInto(Table into) {
        this.into = into;
    }

    public Map<Column, String> getColumns() {
        return columns;
    }

    public void setColumns(Map<Column, String> columns) {
        this.columns = columns;
    }

    public void addColumn(Column column) {
        addColumn(column, "?");
    }

    public void addColumn(Column column, String value) {
        columns.put(column, value);
    }

    @Override
    public void append(StringBuilder query) {
        query.append(insertType == null ? INSERT : insertType.getCommand());
        query.append(" INTO ").append(isQualifyNames() ? into.getQualifiedName(dialect) : into.getName(dialect));
        if (columns.size() == 0) {
            query.append(' ').append(dialect.getNoColumnsInsert());
        } else {
            query.append(" (");
            Iterator<Column> names = columns.keySet().iterator();
            while (names.hasNext()) {
                Column column = names.next();
                query.append(column.getName(dialect));
                if (names.hasNext()) {
                    query.append(", ");
                }
            }
            query.append(") VALUES (");
            Iterator<String> values = columns.values().iterator();
            while (values.hasNext()) {
                query.append(values.next());
                if (values.hasNext()) {
                    query.append(", ");
                }
            }
            query.append(')');
        }
    }
}
