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
import org.hibernate.dialect.Dialect;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * @author Sergey Bushik
 */
public class SelectQuery implements Query {

    private Dialect dialect;
    private boolean qualify;
    private List<Table> tables = new ArrayList<Table>();
    private List<Column> columns = new ArrayList<Column>();
    private List<String> conditions = new ArrayList<String>();

    public void addColumn(Column column) {
        columns.add(column);
    }

    public void addTable(Table table) {
        tables.add(table);
    }

    public void addCondition(String condition) {
        conditions.add(condition);
    }

    @Override
    public String toQuery() {
        StringBuilder query = new StringBuilder();
        query.append("select ");
        //TODO assert for no column?
        for (Iterator<Column> iterator = columns.iterator(); iterator.hasNext(); ) {
            Column column = iterator.next();
            query.append(column.getQuotedName(dialect));
            if (iterator.hasNext()) {
                query.append(", ");
            }
        }
        query.append(" from ");
        for (Iterator<Table> iterator = tables.iterator(); iterator.hasNext(); ) {
            Table table = iterator.next();
            query.append(qualify ? table.getQualifiedName(dialect) : table.getQuotedName(dialect));
            if (iterator.hasNext()) {
                query.append(", ");
            }
        }
        if (conditions.size() > 0) {
            query.append(" where ");
            for (Iterator<String> iterator = conditions.iterator(); iterator.hasNext(); ) {
                query.append(iterator.next());
                if (iterator.hasNext()) {
                    query.append(" and ");
                }
            }
        }
        return query.toString();
    }

    public Dialect getDialect() {
        return dialect;
    }

    public void setDialect(Dialect dialect) {
        this.dialect = dialect;
    }

    public boolean isQualify() {
        return qualify;
    }

    public void setQualify(boolean qualify) {
        this.qualify = qualify;
    }

    public List<Table> getTables() {
        return tables;
    }

    public List<Column> getColumns() {
        return columns;
    }

    public List<String> getConditions() {
        return conditions;
    }

    @Override
    public String toString() {
        return toQuery();
    }
}
