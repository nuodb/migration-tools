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
package com.nuodb.migrator.jdbc.query;

import com.nuodb.migrator.jdbc.dialect.Dialect;
import com.nuodb.migrator.jdbc.metadata.Column;
import com.nuodb.migrator.jdbc.metadata.Table;

import java.util.Collection;
import java.util.Iterator;

import static com.google.common.collect.Lists.newArrayList;
import static com.nuodb.migrator.jdbc.query.QueryUtils.*;

/**
 * @author Sergey Bushik
 */
public class SelectQuery extends QueryBase {

    private Dialect dialect;
    private Collection<Object> from = newArrayList();
    private Collection<Object> columns = newArrayList();
    private Collection<String> where = newArrayList();
    private Collection<OrderBy> orderBy = newArrayList();
    private Collection<Join> joins = newArrayList();

    public void column(Object... columns) {
        for (Object column : columns) {
            column(column);
        }
    }

    public void column(Object column) {
        this.columns.add(column);
    }

    public void from(String from) {
        this.from.add(from);
    }

    public void from(Table table) {
        this.from.add(table);
    }

    public void innerJoin(String table, String condition) {
        join(INNER, table, condition);
    }

    public void leftJoin(String table, String condition) {
        join(LEFT, table, condition);
    }

    public void rightJoin(String table, String condition) {
        join(RIGHT, table, condition);
    }

    public void join(String type, String table, String condition) {
        this.joins.add(new Join(type, table, condition));
    }

    public void where(String filter) {
        this.where.add(filter);
    }

    public void orderBy(Collection<String> columns) {
        orderBy(columns, null);
    }

    public void orderBy(Collection<String> columns, String order) {
        this.orderBy.add(new OrderBy(columns, order));
    }

    @Override
    public void toQuery(StringBuilder query) {
        addSelect(query);
        addFrom(query);
        addJoins(query);
        addWhere(query);
        addOrderBy(query);
    }

    protected void addSelect(StringBuilder query) {
        query.append("SELECT");
        for (Iterator<Object> iterator = columns.iterator(); iterator.hasNext(); ) {
            query.append(" ");
            addColumn(query, iterator.next());
            if (iterator.hasNext()) {
                query.append(",");
            }
        }
    }

    protected void addColumn(StringBuilder query, Object column) {
        if (column instanceof Column) {
            query.append(((Column) column).getName(dialect));
        } else {
            query.append(column);
        }
    }

    protected void addFrom(StringBuilder query) {
        query.append(" FROM");
        for (Iterator<Object> iterator = from.iterator(); iterator.hasNext(); ) {
            query.append(" ");
            addTable(query, iterator.next());
            if (iterator.hasNext()) {
                query.append(", ");
            }
        }
    }

    protected void addJoins(StringBuilder query) {
        QueryUtils.join(query, joins);
    }

    protected void addWhere(StringBuilder query) {
        QueryUtils.where(query, where, AND);
    }

    protected void addOrderBy(StringBuilder query) {
        QueryUtils.orderBy(query, orderBy);
    }

    protected void addTable(StringBuilder query, Object table) {
        if (table instanceof Table) {
            query.append(isQualifyNames() ?
                    ((Table) table).getQualifiedName(dialect) : ((Table) table).getName(dialect));
        } else {
            query.append(table);
        }
    }

    public Dialect getDialect() {
        return dialect;
    }

    public void setDialect(Dialect dialect) {
        this.dialect = dialect;
    }

    public Collection<Object> getFrom() {
        return from;
    }

    public void setFrom(Collection<Object> from) {
        this.from = from;
    }

    public Collection<Object> getColumns() {
        return columns;
    }

    public void setColumns(Collection<Object> columns) {
        this.columns = columns;
    }

    public Collection<String> getWhere() {
        return where;
    }

    public void setWhere(Collection<String> where) {
        this.where = where;
    }

    public Collection<OrderBy> getOrderBy() {
        return orderBy;
    }

    public void setOrderBy(Collection<OrderBy> orderBy) {
        this.orderBy = orderBy;
    }

    public Collection<Join> getJoins() {
        return joins;
    }

    public void setJoins(Collection<Join> joins) {
        this.joins = joins;
    }
}
