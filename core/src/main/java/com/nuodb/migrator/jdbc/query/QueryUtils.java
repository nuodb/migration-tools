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

import java.util.Collection;
import java.util.Iterator;

import static com.nuodb.migrator.utils.StringUtils.isEmpty;
import static java.util.Arrays.asList;
import static java.util.Collections.singleton;

/**
 * @author Sergey Bushik
 */
public class QueryUtils {

    public static final String INNER = "INNER";
    public static final String LEFT = "LEFT";
    public static final String RIGHT = "RIGHT";
    public static final String OR = "OR";
    public static final String AND = "AND";
    public static final String DESC = "DESC";
    public static final String ASC = "ASC";

    private QueryUtils() {
    }

    public static String where(String query, Collection<String> filters, String operator) {
        return where(new StringBuilder(query), filters, operator).toString();
    }

    public static StringBuilder where(StringBuilder query, Collection<String> filters) {
        return where(query, filters, AND);
    }

    public static StringBuilder where(StringBuilder query, Collection<String> filters, String operator) {
        if (filters != null && !filters.isEmpty()) {
            query.append(' ');
            query.append("WHERE");
            for (Iterator<String> iterator = filters.iterator(); iterator.hasNext();) {
                String filter = iterator.next();
                query.append(' ');
                query.append(filter);
                if (iterator.hasNext()) {
                    query.append(' ');
                    query.append(operator != null ? operator : AND);
                }
            }
        }
        return query;
    }

    public static String orderBy(String query, Collection<String> columns, String order) {
        return orderBy(new StringBuilder(query), columns, order).toString();
    }

    public static StringBuilder orderBy(StringBuilder query, Collection<String> columns, String order) {
        return orderBy(query, columns != null ? singleton(new OrderBy(columns, order)) : null);
    }

    public static StringBuilder orderBy(StringBuilder query, Collection<OrderBy> orderBys) {
        if (orderBys != null && !orderBys.isEmpty()) {
            query.append(' ');
            query.append("ORDER BY");
            for (Iterator<OrderBy> orderByIterator = orderBys.iterator(); orderByIterator.hasNext();) {
                OrderBy orderBy = orderByIterator.next();
                for (Iterator<String> columnsIterator = orderBy.getColumns().iterator(); columnsIterator.hasNext();) {
                    String column = columnsIterator.next();
                    query.append(' ');
                    query.append(column);
                    if (columnsIterator.hasNext()) {
                        query.append(',');
                    }
                }
                String sortOrder = orderBy.getSortOrder();
                if (sortOrder != null) {
                    query.append(' ');
                    query.append(sortOrder);
                }
                if (orderByIterator.hasNext()) {
                    query.append(',');
                }
            }

        }
        return query;
    }

    public static StringBuilder join(StringBuilder query, Collection<Join> joins) {
        if (joins != null && !joins.isEmpty()) {
            query.append(' ');
            for (Iterator<Join> iterator = joins.iterator(); iterator.hasNext();) {
                Join join = iterator.next();
                if (!isEmpty(join.getType())) {
                    query.append(join.getType());
                }
                query.append(" JOIN ");
                query.append(join.getTable());
                query.append(" ON ");
                query.append(join.getCondition());
                if (iterator.hasNext()) {
                    query.append(' ');
                }
            }
        }
        return query;
    }

    public static SelectQuery union(SelectQuery... queries) {
        SelectQuery union = null;
        for (SelectQuery query : queries) {
            if (query == null) {
                continue;
            }
            if (union == null) {
                union = query;
            } else {
                union.union(query);
            }
        }
        return union;
    }

    public static StringBuilder eq(StringBuilder query, String column, Object value) {
        return query.append(column).append("=").append(value);
    }

    public static StringBuilder in(StringBuilder query, String column, Collection<? extends Object> values) {
        query.append(column);
        query.append(" IN (");
        for (Iterator<? extends Object> iterator = values.iterator(); iterator.hasNext();) {
            Object value = iterator.next();
            query.append(value);
            if (iterator.hasNext()) {
                query.append(",");
            }
        }
        query.append(")");
        return query;
    }

    public static String eqOrIn(String column, Object... values) {
        return eqOrIn(new StringBuilder(), column, values).toString();
    }

    public static String eqOrIn(String column, Collection<? extends Object> values) {
        return eqOrIn(new StringBuilder(), column, values).toString();
    }

    public static StringBuilder eqOrIn(StringBuilder query, String column, Object... values) {
        return eqOrIn(query, column, asList(values));
    }

    public static StringBuilder eqOrIn(StringBuilder query, String column, Collection<? extends Object> values) {
        return values.size() == 1 ? eq(query, column, values.iterator().next()) : in(query, column, values);
    }
}
