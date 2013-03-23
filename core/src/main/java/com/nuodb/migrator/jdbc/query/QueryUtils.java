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

import java.util.Collection;
import java.util.Iterator;

/**
 * @author Sergey Bushik
 */
public class QueryUtils {

    public static final String OR = "OR";
    public static final String AND = "AND";
    public static final String DESC = "DESC";
    public static final String ASC = "ASC";

    public static String where(String query, Collection<String> filters, String operator) {
        return where(new StringBuilder(query), filters, operator).toString();
    }

    public static StringBuilder where(StringBuilder query, Collection<String> filters) {
        return where(query, filters, AND);
    }

    public static StringBuilder where(StringBuilder query, Collection<String> filters, String operator) {
        if (!filters.isEmpty()) {
            query.append(' ');
            query.append("WHERE");
            for (Iterator<String> iterator = filters.iterator(); iterator.hasNext(); ) {
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

    public static StringBuilder orderBy(StringBuilder query, Collection<String> columns) {
        return orderBy(query, columns, null);
    }

    public static StringBuilder orderBy(StringBuilder query, Collection<String> columns, String order) {
        if (!columns.isEmpty()) {
            query.append(' ');
            query.append("ORDER BY");
            for (Iterator<String> iterator = columns.iterator(); iterator.hasNext(); ) {
                String column = iterator.next();
                query.append(' ');
                query.append(column);
                if (iterator.hasNext()) {
                    query.append(',');
                }
            }
            if (order != null) {
                query.append(' ');
                query.append(order);
            }
        }
        return query;
    }
}
