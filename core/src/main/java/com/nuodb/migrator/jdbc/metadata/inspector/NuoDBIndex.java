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
package com.nuodb.migrator.jdbc.metadata.inspector;

import com.nuodb.migrator.jdbc.query.ParameterizedQuery;
import com.nuodb.migrator.jdbc.query.Query;

import java.util.Collection;

import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.primitives.Ints.asList;
import static com.nuodb.migrator.jdbc.query.Queries.newQuery;
import static com.nuodb.migrator.jdbc.query.QueryUtils.eqOrIn;
import static com.nuodb.migrator.jdbc.query.QueryUtils.where;
import static org.apache.commons.lang3.StringUtils.isEmpty;

/**
 * @author Sergey Bushik
 */
@SuppressWarnings("all")
public class NuoDBIndex {

    private static final String QUERY = "SELECT * FROM SYSTEM.INDEXES AS I INNER JOIN SYSTEM.INDEXFIELDS AS F "
            + "ON I.SCHEMA=F.SCHEMA AND I.TABLENAME=F.TABLENAME AND I.INDEXNAME=F.INDEXNAME";

    public static final int PRIMARY_KEY = 0;
    public static final int UNIQUE = 1;
    public static final int KEY = 2;
    public static final int UNIQUECONSTRAINT = 4;

    public static Query createQuery(TableInspectionScope tableInspectionScope, int... indexTypes) {
        return createQuery(tableInspectionScope.getSchema(), tableInspectionScope.getTable(), indexTypes);
    }

    public static Query createQuery(String schema, String table, int... indexTypes) {
        Collection<Object> parameters = newArrayList();
        Collection<String> filters = newArrayList();
        if (!isEmpty(schema)) {
            parameters.add(schema);
            filters.add("I.SCHEMA=?");
        }
        if (!isEmpty(table)) {
            parameters.add(table);
            filters.add("I.TABLENAME=?");
        }
        if (indexTypes != null && indexTypes.length > 0) {
            filters.add(eqOrIn("I.INDEXTYPE", asList(indexTypes)));
        }
        String query = where(QUERY, filters, "AND");
        return new ParameterizedQuery(newQuery(query), parameters);
    }
}
