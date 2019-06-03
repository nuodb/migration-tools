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
import com.nuodb.migrator.jdbc.query.SelectQuery;
import com.nuodb.migrator.utils.StringUtils;

import java.util.Collection;

import static com.google.common.collect.Lists.newArrayList;
import static com.nuodb.migrator.jdbc.metadata.Table.SYNONYM;
import static com.nuodb.migrator.jdbc.query.QueryUtils.eqOrIn;
import static com.nuodb.migrator.jdbc.query.QueryUtils.union;
import static com.nuodb.migrator.utils.Collections.isEmpty;
import static java.util.Arrays.fill;
import static org.apache.commons.lang3.StringUtils.containsAny;

/**
 * @author Sergey Bushik
 */
public class OracleTableInspector extends SimpleTableInspector {

    @Override
    protected Query createQuery(InspectionContext inspectionContext, TableInspectionScope tableInspectionScope) {
        boolean includeSynonyms;
        boolean includeNotSynonyms;
        Collection<String> tableTypes;
        if (isEmpty(tableInspectionScope.getTableTypes())) {
            tableTypes = null;
            includeSynonyms = false;
            includeNotSynonyms = true;
        } else {
            tableTypes = newArrayList(tableInspectionScope.getTableTypes());
            includeSynonyms = tableTypes.remove(SYNONYM);
            includeNotSynonyms = !includeSynonyms || !tableTypes.isEmpty();
        }
        String schema = tableInspectionScope.getSchema();
        String table = tableInspectionScope.getTable();
        Collection<Object> parameters = newArrayList();
        SelectQuery synonymsQuery = includeSynonyms ? createSelectSynonymsQuery(schema, table, parameters) : null;
        SelectQuery notSynonymsQuery = includeNotSynonyms
                ? createSelectNotSynonymsQuery(schema, table, tableTypes, parameters)
                : null;
        return new ParameterizedQuery(union(synonymsQuery, notSynonymsQuery), parameters);
    }

    protected SelectQuery createSelectSynonymsQuery(String schema, String table, Collection<Object> parameters) {
        SelectQuery query = new SelectQuery();
        query.columns("NULL AS TABLE_CAT", "S.OWNER AS TABLE_SCHEM", "S.SYNONYM_NAME AS TABLE_NAME",
                "'SYNONYM' AS TABLE_TYPE", "C.COMMENTS AS REMARKS");
        query.from("ALL_SYNONYMS S, ALL_OBJECTS O, ALL_TAB_COMMENTS C");
        query.where("S.TABLE_OWNER = O.OWNER");
        query.where("S.TABLE_NAME = O.OBJECT_NAME");
        query.where("O.OBJECT_TYPE IN ('TABLE','VIEW')");
        query.where("O.OWNER = C.OWNER(+)");
        query.where("O.OBJECT_NAME = C.TABLE_NAME(+)");
        if (!StringUtils.isEmpty(schema)) {
            query.where(containsAny(schema, "%") ? "S.OWNER LIKE ? ESCAPE '/'" : "S.OWNER=?");
            parameters.add(schema);
        }
        if (!StringUtils.isEmpty(table)) {
            query.where(containsAny(schema, "%") ? "S.SYNONYM_NAME LIKE ? ESCAPE '/'" : "S.SYNONYM_NAME=?");
            parameters.add(table);
        }
        query.orderBy("TABLE_TYPE", "TABLE_SCHEM", "TABLE_NAME");
        return query;
    }

    protected SelectQuery createSelectNotSynonymsQuery(String schema, String table, Collection<String> tableTypes,
            Collection<Object> parameters) {
        SelectQuery query = new SelectQuery();
        query.columns("NULL AS TABLE_CAT", "O.OWNER AS TABLE_SCHEM", "O.OBJECT_NAME AS TABLE_NAME",
                "O.OBJECT_TYPE AS TABLE_TYPE", "C.COMMENTS AS REMARKS");
        query.from("ALL_OBJECTS O", "ALL_TAB_COMMENTS C");
        query.where("O.OWNER = C.OWNER(+)");
        query.where("O.OBJECT_NAME = C.TABLE_NAME (+)");
        if (!StringUtils.isEmpty(schema)) {
            query.where(containsAny(schema, "%") ? "O.OWNER LIKE ? ESCAPE '/'" : "O.OWNER=?");
            parameters.add(schema);
        }
        if (!StringUtils.isEmpty(table)) {
            query.where(containsAny(schema, "%") ? "O.OBJECT_NAME LIKE ? ESCAPE '/'" : "O.OBJECT_NAME=?");
            parameters.add(table);
        }
        if (!isEmpty(tableTypes)) {
            String[] objectTypes = new String[tableTypes.size()];
            fill(objectTypes, "?");
            query.where(eqOrIn("O.OBJECT_TYPE", objectTypes));
            parameters.addAll(tableTypes);
        }
        return query;
    }
}
