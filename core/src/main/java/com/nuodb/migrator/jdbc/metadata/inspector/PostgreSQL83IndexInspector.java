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

import com.nuodb.migrator.jdbc.metadata.Index;
import com.nuodb.migrator.jdbc.query.ParameterizedQuery;
import com.nuodb.migrator.jdbc.query.Query;
import com.nuodb.migrator.jdbc.query.SelectQuery;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;

import static com.google.common.collect.Lists.newArrayList;
import static org.apache.commons.lang3.StringUtils.isEmpty;

/**
 * @author Sergey Bushik
 */
public class PostgreSQL83IndexInspector extends PostgreSQLIndexInspector {

    @Override
    protected Query createQuery(InspectionContext inspectionContext, TableInspectionScope tableInspectionScope) {
        SelectQuery query = new SelectQuery();
        Collection<Object> parameters = newArrayList();
        query.columns("NULL AS TABLE_CAT", "I.INDISPRIMARY AS PRIMARY");
        query.columns("N.NSPNAME AS TABLE_SCHEM", "CT.RELNAME AS TABLE_NAME");
        query.columns("NOT I.INDISUNIQUE AS NON_UNIQUE", "NULL AS INDEX_QUALIFIER");
        query.column("CI.RELNAME AS INDEX_NAME");
        query.column(
                "CASE I.INDISCLUSTERED WHEN TRUE THEN 1 ELSE CASE AM.AMNAME WHEN 'HASH' THEN 2 ELSE 3 END END AS TYPE");
        query.column("(I.KEYS).N AS ORDINAL_POSITION");
        query.column("PG_CATALOG.PG_GET_INDEXDEF(CI.OID, (I.KEYS).N, FALSE) AS COLUMN_NAME");
        query.column("CASE AM.AMCANORDER WHEN TRUE THEN "
                + "CASE I.INDOPTION [(I.KEYS).N - 1] & 1 WHEN 1 THEN 'D' ELSE 'A' END ELSE NULL END AS ASC_OR_DESC");
        query.columns("CI.RELTUPLES AS CARDINALITY", "CI.RELPAGES AS PAGES");
        query.column("PG_CATALOG.PG_GET_EXPR(I.INDPRED, I.INDRELID) AS FILTER_CONDITION");
        query.from("PG_CATALOG.PG_CLASS CT");
        query.innerJoin("PG_CATALOG.PG_NAMESPACE N", "CT.RELNAMESPACE = N.OID");
        query.innerJoin("(SELECT I.INDEXRELID, I.INDRELID, I.INDOPTION, I.INDISPRIMARY, I.INDISUNIQUE, "
                + "I.INDISCLUSTERED, I.INDPRED, I.INDEXPRS, INFORMATION_SCHEMA._PG_EXPANDARRAY(I.INDKEY) AS KEYS "
                + "FROM PG_CATALOG.PG_INDEX I) I", "CT.OID = I.INDRELID");
        query.innerJoin("PG_CATALOG.PG_CLASS CI", "CI.OID = I.INDEXRELID");
        query.innerJoin("PG_CATALOG.PG_AM AM", "CI.RELAM = AM.OID");
        String schema = tableInspectionScope.getSchema();
        if (!isEmpty(schema)) {
            query.where("N.NSPNAME=?");
            parameters.add(schema);
        }
        String table = tableInspectionScope.getTable();
        if (!isEmpty(table)) {
            query.where("CT.RELNAME=?");
            parameters.add(table);
        }
        query.orderBy("NON_UNIQUE", "TYPE", "INDEX_NAME", "ORDINAL_POSITION");
        return new ParameterizedQuery(query, parameters);
    }

    @Override
    protected void processIndex(InspectionContext inspectionContext, ResultSet indexes, Index index)
            throws SQLException {
        super.processIndex(inspectionContext, indexes, index);
        index.setPrimary(indexes.getBoolean("PRIMARY"));
    }
}
