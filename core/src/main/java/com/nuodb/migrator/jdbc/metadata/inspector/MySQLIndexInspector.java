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

import static com.google.common.collect.Lists.newArrayList;
import static com.nuodb.migrator.jdbc.query.QueryUtils.union;
import static com.nuodb.migrator.utils.StringUtils.isEmpty;
import static org.slf4j.LoggerFactory.getLogger;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;

import org.slf4j.Logger;

import com.nuodb.migrator.jdbc.metadata.Index;
import com.nuodb.migrator.jdbc.query.ParameterizedQuery;
import com.nuodb.migrator.jdbc.query.Query;
import com.nuodb.migrator.jdbc.query.SelectQuery;
import com.nuodb.migrator.utils.StringUtils;

/**
 * @author Sergey Bushik
 */
public class MySQLIndexInspector extends SimpleIndexInspector {

    private static final String PRIMARY = "PRIMARY";

    @Override
    protected void processIndex(InspectionContext inspectionContext, ResultSet indexes, Index index)
            throws SQLException {
        super.processIndex(inspectionContext, indexes, index);
        if (StringUtils.equals(index.getName(), PRIMARY)) {
            index.setPrimary(true);
        }
        /* Extract the Index type from result set. */
        index.setType(indexes.getString("INDEX_TYPE"));
    }

    /**
     * This method is overridden to build a query to fetch information from
     * MySQL INFORMATION_SCHEMA.STATISTICS and INFORMATION_SCHEMA.COLUMNS tables
     * including all index types
     */
    @Override
    protected Query createQuery(InspectionContext inspectionContext, TableInspectionScope tableInspectionScope) {
        SelectQuery statisticsIndex = new SelectQuery();
        Collection<String> parameters = newArrayList();
        statisticsIndex.columns("S.TABLE_CATALOG ", "S.TABLE_SCHEMA AS TABLE_CAT ", "NULL AS TABLE_SCHEM ",
                "S.TABLE_NAME", "S.NON_UNIQUE", "S.INDEX_SCHEMA", "S.INDEX_NAME", "S.INDEX_TYPE", "1 AS TYPE",
                "S.SEQ_IN_INDEX AS ORDINAL_POSITION", "S.COLUMN_NAME", "S.COLLATION AS ASC_OR_DESC", "S.CARDINALITY",
                "S.SUB_PART", "NULL AS FILTER_CONDITION");
        statisticsIndex.from("INFORMATION_SCHEMA.STATISTICS S");
        statisticsIndex.join("INFORMATION_SCHEMA.COLUMNS C",
                "C.COLUMN_NAME = S.COLUMN_NAME AND C.TABLE_NAME = S.TABLE_NAME AND S.TABLE_SCHEMA = C.TABLE_SCHEMA");
        String schema = tableInspectionScope.getCatalog();
        if (!isEmpty(schema)) {
            statisticsIndex.where("S.TABLE_SCHEMA=?");
            parameters.add(schema);
        }
        String table = tableInspectionScope.getTable();
        if (!isEmpty(table)) {
            statisticsIndex.where("S.TABLE_NAME=?");
            parameters.add(table);
        }
        statisticsIndex.orderBy("INDEX_NAME", "SEQ_IN_INDEX");
        return new ParameterizedQuery(union(statisticsIndex, null), parameters);
    }
}
