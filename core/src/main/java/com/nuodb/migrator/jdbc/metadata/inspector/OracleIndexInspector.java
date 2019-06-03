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
import com.nuodb.migrator.jdbc.metadata.Table;
import com.nuodb.migrator.jdbc.query.ParameterizedQuery;
import com.nuodb.migrator.jdbc.query.Query;
import com.nuodb.migrator.jdbc.query.SelectQuery;
import com.nuodb.migrator.jdbc.query.StatementAction;
import com.nuodb.migrator.jdbc.query.StatementFactory;
import com.nuodb.migrator.jdbc.query.StatementTemplate;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;

import static com.google.common.collect.Lists.newArrayList;
import static com.nuodb.migrator.jdbc.query.QueryUtils.union;
import static com.nuodb.migrator.utils.StringUtils.equalsIgnoreCase;
import static com.nuodb.migrator.utils.StringUtils.isEmpty;
import static org.apache.commons.lang3.StringUtils.containsAny;

/**
 * @author Sergey Bushik
 */
public class OracleIndexInspector extends SimpleIndexInspector {

    private static final String FUNCTION_BASED_NORMAL = "FUNCTION-BASED NORMAL";
    private static final String FUNCTION_BASED_BITMAP = "FUNCTION-BASED BITMAP";

    @Override
    protected Query createQuery(InspectionContext inspectionContext, TableInspectionScope tableInspectionScope) {
        SelectQuery statisticsIndex = new SelectQuery();
        Collection<String> parameters = newArrayList();
        statisticsIndex.columns("NULL AS TABLE_CAT", "OWNER AS TABLE_SCHEM", "TABLE_NAME", "0 AS NON_UNIQUE",
                "NULL AS INDEX_QUALIFIER", "NULL AS INDEX_NAME", "0 AS TYPE", "NULL AS INDEX_TYPE",
                "0 AS ORDINAL_POSITION", "NULL AS COLUMN_NAME", "NULL AS ASC_OR_DESC", "NUM_ROWS AS CARDINALITY",
                "BLOCKS AS PAGES", "NULL AS FILTER_CONDITION");
        statisticsIndex.from("ALL_TABLES");
        String schema = tableInspectionScope.getSchema();
        if (!isEmpty(schema)) {
            statisticsIndex.where(containsAny(schema, "%") ? "OWNER LIKE ? ESCAPE '/'" : "OWNER=?");
            parameters.add(schema);
        }
        String table = tableInspectionScope.getTable();
        if (!isEmpty(table)) {
            statisticsIndex.where("TABLE_NAME=?");
            parameters.add(table);
        }
        SelectQuery clusteredIndex = new SelectQuery();
        clusteredIndex.columns("NULL AS TABLE_CAT", "I.OWNER AS TABLE_SCHEM", "I.TABLE_NAME",
                "DECODE(I.UNIQUENESS, 'UNIQUE', 0, 1) AS NON_UNIQUE", "NULL AS INDEX_QUALIFIER", "I.INDEX_NAME",
                "1 AS TYPE", "I.INDEX_TYPE AS INDEX_TYPE", "C.COLUMN_POSITION AS ORDINAL_POSITION", "C.COLUMN_NAME",
                "NULL AS ASC_OR_DESC", "I.DISTINCT_KEYS AS CARDINALITY", "I.LEAF_BLOCKS AS PAGES",
                "NULL AS FILTER_CONDITION");
        clusteredIndex.from("ALL_INDEXES I");
        clusteredIndex.join("ALL_IND_COLUMNS C", "C.INDEX_NAME = I.INDEX_NAME AND C.TABLE_OWNER = I.TABLE_OWNER AND "
                + "I.TABLE_NAME = I.TABLE_NAME AND C.INDEX_OWNER = I.OWNER");
        if (!isEmpty(schema)) {
            clusteredIndex.where(containsAny(schema, "%") ? "I.OWNER LIKE ? ESCAPE '/'" : "I.OWNER=?");
            parameters.add(schema);
        }
        if (!isEmpty(table)) {
            clusteredIndex.where("I.TABLE_NAME=?");
            parameters.add(table);
        }
        clusteredIndex.orderBy("NON_UNIQUE", "TYPE", "INDEX_NAME", "ORDINAL_POSITION");
        return new ParameterizedQuery(union(statisticsIndex, clusteredIndex), parameters);
    }

    @Override
    protected String getExpression(InspectionContext inspectionContext, ResultSet indexes, final Index index,
            String column) throws SQLException {
        String indexType = indexes.getString("INDEX_TYPE");
        String expression = null;
        if (equalsIgnoreCase(indexType, FUNCTION_BASED_NORMAL) || equalsIgnoreCase(indexType, FUNCTION_BASED_BITMAP)) {
            StatementTemplate template = new StatementTemplate(inspectionContext.getConnection());
            expression = template.executeStatement(new StatementFactory<PreparedStatement>() {
                @Override
                public PreparedStatement createStatement(Connection connection) throws SQLException {
                    SelectQuery expressions = new SelectQuery();
                    expressions.column("COLUMN_EXPRESSION");
                    expressions.from("ALL_IND_EXPRESSIONS");
                    expressions.where("TABLE_OWNER = ? AND TABLE_NAME = ? AND INDEX_NAME = ?");
                    return connection.prepareStatement(expressions.toString());
                }
            }, new StatementAction<PreparedStatement, String>() {
                @Override
                public String executeStatement(PreparedStatement statement) throws SQLException {
                    Table table = index.getTable();
                    statement.setString(1, table.getSchema().getName());
                    statement.setString(2, table.getName());
                    statement.setString(3, index.getName());
                    ResultSet expressions = statement.executeQuery();
                    String expression = null;
                    if (expressions.next()) {
                        expression = expressions.getString(1);
                    }
                    return expression;
                }
            });
        }
        return expression;
    }
}
