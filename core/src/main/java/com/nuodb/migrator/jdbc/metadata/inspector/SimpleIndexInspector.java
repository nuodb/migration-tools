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

import com.nuodb.migrator.jdbc.metadata.Identifier;
import com.nuodb.migrator.jdbc.metadata.Index;
import com.nuodb.migrator.jdbc.metadata.SortOrder;
import com.nuodb.migrator.jdbc.metadata.Table;

import java.sql.ResultSet;
import java.sql.SQLException;

import static com.nuodb.migrator.jdbc.metadata.Identifier.valueOf;
import static com.nuodb.migrator.jdbc.metadata.MetaDataType.INDEX;
import static com.nuodb.migrator.jdbc.metadata.inspector.InspectionResultsUtils.addTable;
import static java.sql.DatabaseMetaData.tableIndexStatistic;

/**
 * @author Sergey Bushik
 */
@SuppressWarnings("UnusedParameters")
public class SimpleIndexInspector extends TableInspectorBase<Table, TableInspectionScope> {

    public SimpleIndexInspector() {
        super(INDEX, TableInspectionScope.class);
    }

    @Override
    protected ResultSet openResultSet(InspectionContext inspectionContext, TableInspectionScope tableInspectionScope)
            throws SQLException {
        return inspectionContext.getConnection().getMetaData().getIndexInfo(tableInspectionScope.getCatalog(),
                tableInspectionScope.getSchema(), tableInspectionScope.getTable(), false, true);
    }

    @Override
    protected void processResultSet(InspectionContext inspectionContext, ResultSet indexes) throws SQLException {
        InspectionResults inspectionResults = inspectionContext.getInspectionResults();
        while (indexes.next()) {
            if (indexes.getShort("TYPE") == tableIndexStatistic) {
                continue;
            }
            Table table = addTable(inspectionResults, indexes.getString("TABLE_CAT"), indexes.getString("TABLE_SCHEM"),
                    indexes.getString("TABLE_NAME"));
            Identifier identifier = valueOf(indexes.getString("INDEX_NAME"));
            Index index = table.hasIndex(identifier) ? table.getIndex(identifier)
                    : table.addIndex(new Index(identifier));
            processIndex(inspectionContext, indexes, index);
            inspectionResults.addObject(index);
        }
    }

    protected void processIndex(InspectionContext inspectionContext, ResultSet indexes, Index index)
            throws SQLException {
        index.setUnique(!indexes.getBoolean("NON_UNIQUE"));
        index.setFilterCondition(indexes.getString("FILTER_CONDITION"));
        index.setSortOrder(getSortOrder(inspectionContext, indexes, index, indexes.getString("ASC_OR_DESC")));

        String column = indexes.getString("COLUMN_NAME");
        String expression = getExpression(inspectionContext, indexes, index, column);
        if (expression != null) {
            index.setExpression(expression);
        } else {
            index.addColumn(index.getTable().addColumn(column), indexes.getInt("ORDINAL_POSITION"));
        }
    }

    protected String getExpression(InspectionContext inspectionContext, ResultSet indexes, Index index, String column)
            throws SQLException {
        return null;
    }

    protected SortOrder getSortOrder(InspectionContext inspectionContext, ResultSet indexes, Index index,
            String sortOrder) {
        if (sortOrder != null) {
            return sortOrder.equals("A") ? SortOrder.ASC : SortOrder.DESC;
        } else {
            return null;
        }
    }

    @Override
    protected boolean supportsScope(TableInspectionScope tableInspectionScope) {
        return tableInspectionScope.getTable() != null;
    }
}
