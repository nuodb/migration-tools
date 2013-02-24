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
package com.nuodb.migration.jdbc.metadata.inspector;

import com.nuodb.migration.jdbc.metadata.*;

import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;

import static com.nuodb.migration.jdbc.JdbcUtils.close;
import static com.nuodb.migration.jdbc.metadata.Identifier.valueOf;
import static com.nuodb.migration.jdbc.metadata.inspector.InspectionResultsUtils.addTable;
import static java.sql.DatabaseMetaData.tableIndexStatistic;

/**
 * @author Sergey Bushik
 */
public class SimpleIndexInspector extends TableInspectorBase<Table, TableInspectionScope> {

    public SimpleIndexInspector() {
        super(MetaDataType.INDEX, TableInspectionScope.class);
    }

    @Override
    protected Collection<? extends TableInspectionScope> createInspectionScopes(Collection<? extends Table> tables) {
        return createTableInspectionScopes(tables);
    }

    @Override
    protected void inspectScopes(final InspectionContext inspectionContext,
                                 final Collection<? extends TableInspectionScope> inspectionScopes) throws SQLException {
        DatabaseMetaData databaseMetaData = inspectionContext.getConnection().getMetaData();
        for (TableInspectionScope inspectionScope : inspectionScopes) {
            ResultSet indexes = databaseMetaData.getIndexInfo(
                    inspectionScope.getCatalog(), inspectionScope.getSchema(), inspectionScope.getTable(), false, true);
            try {
                while (indexes.next()) {
                    inspect(inspectionContext, indexes);
                }
            } finally {
                close(indexes);
            }
        }
    }

    protected void inspect(InspectionContext inspectionContext, ResultSet indexes) throws SQLException {
        if (indexes.getShort("TYPE") == tableIndexStatistic) {
            return;
        }
        InspectionResults inspectionResults = inspectionContext.getInspectionResults();
        Table table = addTable(inspectionResults, indexes.getString("TABLE_CAT"),
                indexes.getString("TABLE_SCHEM"), indexes.getString("TABLE_NAME"));

        Identifier identifier = valueOf(indexes.getString("INDEX_NAME"));
        Index index = table.getIndex(identifier);
        if (index == null) {
            index = new Index(identifier);
            table.addIndex(index);
            index.setUnique(!indexes.getBoolean("NON_UNIQUE"));
            index.setFilterCondition(indexes.getString("FILTER_CONDITION"));
            index.setSortOrder(getSortOrder(indexes.getString("ASC_OR_DESC")));
            inspectionResults.addObject(index);
        }
        String expression = indexes.getString("COLUMN_NAME");
        if (isExpression(inspectionContext, index, expression)) {
            index.setExpression(expression);
        } else {
            index.addColumn(table.createColumn(expression), indexes.getInt("ORDINAL_POSITION"));
        }
    }

    protected boolean isExpression(InspectionContext inspectionContext, Index index, String expression) throws SQLException {
        return false;
    }

    public static SortOrder getSortOrder(String ascOrDesc) {
        if (ascOrDesc != null) {
            return ascOrDesc.equals("A") ? SortOrder.ASC : SortOrder.DESC;
        } else {
            return null;
        }
    }

    @Override
    protected boolean supports(TableInspectionScope inspectionScope) {
        return inspectionScope.getTable() != null;
    }
}
