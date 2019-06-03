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

import com.nuodb.migrator.jdbc.metadata.Column;
import com.nuodb.migrator.jdbc.metadata.Table;
import com.nuodb.migrator.jdbc.model.FieldFactory;
import com.nuodb.migrator.jdbc.type.JdbcTypeDesc;

import java.sql.ResultSet;
import java.sql.SQLException;

import static com.nuodb.migrator.jdbc.metadata.DefaultValue.valueOf;
import static com.nuodb.migrator.jdbc.metadata.MetaDataType.COLUMN;
import static com.nuodb.migrator.jdbc.metadata.inspector.InspectionResultsUtils.addTable;
import static com.nuodb.migrator.utils.StringUtils.isEmpty;

/**
 * @author Sergey Bushik
 */
public class SimpleColumnInspector extends TableInspectorBase<Table, TableInspectionScope> {

    public SimpleColumnInspector() {
        super(COLUMN, TableInspectionScope.class);
    }

    @Override
    protected ResultSet openResultSet(InspectionContext inspectionContext, TableInspectionScope tableInspectionScope)
            throws SQLException {
        return inspectionContext.getConnection().getMetaData().getColumns(tableInspectionScope.getCatalog(),
                tableInspectionScope.getSchema(), tableInspectionScope.getTable(), null);
    }

    @Override
    protected void processResultSet(InspectionContext inspectionContext, ResultSet columns) throws SQLException {
        InspectionResults inspectionResults = inspectionContext.getInspectionResults();
        while (columns.next()) {
            Table table = addTable(inspectionResults, columns.getString("TABLE_CAT"), columns.getString("TABLE_SCHEM"),
                    columns.getString("TABLE_NAME"));
            Column column = table.addColumn(columns.getString("COLUMN_NAME"));
            processColumn(inspectionContext, columns, column);
            inspectionResults.addObject(column);
        }
    }

    protected void processColumn(InspectionContext inspectionContext, ResultSet columns, Column column)
            throws SQLException {
        JdbcTypeDesc typeDescAlias = inspectionContext.getDialect().getJdbcTypeAlias(columns.getInt("DATA_TYPE"),
                columns.getString("TYPE_NAME"));
        column.setTypeCode(typeDescAlias.getTypeCode());
        column.setTypeName(typeDescAlias.getTypeName());

        int columnSize = columns.getInt("COLUMN_SIZE");
        column.setSize((long) columnSize);
        column.setPrecision(columnSize);
        column.setScale(columns.getInt("DECIMAL_DIGITS"));

        String comment = columns.getString("REMARKS");
        column.setComment(isEmpty(comment) ? null : comment);
        column.setPosition(columns.getInt("ORDINAL_POSITION"));
        String autoIncrement = FieldFactory.newFieldList(columns.getMetaData()).get("IS_AUTOINCREMENT") != null
                ? columns.getString("IS_AUTOINCREMENT")
                : null;
        column.setAutoIncrement("YES".equals(autoIncrement));
        column.setNullable("YES".equals(columns.getString("IS_NULLABLE")));
        column.setDefaultValue(valueOf(columns.getString("COLUMN_DEF")));
    }

    @Override
    protected boolean supportsScope(TableInspectionScope tableInspectionScope) {
        return tableInspectionScope.getTable() != null;
    }
}
