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
package com.nuodb.migrator.jdbc.metadata.inspector;

import com.nuodb.migrator.jdbc.metadata.Column;
import com.nuodb.migrator.jdbc.metadata.DefaultValue;
import com.nuodb.migrator.jdbc.metadata.Table;
import com.nuodb.migrator.jdbc.model.ColumnFactory;
import com.nuodb.migrator.jdbc.type.JdbcTypeDesc;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;

import static com.nuodb.migrator.jdbc.JdbcUtils.close;
import static com.nuodb.migrator.jdbc.metadata.DefaultValue.valueOf;
import static com.nuodb.migrator.jdbc.metadata.MetaDataType.COLUMN;
import static com.nuodb.migrator.jdbc.metadata.inspector.InspectionResultsUtils.addTable;

/**
 * @author Sergey Bushik
 */
public class SimpleColumnInspector extends TableInspectorBase<Table, TableInspectionScope> {

    public SimpleColumnInspector() {
        super(COLUMN, TableInspectionScope.class);
    }

    @Override
    protected Collection<? extends TableInspectionScope> createInspectionScopes(Collection<? extends Table> tables) {
        return createTableInspectionScopes(tables);
    }

    @Override
    protected void inspectScopes(final InspectionContext inspectionContext,
                                 final Collection<? extends TableInspectionScope> inspectionScopes) throws SQLException {
        for (TableInspectionScope inspectionScope : inspectionScopes) {
            ResultSet columns = inspectionContext.getConnection().getMetaData().getColumns(
                    inspectionScope.getCatalog(), inspectionScope.getSchema(), inspectionScope.getTable(), null);
            InspectionResults inspectionResults = inspectionContext.getInspectionResults();
            try {
                while (columns.next()) {
                    inspectionResults.addObject(getColumn(inspectionContext, columns));
                }
            } finally {
                close(columns);
            }
        }
    }

    protected Column getColumn(InspectionContext inspectionContext, ResultSet columns) throws SQLException {
        InspectionResults inspectionResults = inspectionContext.getInspectionResults();
        Table table = addTable(inspectionResults, columns.getString("TABLE_CAT"),
                columns.getString("TABLE_SCHEM"), columns.getString("TABLE_NAME"));
        Column column = table.addColumn(columns.getString("COLUMN_NAME"));
        initColumn(inspectionContext, columns, column);
        return column;
    }

    protected void initColumn(InspectionContext inspectionContext, ResultSet columns,
                              Column column) throws SQLException {
        initTypeDesc(inspectionContext, columns, column);
        initComment(columns, column);
        initPosition(columns, column);
        initAutoIncrement(inspectionContext, column, columns);
        initNullable(inspectionContext, column, columns);
        initDefaultValue(inspectionContext, column, columns);
    }

    protected void initTypeDesc(InspectionContext inspectionContext, ResultSet columns,
                                Column column) throws SQLException {
        JdbcTypeDesc typeDescAlias = inspectionContext.getDialect().getJdbcTypeAlias(
                columns.getInt("DATA_TYPE"), columns.getString("TYPE_NAME"));
        column.setTypeCode(typeDescAlias.getTypeCode());
        column.setTypeName(typeDescAlias.getTypeName());

        int columnSize = columns.getInt("COLUMN_SIZE");
        column.setSize(columnSize);
        column.setPrecision(columnSize);
        column.setScale(columns.getInt("DECIMAL_DIGITS"));
    }

    protected void initComment(ResultSet columns, Column column) throws SQLException {
        column.setComment(columns.getString("REMARKS"));
    }

    protected void initPosition(ResultSet columns, Column column) throws SQLException {
        column.setPosition(columns.getInt("ORDINAL_POSITION"));
    }

    protected void initAutoIncrement(InspectionContext inspectionContext, Column column,
                                     ResultSet columns) throws SQLException {
        column.setAutoIncrement(isAutoIncrement(inspectionContext, column, columns));
    }

    protected boolean isAutoIncrement(InspectionContext inspectionContext, Column column,
                                      ResultSet columns) throws SQLException {
        String autoIncrement = ColumnFactory.createColumnList(columns.getMetaData()).get("IS_AUTOINCREMENT") != null ?
                columns.getString("IS_AUTOINCREMENT") : null;
        return "YES".equals(autoIncrement);
    }

    protected void initNullable(InspectionContext inspectionContext, Column column,
                                ResultSet columns) throws SQLException {
        column.setNullable(isNullable(inspectionContext, column, columns));
    }

    protected boolean isNullable(InspectionContext inspectionContext, Column column,
                                 ResultSet columns) throws SQLException {
        return "YES".equals(columns.getString("IS_NULLABLE"));
    }

    protected void initDefaultValue(InspectionContext inspectionContext, Column column,
                                    ResultSet columns) throws SQLException {
        column.setDefaultValue(getDefaultValue(inspectionContext, column, columns));
    }

    protected DefaultValue getDefaultValue(InspectionContext inspectionContext, Column column,
                                           ResultSet columns) throws SQLException {
        return valueOf(columns.getString("COLUMN_DEF"));
    }

    @Override
    protected boolean supports(TableInspectionScope inspectionScope) {
        return inspectionScope.getTable() != null;
    }
}
