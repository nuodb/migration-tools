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

import com.nuodb.migration.jdbc.metadata.Column;
import com.nuodb.migration.jdbc.metadata.Database;
import com.nuodb.migration.jdbc.metadata.MetaDataType;
import com.nuodb.migration.jdbc.metadata.Table;
import com.nuodb.migration.jdbc.model.ValueModelList;

import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;

import static com.nuodb.migration.jdbc.JdbcUtils.close;
import static com.nuodb.migration.jdbc.model.ValueModelFactory.createValueModelList;

/**
 * @author Sergey Bushik
 */
public class ColumnReader extends MetaDataReaderBase {

    public ColumnReader() {
        super(MetaDataType.COLUMN);
    }

    @Override
    public void read(DatabaseInspector inspector, Database database, DatabaseMetaData metaData) throws SQLException {
        for (Table table : database.listTables()) {
            readColumns(inspector, database, metaData, table);
        }
    }

    public void readColumns(DatabaseInspector inspector, Database database, DatabaseMetaData metaData,
                            Table table) throws SQLException {
        ResultSet columns = metaData.getColumns(inspector.getCatalog(), inspector.getSchema(), table.getName(), null);
        try {
            ValueModelList valueModelList = createValueModelList(columns.getMetaData());
            while (columns.next()) {
                table = database.createCatalog(columns.getString("TABLE_CAT")).createSchema(
                        columns.getString("TABLE_SCHEM")).createTable(columns.getString("TABLE_NAME"));

                Column column = table.createColumn(columns.getString("COLUMN_NAME"));
                column.setTypeCode(columns.getInt("DATA_TYPE"));
                column.setTypeName(columns.getString("TYPE_NAME"));
                int columnSize = columns.getInt("COLUMN_SIZE");
                column.setSize(columnSize);
                column.setPrecision(columnSize);
                column.setDefaultValue(columns.getString("COLUMN_DEF"));
                column.setScale(columns.getInt("DECIMAL_DIGITS"));
                column.setComment(columns.getString("REMARKS"));
                column.setPosition(columns.getInt("ORDINAL_POSITION"));
                String nullable = columns.getString("IS_NULLABLE");
                column.setNullable("YES".equals(nullable));
                String autoIncrement = valueModelList.get("IS_AUTOINCREMENT") != null ? columns.getString(
                        "IS_AUTOINCREMENT") : null;
                column.setAutoIncrement("YES".equals(autoIncrement));
            }
        } finally {
            close(columns);
        }
    }
}
