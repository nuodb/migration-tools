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

import static com.nuodb.migration.jdbc.JdbcUtils.close;

/**
 * @author Sergey Bushik
 */
public class PrimaryKeyReader extends MetaDataReaderBase {

    public PrimaryKeyReader() {
        super(MetaDataType.PRIMARY_KEY);
    }

    @Override
    public void read(DatabaseInspector inspector, Database database, DatabaseMetaData metaData) throws SQLException {
        for (Table table : database.listTables()) {
            readPrimaryKeys(inspector, database, metaData, table);
        }
    }

    protected void readPrimaryKeys(DatabaseInspector inspector, Database database, DatabaseMetaData metaData,
                                   Table table) throws SQLException {
        ResultSet primaryKeys = metaData.getPrimaryKeys(inspector.getCatalog(), inspector.getSchema(), table.getName());
        try {
            while (primaryKeys.next()) {
                table = database.createCatalog(primaryKeys.getString("TABLE_CAT")).createSchema(
                        primaryKeys.getString("TABLE_SCHEM")).createTable(primaryKeys.getString("TABLE_NAME"));
                final Identifier identifier = Identifier.valueOf(primaryKeys.getString("PK_NAME"));
                PrimaryKey primaryKey = table.getPrimaryKey();
                if (primaryKey == null) {
                    table.setPrimaryKey(primaryKey = new PrimaryKey(identifier));
                }
                primaryKey.addColumn(table.createColumn(primaryKeys.getString("COLUMN_NAME")),
                        primaryKeys.getInt("KEY_SEQ"));
            }
        } finally {
            close(primaryKeys);
        }
    }
}
