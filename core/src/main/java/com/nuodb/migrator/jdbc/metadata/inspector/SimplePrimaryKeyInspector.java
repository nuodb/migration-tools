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

import com.nuodb.migrator.jdbc.metadata.Identifier;
import com.nuodb.migrator.jdbc.metadata.MetaDataType;
import com.nuodb.migrator.jdbc.metadata.PrimaryKey;
import com.nuodb.migrator.jdbc.metadata.Table;

import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;

import static com.nuodb.migrator.jdbc.JdbcUtils.close;
import static com.nuodb.migrator.jdbc.metadata.inspector.InspectionResultsUtils.addTable;

/**
 * @author Sergey Bushik
 */
public class SimplePrimaryKeyInspector extends TableInspectorBase<Table, TableInspectionScope> {

    public SimplePrimaryKeyInspector() {
        super(MetaDataType.PRIMARY_KEY, TableInspectionScope.class);
    }

    @Override
    protected Collection<? extends TableInspectionScope> createInspectionScopes(Collection<? extends Table> tables) {
        return createTableInspectionScopes(tables);
    }

    @Override
    protected void inspectScopes(final InspectionContext inspectionContext,
                                 final Collection<? extends TableInspectionScope> inspectionScopes) throws SQLException {
        InspectionResults inspectionResults = inspectionContext.getInspectionResults();
        DatabaseMetaData databaseMetaData = inspectionContext.getConnection().getMetaData();
        for (TableInspectionScope inspectionScope : inspectionScopes) {
            ResultSet primaryKeys = databaseMetaData.getPrimaryKeys(
                    inspectionScope.getCatalog(), inspectionScope.getSchema(), inspectionScope.getTable());
            try {
                while (primaryKeys.next()) {
                    Table table = addTable(inspectionResults, primaryKeys.getString("TABLE_CAT"),
                            primaryKeys.getString("TABLE_SCHEM"), primaryKeys.getString("TABLE_NAME"));

                    final Identifier identifier = Identifier.valueOf(primaryKeys.getString("PK_NAME"));
                    PrimaryKey primaryKey = table.getPrimaryKey();
                    if (primaryKey == null) {
                        table.setPrimaryKey(primaryKey = new PrimaryKey(identifier));
                        inspectionResults.addObject(primaryKey);
                    }
                    primaryKey.addColumn(table.createColumn(primaryKeys.getString("COLUMN_NAME")),
                            primaryKeys.getInt("KEY_SEQ"));
                }
            } finally {
                close(primaryKeys);
            }
        }
    }

    @Override
    protected boolean supports(TableInspectionScope inspectionScope) {
        return inspectionScope.getTable() != null;
    }
}
