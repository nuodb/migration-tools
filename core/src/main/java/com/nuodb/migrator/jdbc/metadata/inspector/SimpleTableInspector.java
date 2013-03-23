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

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.nuodb.migrator.jdbc.metadata.Schema;
import com.nuodb.migrator.jdbc.metadata.Table;

import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;

import static com.nuodb.migrator.jdbc.JdbcUtils.close;
import static com.nuodb.migrator.jdbc.metadata.MetaDataType.SCHEMA;
import static com.nuodb.migrator.jdbc.metadata.MetaDataType.TABLE;
import static com.nuodb.migrator.jdbc.metadata.inspector.InspectionResultsUtils.addTable;

/**
 * @author Sergey Bushik
 */
public class SimpleTableInspector extends TableInspectorBase<Schema, TableInspectionScope> {

    public SimpleTableInspector() {
        super(TABLE, SCHEMA, TableInspectionScope.class);
    }

    @Override
    protected Collection<? extends TableInspectionScope> createInspectionScopes(Collection<? extends Schema> schemas) {
        return Lists.newArrayList(Iterables.transform(schemas, new Function<Schema, TableInspectionScope>() {
            @Override
            public TableInspectionScope apply(Schema schema) {
                return new TableInspectionScope(schema.getCatalog().getName(), schema.getName());
            }
        }));
    }

    @Override
    protected void inspectScopes(InspectionContext inspectionContext,
                                 Collection<? extends TableInspectionScope> inspectionScopes) throws SQLException {
        for (TableInspectionScope inspectionScope : inspectionScopes) {
            InspectionResults inspectionResults = inspectionContext.getInspectionResults();
            DatabaseMetaData databaseMetaData = inspectionContext.getConnection().getMetaData();
            ResultSet tables = databaseMetaData.getTables(
                    inspectionScope.getCatalog(), inspectionScope.getSchema(),
                    inspectionScope.getTable(), inspectionScope.getTableTypes());
            try {
                while (tables.next()) {
                    Table table = addTable(inspectionResults,
                            tables.getString("TABLE_CAT"), tables.getString("TABLE_SCHEM"),
                            tables.getString("TABLE_NAME"));
                    table.setComment(tables.getString("REMARKS"));
                    table.setType(tables.getString("TABLE_TYPE"));
                }
            } finally {
                close(tables);
            }
        }
    }
}
