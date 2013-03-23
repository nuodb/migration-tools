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
import com.nuodb.migrator.jdbc.metadata.Catalog;
import com.nuodb.migrator.jdbc.model.ValueModel;
import com.nuodb.migrator.jdbc.model.ValueModelList;

import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;

import static com.nuodb.migrator.jdbc.JdbcUtils.close;
import static com.nuodb.migrator.jdbc.metadata.MetaDataType.SCHEMA;
import static com.nuodb.migrator.jdbc.metadata.inspector.InspectionResultsUtils.addSchema;
import static com.nuodb.migrator.jdbc.model.ValueModelFactory.createValueModelList;
import static java.util.Collections.singleton;

/**
 * @author Sergey Bushik
 */
public class SimpleSchemaInspector extends InspectorBase<Catalog, SchemaInspectionScope> {

    public SimpleSchemaInspector() {
        super(SCHEMA, SchemaInspectionScope.class);
    }

    @Override
    public void inspectScope(InspectionContext inspectionContext,
                             SchemaInspectionScope inspectionScope) throws SQLException {
        inspectScopes(inspectionContext, singleton(inspectionScope));
    }

    @Override
    public void inspectObjects(InspectionContext inspectionContext,
                               Collection<? extends Catalog> catalogs) throws SQLException {
        inspectScopes(inspectionContext,
                Lists.newArrayList(Iterables.transform(catalogs, new Function<Catalog, SchemaInspectionScope>() {
                    @Override
                    public SchemaInspectionScope apply(Catalog catalog) {
                        return new SchemaInspectionScope(catalog.getName(), null);
                    }
                })));
    }

    protected void inspectScopes(InspectionContext inspectionContext,
                                 Collection<SchemaInspectionScope> inspectionScopes) throws SQLException {
        for (SchemaInspectionScope inspectionScope : inspectionScopes) {
            InspectionResults inspectionResults = inspectionContext.getInspectionResults();
            DatabaseMetaData databaseMetaData = inspectionContext.getConnection().getMetaData();
            ResultSet schemas = inspectionScope.getCatalog() != null ?
                    databaseMetaData.getSchemas(inspectionScope.getCatalog(), null) : databaseMetaData.getSchemas();
            ValueModelList<ValueModel> columns = createValueModelList(schemas);
            try {
                while (schemas.next()) {
                    addSchema(inspectionResults,
                            columns.get("TABLE_CATALOG") != null ? schemas.getString("TABLE_CATALOG") : null,
                            schemas.getString("TABLE_SCHEM"));
                }
            } finally {
                close(schemas);
            }
        }
    }
}
