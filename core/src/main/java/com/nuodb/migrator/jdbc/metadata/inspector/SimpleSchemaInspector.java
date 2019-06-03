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

import com.nuodb.migrator.jdbc.metadata.Catalog;
import com.nuodb.migrator.jdbc.model.Field;
import com.nuodb.migrator.jdbc.model.FieldFactory;
import com.nuodb.migrator.jdbc.model.FieldList;

import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;

import static com.nuodb.migrator.jdbc.metadata.MetaDataType.SCHEMA;
import static com.nuodb.migrator.jdbc.metadata.inspector.InspectionResultsUtils.addSchema;

/**
 * @author Sergey Bushik
 */
public class SimpleSchemaInspector extends ManagedInspectorBase<Catalog, SchemaInspectionScope> {

    public SimpleSchemaInspector() {
        super(SCHEMA, SchemaInspectionScope.class);
    }

    @Override
    protected SchemaInspectionScope createInspectionScope(Catalog catalog) {
        return new SchemaInspectionScope(catalog.getName(), null);
    }

    @Override
    protected ResultSet openResultSet(InspectionContext inspectionContext, SchemaInspectionScope schemaInspectionScope)
            throws SQLException {
        DatabaseMetaData metaData = inspectionContext.getConnection().getMetaData();
        return schemaInspectionScope.getCatalog() != null
                ? metaData.getSchemas(schemaInspectionScope.getCatalog(), null)
                : metaData.getSchemas();
    }

    @Override
    protected void processResultSet(InspectionContext inspectionContext, ResultSet schemas) throws SQLException {
        InspectionResults inspectionResults = inspectionContext.getInspectionResults();
        FieldList<Field> fields = FieldFactory.newFieldList(schemas);
        while (schemas.next()) {
            addSchema(inspectionResults,
                    fields.get("TABLE_CATALOG") != null ? schemas.getString("TABLE_CATALOG") : null,
                    schemas.getString("TABLE_SCHEM"));
        }
    }
}
