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
import com.nuodb.migrator.jdbc.metadata.Schema;
import com.nuodb.migrator.jdbc.query.ParameterizedQuery;
import com.nuodb.migrator.jdbc.query.Query;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;

import static com.google.common.collect.Lists.newArrayList;
import static com.nuodb.migrator.jdbc.metadata.MetaDataType.SCHEMA;
import static com.nuodb.migrator.jdbc.metadata.inspector.InspectionResultsUtils.addSchema;
import static com.nuodb.migrator.jdbc.query.Queries.newQuery;
import static com.nuodb.migrator.jdbc.query.QueryUtils.*;
import static java.util.Collections.singleton;
import static org.apache.commons.lang3.StringUtils.containsAny;

/**
 * @author Sergey Bushik
 */
public class NuoDBSchemaInspector extends ManagedInspectorBase<Catalog, SchemaInspectionScope> {

    private static final String QUERY = "SELECT DISTINCT SCHEMA FROM SYSTEM.TABLES";

    public NuoDBSchemaInspector() {
        super(SCHEMA, SchemaInspectionScope.class);
    }

    @Override
    protected Query createQuery(InspectionContext inspectionContext, SchemaInspectionScope inspectionScope) {
        final Collection<String> filters = newArrayList();
        final Collection<Object> parameters = newArrayList();
        String schemaName = inspectionScope.getSchema();
        if (schemaName != null) {
            filters.add(containsAny(schemaName, "%_") ? "SCHEMA LIKE ?" : "SCHEMA=?");
            parameters.add(schemaName);
        }
        final StringBuilder query = new StringBuilder(QUERY);
        where(query, filters, "AND");
        orderBy(query, singleton("SCHEMA"), ASC);
        return new ParameterizedQuery(newQuery(query.toString()), parameters);
    }

    @Override
    protected void processResultSet(InspectionContext inspectionContext, ResultSet schemas) throws SQLException {
        InspectionResults inspectionResults = inspectionContext.getInspectionResults();
        while (schemas.next()) {
            Schema schema = addSchema(inspectionResults, null, schemas.getString("SCHEMA"));
            inspectionResults.addObject(schema);
        }
    }

    @Override
    protected SchemaInspectionScope createInspectionScope(Catalog catalog) {
        return new SchemaInspectionScope();
    }
}
