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

import com.nuodb.migration.jdbc.metadata.Catalog;
import com.nuodb.migration.jdbc.metadata.MetaDataType;
import com.nuodb.migration.jdbc.query.QueryUtils;
import com.nuodb.migration.jdbc.query.StatementCallback;
import com.nuodb.migration.jdbc.query.StatementCreator;
import com.nuodb.migration.jdbc.query.StatementTemplate;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Iterator;

import static com.google.common.collect.Lists.newArrayList;
import static com.nuodb.migration.jdbc.metadata.inspector.InspectionResultsUtils.addSchema;
import static com.nuodb.migration.jdbc.metadata.inspector.NuoDBInspectorUtils.validateInspectionScope;
import static java.sql.ResultSet.CONCUR_READ_ONLY;
import static java.sql.ResultSet.TYPE_FORWARD_ONLY;
import static java.util.Collections.singleton;
import static org.apache.commons.lang3.StringUtils.containsAny;

/**
 * @author Sergey Bushik
 */
public class NuoDBSchemaInspector extends InspectorBase<Catalog, SchemaInspectionScope> {

    private static final String QUERY = "SELECT DISTINCT SCHEMA FROM SYSTEM.TABLES";

    public NuoDBSchemaInspector() {
        super(MetaDataType.SCHEMA, SchemaInspectionScope.class);
    }

    @Override
    public void inspectObjects(InspectionContext inspectionContext,
                               Collection<? extends Catalog> catalogs) throws SQLException {
        inspectScope(inspectionContext, new SchemaInspectionScope());
    }

    @Override
    public void inspectScope(final InspectionContext inspectionContext,
                             final SchemaInspectionScope inspectionScope) throws SQLException {
        validateInspectionScope(inspectionScope);

        final Collection<String> filters = newArrayList();
        final Collection<String> parameters = newArrayList();
        String schemaName = inspectionScope.getSchema();
        if (schemaName != null) {
            filters.add(containsAny(schemaName, "%_") ? "SCHEMA LIKE ?" : "SCHEMA=?");
            parameters.add(schemaName);
        }
        final StringBuilder query = new StringBuilder(QUERY);
        QueryUtils.where(query, filters, "AND");
        QueryUtils.orderBy(query, singleton("SCHEMA"), "ASC");

        StatementTemplate template = new StatementTemplate(inspectionContext.getConnection());
        template.execute(
                new StatementCreator<PreparedStatement>() {
                    @Override
                    public PreparedStatement create(Connection connection) throws SQLException {
                        return connection.prepareStatement(query.toString(), TYPE_FORWARD_ONLY, CONCUR_READ_ONLY);
                    }
                },
                new StatementCallback<PreparedStatement>() {
                    @Override
                    public void execute(PreparedStatement statement) throws SQLException {
                        int parameter = 1;
                        for (Iterator<String> iterator = parameters.iterator(); iterator.hasNext(); ) {
                            statement.setString(parameter++, iterator.next());
                        }
                        inspect(inspectionContext, statement.executeQuery());
                    }
                }
        );
    }

    private void inspect(InspectionContext inspectionContext, ResultSet schemas) throws SQLException {
        InspectionResults inspectionResults = inspectionContext.getInspectionResults();
        while (schemas.next()) {
            addSchema(inspectionResults, null, schemas.getString("SCHEMA"));
        }
    }
}
