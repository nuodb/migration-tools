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
import com.nuodb.migrator.jdbc.metadata.Schema;
import com.nuodb.migrator.jdbc.query.StatementCallback;
import com.nuodb.migrator.jdbc.query.StatementFactory;
import com.nuodb.migrator.jdbc.query.StatementTemplate;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;

import static com.nuodb.migrator.jdbc.metadata.MetaDataType.SCHEMA;
import static com.nuodb.migrator.jdbc.metadata.inspector.InspectionResultsUtils.addSchema;
import static java.util.Collections.singleton;

/**
 * @author Sergey Bushik
 */
public class PostgreSQLSchemaInspector extends InspectorBase<Catalog, SchemaInspectionScope> {

    private static final String QUERY =
            "SELECT NSPNAME AS TABLE_SCHEM FROM PG_CATALOG.PG_NAMESPACE\n" +
            "WHERE NSPNAME !~ '^PG_TOAST' AND NSPNAME !~ '^PG_TEMP' AND NSPNAME LIKE ? ORDER BY TABLE_SCHEM";

    public PostgreSQLSchemaInspector() {
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

    protected void inspectScopes(final InspectionContext inspectionContext,
                                 final Collection<SchemaInspectionScope> inspectionScopes) throws SQLException {
        StatementTemplate template = new StatementTemplate(inspectionContext.getConnection());
        template.execute(
                new StatementFactory<PreparedStatement>() {
                    @Override
                    public PreparedStatement create(Connection connection) throws SQLException {
                        return connection.prepareStatement(QUERY);
                    }
                }, new StatementCallback<PreparedStatement>() {
                    @Override
                    public void process(PreparedStatement statement) throws SQLException {
                        for (SchemaInspectionScope inspectionScope : inspectionScopes) {
                            String schema = inspectionScope.getSchema();
                            statement.setString(1, schema != null ? schema : "%");
                            inspect(inspectionContext, statement.executeQuery());
                        }
                    }
                }
        );
    }

    private void inspect(InspectionContext inspectionContext, ResultSet schemas) throws SQLException {
        InspectionResults inspectionResults = inspectionContext.getInspectionResults();
        while (schemas.next()) {
            Schema schema = addSchema(inspectionResults, null, schemas.getString("TABLE_SCHEM"));
            inspectionResults.addObject(schema);
        }
    }
}
