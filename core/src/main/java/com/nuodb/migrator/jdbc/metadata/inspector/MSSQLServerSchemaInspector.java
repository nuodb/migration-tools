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
import com.nuodb.migrator.jdbc.metadata.Catalog;
import com.nuodb.migrator.jdbc.query.StatementCallback;
import com.nuodb.migrator.jdbc.query.StatementFactory;
import com.nuodb.migrator.jdbc.query.StatementTemplate;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Iterator;

import static com.google.common.collect.Lists.newArrayList;
import static com.nuodb.migrator.jdbc.JdbcUtils.close;
import static com.nuodb.migrator.jdbc.metadata.MetaDataType.SCHEMA;
import static com.nuodb.migrator.jdbc.metadata.inspector.InspectionResultsUtils.addSchema;
import static com.nuodb.migrator.jdbc.query.QueryUtils.orderBy;
import static com.nuodb.migrator.jdbc.query.QueryUtils.where;
import static java.lang.String.format;
import static java.sql.ResultSet.CONCUR_READ_ONLY;
import static java.sql.ResultSet.TYPE_FORWARD_ONLY;
import static java.util.Collections.singleton;
import static org.apache.commons.lang3.StringUtils.isEmpty;

/**
 * @author Sergey Bushik
 */
public class MSSQLServerSchemaInspector extends InspectorBase<Catalog, SchemaInspectionScope> {

    private static final String QUERY =
            "SELECT NAME AS 'TABLE_SCHEM',\n" +
            "CASE WHEN NAME IN ('DBO', 'GUEST', 'INFORMATION_SCHEMA', 'SYS', 'DB_OWNER', 'DB_ACCESSADMIN',\n" +
            "                   'DB_SECURITYADMIN', 'DB_DDLADMIN', 'DB_BACKUPOPERATOR', 'DB_DATAREADER',\n" +
            "                   'DB_DATAWRITER', 'DB_DENYDATAREADER', 'DB_DENYDATAWRITER') THEN NULL\n" +
            "ELSE %s END AS 'TABLE_CATALOG' FROM %s";

    public MSSQLServerSchemaInspector() {
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
                newArrayList(Iterables.transform(catalogs, new Function<Catalog, SchemaInspectionScope>() {
                    @Override
                    public SchemaInspectionScope apply(Catalog catalog) {
                        return new SchemaInspectionScope(catalog.getName(), null);
                    }
                })));
    }


    protected void inspectScopes(final InspectionContext inspectionContext,
                                 Collection<SchemaInspectionScope> inspectionScopes) throws SQLException {
        for (SchemaInspectionScope inspectionScope : inspectionScopes) {
            final Collection<String> parameters = newArrayList();
            String catalog;
            if (isEmpty(inspectionScope.getCatalog())) {
                catalog = "DB_NAME()";
            } else {
                catalog = "?";
                parameters.add(inspectionScope.getCatalog());
            }
            String table;
            if (isEmpty(inspectionScope.getCatalog())) {
                table = "SYS.SCHEMAS";
            } else {
                table = inspectionScope.getCatalog() + ".SYS.SCHEMAS";
            }
            final StringBuilder query = new StringBuilder(format(QUERY, catalog, table));
            if (!isEmpty(inspectionScope.getSchema())) {
                where(query, newArrayList("NAME LIKE ?"));
                parameters.add(inspectionScope.getSchema());
            }
            orderBy(query, newArrayList("2", "1"));
            StatementTemplate template = new StatementTemplate(inspectionContext.getConnection());
            template.execute(
                    new StatementFactory<PreparedStatement>() {
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
    }

    protected void inspect(InspectionContext inspectionContext, ResultSet schemas) throws SQLException {
        InspectionResults inspectionResults = inspectionContext.getInspectionResults();
        try {
            while (schemas.next()) {
                addSchema(inspectionResults, schemas.getString("TABLE_CATALOG"), schemas.getString("TABLE_SCHEM"));
            }
        } finally {
            close(schemas);
        }
    }
}
