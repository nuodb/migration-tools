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
package com.nuodb.tools.migration.dump;

import com.nuodb.tools.migration.dump.output.OutputFormat;
import com.nuodb.tools.migration.dump.output.OutputFormatLookup;
import com.nuodb.tools.migration.dump.output.OutputFormatLookupImpl;
import com.nuodb.tools.migration.dump.query.NativeQueryBuilder;
import com.nuodb.tools.migration.dump.query.Query;
import com.nuodb.tools.migration.dump.query.SelectQuery;
import com.nuodb.tools.migration.dump.query.SelectQueryBuilder;
import com.nuodb.tools.migration.jdbc.JdbcServices;
import com.nuodb.tools.migration.jdbc.JdbcServicesImpl;
import com.nuodb.tools.migration.jdbc.connection.ConnectionProvider;
import com.nuodb.tools.migration.jdbc.metamodel.Database;
import com.nuodb.tools.migration.jdbc.metamodel.DatabaseIntrospector;
import com.nuodb.tools.migration.jdbc.metamodel.Table;
import com.nuodb.tools.migration.spec.ConnectionSpec;
import com.nuodb.tools.migration.spec.DriverManagerConnectionSpec;
import com.nuodb.tools.migration.spec.DumpSpec;
import com.nuodb.tools.migration.spec.NativeQuerySpec;
import com.nuodb.tools.migration.spec.OutputSpec;
import com.nuodb.tools.migration.spec.SelectQuerySpec;
import com.nuodb.tools.migration.context.support.ApplicationSupport;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static com.nuodb.tools.migration.jdbc.metamodel.ObjectType.*;

/**
 * @author Sergey Bushik
 */
@SuppressWarnings("unchecked")
public class DumpWriter extends ApplicationSupport {

    protected final Log log = LogFactory.getLog(getClass());

    private OutputFormatLookup outputFormatLookup = new OutputFormatLookupImpl();

    public void dump(DumpSpec dumpSpec) throws SQLException {
        dump(dumpSpec.getConnectionSpec(), dumpSpec.getSelectQuerySpecs(), dumpSpec.getNativeQuerySpecs(), dumpSpec.getOutputSpec());
    }

    public void dump(ConnectionSpec connectionSpec, Collection<SelectQuerySpec> selectQuerySpecs,
                     Collection<NativeQuerySpec> nativeQuerySpecs, OutputSpec outputSpec) throws SQLException {
        dump(new JdbcServicesImpl((DriverManagerConnectionSpec) connectionSpec), selectQuerySpecs, nativeQuerySpecs, outputSpec);
    }

    public void dump(JdbcServices jdbcServices, Collection<SelectQuerySpec> selectQuerySpecs,
                     Collection<NativeQuerySpec> nativeQuerySpecs, OutputSpec outputSpec) throws SQLException {
        ConnectionProvider connectionProvider = jdbcServices.getConnectionProvider();
        Connection connection = connectionProvider.getConnection();
        try {
            Database database = introspect(jdbcServices, connection);
            DumpCatalog catalog = createDumpCatalog(outputSpec);
            catalog.open();
            for (Query query : createQueries(database, selectQuerySpecs, nativeQuerySpecs)) {
                OutputFormat format = getOutputFormatLookup().lookup(outputSpec.getType());
                format.setAttributes(outputSpec.getAttributes());
                format.setJdbcTypeExtractor(jdbcServices.getJdbcTypeExtractor());

                OutputStream output = catalog.addEntry(query, format.getType());
                format.setOutputStream(output);
                dump(connection, query, format);
                catalog.closeEntry(output);
            }
            catalog.close();
        } finally {
            connectionProvider.closeConnection(connection);
        }
    }

    protected Database introspect(JdbcServices jdbcServices, Connection connection) throws SQLException {
        DatabaseIntrospector introspector = jdbcServices.getDatabaseIntrospector();
        introspector.withConnection(connection);
        introspector.withObjectTypes(CATALOG, SCHEMA, TABLE, COLUMN);
        return introspector.introspect();
    }

    protected DumpCatalog createDumpCatalog(OutputSpec outputSpec) {
        return new DumpCatalog(outputSpec.getPath());
    }

    protected void dump(Connection connection, Query query, OutputFormat format) throws SQLException {
        if (log.isTraceEnabled()) {
            log.trace(String.format("Writing dump with %1$s", format.getClass().getName()));
        }
        if (log.isDebugEnabled()) {
            log.debug(String.format("Preparing SQL query: %1$s", query.toQuery()));
        }
        PreparedStatement statement = connection.prepareStatement(query.toQuery());
        ResultSet resultSet = statement.executeQuery();
        try {
            format.outputBegin(resultSet);
            while (resultSet.next()) {
                format.outputRow(resultSet);
            }
            format.outputEnd(resultSet);
        } catch (IOException e) {
            throw new DumpException(e);
        }
    }

    protected List<Query> createQueries(Database database, Collection<SelectQuerySpec> selectQuerySpecs,
                                        Collection<NativeQuerySpec> nativeQuerySpecs) {
        List<Query> queries = new ArrayList<Query>();
        if (selectQuerySpecs != null) {
            Collection<Table> tables = database.listTables();
            if (selectQuerySpecs.isEmpty()) {
                queries.addAll(createSelectQueries(tables, null));
            } else {
                for (SelectQuerySpec selectQuerySpec : selectQuerySpecs) {
                    queries.addAll(createSelectQueries(tables, selectQuerySpec));
                }
            }
        }
        if (nativeQuerySpecs != null) {
            for (NativeQuerySpec nativeQuerySpec : nativeQuerySpecs) {
                NativeQueryBuilder nativeQueryBuilder = new NativeQueryBuilder();
                nativeQueryBuilder.setQuery(nativeQuerySpec.getQuery());
                queries.add(nativeQueryBuilder.build());
            }
        }
        return queries;
    }

    protected List<SelectQuery> createSelectQueries(Collection<Table> tables, SelectQuerySpec selectQuerySpec) {
        List<SelectQuery> selectQueries = new ArrayList<SelectQuery>();
        String tableName = selectQuerySpec != null ? selectQuerySpec.getTable() : null;
        for (Table table : tables) {
            if (tableName == null || tableName.equals(table.getName().value())) {
                SelectQueryBuilder selectQueryBuilder = new SelectQueryBuilder();
                selectQueryBuilder.setTable(table);
                if (selectQuerySpec != null) {
                    selectQueryBuilder.setColumns(selectQuerySpec.getColumns());
                    selectQueryBuilder.addFilter(selectQuerySpec.getFilter());
                }
                selectQueries.add(selectQueryBuilder.build());
            }
        }
        return selectQueries;
    }

    public OutputFormatLookup getOutputFormatLookup() {
        return outputFormatLookup;
    }

    public void setOutputFormatLookup(OutputFormatLookup outputFormatLookup) {
        this.outputFormatLookup = outputFormatLookup;
    }
}
