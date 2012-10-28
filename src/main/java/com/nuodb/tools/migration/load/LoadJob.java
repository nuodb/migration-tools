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
package com.nuodb.tools.migration.load;

import com.nuodb.tools.migration.format.catalog.QueryEntryCatalog;
import com.nuodb.tools.migration.format.catalog.QueryEntryReader;
import com.nuodb.tools.migration.jdbc.JdbcServices;
import com.nuodb.tools.migration.jdbc.connection.ConnectionCallback;
import com.nuodb.tools.migration.jdbc.connection.ConnectionProvider;
import com.nuodb.tools.migration.jdbc.metamodel.Database;
import com.nuodb.tools.migration.jdbc.metamodel.DatabaseIntrospector;
import com.nuodb.tools.migration.job.JobBase;
import com.nuodb.tools.migration.job.JobExecution;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;

import static com.nuodb.tools.migration.jdbc.metamodel.ObjectType.*;

/**
 * @author Sergey Bushik
 */
public class LoadJob extends JobBase {

    private JdbcServices jdbcServices;
    private QueryEntryCatalog queryEntryCatalog;
    private String inputType;
    private Map<String, String> inputAttributes;

    @Override
    public void execute(final JobExecution execution) throws Exception {
        ConnectionProvider connectionProvider = jdbcServices.getConnectionProvider();
        connectionProvider.execute(new ConnectionCallback() {
            @Override
            public void execute(Connection connection) throws SQLException {
                DatabaseIntrospector databaseIntrospector = jdbcServices.getDatabaseIntrospector();
                databaseIntrospector.withObjectTypes(CATALOG, SCHEMA, TABLE, COLUMN);
                databaseIntrospector.withConnection(connection);
                Database database = databaseIntrospector.introspect();

                QueryEntryReader reader = queryEntryCatalog.openQueryEntryReader();
                try {
                    load(execution, connection, database, reader);
                } finally {
                    reader.close();
                }
            }
        });
    }

    protected void load(JobExecution execution, Connection connection, Database database, QueryEntryReader reader) {
        System.out.println("LoadJob.load");
    }

    public JdbcServices getJdbcServices() {
        return jdbcServices;
    }

    public void setJdbcServices(JdbcServices jdbcServices) {
        this.jdbcServices = jdbcServices;
    }

    public QueryEntryCatalog getQueryEntryCatalog() {
        return queryEntryCatalog;
    }

    public void setQueryEntryCatalog(QueryEntryCatalog queryEntryCatalog) {
        this.queryEntryCatalog = queryEntryCatalog;
    }

    public String getInputType() {
        return inputType;
    }

    public void setInputType(String inputType) {
        this.inputType = inputType;
    }

    public Map<String, String> getInputAttributes() {
        return inputAttributes;
    }

    public void setInputAttributes(Map<String, String> inputAttributes) {
        this.inputAttributes = inputAttributes;
    }
}
