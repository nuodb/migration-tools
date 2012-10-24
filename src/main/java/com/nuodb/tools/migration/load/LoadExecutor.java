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

import com.nuodb.tools.migration.dump.catalog.Catalog;
import com.nuodb.tools.migration.dump.catalog.CatalogImpl;
import com.nuodb.tools.migration.dump.catalog.CatalogReader;
import com.nuodb.tools.migration.jdbc.JdbcServices;
import com.nuodb.tools.migration.jdbc.JdbcServicesImpl;
import com.nuodb.tools.migration.jdbc.connection.ConnectionProvider;
import com.nuodb.tools.migration.spec.ConnectionSpec;
import com.nuodb.tools.migration.spec.DriverManagerConnectionSpec;
import com.nuodb.tools.migration.spec.FormatSpec;
import com.nuodb.tools.migration.spec.FormatSpecBase;
import com.nuodb.tools.migration.spec.LoadSpec;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;

/**
 * @author Sergey Bushik
 */
public class LoadExecutor {

    protected final Log log = LogFactory.getLog(getClass());

    public void execute(LoadSpec loadSpec) throws LoadException {
        execute(createJdbcServices(loadSpec.getConnectionSpec()),
                createCatalog(loadSpec.getInputSpec()).openReader(), loadSpec.getInputSpec());
    }

    public void execute(JdbcServices jdbcServices, CatalogReader reader, FormatSpec inputSpec) throws LoadException {
        try {
            doExecute(jdbcServices, reader, inputSpec);
        } catch (SQLException exception) {
            doTranslate(exception);
        } finally {
            reader.close();
        }
    }

    protected void doExecute(JdbcServices jdbcServices, CatalogReader reader, FormatSpec inputSpec) throws SQLException {
        ConnectionProvider connectionProvider = jdbcServices.getConnectionProvider();
        Connection connection = connectionProvider.getConnection();
        try {
            // TODO: implement
        } finally {
            connectionProvider.closeConnection(connection);
        }
    }

    protected void doTranslate(SQLException exception) {
        throw new LoadException(exception);
    }

    protected JdbcServicesImpl createJdbcServices(ConnectionSpec connectionSpec) {
        return new JdbcServicesImpl((DriverManagerConnectionSpec) connectionSpec);
    }

    protected Catalog createCatalog(FormatSpec inputSpec) {
        return new CatalogImpl(inputSpec.getPath());
    }

    public static void main(String[] args) throws LoadException {
        DriverManagerConnectionSpec connectionSpec = new DriverManagerConnectionSpec();
        connectionSpec.setDriver("com.mysql.jdbc.Driver");
        connectionSpec.setUrl("jdbc:mysql://localhost:3306/test");
        connectionSpec.setUsername("root");

        FormatSpec inputSpec = new FormatSpecBase();
        inputSpec.setPath("/tmp/test/dump.cat");
        inputSpec.setAttributes(new HashMap<String, String>() {
            {
                put("xml.row.element", "row");
                put("xml.document.element", "rows");
                put("csv.quote", "\"");
                put("csv.delimiter", ",");
                put("csv.quoting", "false");
                put("csv.escape", "|");
            }
        });
        LoadSpec loadSpec = new LoadSpec();
        loadSpec.setConnectionSpec(connectionSpec);
        loadSpec.setInputSpec(inputSpec);
        new LoadExecutor().execute(loadSpec);
    }
}
