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
package com.nuodb.migrator.jdbc.metadata.generator;

import com.google.common.io.Files;
import com.google.common.io.NullOutputStream;
import com.nuodb.migrator.jdbc.connection.ConnectionProvider;
import com.nuodb.migrator.jdbc.dialect.NuoDBDialect;
import com.nuodb.migrator.jdbc.session.SessionFactories;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.File;
import java.sql.Connection;
import java.sql.Statement;
import java.util.Collection;

import static com.google.common.collect.Lists.newArrayList;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * @author Sergey Bushik
 */
public class ScriptExporterTest {

    private Collection<Script> scripts;

    @BeforeMethod
    public void setUp() {
        scripts = newArrayList(new Script("CREATE TABLE \"users\" (\"used_id\" INTEGER);"),
                new Script("CREATE TABLE \"links\" (\"link_id\" INTEGER);"));
    }

    @DataProvider(name = "exportScripts")
    public Object[][] createExportScriptsData() throws Exception {
        File dir = Files.createTempDir();
        dir.deleteOnExit();
        File file = new File(dir, "schema.sql");
        file.deleteOnExit();

        ConnectionProvider connectionProvider = mock(ConnectionProvider.class);
        Connection connection = mock(Connection.class);
        when(connectionProvider.getConnection()).thenReturn(connection);

        Statement statement = mock(Statement.class);
        when(connection.createStatement()).thenReturn(statement);

        return new Object[][] { { new FileScriptExporter(file) },
                { new ConnectionScriptExporter(SessionFactories
                        .newSessionFactory(connectionProvider, new NuoDBDialect(), false).openSession()) },
                { new WriterScriptExporter(new NullOutputStream()) } };
    }

    @Test(dataProvider = "exportScripts")
    public void testExportScripts(ScriptExporter scriptExporter) throws Exception {
        scriptExporter.open();
        scriptExporter.exportScripts(scripts);
        scriptExporter.close();
    }
}
