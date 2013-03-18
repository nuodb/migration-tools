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
package com.nuodb.migrator.dump;

import com.nuodb.migrator.jdbc.connection.ConnectionProvider;
import com.nuodb.migrator.jdbc.connection.ConnectionServices;
import com.nuodb.migrator.jdbc.dialect.DialectResolver;
import com.nuodb.migrator.jdbc.dialect.NuoDBDialect;
import com.nuodb.migrator.jdbc.dialect.SimpleDialectResolver;
import com.nuodb.migrator.jdbc.metadata.Column;
import com.nuodb.migrator.jdbc.metadata.Database;
import com.nuodb.migrator.jdbc.metadata.Table;
import com.nuodb.migrator.jdbc.query.Query;
import com.nuodb.migrator.jdbc.resolve.DatabaseInfo;
import com.nuodb.migrator.job.JobExecution;
import com.nuodb.migrator.job.JobExecutor;
import com.nuodb.migrator.resultset.catalog.Catalog;
import com.nuodb.migrator.resultset.format.FormatFactory;
import com.nuodb.migrator.resultset.format.SimpleFormatFactory;
import com.nuodb.migrator.resultset.format.csv.CsvAttributes;
import com.nuodb.migrator.resultset.format.value.SimpleValueFormatRegistryResolver;
import com.nuodb.migrator.resultset.format.value.ValueFormatRegistryResolver;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Spy;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.sql.*;
import java.util.HashMap;
import java.util.Map;

import static com.nuodb.migrator.jdbc.metadata.Identifier.EMPTY_IDENTIFIER;
import static com.nuodb.migrator.job.JobExecutors.createJobExecutor;
import static org.mockito.BDDMockito.*;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.MockitoAnnotations.initMocks;
import static org.testng.Assert.assertNotNull;

/**
 * @author Sergey Bushik
 */
public class DumpJobTest {

    @Mock
    private Catalog catalog;
    @Mock
    private ConnectionProvider connectionProvider;
    @Spy
    private DialectResolver dialectResolver = new SimpleDialectResolver();
    @Spy
    private FormatFactory formatFactory = new SimpleFormatFactory();
    @Spy
    private ValueFormatRegistryResolver valueFormatRegistryResolver = new SimpleValueFormatRegistryResolver();
    @Mock
    private Connection connection;
    @Mock
    private ConnectionServices connectionServices;
    @Spy
    @InjectMocks
    private DumpJob dumpJob = new DumpJob();

    private JobExecutor jobExecutor;
    private Map jobContext;

    @BeforeMethod
    public void setUp() throws Exception {
        initMocks(this);
        dumpJob.setOutputType(CsvAttributes.FORMAT_TYPE);

        DatabaseMetaData databaseMetaData = mock(DatabaseMetaData.class);
        given(databaseMetaData.getDatabaseProductName()).willReturn("NuoDB");
        given(connection.getMetaData()).willReturn(databaseMetaData);

        given(connectionServices.getConnection()).willReturn(connection);
        given(connectionProvider.getConnection()).willReturn(connection);
        given(connectionProvider.getConnectionServices()).willReturn(connectionServices);

        jobContext = new HashMap();
        jobExecutor = createJobExecutor(dumpJob);
    }

    @Test
    public void validateCatalog() throws Exception {
        dumpJob.setCatalog(null);
        jobExecutor.execute(jobContext);
        verifyValidate();
    }

    @Test
    public void validateConnectionProvider() throws Exception {
        dumpJob.setConnectionProvider(null);
        jobExecutor.execute(jobContext);
        verifyValidate();
    }

    @Test
    public void validateDialectResolver() throws Exception {
        dumpJob.setDialectResolver(null);
        jobExecutor.execute(jobContext);

        verifyValidate();
    }

    @Test
    public void validateValueFormatRegistryResolver() throws Exception {
        dumpJob.setValueFormatRegistryResolver(null);
        jobExecutor.execute(jobContext);

        verifyValidate();
    }

    @Test
    public void validateFormatFactory() throws Exception {
        dumpJob.setFormatFactory(null);
        jobExecutor.execute(jobContext);

        verifyValidate();
    }

    @Test
    public void validateOutputType() throws Exception {
        dumpJob.setOutputType(null);
        jobExecutor.execute(jobContext);
        verifyValidate();
    }

    private void verifyValidate() throws Exception {
        assertNotNull(jobExecutor.getJobStatus().getFailure());
        verify(dumpJob).initSessionContext(any(JobExecution.class));
        verify(dumpJob, never()).executeSessionContext(any(JobExecution.class));
    }

    @Test
    public void testExecuteInSession() throws Throwable {
        Database database = new Database();
        database.setDialect(new NuoDBDialect(new DatabaseInfo("NuoDB")));

        Table table = database.addCatalog(EMPTY_IDENTIFIER).addSchema("schema").addTable("table");
        Column column1 = table.addColumn("column1");
        column1.setTypeCode(Types.BIGINT);
        Column column2 = table.addColumn("column2");
        column2.setTypeCode(Types.LONGVARCHAR);

        willReturn(database).given(dumpJob).inspect(any(JobExecution.class));
        willDoNothing().given(dumpJob).dump(any(JobExecution.class), any(Query.class));

        jobExecutor.execute(jobContext);

        Throwable failure = jobExecutor.getJobStatus().getFailure();
        if (failure != null) {
            throw failure;
        }
    }

    public void test() throws SQLException {
        // dump prepared statement
        PreparedStatement preparedStatement = mock(PreparedStatement.class);
        given(connection.prepareStatement(anyString(), anyInt(), anyInt())).willReturn(preparedStatement);
    }

}
