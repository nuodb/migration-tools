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
package com.nuodb.migrator.dump;

import com.nuodb.migrator.backup.writer.BackupWriter;
import com.nuodb.migrator.backup.format.FormatFactory;
import com.nuodb.migrator.backup.format.value.ValueFormatRegistryResolver;
import com.nuodb.migrator.jdbc.connection.ConnectionProvider;
import com.nuodb.migrator.jdbc.connection.ConnectionProviderFactory;
import com.nuodb.migrator.jdbc.dialect.Dialect;
import com.nuodb.migrator.jdbc.dialect.DialectResolver;
import com.nuodb.migrator.jdbc.dialect.NuoDBDialect;
import com.nuodb.migrator.jdbc.metadata.Column;
import com.nuodb.migrator.jdbc.metadata.Database;
import com.nuodb.migrator.jdbc.metadata.DatabaseInfo;
import com.nuodb.migrator.jdbc.metadata.Table;
import com.nuodb.migrator.jdbc.metadata.inspector.InspectionManager;
import com.nuodb.migrator.job.JobExecution;
import com.nuodb.migrator.job.JobExecutor;
import com.nuodb.migrator.spec.DumpJobSpec;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Spy;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.Types;
import java.util.Map;

import static com.google.common.collect.Maps.newHashMap;
import static com.nuodb.migrator.jdbc.metadata.Identifier.EMPTY;
import static com.nuodb.migrator.job.JobExecutors.createJobExecutor;
import static org.mockito.BDDMockito.*;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.MockitoAnnotations.initMocks;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;

/**
 * @author Sergey Bushik
 */
public class DumpJobTest {

    @Mock
    private InspectionManager inspectionManager;
    @Mock
    private ConnectionProvider connectionProvider;
    @Mock
    private ConnectionProviderFactory connectionProviderFactory;
    @Mock
    private DialectResolver dialectResolver;
    @Mock
    private FormatFactory formatFactory;
    @Mock
    private ValueFormatRegistryResolver valueFormatRegistryResolver;
    @Mock
    private Dialect dialect;
    @Mock
    private Connection connection;
    @Spy
    @InjectMocks
    private DumpJob dumpJob = new DumpJob(new DumpJobSpec());
    @Spy
    @InjectMocks
    private BackupWriter backupWriter = new BackupWriter();

    private JobExecutor jobExecutor;
    private Map<Object, Object> jobContext;

    @BeforeMethod
    public void setUp() throws Exception {
        initMocks(this);
        DatabaseMetaData databaseMetaData = mock(DatabaseMetaData.class);
        given(databaseMetaData.getDatabaseProductName()).willReturn("NuoDB");
        given(connection.getMetaData()).willReturn(databaseMetaData);

        willDoNothing().given(dumpJob).init();
        willDoNothing().given(dumpJob).execute();

        jobContext = newHashMap();
        jobExecutor = createJobExecutor(dumpJob);
    }

    @Test
    public void validateConnectionProvider() throws Exception {
        dumpJob.setConnectionProviderFactory(null);
        jobExecutor.execute(jobContext);
        verifyInit();
    }

    @Test
    public void validateDialectResolver() throws Exception {
        dumpJob.setDialectResolver(null);
        jobExecutor.execute(jobContext);
        verifyInit();
    }

    @Test
    public void validateValueFormatRegistryResolver() throws Exception {
        dumpJob.setValueFormatRegistryResolver(null);
        jobExecutor.execute(jobContext);
        verifyInit();
    }

    @Test
    public void validateFormatFactory() throws Exception {
        dumpJob.setFormatFactory(null);
        jobExecutor.execute(jobContext);
        verifyInit();
    }

    private void verifyInit() throws Exception {
        assertNull(jobExecutor.getJobStatus().getFailure());
        verify(dumpJob).init(any(JobExecution.class));
        verify(dumpJob, times(1)).execute();
    }

    @Test
    public void testExecuteInSession() throws Throwable {
        Database database = new Database();
        database.setDialect(new NuoDBDialect(new DatabaseInfo("NuoDB")));

        Table table = database.addCatalog(EMPTY).addSchema("schema").addTable("table");
        Column column1 = table.addColumn("column1");
        column1.setTypeCode(Types.BIGINT);
        Column column2 = table.addColumn("column2");
        column2.setTypeCode(Types.LONGVARCHAR);

        willReturn(backupWriter).given(dumpJob).getBackupWriter();

        jobExecutor.execute(jobContext);

        Throwable failure = jobExecutor.getJobStatus().getFailure();
        if (failure != null) {
            throw failure;
        }
    }
}
