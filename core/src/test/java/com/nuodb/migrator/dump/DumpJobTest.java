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
import com.nuodb.migrator.jdbc.dialect.DialectResolver;
import com.nuodb.migrator.job.JobExecutor;
import com.nuodb.migrator.resultset.catalog.Catalog;
import com.nuodb.migrator.resultset.format.value.ValueFormatRegistryResolver;
import com.nuodb.migrator.utils.ValidationException;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Spy;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.Map;

import static com.nuodb.migrator.job.JobExecutors.createJobExecutor;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;
import static org.mockito.MockitoAnnotations.initMocks;
import static org.testng.Assert.assertTrue;

/**
 * @author Sergey Bushik
 */
public class DumpJobTest {

    @Spy
    @InjectMocks
    private DumpJob dumpJob;
    @Mock
    private Catalog catalog;
    @Mock
    private ConnectionProvider connectionProvider;
    @Mock
    private DialectResolver dialectResolver;
    @Mock
    private ValueFormatRegistryResolver valueFormatRegistryResolver;

    private JobExecutor jobExecutor;
    private Map<String, Object> jobContext;

    @BeforeMethod
    public void setUp() {
        initMocks(this);

        dumpJob = spy(new DumpJob());
        jobContext = new HashMap<String, Object>();
        jobExecutor = createJobExecutor(dumpJob);
    }

    @Test
    public void testValidateCatalog() throws Exception {
        dumpJob.setCatalog(null);
        jobExecutor.execute(jobContext);

        verifyValidation();
    }

    @Test
    public void testValidateConnectionProvider() throws Exception {
        dumpJob.setConnectionProvider(null);
        jobExecutor.execute(jobContext);

        verifyValidation();
    }

    @Test
    public void testValidateDialectResolver() throws Exception {
        dumpJob.setDialectResolver(null);
        jobExecutor.execute(jobContext);

        verifyValidation();
    }

    @Test
    public void testValidateValueFormatRegistryResolver() throws Exception {
        dumpJob.setValueFormatRegistryResolver(null);
        jobExecutor.execute(jobContext);

        verifyValidation();
    }

    private void verifyValidation() throws Exception {
        assertTrue(jobExecutor.getJobStatus().getFailure() instanceof ValidationException);
        verify(dumpJob).validate();
        verify(dumpJob, never()).execute(any(DumpJobExecution.class));
    }
}
