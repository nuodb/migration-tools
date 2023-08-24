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
package com.nuodb.migrator.job;

import com.nuodb.migrator.MigratorException;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.Map;

import static com.nuodb.migrator.job.JobExecutors.createJobExecutor;
import static junit.framework.Assert.*;
import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.willThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

/**
 * @author Sergey Bushik
 */
public class JobExecutorTest {

    private Job job;
    private JobExecutor jobExecutor;
    private Map<Object, Object> context;

    @BeforeMethod
    public void setUp() {
        job = mock(Job.class);
        given(job.getName()).willReturn("mock");
        jobExecutor = createJobExecutor(job);

        context = new HashMap<Object, Object>();
    }

    @Test
    public void testExecute() throws Exception {
        JobStatus jobStatus = jobExecutor.getJobStatus();
        assertFalse(jobStatus.isRunning());

        assertTrue(jobExecutor.execute(context));

        verify(job).execute();
        assertEquals(jobStatus.getJobStatusType(), JobStatusType.FINISHED);
    }

    @Test
    public void testExecuteFailure() throws Exception {
        MigratorException failure = new MigratorException("Failure");
        willThrow(failure).given(job).execute();

        JobStatus jobStatus = jobExecutor.getJobStatus();
        assertFalse(jobStatus.isRunning());

        assertTrue(jobExecutor.execute(context));

        verify(job).execute();
        assertEquals(failure, jobStatus.getFailure());
        assertEquals(jobStatus.getJobStatusType(), JobStatusType.FINISHED);
    }
}
