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

import com.nuodb.tools.migration.jdbc.JdbcServices;
import com.nuodb.tools.migration.jdbc.JdbcServicesImpl;
import com.nuodb.tools.migration.job.JobExecutor;
import com.nuodb.tools.migration.job.JobExecutors;
import com.nuodb.tools.migration.job.JobFactory;
import com.nuodb.tools.migration.job.TraceJobExecutionListener;
import com.nuodb.tools.migration.result.catalog.ResultCatalog;
import com.nuodb.tools.migration.result.catalog.ResultCatalogImpl;
import com.nuodb.tools.migration.result.format.ResultFormatFactory;
import com.nuodb.tools.migration.result.format.ResultFormatFactoryImpl;
import com.nuodb.tools.migration.result.format.csv.CsvAttributes;
import com.nuodb.tools.migration.spec.*;

import java.util.Collections;
import java.util.HashMap;

/**
 * @author Sergey Bushik
 */
public class LoadJobFactory implements JobFactory<LoadJob> {

    private LoadSpec loadSpec;
    private ResultFormatFactory resultFormatFactory = new ResultFormatFactoryImpl();

    @Override
    public LoadJob createJob() {
        ConnectionSpec connectionSpec = loadSpec.getConnectionSpec();
        FormatSpec inputSpec = loadSpec.getInputSpec();

        LoadJob job = new LoadJob();
        job.setJdbcServices(createJdbcServices(connectionSpec));
        job.setInputType(inputSpec.getType());
        job.setInputAttributes(inputSpec.getAttributes());
        job.setResultCatalog(createResultCatalog(inputSpec.getPath()));
        job.setResultFormatFactory(resultFormatFactory);
        return job;
    }

    protected JdbcServices createJdbcServices(ConnectionSpec connectionSpec) {
        return new JdbcServicesImpl((DriverManagerConnectionSpec) connectionSpec);
    }

    protected ResultCatalog createResultCatalog(String path) {
        return new ResultCatalogImpl(path);
    }

    public LoadSpec getLoadSpec() {
        return loadSpec;
    }

    public void setLoadSpec(LoadSpec loadSpec) {
        this.loadSpec = loadSpec;
    }

    public ResultFormatFactory getResultFormatFactory() {
        return resultFormatFactory;
    }

    public void setResultFormatFactory(ResultFormatFactory resultFormatFactory) {
        this.resultFormatFactory = resultFormatFactory;
    }

    public static void main(String[] args) throws LoadException {
        DriverManagerConnectionSpec connectionSpec = new DriverManagerConnectionSpec();
        connectionSpec.setDriver("com.mysql.jdbc.Driver");
        connectionSpec.setUrl("jdbc:mysql://localhost:3306/enron-load");
        connectionSpec.setUsername("root");
        connectionSpec.setCatalog("enron-load");

        FormatSpec inputSpec = new FormatSpecBase();
        inputSpec.setType(CsvAttributes.TYPE);
        inputSpec.setPath("/tmp/test/dump.cat");
        inputSpec.setAttributes(new HashMap<String, String>() {
            {
                put("csv.quote", "\"");
                put("csv.delimiter", ",");
                put("csv.quoting", "false");
                put("csv.escape", "|");
            }
        });
        LoadSpec loadSpec = new LoadSpec();
        loadSpec.setConnectionSpec(connectionSpec);
        loadSpec.setInputSpec(inputSpec);

        LoadJobFactory jobFactory = new LoadJobFactory();
        jobFactory.setLoadSpec(loadSpec);

        JobExecutor executor = JobExecutors.createJobExecutor(jobFactory.createJob());
        executor.addJobExecutionListener(new TraceJobExecutionListener());
        executor.execute(Collections.<String, Object>emptyMap());
    }
}
