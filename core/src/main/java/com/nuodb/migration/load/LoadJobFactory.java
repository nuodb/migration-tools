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
package com.nuodb.migration.load;

import com.google.common.collect.Maps;
import com.nuodb.migration.jdbc.connection.ConnectionProvider;
import com.nuodb.migration.jdbc.connection.JdbcConnectionProvider;
import com.nuodb.migration.jdbc.dialect.DatabaseDialectResolver;
import com.nuodb.migration.jdbc.dialect.DatabaseDialectResolverImpl;
import com.nuodb.migration.job.JobExecutor;
import com.nuodb.migration.job.JobExecutors;
import com.nuodb.migration.job.JobFactory;
import com.nuodb.migration.job.TraceJobExecutionListener;
import com.nuodb.migration.resultset.catalog.Catalog;
import com.nuodb.migration.resultset.catalog.CatalogImpl;
import com.nuodb.migration.resultset.format.ResultSetFormatFactory;
import com.nuodb.migration.resultset.format.ResultSetFormatFactoryImpl;
import com.nuodb.migration.resultset.format.jdbc.JdbcTypeValueFormatRegistryResolver;
import com.nuodb.migration.resultset.format.jdbc.JdbcTypeValueFormatRegistryResolverImpl;
import com.nuodb.migration.spec.*;

/**
 * @author Sergey Bushik
 */
public class LoadJobFactory implements JobFactory<LoadJob> {

    private LoadSpec loadSpec;
    private DatabaseDialectResolver databaseDialectResolver =
            new DatabaseDialectResolverImpl();
    private ResultSetFormatFactory resultSetFormatFactory =
            new ResultSetFormatFactoryImpl();
    private JdbcTypeValueFormatRegistryResolver jdbcTypeValueFormatRegistryResolver =
            new JdbcTypeValueFormatRegistryResolverImpl();

    @Override
    public LoadJob createJob() {
        FormatSpec inputSpec = loadSpec.getInputSpec();

        LoadJob job = new LoadJob();
        job.setConnectionProvider(createConnectionProvider(loadSpec.getTargetSpec()));
        job.setAttributes(inputSpec.getAttributes());
        job.setCatalog(createCatalog(inputSpec.getPath()));
        job.setTimeZone(loadSpec.getTimeZone());
        job.setDatabaseDialectResolver(databaseDialectResolver);
        job.setResultSetFormatFactory(resultSetFormatFactory);
        job.setJdbcTypeValueFormatRegistryResolver(jdbcTypeValueFormatRegistryResolver);
        return job;
    }

    protected ConnectionProvider createConnectionProvider(ConnectionSpec connectionSpec) {
        return new JdbcConnectionProvider((JdbcConnectionSpec) connectionSpec, false);
    }

    protected Catalog createCatalog(String path) {
        return new CatalogImpl(path);
    }

    public LoadSpec getLoadSpec() {
        return loadSpec;
    }

    public void setLoadSpec(LoadSpec loadSpec) {
        this.loadSpec = loadSpec;
    }

    public DatabaseDialectResolver getDatabaseDialectResolver() {
        return databaseDialectResolver;
    }

    public void setDatabaseDialectResolver(DatabaseDialectResolver databaseDialectResolver) {
        this.databaseDialectResolver = databaseDialectResolver;
    }

    public ResultSetFormatFactory getResultSetFormatFactory() {
        return resultSetFormatFactory;
    }

    public void setResultSetFormatFactory(ResultSetFormatFactory resultSetFormatFactory) {
        this.resultSetFormatFactory = resultSetFormatFactory;
    }

    public JdbcTypeValueFormatRegistryResolver getJdbcTypeValueFormatRegistryResolver() {
        return jdbcTypeValueFormatRegistryResolver;
    }

    public void setJdbcTypeValueFormatRegistryResolver(
            JdbcTypeValueFormatRegistryResolver jdbcTypeValueFormatRegistryResolver) {
        this.jdbcTypeValueFormatRegistryResolver = jdbcTypeValueFormatRegistryResolver;
    }

    public static void main(String[] args) {
        LoadJobFactory jobFactory = new LoadJobFactory();
        jobFactory.setLoadSpec(new LoadSpec() {
            {
                JdbcConnectionSpec connectionSpec = new JdbcConnectionSpec();
                connectionSpec.setDriverClassName("com.mysql.jdbc.Driver");
                connectionSpec.setUrl("jdbc:mysql://localhost:3306/enron-load");
                connectionSpec.setUsername("root");
                setTargetSpec(connectionSpec);

                FormatSpecBase inputSpec = new FormatSpecBase();
                inputSpec.setPath("/tmp/test/dump.cat");
                setInputSpec(inputSpec);
            }
        });
        JobExecutor executor = JobExecutors.createJobExecutor(jobFactory.createJob());
        executor.addJobExecutionListener(new TraceJobExecutionListener());
        executor.execute(Maps.<String, Object>newHashMap());
    }
}
