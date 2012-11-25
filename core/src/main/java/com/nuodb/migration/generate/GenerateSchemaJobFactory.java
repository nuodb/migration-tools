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
package com.nuodb.migration.generate;

import com.google.common.collect.Maps;
import com.nuodb.migration.jdbc.connection.ConnectionProvider;
import com.nuodb.migration.jdbc.connection.JdbcConnectionProvider;
import com.nuodb.migration.job.JobExecutor;
import com.nuodb.migration.job.JobExecutors;
import com.nuodb.migration.job.JobFactory;
import com.nuodb.migration.job.TraceJobExecutionListener;
import com.nuodb.migration.spec.ConnectionSpec;
import com.nuodb.migration.spec.JdbcConnectionSpec;
import com.nuodb.migration.spec.GenerateSchemaSpec;

/**
 * @author Sergey Bushik
 */
public class GenerateSchemaJobFactory implements JobFactory<GenerateSchemaJob> {

    private GenerateSchemaSpec generateSchemaSpec;

    @Override
    public GenerateSchemaJob createJob() {
        GenerateSchemaJob job = new GenerateSchemaJob();
        job.setSourceConnectionProvider(createConnectionProvider(generateSchemaSpec.getSourceConnectionSpec()));
        job.setTargetConnectionProvider(createConnectionProvider(generateSchemaSpec.getTargetConnectionSpec()));
        return job;
    }

    protected ConnectionProvider createConnectionProvider(ConnectionSpec connectionSpec) {
        return new JdbcConnectionProvider((JdbcConnectionSpec) connectionSpec, false);
    }

    public GenerateSchemaSpec getGenerateSchemaSpec() {
        return generateSchemaSpec;
    }

    public void setGenerateSchemaSpec(GenerateSchemaSpec generateSchemaSpec) {
        this.generateSchemaSpec = generateSchemaSpec;
    }

    public static void main(String[] args) {
        GenerateSchemaJobFactory jobFactory = new GenerateSchemaJobFactory();
        jobFactory.setGenerateSchemaSpec(new GenerateSchemaSpec() {
            {
                JdbcConnectionSpec sourceSpec = new JdbcConnectionSpec();
                sourceSpec.setDriverClassName("com.mysql.jdbc.Driver");
                sourceSpec.setUrl("jdbc:mysql://localhost:3306/enron-load");
                sourceSpec.setUsername("root");
                setSourceConnectionSpec(sourceSpec);

                JdbcConnectionSpec targetSpec = new JdbcConnectionSpec();
                targetSpec.setDriverClassName("com.nuodb.jdbc.Driver");
                targetSpec.setUrl("jdbc:com.nuodb://localhost/test");
                targetSpec.setUsername("dba");
                targetSpec.setPassword("goalie");
                setTargetConnectionSpec(targetSpec);
            }
        });
        JobExecutor executor = JobExecutors.createJobExecutor(jobFactory.createJob());
        executor.addJobExecutionListener(new TraceJobExecutionListener());
        executor.execute(Maps.<String, Object>newHashMap());
    }
}