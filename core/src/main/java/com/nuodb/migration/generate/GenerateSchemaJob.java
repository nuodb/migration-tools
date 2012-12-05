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

import com.google.common.collect.Lists;
import com.nuodb.migration.jdbc.connection.ConnectionProvider;
import com.nuodb.migration.jdbc.connection.ConnectionServices;
import com.nuodb.migration.jdbc.dialect.NuoDBDialect;
import com.nuodb.migration.jdbc.metadata.Database;
import com.nuodb.migration.jdbc.metadata.generator.*;
import com.nuodb.migration.job.JobBase;
import com.nuodb.migration.job.JobExecution;
import com.nuodb.migration.spec.ResourceSpec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;

import static com.nuodb.migration.jdbc.JdbcUtils.close;
import static com.nuodb.migration.utils.ValidationUtils.isNotNull;
import static java.lang.String.format;

/**
 * @author Sergey Bushik
 */
public class GenerateSchemaJob extends JobBase {

    public static final String OUTPUT_ENCODING = "UTF-8";

    protected final Logger logger = LoggerFactory.getLogger(getClass());

    private ConnectionProvider sourceConnectionProvider;
    private ConnectionProvider targetConnectionProvider;
    private ResourceSpec outputSpec;

    @Override
    public void execute(JobExecution execution) throws Exception {
        validate();
        execution(new GenerateSchemaJobExecution(execution));
    }

    protected void validate() {
        isNotNull(getSourceConnectionProvider(), "Source connection provider is required");
    }

    protected void execution(GenerateSchemaJobExecution execution) throws Exception {
        ConnectionServices sourceConnectionServices = getSourceConnectionProvider().getConnectionServices();
        ConnectionServices targetConnectionServices = null;
        if (getTargetConnectionProvider() != null) {
            targetConnectionServices = getTargetConnectionProvider().getConnectionServices();
        }
        try {
            execution.setSourceConnectionServices(sourceConnectionServices);
            execution.setTargetConnectionServices(targetConnectionServices);
            generate(execution);
        } finally {
            close(sourceConnectionServices);
            close(targetConnectionServices);
        }
    }

    protected void generate(GenerateSchemaJobExecution execution) throws Exception {
        ConnectionServices sourceConnectionServices = execution.getSourceConnectionServices();
        Database sourceDatabase = sourceConnectionServices.createDatabaseInspector().inspect();

        Collection<ScriptExporter> scriptExporters = Lists.newArrayList();
        if (execution.getTargetConnectionServices() != null) {
            if (logger.isDebugEnabled()) {
                logger.debug("Exporting schema to the target database");
            }
            scriptExporters.add(new ConnectionScriptExporter(
                    execution.getTargetConnectionServices()
            ));
        }
        if (outputSpec != null) {
            if (logger.isDebugEnabled()) {
                logger.debug(format("Exporting schema to file %s", outputSpec.getPath()));
            }
            scriptExporters.add(new FileScriptExporter(
                    outputSpec.getPath(), OUTPUT_ENCODING));
        }
        // Fallback to standard output if neither target connection nor target file are specified
        if (scriptExporters.isEmpty()) {
            scriptExporters.add(StdOutScriptExporter.INSTANCE);
        }

        ScriptGenerate scriptGenerate = new ScriptGenerate();
        scriptGenerate.setDialect(new NuoDBDialect());
        scriptGenerate.setScriptExporter(new CompositeScriptExporter(scriptExporters));
        scriptGenerate.generate(sourceDatabase);
    }

    public ConnectionProvider getSourceConnectionProvider() {
        return sourceConnectionProvider;
    }

    public void setSourceConnectionProvider(ConnectionProvider sourceConnectionProvider) {
        this.sourceConnectionProvider = sourceConnectionProvider;
    }

    public ConnectionProvider getTargetConnectionProvider() {
        return targetConnectionProvider;
    }

    public void setTargetConnectionProvider(ConnectionProvider targetConnectionProvider) {
        this.targetConnectionProvider = targetConnectionProvider;
    }

    public ResourceSpec getOutputSpec() {
        return outputSpec;
    }

    public void setOutputSpec(ResourceSpec outputSpec) {
        this.outputSpec = outputSpec;
    }
}
