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
package com.nuodb.migration.schema;

import com.nuodb.migration.jdbc.connection.ConnectionProvider;
import com.nuodb.migration.jdbc.connection.ConnectionServices;
import com.nuodb.migration.jdbc.metadata.Database;
import com.nuodb.migration.jdbc.metadata.generator.ScriptExporter;
import com.nuodb.migration.jdbc.metadata.generator.ScriptGeneratorContext;
import com.nuodb.migration.job.JobBase;
import com.nuodb.migration.job.JobExecution;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;

import static com.nuodb.migration.jdbc.JdbcUtils.close;
import static com.nuodb.migration.utils.ValidationUtils.isNotNull;

/**
 * @author Sergey Bushik
 */
public class SchemaJob extends JobBase {

    protected final Logger logger = LoggerFactory.getLogger(getClass());

    private ScriptGeneratorContext scriptGeneratorContext;
    private ScriptExporter scriptExporter;
    private ConnectionProvider connectionProvider;
    private boolean failOnEmptyScripts;

    @Override
    public void execute(JobExecution execution) throws Exception {
        validate();
        execution(new SchemaJobExecution(execution));
    }

    protected void validate() {
        isNotNull(getConnectionProvider(), "Source connection provider is required");
        isNotNull(getScriptGeneratorContext(), "Script generator context is required");
        isNotNull(getScriptExporter(), "Script exporter is required");
    }

    protected void execution(SchemaJobExecution execution) throws Exception {
        ConnectionServices connectionServices = getConnectionProvider().getConnectionServices();
        try {
            execution.setConnectionServices(connectionServices);
            generate(execution);
        } finally {
            close(connectionServices);
        }
    }

    protected void generate(SchemaJobExecution execution) throws Exception {
        ConnectionServices connectionServices = execution.getConnectionServices();
        Database database = connectionServices.getDatabaseInspector().inspect();
        Collection<String> scripts = getScriptGeneratorContext().getScripts(database);
        if (isFailOnEmptyScripts() && scripts.isEmpty()) {
            throw new SchemaJobException("Schema scripts are empty");
        }
        ScriptExporter scriptExporter = getScriptExporter();
        try {
            scriptExporter.open();
            scriptExporter.exportScripts(scripts);
        } finally {
            scriptExporter.close();
        }
    }

    public boolean isFailOnEmptyScripts() {
        return failOnEmptyScripts;
    }

    public void setFailOnEmptyScripts(boolean failOnEmptyScripts) {
        this.failOnEmptyScripts = failOnEmptyScripts;
    }

    public ConnectionProvider getConnectionProvider() {
        return connectionProvider;
    }

    public void setConnectionProvider(ConnectionProvider connectionProvider) {
        this.connectionProvider = connectionProvider;
    }

    public ScriptGeneratorContext getScriptGeneratorContext() {
        return scriptGeneratorContext;
    }

    public void setScriptGeneratorContext(ScriptGeneratorContext scriptGeneratorContext) {
        this.scriptGeneratorContext = scriptGeneratorContext;
    }

    public ScriptExporter getScriptExporter() {
        return scriptExporter;
    }

    public void setScriptExporter(ScriptExporter scriptExporter) {
        this.scriptExporter = scriptExporter;
    }
}
