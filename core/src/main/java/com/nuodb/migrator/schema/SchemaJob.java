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
package com.nuodb.migrator.schema;

import com.nuodb.migrator.jdbc.connection.ConnectionProvider;
import com.nuodb.migrator.jdbc.connection.ConnectionServices;
import com.nuodb.migrator.jdbc.metadata.Database;
import com.nuodb.migrator.jdbc.metadata.MetaDataType;
import com.nuodb.migrator.jdbc.metadata.generator.ScriptExporter;
import com.nuodb.migrator.jdbc.metadata.generator.ScriptGeneratorContext;
import com.nuodb.migrator.jdbc.metadata.inspector.InspectionManager;
import com.nuodb.migrator.jdbc.metadata.inspector.TableInspectionScope;
import com.nuodb.migrator.job.decorate.DecoratingJobBase;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collection;

import static com.nuodb.migrator.jdbc.JdbcUtils.close;
import static com.nuodb.migrator.utils.ValidationUtils.isNotNull;

/**
 * @author Sergey Bushik
 */
public class SchemaJob extends DecoratingJobBase<SchemaJobExecution> {

    private ScriptGeneratorContext context;
    private ScriptExporter scriptExporter;
    private ConnectionProvider connectionProvider;
    private boolean failOnEmptyScripts;

    public SchemaJob() {
        super(SchemaJobExecution.class);
    }

    @Override
    protected void initExecution(SchemaJobExecution execution) throws Exception {
        isNotNull(getConnectionProvider(), "Connection provider is required");
        isNotNull(getContext(), "Script generator context is required");
        isNotNull(getScriptExporter(), "Script exporter is required");

        ConnectionServices connectionServices = getConnectionProvider().getConnectionServices();
        execution.setConnectionServices(connectionServices);
        Connection connection = connectionServices.getConnection();
        execution.setConnection(connection);
    }

    @Override
    protected void executeWith(SchemaJobExecution execution) throws Exception {
        Database database = inspect(execution);
        Collection<String> scripts = getContext().getScripts(database);
        if (isFailOnEmptyScripts() && scripts.isEmpty()) {
            throw new SchemaJobException("Scripts are empty");
        }
        ScriptExporter scriptExporter = getScriptExporter();
        try {
            scriptExporter.open();
            scriptExporter.exportScripts(scripts);
        } finally {
            scriptExporter.close();
        }
    }

    private Database inspect(SchemaJobExecution execution) throws SQLException {
        InspectionManager inspectionManager = new InspectionManager();
        inspectionManager.setConnection(execution.getConnection());
        ConnectionServices connectionServices = execution.getConnectionServices();
        return inspectionManager.inspect(new TableInspectionScope(
                connectionServices.getCatalog(), connectionServices.getSchema()), MetaDataType.TYPES
        ).getObject(MetaDataType.DATABASE);
    }

    @Override
    protected void releaseExecution(SchemaJobExecution execution) throws Exception {
        close(execution.getConnectionServices());
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

    public ScriptGeneratorContext getContext() {
        return context;
    }

    public void setContext(ScriptGeneratorContext context) {
        this.context = context;
    }

    public ScriptExporter getScriptExporter() {
        return scriptExporter;
    }

    public void setScriptExporter(ScriptExporter scriptExporter) {
        this.scriptExporter = scriptExporter;
    }
}
