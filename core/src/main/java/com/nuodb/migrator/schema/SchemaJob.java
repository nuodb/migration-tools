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

import com.nuodb.migrator.jdbc.metadata.Database;
import com.nuodb.migrator.jdbc.metadata.generator.*;
import com.nuodb.migrator.jdbc.metadata.inspector.InspectionScope;
import com.nuodb.migrator.jdbc.metadata.inspector.TableInspectionScope;
import com.nuodb.migrator.jdbc.session.Session;
import com.nuodb.migrator.jdbc.session.SessionFactory;
import com.nuodb.migrator.job.SchemaGeneratorJobBase;
import com.nuodb.migrator.spec.ConnectionSpec;
import com.nuodb.migrator.spec.ResourceSpec;
import com.nuodb.migrator.spec.SchemaJobSpec;

import java.sql.SQLException;
import java.util.Collection;

import static com.google.common.collect.Lists.newArrayList;
import static com.nuodb.migrator.jdbc.JdbcUtils.close;
import static com.nuodb.migrator.jdbc.metadata.MetaDataType.DATABASE;
import static com.nuodb.migrator.jdbc.metadata.MetaDataType.TYPES;
import static com.nuodb.migrator.jdbc.metadata.generator.WriterScriptExporter.SYSTEM_OUT;
import static com.nuodb.migrator.jdbc.session.SessionFactories.newSessionFactory;

/**
 * @author Sergey Bushik
 */
public class SchemaJob extends SchemaGeneratorJobBase<SchemaJobSpec> {

    public static final boolean FAIL_ON_EMPTY_SCRIPTS = true;

    private boolean failOnEmptyScripts = FAIL_ON_EMPTY_SCRIPTS;
    private ScriptExporter scriptExporter;
    private ScriptGeneratorManager scriptGeneratorManager;

    @Override
    protected void init() throws Exception {
        super.init();

        SessionFactory sessionFactory = newSessionFactory(
                createConnectionProviderFactory().createConnectionProvider(
                        getSourceSpec()), createDialectResolver());
        Session session = sessionFactory.openSession();
        setSourceSession(session);
        setScriptExporter(createScriptExporter());
        setScriptGeneratorManager(createScriptGeneratorManager());
    }

    protected ScriptExporter createScriptExporter() {
        Collection<ScriptExporter> exporters = newArrayList();
        ConnectionSpec targetSpec = getTargetSpec();
        if (targetSpec != null) {
            try {
                exporters.add(new ConnectionScriptExporter(createConnectionProviderFactory().
                        createConnectionProvider(targetSpec).getConnection()));
            } catch (SQLException exception) {
                throw new SchemaException("Failed creating connection script exporter", exception);
            }
        }
        ResourceSpec outputSpec = getOutputSpec();
        if (outputSpec != null) {
            exporters.add(new FileScriptExporter(outputSpec.getPath(), outputSpec.getEncoding()));
        }
        // Fallback to system out if neither database connection nor target file were provided
        if (exporters.isEmpty()) {
            exporters.add(SYSTEM_OUT);
        }
        return new CompositeScriptExporter(exporters);
    }

    @Override
    public void execute() throws Exception {
        Collection<String> scripts = getScriptGeneratorManager().getScripts(inspect());
        if (isFailOnEmptyScripts() && scripts.isEmpty()) {
            throw new SchemaException(
                    "Database is empty: no scripts to export. Verify connection & data inspection settings");
        }
        ScriptExporter scriptExporter = getScriptExporter();
        scriptExporter.open();
        scriptExporter.exportScripts(scripts);
    }

    protected Database inspect() throws SQLException {
        InspectionScope inspectionScope = new TableInspectionScope(
                getSourceSpec().getCatalog(), getSourceSpec().getSchema(), getTableTypes());
        return createInspectionManager().inspect(
                getSourceSession().getConnection(), inspectionScope, TYPES).getObject(DATABASE);
    }

    @Override
    public void release() throws Exception {
        super.release();
        close(getScriptExporter());
    }

    public boolean isFailOnEmptyScripts() {
        return failOnEmptyScripts;
    }

    public void setFailOnEmptyScripts(boolean failOnEmptyScripts) {
        this.failOnEmptyScripts = failOnEmptyScripts;
    }

    public ScriptExporter getScriptExporter() {
        return scriptExporter;
    }

    public void setScriptExporter(ScriptExporter scriptExporter) {
        this.scriptExporter = scriptExporter;
    }

    public ScriptGeneratorManager getScriptGeneratorManager() {
        return scriptGeneratorManager;
    }

    public void setScriptGeneratorManager(ScriptGeneratorManager scriptGeneratorManager) {
        this.scriptGeneratorManager = scriptGeneratorManager;
    }
}
