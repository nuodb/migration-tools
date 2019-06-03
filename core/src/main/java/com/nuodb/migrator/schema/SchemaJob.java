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
package com.nuodb.migrator.schema;

import com.nuodb.migrator.jdbc.dialect.Dialect;
import com.nuodb.migrator.jdbc.dialect.DialectResolver;
import com.nuodb.migrator.jdbc.dialect.TranslationConfig;
import com.nuodb.migrator.jdbc.metadata.Database;
import com.nuodb.migrator.jdbc.metadata.filter.MetaDataFilterManager;
import com.nuodb.migrator.jdbc.metadata.generator.CompositeScriptExporter;
import com.nuodb.migrator.jdbc.metadata.generator.FileScriptExporter;
import com.nuodb.migrator.jdbc.metadata.generator.NamingStrategy;
import com.nuodb.migrator.jdbc.metadata.generator.ScriptExporter;
import com.nuodb.migrator.jdbc.metadata.generator.ScriptGeneratorManager;
import com.nuodb.migrator.jdbc.metadata.generator.SessionScriptExporter;
import com.nuodb.migrator.jdbc.metadata.generator.Script;
import com.nuodb.migrator.jdbc.metadata.inspector.InspectionScope;
import com.nuodb.migrator.jdbc.metadata.inspector.TableInspectionScope;
import com.nuodb.migrator.jdbc.session.Session;
import com.nuodb.migrator.jdbc.session.SessionFactory;
import com.nuodb.migrator.jdbc.type.JdbcTypeNameMap;
import com.nuodb.migrator.job.ScriptGeneratorJobBase;
import com.nuodb.migrator.spec.ConnectionSpec;
import com.nuodb.migrator.spec.JdbcTypeSpec;
import com.nuodb.migrator.spec.ResourceSpec;
import com.nuodb.migrator.spec.SchemaJobSpec;
import com.nuodb.migrator.utils.PrioritySet;

import java.sql.SQLException;
import java.util.Collection;

import static com.google.common.collect.Lists.newArrayList;
import static com.nuodb.migrator.context.ContextUtils.getMessages;
import static com.nuodb.migrator.jdbc.JdbcUtils.closeQuietly;
import static com.nuodb.migrator.jdbc.metadata.DatabaseInfos.NUODB;
import static com.nuodb.migrator.jdbc.metadata.MetaDataType.DATABASE;
import static com.nuodb.migrator.jdbc.metadata.MetaDataType.TYPES;
import static com.nuodb.migrator.jdbc.metadata.generator.HasTablesScriptGenerator.GROUP_SCRIPTS_BY;
import static com.nuodb.migrator.jdbc.metadata.generator.WriterScriptExporter.SYSTEM_OUT;
import static com.nuodb.migrator.jdbc.session.SessionFactories.newSessionFactory;
import static com.nuodb.migrator.jdbc.type.JdbcTypeOptions.newOptions;

/**
 * @author Sergey Bushik
 */
@SuppressWarnings("all")
public class SchemaJob extends ScriptGeneratorJobBase<SchemaJobSpec> {

    public static final String EMPTY_DATABASE_ERROR = "com.nuodb.migrator.schema.empty.database.error";

    private Session sourceSession;
    private Session targetSession;
    private ScriptExporter scriptExporter;
    private ScriptGeneratorManager scriptGeneratorManager;

    public SchemaJob() {
    }

    public SchemaJob(SchemaJobSpec jobSpec) {
        super(jobSpec);
    }

    @Override
    protected void init() throws Exception {
        super.init();

        ConnectionSpec sourceSpec = getSourceSpec();
        SessionFactory sourceSessionFactory = newSessionFactory(
                createConnectionProviderFactory().createConnectionProvider(sourceSpec), createDialectResolver());
        Session sourceSession = sourceSessionFactory.openSession();
        setSourceSession(sourceSession);

        Session targetSession = null;
        ConnectionSpec targetSpec = getTargetSpec();
        if (targetSpec != null) {
            SessionFactory targetSessionFactory = newSessionFactory(
                    createConnectionProviderFactory().createConnectionProvider(targetSpec), createDialectResolver());
            targetSession = targetSessionFactory.openSession();
        }
        setTargetSession(targetSession);

        setScriptExporter(createScriptExporter());
        setScriptGeneratorManager(createScriptGeneratorManager());
    }

    protected ScriptExporter createScriptExporter() {
        Collection<ScriptExporter> exporters = newArrayList();
        Session targetSession = getTargetSession();
        if (targetSession != null) {
            exporters.add(new SessionScriptExporter(targetSession));
        }
        ResourceSpec outputSpec = getOutputSpec();
        if (outputSpec != null) {
            exporters.add(new FileScriptExporter(outputSpec.getPath(), outputSpec.getEncoding()));
        }
        // Fallback to system out if neither database connection nor target file
        // were provided
        if (exporters.isEmpty()) {
            exporters.add(SYSTEM_OUT);
        }
        return new CompositeScriptExporter(exporters);
    }

    @Override
    public void execute() throws Exception {
        Collection<Script> scripts = getScriptGeneratorManager().getScripts(inspect());
        if (scripts.isEmpty()) {
            if (isFailOnEmptyDatabase()) {
                throw new SchemaException(getMessages().getMessage(EMPTY_DATABASE_ERROR));
            } else if (logger.isWarnEnabled()) {
                logger.warn(getMessages().getMessage(EMPTY_DATABASE_ERROR));
            }
        }
        ScriptExporter scriptExporter = getScriptExporter();
        scriptExporter.open();
        scriptExporter.exportScripts(scripts);
    }

    protected Database inspect() throws SQLException {
        InspectionScope inspectionScope = new TableInspectionScope(getSourceSpec().getCatalog(),
                getSourceSpec().getSchema(), getTableTypes());
        return createInspectionManager().inspect(getSourceSession().getConnection(), inspectionScope, TYPES)
                .getObject(DATABASE);
    }

    protected ScriptGeneratorManager createScriptGeneratorManager() throws SQLException {
        ScriptGeneratorManager scriptGeneratorManager = new ScriptGeneratorManager();
        scriptGeneratorManager.getAttributes().put(GROUP_SCRIPTS_BY, getGroupScriptsBy());
        scriptGeneratorManager.setObjectTypes(getObjectTypes());
        scriptGeneratorManager.setScriptTypes(getScriptTypes());
        ConnectionSpec sourceSpec = getSourceSpec();
        scriptGeneratorManager.setSourceCatalog(sourceSpec != null ? sourceSpec.getCatalog() : null);
        scriptGeneratorManager.setSourceSchema(sourceSpec != null ? sourceSpec.getSchema() : null);
        scriptGeneratorManager.setSourceSession(getSourceSession());
        scriptGeneratorManager.setMetaDataFilterManager(getMetaDataFilterManager());

        ConnectionSpec targetSpec = getTargetSpec();
        if (targetSpec != null) {
            scriptGeneratorManager.setTargetCatalog(targetSpec.getCatalog());
            scriptGeneratorManager.setTargetSchema(targetSpec.getSchema());
        }

        DialectResolver dialectResolver = createDialectResolver();
        Dialect dialect = getTargetSession() != null ? dialectResolver.resolve(getTargetSession().getConnection())
                : dialectResolver.resolve(NUODB);
        dialect.getTranslationManager().setTranslationConfig(getTranslationConfig());
        JdbcTypeNameMap jdbcTypeNameMap = dialect.getJdbcTypeNameMap();
        for (JdbcTypeSpec jdbcTypeSpec : getJdbcTypeSpecs()) {
            jdbcTypeNameMap.addJdbcTypeName(jdbcTypeSpec.getTypeCode(),
                    newOptions(jdbcTypeSpec.getSize(), jdbcTypeSpec.getPrecision(), jdbcTypeSpec.getScale()),
                    jdbcTypeSpec.getTypeName());
        }
        dialect.setIdentifierQuoting(getIdentifierQuoting());
        dialect.setIdentifierNormalizer(getIdentifierNormalizer());
        for (PrioritySet.Entry<NamingStrategy> entry : getNamingStrategies().entries()) {
            scriptGeneratorManager.addNamingStrategy(entry.getValue(), entry.getPriority());
        }
        scriptGeneratorManager.setTargetDialect(dialect);
        return scriptGeneratorManager;
    }

    @Override
    public void close() throws Exception {
        closeQuietly(getSourceSession());
        closeQuietly(getTargetSession());
        closeQuietly(getScriptExporter());
    }

    protected MetaDataFilterManager getMetaDataFilterManager() {
        return getJobSpec().getMetaDataFilterManager();
    }

    protected boolean isFailOnEmptyDatabase() {
        return getJobSpec().isFailOnEmptyDatabase();
    }

    protected ConnectionSpec getSourceSpec() {
        return getJobSpec().getSourceSpec();
    }

    protected ResourceSpec getOutputSpec() {
        return getJobSpec().getOutputSpec();
    }

    protected TranslationConfig getTranslationConfig() {
        return getJobSpec().getTranslationConfig();
    }

    protected Session getSourceSession() {
        return sourceSession;
    }

    protected void setSourceSession(Session sourceSession) {
        this.sourceSession = sourceSession;
    }

    protected Session getTargetSession() {
        return targetSession;
    }

    protected void setTargetSession(Session targetSession) {
        this.targetSession = targetSession;
    }

    protected ScriptExporter getScriptExporter() {
        return scriptExporter;
    }

    protected void setScriptExporter(ScriptExporter scriptExporter) {
        this.scriptExporter = scriptExporter;
    }

    protected ScriptGeneratorManager getScriptGeneratorManager() {
        return scriptGeneratorManager;
    }

    protected void setScriptGeneratorManager(ScriptGeneratorManager scriptGeneratorManager) {
        this.scriptGeneratorManager = scriptGeneratorManager;
    }
}
