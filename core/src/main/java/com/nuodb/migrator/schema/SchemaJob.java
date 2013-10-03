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

import com.nuodb.migrator.jdbc.JdbcUtils;
import com.nuodb.migrator.jdbc.connection.ConnectionProvider;
import com.nuodb.migrator.jdbc.connection.ConnectionProviderFactory;
import com.nuodb.migrator.jdbc.dialect.Dialect;
import com.nuodb.migrator.jdbc.dialect.DialectResolver;
import com.nuodb.migrator.jdbc.dialect.IdentifierNormalizer;
import com.nuodb.migrator.jdbc.dialect.IdentifierQuoting;
import com.nuodb.migrator.jdbc.metadata.Database;
import com.nuodb.migrator.jdbc.metadata.MetaDataType;
import com.nuodb.migrator.jdbc.metadata.generator.*;
import com.nuodb.migrator.jdbc.metadata.inspector.InspectionManager;
import com.nuodb.migrator.jdbc.metadata.inspector.InspectionScope;
import com.nuodb.migrator.jdbc.metadata.inspector.TableInspectionScope;
import com.nuodb.migrator.jdbc.session.Session;
import com.nuodb.migrator.jdbc.session.SessionFactory;
import com.nuodb.migrator.jdbc.type.JdbcTypeNameMap;
import com.nuodb.migrator.job.JobBase;
import com.nuodb.migrator.job.JobExecution;
import com.nuodb.migrator.spec.ConnectionSpec;
import com.nuodb.migrator.spec.JdbcTypeSpec;
import com.nuodb.migrator.spec.ResourceSpec;
import com.nuodb.migrator.spec.SchemaSpec;
import org.slf4j.Logger;

import java.sql.SQLException;
import java.util.Collection;

import static com.google.common.collect.Lists.newArrayList;
import static com.nuodb.migrator.context.ContextUtils.getService;
import static com.nuodb.migrator.jdbc.metadata.generator.HasTablesScriptGenerator.GROUP_SCRIPTS_BY;
import static com.nuodb.migrator.jdbc.metadata.generator.WriterScriptExporter.SYSTEM_OUT;
import static com.nuodb.migrator.jdbc.resolve.DatabaseInfoUtils.NUODB;
import static com.nuodb.migrator.jdbc.session.SessionFactories.newSessionFactory;
import static com.nuodb.migrator.jdbc.type.JdbcTypeSpecifiers.newSpecifiers;
import static com.nuodb.migrator.utils.ValidationUtils.isNotNull;
import static org.slf4j.LoggerFactory.getLogger;

/**
 * @author Sergey Bushik
 */
public class SchemaJob extends JobBase {

    protected final Logger logger = getLogger(getClass());

    public static final boolean FAIL_ON_EMPTY_SCRIPTS = true;

    private SchemaSpec schemaSpec;
    private DialectResolver dialectResolver;
    private InspectionManager inspectionManager;
    private ConnectionProviderFactory connectionProviderFactory;

    private boolean failOnEmptyScripts = FAIL_ON_EMPTY_SCRIPTS;
    private SchemaJobContext schemaJobContext = new SchemaJobContext();

    public SchemaJob(SchemaSpec schemaSpec) {
        this.schemaSpec = schemaSpec;
    }

    @Override
    public void init(JobExecution execution) throws Exception {
        isNotNull(getSchemaSpec(), "Schema spec is required");
        isNotNull(getDialectResolver(), "Dialect resolver is required");
        isNotNull(getInspectionManager(), "Inspection manager is required");
        isNotNull(getConnectionProviderFactory(), "Connection provider factory is required");

        init();
    }

    protected void init() throws SQLException {
        if (logger.isDebugEnabled()) {
            logger.debug("Initializing schema job context");
        }
        SessionFactory sessionFactory = newSessionFactory(
                getConnectionProviderFactory().createConnectionProvider(
                        getSourceConnectionSpec()), getDialectResolver());
        Session session = sessionFactory.openSession();
        schemaJobContext.setSession(session);
        schemaJobContext.setScriptExporter(createScriptExporter());
        schemaJobContext.setScriptGeneratorContext(createScriptGeneratorContext(session));
    }

    protected ScriptExporter createScriptExporter() {
        Collection<ScriptExporter> exporters = newArrayList();
        ConnectionSpec connectionSpec = getTargetConnectionSpec();
        if (connectionSpec != null) {
            try {
                ConnectionProvider connectionProvider = getConnectionProviderFactory().createConnectionProvider(
                        connectionSpec);
                exporters.add(new ConnectionScriptExporter(connectionProvider.getConnection()));
            } catch (SQLException exception) {
                throw new SchemaJobException("Failed creating connection script exporter", exception);
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

    protected ScriptGeneratorContext createScriptGeneratorContext(Session session) {
        ScriptGeneratorContext scriptGeneratorContext = new ScriptGeneratorContext();
        scriptGeneratorContext.getAttributes().put(GROUP_SCRIPTS_BY, schemaSpec.getGroupScriptsBy());
        scriptGeneratorContext.setObjectTypes(getObjectTypes());
        scriptGeneratorContext.setScriptTypes(getScriptTypes());
        scriptGeneratorContext.setSourceSession(session);

        Dialect dialect = getService(DialectResolver.class).resolve(NUODB);
        JdbcTypeNameMap jdbcTypeNameMap = dialect.getJdbcTypeNameMap();
        for (JdbcTypeSpec jdbcTypeSpec : getJdbcTypeSpecs()) {
            jdbcTypeNameMap.addJdbcTypeName(
                    jdbcTypeSpec.getTypeCode(), newSpecifiers(
                    jdbcTypeSpec.getSize(), jdbcTypeSpec.getPrecision(), jdbcTypeSpec.getScale()),
                    jdbcTypeSpec.getTypeName()
            );
        }
        dialect.setIdentifierQuoting(getIdentifierQuoting());
        dialect.setIdentifierNormalizer(getIdentifierNormalizer());
        scriptGeneratorContext.setTargetDialect(dialect);

        ConnectionSpec sourceConnectionSpec = getSourceConnectionSpec();
        scriptGeneratorContext.setSourceCatalog(sourceConnectionSpec.getCatalog());
        scriptGeneratorContext.setSourceSchema(sourceConnectionSpec.getSchema());

        ConnectionSpec targetConnectionSpec = getTargetConnectionSpec();
        if (targetConnectionSpec != null) {
            scriptGeneratorContext.setTargetCatalog(targetConnectionSpec.getCatalog());
            scriptGeneratorContext.setTargetSchema(targetConnectionSpec.getSchema());
        }
        return scriptGeneratorContext;
    }

    @Override
    public void execute(JobExecution execution) throws Exception {
        schema();
    }

    protected void schema() throws Exception {
        if (logger.isDebugEnabled()) {
            logger.debug("Inspecting target database");
        }
        InspectionScope inspectionScope = new TableInspectionScope(
                getSourceConnectionSpec().getCatalog(), getSourceConnectionSpec().getSchema(), getTableTypes());
        Database database = getInspectionManager().inspect(schemaJobContext.getSession().getConnection(), inspectionScope,
                MetaDataType.TYPES).getObject(MetaDataType.DATABASE);

        Collection<String> scripts = schemaJobContext.getScriptGeneratorContext().getScripts(database);
        if (isFailOnEmptyScripts() && scripts.isEmpty()) {
            throw new SchemaJobException(
                    "Database is empty: no scripts to export. Verify connection & data inspection settings");
        }
        ScriptExporter scriptExporter = schemaJobContext.getScriptExporter();
        scriptExporter.open();
        scriptExporter.exportScripts(scripts);
    }

    @Override
    public void release(JobExecution execution) throws Exception {
        release();
    }

    protected void release() throws Exception {
        ScriptExporter scriptExporter = schemaJobContext.getScriptExporter();
        if (scriptExporter != null) {
            scriptExporter.close();
        }
        JdbcUtils.close(schemaJobContext.getSession());
    }

    public SchemaSpec getSchemaSpec() {
        return schemaSpec;
    }

    public DialectResolver getDialectResolver() {
        return dialectResolver;
    }

    public void setDialectResolver(DialectResolver dialectResolver) {
        this.dialectResolver = dialectResolver;
    }

    public InspectionManager getInspectionManager() {
        return inspectionManager;
    }

    public void setInspectionManager(InspectionManager inspectionManager) {
        this.inspectionManager = inspectionManager;
    }

    public ConnectionProviderFactory getConnectionProviderFactory() {
        return connectionProviderFactory;
    }

    public void setConnectionProviderFactory(ConnectionProviderFactory connectionProviderFactory) {
        this.connectionProviderFactory = connectionProviderFactory;
    }

    public boolean isFailOnEmptyScripts() {
        return failOnEmptyScripts;
    }

    public void setFailOnEmptyScripts(boolean failOnEmptyScripts) {
        this.failOnEmptyScripts = failOnEmptyScripts;
    }

    protected String[] getTableTypes() {
        return schemaSpec.getTableTypes();
    }

    protected ConnectionSpec getSourceConnectionSpec() {
        return schemaSpec.getSourceConnectionSpec();
    }

    protected ConnectionSpec getTargetConnectionSpec() {
        return schemaSpec.getTargetConnectionSpec();
    }

    protected ResourceSpec getOutputSpec() {
        return schemaSpec.getOutputSpec();
    }

    protected Collection<MetaDataType> getObjectTypes() {
        return schemaSpec.getObjectTypes();
    }

    protected Collection<ScriptType> getScriptTypes() {
        return schemaSpec.getScriptTypes();
    }

    protected GroupScriptsBy getGroupScriptsBy() {
        return schemaSpec.getGroupScriptsBy();
    }

    protected Collection<JdbcTypeSpec> getJdbcTypeSpecs() {
        return schemaSpec.getJdbcTypeSpecs();
    }

    protected IdentifierQuoting getIdentifierQuoting() {
        return schemaSpec.getIdentifierQuoting();
    }

    protected IdentifierNormalizer getIdentifierNormalizer() {
        return schemaSpec.getIdentifierNormalizer();
    }
}
