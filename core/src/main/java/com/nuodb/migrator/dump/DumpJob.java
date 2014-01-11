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
package com.nuodb.migrator.dump;

import com.google.common.collect.Lists;
import com.nuodb.migrator.backup.Backup;
import com.nuodb.migrator.backup.BackupManager;
import com.nuodb.migrator.backup.Script;
import com.nuodb.migrator.backup.XmlBackupManager;
import com.nuodb.migrator.backup.format.sql.SqlAttributes;
import com.nuodb.migrator.jdbc.metadata.*;
import com.nuodb.migrator.jdbc.metadata.generator.WriterScriptExporter;
import com.nuodb.migrator.jdbc.query.QueryLimit;
import com.nuodb.migrator.jdbc.metadata.generator.ScriptExporter;
import com.nuodb.migrator.jdbc.metadata.generator.ScriptGeneratorManager;
import com.nuodb.migrator.jdbc.metadata.inspector.InspectionScope;
import com.nuodb.migrator.jdbc.metadata.inspector.TableInspectionScope;
import com.nuodb.migrator.jdbc.session.Session;
import com.nuodb.migrator.jdbc.session.SessionFactory;
import com.nuodb.migrator.job.SchemaGeneratorJobBase;
import com.nuodb.migrator.spec.ConnectionSpec;
import com.nuodb.migrator.spec.DumpJobSpec;
import com.nuodb.migrator.spec.MigrationMode;
import com.nuodb.migrator.spec.QuerySpec;
import com.nuodb.migrator.spec.ResourceSpec;
import com.nuodb.migrator.spec.TableSpec;
import com.nuodb.migrator.utils.Collections;

import java.sql.SQLException;
import java.util.Collection;
import java.util.TimeZone;

import static com.google.common.collect.Lists.newArrayList;
import static com.nuodb.migrator.backup.format.sql.SqlAttributes.FORMAT;
import static com.nuodb.migrator.dump.DumpWriter.THREADS;
import static com.nuodb.migrator.jdbc.JdbcUtils.close;
import static com.nuodb.migrator.jdbc.metadata.MetaDataType.DATABASE;
import static com.nuodb.migrator.jdbc.session.SessionFactories.newSessionFactory;
import static com.nuodb.migrator.jdbc.session.SessionObservers.newSessionTimeZoneSetter;
import static com.nuodb.migrator.jdbc.session.SessionObservers.newTransactionIsolationSetter;
import static com.nuodb.migrator.spec.MigrationMode.DATA;
import static com.nuodb.migrator.spec.MigrationMode.SCHEMA;
import static com.nuodb.migrator.utils.Collections.contains;
import static com.nuodb.migrator.utils.Collections.isEmpty;
import static java.lang.String.format;
import static java.sql.Connection.TRANSACTION_READ_COMMITTED;
import static java.sql.Connection.TRANSACTION_REPEATABLE_READ;
import static org.apache.commons.lang3.ArrayUtils.indexOf;
import static org.apache.commons.lang3.StringUtils.join;
import static org.apache.commons.lang3.StringUtils.lowerCase;

/**
 * @author Sergey Bushik
 */
@SuppressWarnings("unchecked")
public class DumpJob extends SchemaGeneratorJobBase<DumpJobSpec> {

    private static final Collection<MetaDataType> OBJECT_TYPES = newArrayList(
            MetaDataType.DATABASE, MetaDataType.CATALOG, MetaDataType.SCHEMA, MetaDataType.TABLE,
            MetaDataType.COLUMN, MetaDataType.INDEX, MetaDataType.PRIMARY_KEY
    );

    private BackupManager backupManager;
    private DumpWriter dumpWriter;
    private ScriptGeneratorManager scriptGeneratorManager;

    public DumpJob() {
    }

    public DumpJob(DumpJobSpec jobSpec) {
        super(jobSpec);
    }

    @Override
    protected void init() throws Exception {
        super.init();

        SessionFactory sessionFactory = createSessionFactory();
        Session session = sessionFactory.openSession();
        setSourceSession(session);

        ResourceSpec outputSpec = getOutputSpec();
        BackupManager backupManager = createBackupManager(outputSpec.getPath());
        setBackupManager(backupManager);

        Collection<MigrationMode> migrationModes = getMigrationModes();
        ScriptGeneratorManager scriptGeneratorManager = null;
        if (contains(migrationModes, SCHEMA)) {
            scriptGeneratorManager = createScriptGeneratorManager();
        }
        setScriptGeneratorManager(scriptGeneratorManager);

        DumpWriter dumpWriter = null;
        if (contains(migrationModes, DATA)) {
            dumpWriter = new DumpWriter();
            dumpWriter.setQueryLimit(getQueryLimit());
            dumpWriter.setThreads(getThreads() != null ? getThreads() : THREADS);
            dumpWriter.setTimeZone(getTimeZone());

            dumpWriter.setBackupManager(backupManager);
            dumpWriter.setFormat(outputSpec.getType());
            dumpWriter.setFormatAttributes(outputSpec.getAttributes());
            dumpWriter.setFormatFactory(createFormatFactory());
            dumpWriter.setSession(session);
            dumpWriter.setSessionFactory(sessionFactory);
            dumpWriter.setValueFormatRegistry(
                    createValueFormatRegistryResolver().resolve(session.getConnection()));
        }
        setDumpWriter(dumpWriter);
    }

    protected BackupManager createBackupManager(String path) {
        return new XmlBackupManager(path);
    }

    protected SessionFactory createSessionFactory() {
        SessionFactory sessionFactory = newSessionFactory(
                createConnectionProviderFactory().
                        createConnectionProvider(getSourceSpec()), createDialectResolver());
        sessionFactory.addSessionObserver(newTransactionIsolationSetter(new int[]{
                TRANSACTION_REPEATABLE_READ, TRANSACTION_READ_COMMITTED
        }));
        sessionFactory.addSessionObserver(newSessionTimeZoneSetter(getTimeZone()));
        return sessionFactory;
    }

    @Override
    public void execute() throws Exception {
        Database database = inspect();
        Backup backup = new Backup();
        backup.setFormat(getOutputSpec().getType());
        backup.setDatabaseInfo(database.getDatabaseInfo());
        Collection<MigrationMode> migrationModes = getMigrationModes();
        if (contains(migrationModes, DATA)) {
            DumpWriter dumpWriter = getDumpWriter();
            dumpWriter.setDatabase(database);
            Collection<TableSpec> tableSpecs = getTableSpecs();
            if (isEmpty(tableSpecs)) {
                String[] tableTypes = getTableTypes();
                for (Table table : database.getTables()) {
                    if (isEmpty(tableTypes) || indexOf(tableTypes, table.getType()) != -1) {
                        dumpWriter.addTable(table);
                    } else {
                        if (logger.isTraceEnabled()) {
                            logger.trace(format("Table %s %s is not in the allowed types, table skipped",
                                    table.getQualifiedName(null), table.getType()));
                        }
                    }
                }
            } else {
                for (TableSpec tableSpec : tableSpecs) {
                    Table table = database.findTable(tableSpec.getTable());
                    Collection<Column> columns;
                    if (isEmpty(tableSpec.getColumns())) {
                        columns = table.getColumns();
                    } else {
                        columns = newArrayList();
                        for (String column : tableSpec.getColumns()) {
                            columns.add(table.getColumn(column));
                        }
                    }
                    String filter = tableSpec.getFilter();
                    dumpWriter.addTable(table, columns, filter);
                }
            }
            for (QuerySpec querySpec : getQuerySpecs()) {
                dumpWriter.addQuery(querySpec.getQuery());
            }
            dumpWriter.write(backup);
        }
        if (contains(migrationModes, SCHEMA)) {
            ScriptGeneratorManager scriptGeneratorManager = createScriptGeneratorManager();
            for (Schema schema : database.getSchemas()) {
                Collection<String> scripts = scriptGeneratorManager.getScripts(schema);
                if (isEmpty(scripts)) {
                    continue;
                }
                Script script = createScript(schema, scripts);
                ScriptExporter scriptExporter = createScriptExporter(script);
                try {
                    scriptExporter.open();
                    scriptExporter.exportScripts(scripts);
                } finally {
                    close(scriptExporter);
                }
                backup.addScript(script);
            }
        }
        getBackupManager().writeBackup(backup);
    }

    protected Database inspect() throws SQLException {
        InspectionScope inspectionScope = new TableInspectionScope(
                getSourceSpec().getCatalog(), getSourceSpec().getSchema(), getTableTypes());
        Collection<MetaDataType> objectTypes = getMigrationModes().contains(SCHEMA) ?
                getObjectTypes() : OBJECT_TYPES;
        return createInspectionManager().inspect(getSourceSession().getConnection(), inspectionScope,
                objectTypes.toArray(new MetaDataType[objectTypes.size()])).getObject(DATABASE);
    }

    protected Script createScript(Schema schema, Collection<String> scripts) {
        Script script = new Script();
        Collection<String> parts = newArrayList();
        Catalog catalog = schema.getCatalog();
        if (catalog.getName() != null) {
            parts.add(catalog.getName());
            script.setCatalogName(catalog.getName());
        }
        if (schema.getName() != null) {
            parts.add(schema.getName());
            script.setSchemaName(schema.getName());
        }
        parts.add(FORMAT);
        script.setName(lowerCase(join(parts, ".")));
        script.setScriptCount(scripts.size());
        return script;
    }

    protected ScriptExporter createScriptExporter(Script script) {
        return new WriterScriptExporter(getBackupManager().openOutput(script.getName()));
    }

    public DumpWriter getDumpWriter() {
        return dumpWriter;
    }

    public void setDumpWriter(DumpWriter dumpWriter) {
        this.dumpWriter = dumpWriter;
    }

    public BackupManager getBackupManager() {
        return backupManager;
    }

    public void setBackupManager(BackupManager backupManager) {
        this.backupManager = backupManager;
    }

    public ScriptGeneratorManager getScriptGeneratorManager() {
        return scriptGeneratorManager;
    }

    public void setScriptGeneratorManager(ScriptGeneratorManager scriptGeneratorManager) {
        this.scriptGeneratorManager = scriptGeneratorManager;
    }

    protected Collection<MigrationMode> getMigrationModes() {
        return getJobSpec().getMigrationModes();
    }

    protected Integer getThreads() {
        return getJobSpec().getThreads();
    }

    protected ResourceSpec getOutputSpec() {
        return getJobSpec().getOutputSpec();
    }

    protected Collection<QuerySpec> getQuerySpecs() {
        return getJobSpec().getQuerySpecs();
    }

    protected Collection<TableSpec> getTableSpecs() {
        return getJobSpec().getTableSpecs();
    }

    protected String[] getTableTypes() {
        return getJobSpec().getTableTypes();
    }

    protected TimeZone getTimeZone() {
        return getJobSpec().getTimeZone();
    }

    protected ConnectionSpec getSourceSpec() {
        return getJobSpec().getSourceSpec();
    }

    public QueryLimit getQueryLimit() {
        return getJobSpec().getQueryLimit();
    }
}
