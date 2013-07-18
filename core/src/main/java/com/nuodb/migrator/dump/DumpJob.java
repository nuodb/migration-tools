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

import com.nuodb.migrator.MigratorException;
import com.nuodb.migrator.backup.catalog.XmlCatalogManager;
import com.nuodb.migrator.backup.format.FormatFactory;
import com.nuodb.migrator.backup.format.value.ValueFormatRegistryResolver;
import com.nuodb.migrator.jdbc.connection.ConnectionProviderFactory;
import com.nuodb.migrator.jdbc.dialect.Dialect;
import com.nuodb.migrator.jdbc.dialect.DialectResolver;
import com.nuodb.migrator.jdbc.metadata.Column;
import com.nuodb.migrator.jdbc.metadata.Database;
import com.nuodb.migrator.jdbc.metadata.MetaDataType;
import com.nuodb.migrator.jdbc.metadata.Table;
import com.nuodb.migrator.jdbc.metadata.inspector.InspectionManager;
import com.nuodb.migrator.jdbc.metadata.inspector.InspectionResults;
import com.nuodb.migrator.jdbc.metadata.inspector.InspectionScope;
import com.nuodb.migrator.jdbc.metadata.inspector.TableInspectionScope;
import com.nuodb.migrator.job.JobBase;
import com.nuodb.migrator.job.JobExecution;
import com.nuodb.migrator.spec.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collection;
import java.util.TimeZone;

import static com.google.common.collect.Lists.newArrayList;
import static com.nuodb.migrator.jdbc.JdbcUtils.close;
import static com.nuodb.migrator.jdbc.metadata.MetaDataType.*;
import static com.nuodb.migrator.utils.CollectionUtils.isEmpty;
import static com.nuodb.migrator.utils.ValidationUtils.isNotNull;
import static java.lang.String.format;
import static java.sql.Connection.TRANSACTION_READ_COMMITTED;
import static java.sql.Connection.TRANSACTION_REPEATABLE_READ;
import static org.apache.commons.lang3.ArrayUtils.indexOf;

/**
 * @author Sergey Bushik
 */
@SuppressWarnings("unchecked")
public class DumpJob extends JobBase {

    private static final MetaDataType[] META_DATA_TYPES = new MetaDataType[]{
            DATABASE, CATALOG, SCHEMA, TABLE, COLUMN, INDEX, PRIMARY_KEY
    };
    protected final Logger logger = LoggerFactory.getLogger(getClass());

    private DumpSpec dumpSpec;
    private FormatFactory formatFactory;
    private DialectResolver dialectResolver;
    private InspectionManager inspectionManager;
    private ConnectionProviderFactory connectionProviderFactory;
    private ValueFormatRegistryResolver valueFormatRegistryResolver;

    private DumpContext dumpContext = new DumpContext();

    public DumpJob(DumpSpec dumpSpec) {
        this.dumpSpec = dumpSpec;
    }

    @Override
    public void init(JobExecution execution) throws Exception {
        isNotNull(getDumpSpec(), "Dump spec is required");
        isNotNull(getFormatFactory(), "Backup format factory is required");
        isNotNull(getDialectResolver(), "Dialect resolver is required");
        isNotNull(getConnectionProviderFactory(), "Connection provider factory is required");
        isNotNull(getValueFormatRegistryResolver(), "Value format registry resolver is required");

        initDumpContext();
    }

    protected void initDumpContext() throws SQLException {
        if (logger.isDebugEnabled()) {
            logger.debug("Initializing dump job context");
        }
        dumpContext.setTimeZone(getTimeZone());

        ResourceSpec outputSpec = getOutputSpec();
        dumpContext.setFormat(outputSpec.getType());
        dumpContext.setFormatAttributes(outputSpec.getAttributes());
        dumpContext.setCatalogManager(new XmlCatalogManager(outputSpec.getPath()));

        Connection connection = getConnectionProviderFactory().createConnectionProvider(
                getConnectionSpec()).getConnection();
        dumpContext.setConnection(connection);
        dumpContext.setDialect(getDialectResolver().resolve(connection));
        dumpContext.setFormatFactory(getFormatFactory());
        dumpContext.setValueFormatRegistry(getValueFormatRegistryResolver().resolve(connection));
    }

    /**
     * Executes dump in the provided context. First off, it inspects source database meta data and creates SELECT
     * queries for discovered tables or use complete user-provided SELECT statements ("native queries") to retrieve
     * data.
     *
     * @param execution dump job execution instance.
     * @throws SQLException
     */
    @Override
    public void execute(JobExecution execution) throws Exception {
        Dialect dialect = dumpContext.getDialect();
        Connection connection = dumpContext.getConnection();
        try {
            dialect.setTransactionIsolation(connection,
                    new int[]{TRANSACTION_REPEATABLE_READ, TRANSACTION_READ_COMMITTED});
            if (dialect.supportsSessionTimeZone()) {
                dialect.setSessionTimeZone(connection, getTimeZone());
            }
            dumpContext.setDatabase(inspect());

            getDumpWriter().write();

            connection.commit();
        } catch (MigratorException exception) {
            throw exception;
        } catch (Exception exception) {
            throw new DumpException(exception);
        } finally {
            if (dialect.supportsSessionTimeZone()) {
                dialect.setSessionTimeZone(connection, null);
            }
        }
    }

    protected Database inspect() throws SQLException {
        if (logger.isDebugEnabled()) {
            logger.debug("Inspecting source database");
        }
        InspectionScope inspectionScope = new TableInspectionScope(
                getConnectionSpec().getCatalog(), getConnectionSpec().getSchema(), getTableTypes());
        InspectionResults inspectionResults = getInspectionManager().inspect(
                dumpContext.getConnection(), inspectionScope, META_DATA_TYPES);
        return inspectionResults.getObject(DATABASE);
    }

    protected DumpWriter getDumpWriter() {
        if (logger.isDebugEnabled()) {
            logger.debug("Initializing dump writer");
        }
        DumpWriter dumpWriter = new DumpWriter(getDumpContext());
        dumpWriter.setThreads(1);
        // TODO: dumpContext.setThreads(Runtime.getRuntime().availableProcessors());
        // TODO: configure default query limit from CLI parameter
        // dumpWriter.setDefaultQueryLimit(new QueryLimit(100000L));

        Database database = dumpContext.getDatabase();
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
        return dumpWriter;
    }

    @Override
    public void release(JobExecution execution) throws Exception {
        releaseDumpContext();
    }

    protected void releaseDumpContext() {
        close(dumpContext.getConnection());
    }

    public DumpSpec getDumpSpec() {
        return dumpSpec;
    }

    public DumpContext getDumpContext() {
        return dumpContext;
    }

    public void setDumpContext(DumpContext dumpContext) {
        this.dumpContext = dumpContext;
    }

    public FormatFactory getFormatFactory() {
        return formatFactory;
    }

    public void setFormatFactory(FormatFactory formatFactory) {
        this.formatFactory = formatFactory;
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

    public ValueFormatRegistryResolver getValueFormatRegistryResolver() {
        return valueFormatRegistryResolver;
    }

    public void setValueFormatRegistryResolver(ValueFormatRegistryResolver valueFormatRegistryResolver) {
        this.valueFormatRegistryResolver = valueFormatRegistryResolver;
    }

    protected ResourceSpec getOutputSpec() {
        return dumpSpec.getOutputSpec();
    }

    protected Collection<QuerySpec> getQuerySpecs() {
        return dumpSpec.getQuerySpecs();
    }

    protected Collection<TableSpec> getTableSpecs() {
        return dumpSpec.getTableSpecs();
    }

    protected String[] getTableTypes() {
        return dumpSpec.getTableTypes();
    }

    protected TimeZone getTimeZone() {
        return dumpSpec.getTimeZone();
    }

    protected ConnectionSpec getConnectionSpec() {
        return dumpSpec.getConnectionSpec();
    }
}
