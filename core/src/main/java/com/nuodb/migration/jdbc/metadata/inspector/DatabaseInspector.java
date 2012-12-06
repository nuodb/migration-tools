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
package com.nuodb.migration.jdbc.metadata.inspector;

import com.google.common.collect.Maps;
import com.nuodb.migration.jdbc.connection.ConnectionProvider;
import com.nuodb.migration.jdbc.connection.ConnectionServices;
import com.nuodb.migration.jdbc.connection.DriverPoolingConnectionProvider;
import com.nuodb.migration.jdbc.dialect.DialectResolver;
import com.nuodb.migration.jdbc.dialect.SimpleDialectResolver;
import com.nuodb.migration.jdbc.metadata.Database;
import com.nuodb.migration.jdbc.metadata.DatabaseInfo;
import com.nuodb.migration.jdbc.metadata.DriverInfo;
import com.nuodb.migration.jdbc.metadata.MetaDataType;
import com.nuodb.migration.spec.DriverConnectionSpec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Map;

import static com.google.common.collect.Lists.newArrayList;
import static com.nuodb.migration.jdbc.metadata.MetaDataType.ALL_TYPES;
import static com.nuodb.migration.jdbc.metadata.MetaDataType.COLUMN_CHECK;
import static com.nuodb.migration.utils.ReflectionUtils.newInstance;
import static java.lang.String.format;

/**
 * Reads database meta data and creates its meta model. Root meta model object is {@link com.nuodb.migration.jdbc.metadata.Database} containing set of
 * catalogs, each catalog has a collection of schemas and schema is a wrapper of collection of a tables.
 *
 * @author Sergey Bushik
 */
public class DatabaseInspector {

    private transient final Logger logger = LoggerFactory.getLogger(getClass());

    private Class<? extends Database> databaseClass = Database.class;
    private String catalog;
    private String schema;
    private String table;
    private String[] tableTypes;
    private Connection connection;
    private DialectResolver dialectResolver = new SimpleDialectResolver();
    private Map<MetaDataType, MetaDataReader> metaDataReaderMap = Maps.newLinkedHashMap();
    private MetaDataType[] metaDataTypes = MetaDataType.ALL_TYPES;

    public DatabaseInspector() {
        withMetaDataReader(new TableReader());
        withMetaDataReader(new SchemaReader());
        withMetaDataReader(new CatalogReader());
        withMetaDataReader(new ColumnReader());
        withMetaDataReader(new IndexReader());
        withMetaDataReader(new ForeignKeyReader());
        withMetaDataReader(new PrimaryKeyReader());

        MetaDataReaderResolver metaDataReader = new MetaDataReaderResolver(COLUMN_CHECK);
        metaDataReader.registerObject("NuoDB", NuoDBColumnCheckReader.class);
        withMetaDataReader(metaDataReader);
    }

    public Database inspect() throws SQLException {
        Database database = createDatabase();
        readInfo(database);
        readMetaData(database);
        if (catalog != null) {
            database.getCatalog(catalog);
        }
        if (schema != null) {
            database.getCatalog(catalog).getSchema(schema);
        }
        return database;
    }

    protected void readInfo(Database database) throws SQLException {
        DatabaseMetaData metaData = getConnection().getMetaData();
        DriverInfo driverInfo = new DriverInfo();
        driverInfo.setName(metaData.getDriverName());
        driverInfo.setVersion(metaData.getDriverVersion());
        driverInfo.setMinorVersion(metaData.getDriverMinorVersion());
        driverInfo.setMajorVersion(metaData.getDriverMajorVersion());
        if (logger.isDebugEnabled()) {
            logger.debug(format("DriverInfo: %s", driverInfo));
        }
        database.setDriverInfo(driverInfo);

        DatabaseInfo databaseInfo = new DatabaseInfo();
        databaseInfo.setProductName(metaData.getDatabaseProductName());
        databaseInfo.setProductVersion(metaData.getDatabaseProductVersion());
        databaseInfo.setMinorVersion(metaData.getDatabaseMinorVersion());
        databaseInfo.setMajorVersion(metaData.getDatabaseMajorVersion());
        if (logger.isDebugEnabled()) {
            logger.debug(format("DatabaseInfo: %s", databaseInfo));
        }
        database.setDatabaseInfo(databaseInfo);
        database.setDialect(getDialectResolver().resolveObject(metaData));
    }

    protected void readMetaData(Database database) throws SQLException {
        DatabaseMetaData metaData = getConnection().getMetaData();
        Collection<MetaDataType> metaDataTypes = newArrayList(
                getMetaDataTypes() != null ? getMetaDataTypes() : ALL_TYPES);
        for (MetaDataReader metaDataReader : getMetaDataReaders()) {
            MetaDataType metaDataType = metaDataReader.getMetaDataType();
            if (metaDataTypes.contains(metaDataType)) {
                metaDataReader.read(this, database, metaData);
            } else {
                if (logger.isWarnEnabled()) {
                    logger.warn(format("Meta data reader for '%s' is not found", metaDataType));
                }
            }
        }
    }

    protected Database createDatabase() {
        return newInstance(databaseClass);
    }

    public String getCatalog() {
        return catalog;
    }

    public DatabaseInspector withCatalog(String catalog) {
        this.catalog = catalog;
        return this;
    }

    public String getSchema() {
        return schema;
    }

    public DatabaseInspector withSchema(String schema) {
        this.schema = schema;
        return this;
    }

    public String getTable() {
        return table;
    }

    public DatabaseInspector withTable(String table) {
        this.table = table;
        return this;
    }

    public String[] getTableTypes() {
        return tableTypes;
    }

    public DatabaseInspector withTableTypes(String[] tableTypes) {
        this.tableTypes = tableTypes;
        return this;
    }

    public Connection getConnection() {
        return connection;
    }

    public DatabaseInspector withConnection(Connection connection) {
        this.connection = connection;
        return this;
    }

    public DialectResolver getDialectResolver() {
        return dialectResolver;
    }

    public DatabaseInspector withDatabaseDialectResolver(DialectResolver dialectResolver) {
        this.dialectResolver = dialectResolver;
        return this;
    }

    public MetaDataType[] getMetaDataTypes() {
        return metaDataTypes;
    }

    public DatabaseInspector withMetaDataTypes(MetaDataType... metaDataTypes) {
        this.metaDataTypes = metaDataTypes;
        return this;
    }

    public DatabaseInspector withMetaDataReader(MetaDataReader metaDataReader) {
        metaDataReaderMap.put(metaDataReader.getMetaDataType(), metaDataReader);
        return this;
    }

    public Collection<MetaDataReader> getMetaDataReaders() {
        return metaDataReaderMap.values();
    }

    public static void main(String[] args) throws Exception {
        DriverConnectionSpec mysql = new DriverConnectionSpec();
        mysql.setDriverClassName("com.mysql.jdbc.Driver");
        mysql.setUrl("jdbc:mysql://localhost:3306/generate-schema");
        mysql.setUsername("root");

        DriverConnectionSpec nuodb = new DriverConnectionSpec();
        nuodb.setDriverClassName("com.nuodb.jdbc.Driver");
        nuodb.setUrl("jdbc:com.nuodb://localhost/test");
        nuodb.setSchema("hockey");
        nuodb.setUsername("dba");
        nuodb.setPassword("goalie");

        ConnectionProvider connectionProvider = new DriverPoolingConnectionProvider(nuodb);
        ConnectionServices connectionServices = connectionProvider.getConnectionServices();
        try {
            DatabaseInspector databaseInspector = connectionServices.createDatabaseInspector();
            Database database = databaseInspector.inspect();
            System.out.println(database);
        } finally {
            connectionServices.close();
        }
    }
}
