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
import com.nuodb.migration.jdbc.connection.JdbcConnectionProvider;
import com.nuodb.migration.jdbc.dialect.DatabaseDialectResolver;
import com.nuodb.migration.jdbc.dialect.SimpleDatabaseDialectResolver;
import com.nuodb.migration.jdbc.metadata.Database;
import com.nuodb.migration.jdbc.metadata.DatabaseInfo;
import com.nuodb.migration.jdbc.metadata.DriverInfo;
import com.nuodb.migration.jdbc.metadata.MetaModelException;
import com.nuodb.migration.spec.JdbcConnectionSpec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Map;

import static com.nuodb.migration.utils.ReflectionUtils.newInstance;
import static java.lang.String.format;

/**
 * Reads database meta data and creates its meta meta. Root meta meta object is {@link com.nuodb.migration.jdbc.metadata.Database} containing set of
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
    private DatabaseDialectResolver databaseDialectResolver = new SimpleDatabaseDialectResolver();
    private Map<MetaDataType, MetaDataReader> metaDataReaderMap = Maps.newLinkedHashMap();
    private MetaDataType[] metaDataTypes = MetaDataType.ALL_OBJECTS;

    public DatabaseInspector() {
        withMetaDataReader(new CatalogMetaDataReader());
        withMetaDataReader(new SchemaMetaDataReader());
        withMetaDataReader(new TableMetaDataReader());
        withMetaDataReader(new ColumnMetaDataReader());
        withMetaDataReader(new IndexMetaDataReader());
        withMetaDataReader(new ForeignKeyMetaDataReader());
        withMetaDataReader(new PrimaryKeyMetaDataReader());
    }

    public Database inspect() throws SQLException {
        Database database = createDatabase();
        readInfo(database);
        readMetaData(database);
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
        database.setDatabaseDialect(getDatabaseDialectResolver().resolve(metaData));
    }

    protected void readMetaData(Database database) throws SQLException {
        DatabaseMetaData metaData = getConnection().getMetaData();
        MetaDataType[] metaDataTypes = getMetaDataTypes();
        if (metaDataTypes != null) {
            for (MetaDataType metaDataType : metaDataTypes) {
                MetaDataReader metaDataReader = getMetaDataReader(metaDataType);
                if (metaDataReader != null) {
                    metaDataReader.read(this, database, metaData);
                } else {
                    throw new MetaModelException(
                            format("Meta data reader for '%s' meta data type not found", metaDataType));
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

    public DatabaseDialectResolver getDatabaseDialectResolver() {
        return databaseDialectResolver;
    }

    public DatabaseInspector withDatabaseDialectResolver(DatabaseDialectResolver databaseDialectResolver) {
        this.databaseDialectResolver = databaseDialectResolver;
        return this;
    }

    public MetaDataType[] getMetaDataTypes() {
        return metaDataTypes;
    }

    public DatabaseInspector withMetaDataTypes(MetaDataType... metaDataTypes) {
        this.metaDataTypes = metaDataTypes;
        return this;
    }

    public MetaDataReader getMetaDataReader(MetaDataType metaDataType) {
        return metaDataReaderMap.get(metaDataType);
    }

    public DatabaseInspector withMetaDataReader(MetaDataReader metaDataReader) {
        metaDataReaderMap.put(metaDataReader.getMetaDataType(), metaDataReader);
        return this;
    }

    public Collection<MetaDataReader> getMetaDataReaders() {
        return metaDataReaderMap.values();
    }

    public static void main(String[] args) throws Exception {
        JdbcConnectionSpec mysql = new JdbcConnectionSpec();
        mysql.setDriverClassName("com.mysql.jdbc.Driver");
        mysql.setUrl("jdbc:mysql://localhost:3306/test");
        mysql.setUsername("root");

        JdbcConnectionSpec nuodb = new JdbcConnectionSpec();
        nuodb.setDriverClassName("com.nuodb.jdbc.Driver");
        nuodb.setUrl("jdbc:com.nuodb://localhost/test");
        nuodb.setUsername("dba");
        nuodb.setPassword("goalie");

        JdbcConnectionProvider connectionProvider = new JdbcConnectionProvider(nuodb);
        Connection connection = connectionProvider.getConnection();
        try {
            DatabaseInspector inspector = new DatabaseInspector();
            inspector.withConnection(connection);
            Database database = inspector.inspect();
            System.out.println(database);
        } finally {
            connectionProvider.closeConnection(connection);
        }
    }
}
