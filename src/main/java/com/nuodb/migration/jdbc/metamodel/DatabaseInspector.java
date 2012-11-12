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
package com.nuodb.migration.jdbc.metamodel;

import com.nuodb.migration.jdbc.connection.ConnectionProvider;
import com.nuodb.migration.jdbc.connection.DriverManagerConnectionProvider;
import com.nuodb.migration.jdbc.dialect.DatabaseDialectResolver;
import com.nuodb.migration.jdbc.dialect.resolve.DatabaseDialectResolverImpl;
import com.nuodb.migration.spec.DriverManagerConnectionSpec;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

/**
 * Reads database meta data and creates its meta meta. Root meta meta object is {@link Database} containing set of
 * catalogs, each catalog has a collection of schemas and schema is a wrapper of collection of a tables.
 *
 * @author Sergey Bushik
 */
public class DatabaseInspector {

    private static final List<ObjectType> OBJECT_TYPES_ALL = Arrays.asList(ObjectType.values());
    private static final String[] TABLE_TYPES = null;

    private transient final Log log = LogFactory.getLog(getClass());

    protected String catalog;
    protected String schema;
    protected String table;
    protected List<ObjectType> objectTypes = OBJECT_TYPES_ALL;
    protected String[] tableTypes = TABLE_TYPES;
    protected Connection connection;
    protected ConnectionProvider connectionProvider;
    protected DatabaseDialectResolver databaseDialectResolver = new DatabaseDialectResolverImpl();

    public DatabaseInspector withObjectTypes(ObjectType... types) {
        this.objectTypes = Arrays.asList(types);
        return this;
    }

    public DatabaseInspector withCatalog(String catalog) {
        this.catalog = catalog;
        return this;
    }

    public DatabaseInspector withSchema(String schema) {
        this.schema = schema;
        return this;
    }

    public DatabaseInspector withTables(String table, String... tableTypes) {
        this.table = table;
        this.tableTypes = tableTypes;
        return this;
    }

    public DatabaseInspector withConnection(Connection connection) {
        this.connection = connection;
        return this;
    }

    public DatabaseInspector withConnectionProvider(ConnectionProvider connectionProvider) {
        this.connectionProvider = connectionProvider;
        return this;
    }

    public DatabaseInspector withDatabaseDialectResolver(DatabaseDialectResolver databaseDialectResolver) {
        this.databaseDialectResolver = databaseDialectResolver;
        return this;
    }

    public Database inspect() throws SQLException {
        Database database = createDatabase();
        Connection connection = this.connection;
        boolean closeConnection = false;
        if (connection == null) {
            connection = connectionProvider.getConnection();
            closeConnection = true;
        }
        try {
            DatabaseMetaData metaData = connection.getMetaData();
            readDialect(metaData, database);
            readInfo(metaData, database);
            readObjects(metaData, database);
        } finally {
            if (closeConnection) {
                connectionProvider.closeConnection(connection);
            }
        }
        return database;
    }

    protected Database createDatabase() {
        return new Database();
    }

    protected void readInfo(DatabaseMetaData metaData, Database database) throws SQLException {
        DriverInfo driverInfo = new DriverInfo();
        driverInfo.setName(metaData.getDriverName());
        driverInfo.setVersion(metaData.getDriverVersion());
        driverInfo.setMinorVersion(metaData.getDriverMinorVersion());
        driverInfo.setMajorVersion(metaData.getDriverMajorVersion());
        if (log.isDebugEnabled()) {
            log.debug(String.format("DriverInfo: %s", driverInfo));
        }
        database.setDriverInfo(driverInfo);

        DatabaseInfo databaseInfo = new DatabaseInfo();
        databaseInfo.setProductName(metaData.getDatabaseProductName());
        databaseInfo.setProductVersion(metaData.getDatabaseProductVersion());
        databaseInfo.setMinorVersion(metaData.getDatabaseMinorVersion());
        databaseInfo.setMajorVersion(metaData.getDatabaseMajorVersion());
        if (log.isDebugEnabled()) {
            log.debug(String.format("DatabaseInfo: %s", databaseInfo));
        }
        database.setDatabaseInfo(databaseInfo);
    }

    protected void readDialect(DatabaseMetaData metaData, Database database) throws SQLException {
        database.setDatabaseDialect(databaseDialectResolver.resolve(metaData));
    }

    protected void readObjects(DatabaseMetaData metaData, Database database) throws SQLException {
        if (objectTypes.contains(ObjectType.CATALOG)) {
            readCatalogs(metaData, database);
        }
        if (objectTypes.contains(ObjectType.SCHEMA)) {
            readSchemas(metaData, database);
        }
        if (objectTypes.contains(ObjectType.TABLE)) {
            readTables(metaData, database);
        }
    }

    protected void readCatalogs(DatabaseMetaData metaData, Database database) throws SQLException {
        if (database.getDatabaseDialect().supportsReadCatalogs()) {
            ResultSet catalogs = metaData.getCatalogs();
            try {
                while (catalogs.next()) {
                    database.createCatalog(catalogs.getString("TABLE_CAT"));
                }
            } finally {
                catalogs.close();
            }
            if (catalog != null) {
                database.getCatalog(catalog);
            }
        } else {
            if (log.isDebugEnabled()) {
                log.debug("Read catalogs is unsupported");
            }
        }
    }

    protected void readSchemas(DatabaseMetaData metaData, Database database) throws SQLException {
        if (database.getDatabaseDialect().supportsReadSchemas()) {
            ResultSet schemas = catalog != null ? metaData.getSchemas(catalog, null) : metaData.getSchemas();
            try {
                while (schemas.next()) {
                    database.createSchema(schemas.getString("TABLE_CATALOG"), schemas.getString("TABLE_SCHEM"));
                }
            } finally {
                schemas.close();
            }
            if (schema != null) {
                database.getCatalog(catalog).getSchema(schema);
            }
        } else {
            if (log.isDebugEnabled()) {
                log.debug("Read schemas is unsupported");
            }
        }
    }

    protected void readTables(DatabaseMetaData metaData, Database database) throws SQLException {
        ResultSet tables = metaData.getTables(catalog, schema, table, tableTypes);
        try {
            while (tables.next()) {
                Schema schema = database.createSchema(tables.getString("TABLE_CAT"), tables.getString("TABLE_SCHEM"));
                Table table = schema.createTable(tables.getString("TABLE_NAME"), tables.getString("TABLE_TYPE"));
                if (objectTypes.contains(ObjectType.COLUMN)) {
                    readTableColumns(metaData, table);
                }
            }
        } finally {
            tables.close();
        }
    }

    protected void readTableColumns(DatabaseMetaData metaData, Table table) throws SQLException {
        ResultSet columns = metaData.getColumns(catalog, schema, table.getName(), null);
        try {
            ColumnModelSet model = ColumnModelFactory.createColumnModelSet(columns.getMetaData());
            while (columns.next()) {
                Column column = table.createColumn(columns.getString("COLUMN_NAME"));
                column.setTypeCode(columns.getInt("DATA_TYPE"));
                column.setTypeName(columns.getString("TYPE_NAME"));
                int columnSize = columns.getInt("COLUMN_SIZE");
                column.setSize(columnSize);
                column.setPrecision(columnSize);
                column.setDefaultValue(columns.getString("COLUMN_DEF"));
                column.setScale(columns.getInt("DECIMAL_DIGITS"));
                column.setComment(columns.getString("REMARKS"));
                column.setPosition(columns.getInt("ORDINAL_POSITION"));
                String nullable = columns.getString("IS_NULLABLE");
                column.setNullable("YES".equals(nullable));
                String autoIncrement = model.item("IS_AUTOINCREMENT") != null ? columns.getString(
                        "IS_AUTOINCREMENT") : null;
                column.setAutoIncrement("YES".equals(autoIncrement));
            }
        } finally {
            columns.close();
        }
    }

    public static void main(String[] args) throws Exception {
        DriverManagerConnectionSpec mysql = new DriverManagerConnectionSpec();
        mysql.setDriver("com.mysql.jdbc.Driver");
        mysql.setUrl("jdbc:mysql://localhost:3306/test");
        mysql.setUsername("root");

        DriverManagerConnectionSpec nuodb = new DriverManagerConnectionSpec();
        nuodb.setDriver("com.nuodb.jdbc.Driver");
        nuodb.setUrl("jdbc:com.nuodb://localhost/test");
        nuodb.setUsername("dba");
        nuodb.setPassword("goalie");

        DriverManagerConnectionProvider connectionProvider = new DriverManagerConnectionProvider();
        connectionProvider.setConnectionSpec(nuodb);
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
