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
package com.nuodb.tools.migration.jdbc.metamodel;

import com.nuodb.tools.migration.definition.JdbcConnection;
import com.nuodb.tools.migration.jdbc.connection.JdbcConnectionProvider;
import com.nuodb.tools.migration.jdbc.connection.JdbcConnectionProviderImpl;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

/**
 * Reads database meta data and creates its meta model. Root meta model object is {@link Database} containing set of
 * catalogs, each catalog has a collection of schemas and schema is a wrapper of collection of a tables.
 *
 * @author Sergey Bushik
 */
public class DatabaseIntrospector {

    private static final List<ObjectType> OBJECT_TYPES_ALL = Arrays.asList(ObjectType.values());
    private static final String[] TABLE_TYPES = null;

    private transient final Log log = LogFactory.getLog(getClass());

    private JdbcConnectionProvider connectionProvider;

    private String catalog;
    private String schema;
    private String table;
    private String[] tableTypes = TABLE_TYPES;
    private List<ObjectType> objectTypes = OBJECT_TYPES_ALL;

    public DatabaseIntrospector withCatalog(String catalog) {
        this.catalog = catalog;
        return this;
    }

    public DatabaseIntrospector withSchema(String schema) {
        this.schema = schema;
        return this;
    }

    public DatabaseIntrospector withConnection(JdbcConnection connection) {
        this.catalog = connection.getCatalog();
        this.schema = connection.getSchema();
        this.connectionProvider = new JdbcConnectionProviderImpl(connection);
        return this;
    }

    public DatabaseIntrospector withConnectionProvider(JdbcConnectionProvider connectionProvider) {
        this.connectionProvider = connectionProvider;
        return this;
    }

    public DatabaseIntrospector withObjectTypes(ObjectType... types) {
        this.objectTypes = Arrays.asList(types);
        return this;
    }

    public DatabaseIntrospector withTables(String tables, String... tableTypes) {
        this.table = tables;
        this.tableTypes = tableTypes;
        return this;
    }

    public String getCatalog() {
        return this.catalog;
    }

    public String getSchema() {
        return this.schema;
    }

    public Database introspect() throws SQLException {
        JdbcConnectionProvider provider = getConnectionProvider();
        Connection connection = provider.getConnection();
        DatabaseMetaData meta = connection.getMetaData();

        Database database = createDatabase();
        readInfo(meta, database);
        readObjects(meta, database);

        provider.closeConnection(connection);
        return database;
    }

    protected JdbcConnectionProvider getConnectionProvider() {
        return connectionProvider;
    }

    protected Database createDatabase() {
        return new Database();
    }

    protected void readObjects(DatabaseMetaData meta, Database database) throws SQLException {
        if (objectTypes.contains(ObjectType.CATALOG)) {
            readCatalogs(meta, database);
        }
        if (objectTypes.contains(ObjectType.SCHEMA)) {
            readSchemas(meta, database);
        }
        if (objectTypes.contains(ObjectType.TABLE)) {
            readTables(meta, database);
        }
    }

    protected void readInfo(DatabaseMetaData meta, Database database) throws SQLException {
        DriverInfo driverInfo = new DriverInfo();
        driverInfo.setName(meta.getDriverName());
        driverInfo.setVersion(meta.getDriverName());
        driverInfo.setMinorVersion(meta.getDriverName());
        driverInfo.setMajorVersion(meta.getDriverName());
        if (log.isDebugEnabled()) {
            log.debug(String.format("DriverInfo: %s", driverInfo));
        }
        database.setDriverInfo(driverInfo);

        DatabaseInfo databaseInfo = new DatabaseInfo();
        databaseInfo.setProductName(meta.getDatabaseProductName());
        databaseInfo.setProductVersion(meta.getDatabaseProductVersion());
        databaseInfo.setMinorVersion(meta.getDatabaseMinorVersion());
        databaseInfo.setMajorVersion(meta.getDatabaseMajorVersion());
        if (log.isDebugEnabled()) {
            log.debug(String.format("DatabaseInfo: %s", databaseInfo));
        }
        database.setDatabaseInfo(databaseInfo);
    }

    protected void readCatalogs(DatabaseMetaData meta, Database database) throws SQLException {
        ResultSet catalogs = meta.getCatalogs();
        try {
            while (catalogs.next()) {
                database.createCatalog(catalogs.getString("TABLE_CAT"));
            }
        } finally {
            catalogs.close();
        }
    }

    protected void readSchemas(DatabaseMetaData meta, Database database) throws SQLException {
        String catalog = getCatalog();
        ResultSet schemas = catalog != null ? meta.getSchemas(catalog, null) : meta.getSchemas();
        try {
            while (schemas.next()) {
                database.createSchema(schemas.getString("TABLE_CATALOG"), schemas.getString("TABLE_SCHEM"));
            }
        } finally {
            schemas.close();
        }
    }

    protected void readTables(DatabaseMetaData meta, Database database) throws SQLException {
        ResultSet tables = meta.getTables(getCatalog(), getSchema(), this.table, this.tableTypes);
        try {
            while (tables.next()) {
                Schema schema = database.getSchema(tables.getString("TABLE_CAT"), tables.getString("TABLE_SCHEM"));
                Table table = schema.createTable(tables.getString("TABLE_NAME"), tables.getString("TABLE_TYPE"));
                if (objectTypes.contains(ObjectType.COLUMN)) {
                    readTableColumns(meta, table);
                }
            }
        } finally {
            tables.close();
        }
    }

    protected void readTableColumns(DatabaseMetaData meta, Table table) throws SQLException {
        ResultSet columns = meta.getColumns(getCatalog(), getSchema(), table.getName().value(), null);
        try {
            ResultSetMetaModel model = new ResultSetMetaModel(columns.getMetaData());
            while (columns.next()) {
                Column column = table.createColumn(columns.getString("COLUMN_NAME"));
                column.setType(new ColumnType(columns.getInt("DATA_TYPE"), columns.getString("TYPE_NAME")));
                column.setSize(columns.getInt("COLUMN_SIZE"));
                column.setDefaultValue(columns.getString("COLUMN_DEF"));
                column.setScale(columns.getInt("DECIMAL_DIGITS"));
                column.setComment(columns.getString("REMARKS"));
                column.setPosition(columns.getInt("ORDINAL_POSITION"));
                String nullable = columns.getString("IS_NULLABLE");
                column.setNullable("YES".equals(nullable));
                String autoIncrement = model.hasColumn("IS_AUTOINCREMENT") ? columns.getString("IS_AUTOINCREMENT") : null;
                column.setAutoIncrement("YES".equals(autoIncrement));
            }
        } finally {
            columns.close();
        }
    }

    public static void main(String[] args) throws Exception {
        JdbcConnection mysql = new JdbcConnection();
        mysql.setDriver("com.mysql.jdbc.Driver");
        mysql.setUrl("jdbc:mysql://localhost:3306/test");
        mysql.setUsername("root");

        JdbcConnection nuodb = new JdbcConnection();
        nuodb.setDriver("com.nuodb.jdbc.Driver");
        nuodb.setUrl("jdbc:com.nuodb://localhost/test");
        nuodb.setUsername("dba");
        nuodb.setPassword("goalie");

        DatabaseIntrospector introspector = new DatabaseIntrospector();
        introspector.withConnection(nuodb);
        Database database = introspector.introspect();
        System.out.println(database);
    }
}
