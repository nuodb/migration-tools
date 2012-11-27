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
package com.nuodb.migration.jdbc.model;

import com.google.common.collect.Lists;
import com.nuodb.migration.jdbc.connection.ConnectionProvider;
import com.nuodb.migration.jdbc.connection.JdbcConnectionProvider;
import com.nuodb.migration.jdbc.dialect.DatabaseDialectResolver;
import com.nuodb.migration.jdbc.dialect.DatabaseDialectResolverImpl;
import com.nuodb.migration.spec.JdbcConnectionSpec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

import static com.nuodb.migration.jdbc.JdbcUtils.close;
import static com.nuodb.migration.jdbc.model.Identifier.valueOf;
import static com.nuodb.migration.jdbc.model.ObjectType.*;
import static java.sql.DatabaseMetaData.tableIndexStatistic;

/**
 * Reads database meta data and creates its meta meta. Root meta meta object is {@link Database} containing set of
 * catalogs, each catalog has a collection of schemas and generate is a wrapper of collection of a tables.
 *
 * @author Sergey Bushik
 */
public class DatabaseInspector {

    public static final List<ObjectType> OBJECT_TYPES_ALL = Lists.newArrayList(ObjectType.values());

    private static final String[] TABLE_TYPES = null;

    private transient final Logger logger = LoggerFactory.getLogger(getClass());

    protected String catalog;
    protected String schema;
    protected String table;
    protected List<ObjectType> objectTypes = OBJECT_TYPES_ALL;
    protected String[] tableTypes = TABLE_TYPES;
    protected Connection connection;
    protected ConnectionProvider connectionProvider;
    protected ForeignKeyRuleMap foreignKeyRuleMap;
    protected ForeignKeyDeferrabilityMap foreignKeyDeferrabilityMap;
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

    public DatabaseInspector withForeignKeyRuleMap(ForeignKeyRuleMap foreignKeyRuleMap) {
        this.foreignKeyRuleMap = foreignKeyRuleMap;
        return this;
    }

    public DatabaseInspector withForeignKeyDeferrabilityMap(ForeignKeyDeferrabilityMap foreignKeyDeferrabilityMap) {
        this.foreignKeyDeferrabilityMap = foreignKeyDeferrabilityMap;
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
            readInfo(metaData, database);
            resolveDialect(metaData, database);
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
        if (logger.isDebugEnabled()) {
            logger.debug(String.format("DriverInfo: %s", driverInfo));
        }
        database.setDriverInfo(driverInfo);

        DatabaseInfo databaseInfo = new DatabaseInfo();
        databaseInfo.setProductName(metaData.getDatabaseProductName());
        databaseInfo.setProductVersion(metaData.getDatabaseProductVersion());
        databaseInfo.setMinorVersion(metaData.getDatabaseMinorVersion());
        databaseInfo.setMajorVersion(metaData.getDatabaseMajorVersion());
        if (logger.isDebugEnabled()) {
            logger.debug(String.format("DatabaseInfo: %s", databaseInfo));
        }
        database.setDatabaseInfo(databaseInfo);
    }

    protected void resolveDialect(DatabaseMetaData metaData, Database database) throws SQLException {
        database.setDatabaseDialect(databaseDialectResolver.resolve(metaData));
    }

    protected void readObjects(DatabaseMetaData metaData, Database database) throws SQLException {
        if (objectTypes.contains(CATALOG)) {
            readCatalogs(metaData, database);
        }
        if (objectTypes.contains(SCHEMA)) {
            readSchemas(metaData, database);
        }
        if (objectTypes.contains(TABLE)) {
            readTables(metaData, database);
        }
        if (objectTypes.contains(COLUMN)) {
            readColumns(metaData, database);
        }
        if (objectTypes.contains(INDEX)) {
            readIndexes(metaData, database);
        }
        if (objectTypes.contains(FOREIGN_KEY)) {
            readForeignKeys(metaData, database);
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
                close(catalogs);
            }
            if (catalog != null) {
                database.getCatalog(catalog);
            }
        } else {
            if (logger.isDebugEnabled()) {
                logger.debug("Reading catalogs is unsupported");
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
                close(schemas);
            }
            if (schema != null) {
                database.getCatalog(catalog).getSchema(schema);
            }
        } else {
            if (logger.isDebugEnabled()) {
                logger.debug("Reading schemas is unsupported");
            }
        }
    }

    protected void readTables(DatabaseMetaData metaData, Database database) throws SQLException {
        ResultSet tables = metaData.getTables(catalog, schema, table, tableTypes);
        try {
            while (tables.next()) {
                Schema schema = database.createSchema(tables.getString("TABLE_CAT"), tables.getString("TABLE_SCHEM"));
                Table table = schema.createTable(tables.getString("TABLE_NAME"));
                table.setType(tables.getString("TABLE_TYPE"));

            }
        } finally {
            close(tables);
        }
    }

    protected void readColumns(DatabaseMetaData metaData, Database database) throws SQLException {
        for (Table table : database.listTables()) {
            readColumns(metaData, database, table.getName());
        }
    }

    protected void readColumns(DatabaseMetaData metaData, Database database, String tableName) throws SQLException {
        ResultSet columns = metaData.getColumns(catalog, schema, tableName, null);
        try {
            ColumnModelSet modelSet = ColumnModelFactory.createColumnModelSet(columns.getMetaData());
            while (columns.next()) {
                Table table = database.createCatalog(columns.getString("TABLE_CAT")).createSchema(
                        columns.getString("TABLE_SCHEM")).createTable(columns.getString("TABLE_NAME"));

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
                String autoIncrement = modelSet.get("IS_AUTOINCREMENT") != null ? columns.getString(
                        "IS_AUTOINCREMENT") : null;
                column.setAutoIncrement("YES".equals(autoIncrement));
            }
        } finally {
            close(columns);
        }
    }

    protected void readIndexes(DatabaseMetaData metaData, Database database) throws SQLException {
        for (Table table : database.listTables()) {
            readIndexes(metaData, database, table.getName());
        }
    }

    protected void readIndexes(DatabaseMetaData metaData, Database database, String tableName) throws SQLException {
        ResultSet indexes = metaData.getIndexInfo(catalog, schema, tableName, false, true);
        try {
            while (indexes.next()) {
                if (indexes.getShort("TYPE") == tableIndexStatistic) {
                    continue;
                }
                Table table = database.createCatalog(indexes.getString("TABLE_CAT")).createSchema(
                        indexes.getString("TABLE_SCHEM")).getTable(indexes.getString("TABLE_NAME"));
                Identifier identifier = valueOf(indexes.getString("INDEX_NAME"));
                Index index = table.getIndex(identifier);
                if (index == null) {
                    table.addIndex(index = new Index(identifier));
                    index.setUnique(!indexes.getBoolean("NON_UNIQUE"));
                    index.setFilterCondition(indexes.getString("FILTER_CONDITION"));
                    index.setSortOrder(getIndexSortOrder(indexes.getString("ASC_OR_DESC")));
                }
                index.addColumn(table.createColumn(indexes.getString("COLUMN_NAME")), indexes.getInt("ORDINAL_POSITION"));

            }
        } finally {
            close(indexes);
        }
    }

    protected void readForeignKeys(DatabaseMetaData metaData, Database database) throws SQLException {
        for (Table table : database.listTables()) {
            readForeignKeys(metaData, database, table.getName());
        }
    }

    protected void readForeignKeys(DatabaseMetaData metaData, Database database, String tableName) throws SQLException {
        ResultSet foreignKeys = metaData.getImportedKeys(catalog, schema, tableName);
        try {
            while (foreignKeys.next()) {
                Identifier identifier = valueOf(foreignKeys.getString("FK_NAME"));
                Table targetTable = database.createCatalog(foreignKeys.getString("FKTABLE_CAT")).createSchema(
                        foreignKeys.getString("FKTABLE_SCHEM")).createTable(foreignKeys.getString("FKTABLE_NAME"));

                ForeignKey foreignKey = targetTable.getForeignKey(identifier);
                if (foreignKey == null) {
                    targetTable.addForeignKey(foreignKey = new ForeignKey(identifier));
                    foreignKey.setUpdateRule(getForeignKeyRule(foreignKeys.getInt("UPDATE_RULE")));
                    foreignKey.setDeleteRule(getForeignKeyRule(foreignKeys.getInt("DELETE_RULE")));
                    foreignKey.setDeferrability(getForeignKeyDeferrability(foreignKeys.getInt("DEFERRABILITY")));
                }
                Column targetColumn = targetTable.createColumn(foreignKeys.getString("FKCOLUMN_NAME"));

                Table sourceTable = database.createCatalog(foreignKeys.getString("PKTABLE_CAT")).createSchema(
                        foreignKeys.getString("PKTABLE_SCHEM")).createTable(foreignKeys.getString("PKTABLE_NAME"));
                Column sourceColumn = sourceTable.createColumn(foreignKeys.getString("PKCOLUMN_NAME"));

                foreignKey.addReference(
                        sourceColumn, foreignKeys.getString("PK_NAME"),
                        targetColumn, foreignKeys.getString("FK_NAME"), foreignKeys.getShort("KEY_SEQ"));
            }
        } finally {
            close(foreignKeys);
        }
    }

    protected ForeignKeyRule getForeignKeyRule(int ruleValue) {
        ForeignKeyRuleMap foreignKeyRuleMap = this.foreignKeyRuleMap != null ?
                this.foreignKeyRuleMap : ForeignKeyRuleMap.getInstance();
        return foreignKeyRuleMap.get(ruleValue);
    }

    protected ForeignKeyDeferrability getForeignKeyDeferrability(int deferrabilityValue) {
        ForeignKeyDeferrabilityMap foreignKeyDeferrabilityMap = this.foreignKeyDeferrabilityMap != null ?
                this.foreignKeyDeferrabilityMap : ForeignKeyDeferrabilityMap.getInstance();
        return foreignKeyDeferrabilityMap.getDeferrability(deferrabilityValue);
    }

    protected IndexSortOrder getIndexSortOrder(String ascOrDesc) {
        if (ascOrDesc != null) {
            return ascOrDesc.equals("A") ? IndexSortOrder.ASC : IndexSortOrder.DESC;
        } else {
            return null;
        }
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
