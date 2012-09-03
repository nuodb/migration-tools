package com.nuodb.tool.migration.jdbc.metamodel;

import com.nuodb.tool.migration.definition.JdbcConnection;
import com.nuodb.tool.migration.definition.JdbcConnectionSettings;
import com.nuodb.tool.migration.jdbc.connection.JdbcConnectionProvider;
import com.nuodb.tool.migration.jdbc.connection.JdbcConnectionProviderImpl;
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
    private static final String[] TABLE_TYPES = new String[]{"TABLE", "VIEW"};

    private transient final Log log = LogFactory.getLog(getClass());

    private JdbcConnectionProvider connectionProvider;
    private JdbcConnectionSettings connectionSettings;
    private String catalog;
    private String schema;
    private String table;
    private String[] tableTypes = TABLE_TYPES;
    private List<ObjectType> objectTypes = OBJECT_TYPES_ALL;

    public DatabaseIntrospector withConnection(JdbcConnection connection) {
        this.connectionSettings = connection;
        return withConnectionProvider(new JdbcConnectionProviderImpl(connection));
    }

    public DatabaseIntrospector withConnectionProvider(JdbcConnectionProvider connectionProvider) {
        this.connectionProvider = connectionProvider;
        return this;
    }

    public DatabaseIntrospector withConnectionSettings(JdbcConnectionSettings connectionSettings) {
        this.connectionSettings = connectionSettings;
        return this;
    }

    public DatabaseIntrospector withCatalog(String catalog) {
        this.catalog = catalog;
        return this;
    }

    public DatabaseIntrospector withSchema(String schema) {
        this.schema = schema;
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
        return this.catalog != null ? this.catalog : connectionSettings.getCatalog();
    }

    public String getSchema() {
        return this.schema != null ? this.schema : connectionSettings.getSchema();
    }

    public Database introspect() throws SQLException {
        Database database = new Database();
        Connection connection = connectionProvider.getConnection();
        DatabaseMetaData meta = connection.getMetaData();

        DriverInfo driverInfo = new DriverInfo();
        driverInfo.setName(meta.getDriverName());
        driverInfo.setVersion(meta.getDriverName());
        driverInfo.setMinorVersion(meta.getDriverName());
        driverInfo.setMajorVersion(meta.getDriverName());
        if (log.isDebugEnabled()) {
            log.debug(String.format("DriverInfo: %s", driverInfo));
        }

        DatabaseInfo databaseInfo = new DatabaseInfo();
        databaseInfo.setProductName(meta.getDatabaseProductName());
        databaseInfo.setProductVersion(meta.getDatabaseProductVersion());
        databaseInfo.setMinorVersion(meta.getDatabaseMinorVersion());
        databaseInfo.setMajorVersion(meta.getDatabaseMajorVersion());
        if (log.isDebugEnabled()) {
            log.debug(String.format("DatabaseInfo: %s", databaseInfo));
        }

        database.setDriverInfo(driverInfo);
        database.setDatabaseInfo(databaseInfo);
        if (objectTypes.contains(ObjectType.CATALOG)) {
            readCatalogs(meta, database);
        }
        if (objectTypes.contains(ObjectType.SCHEMA)) {
            readSchemas(meta, database);
        }
        if (objectTypes.contains(ObjectType.TABLE)) {
            readTables(meta, database);
        }
        connectionProvider.closeConnection(connection);
        return database;
    }

    protected void readCatalogs(DatabaseMetaData meta, Database database) throws SQLException {
        ResultSet catalogs = meta.getCatalogs();
        try {
            while (catalogs.next()) {
                database.createCatalog(catalogs.getString("TABLE_CAT"));
            }
        } finally {
            if (catalogs != null) {
                catalogs.close();
            }
        }
    }

    protected void readSchemas(DatabaseMetaData meta, Database database) throws SQLException {
        String catalog = getCatalog();
        ResultSet schemas = catalog != null ? meta.getSchemas(catalog, null) : meta.getSchemas();
        try {
            while (schemas.next()) {
                database.createSchema(schemas.getString("TABLE_CATALOG"), schemas.getString("TABLE_SCHEM"));
            }
            schemas.close();
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
            tables.close();
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

        DatabaseIntrospector builder = new DatabaseIntrospector();
        builder.withConnection(nuodb);
        System.out.println(builder.introspect());
    }
}
