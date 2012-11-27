package com.nuodb.migration;


import com.nuodb.migration.jdbc.model.Column;
import com.nuodb.migration.jdbc.model.Database;
import com.nuodb.migration.jdbc.model.Schema;
import com.nuodb.migration.jdbc.model.Table;
import com.nuodb.migration.jdbc.query.SelectQuery;
import com.nuodb.migration.spec.JdbcConnectionSpec;

public class TestConstants {

    public final static String[] ARGUMENTS = new String[]{
            "dump",
            "--source.driver=com.mysql.jdbc.Driver",
            "--source.url=jdbc:mysql://localhost:3306/test",
            "--source.username=root",
            "--output.type=cvs",
            "--output.path=/tmp/"
    };

    public static final String TEST_CATALOG_NAME = "";
    public static final String TEST_SCHEMA_NAME = "HOCKEY";
    public static final String TEST_TABLE_NAME = "hockey";
    public static final String FIRST_COLUMN_NAME = "ID";
    public static final String SECOND_COLUMN_NAME = "Name";

    public static JdbcConnectionSpec createTestNuoDBConnectionSpec() {
        final JdbcConnectionSpec connectionSpec = new JdbcConnectionSpec();
        connectionSpec.setUrl("jdbc:com.nuodb://localhost/test");
        connectionSpec.setUsername("dba");
        connectionSpec.setPassword("goalie");
        connectionSpec.setDriverClassName("com.nuodb.jdbc.Driver");
        return connectionSpec;
    }

    public static SelectQuery createTestSelectQuery() {

        final SelectQuery query = new SelectQuery();
        final Database database = new Database();
        final Schema schema = database.createSchema(TEST_CATALOG_NAME, TEST_SCHEMA_NAME);
        final Table table = schema.createTable(TEST_TABLE_NAME);
        //testTable.createColumn(FIRST_COLUMN_NAME);
        //testTable.createColumn(SECOND_COLUMN_NAME);
        query.addTable(table);
        query.addColumn(new Column(table, FIRST_COLUMN_NAME));

        return query;
    }
}
