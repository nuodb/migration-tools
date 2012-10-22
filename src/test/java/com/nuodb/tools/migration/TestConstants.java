package com.nuodb.tools.migration;


import com.nuodb.tools.migration.dump.output.OutputFormat;
import com.nuodb.tools.migration.dump.output.OutputFormatLookupImpl;
import com.nuodb.tools.migration.dump.query.SelectQuery;
import com.nuodb.tools.migration.jdbc.metamodel.*;
import com.nuodb.tools.migration.spec.DriverManagerConnectionSpec;

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

    public static DriverManagerConnectionSpec createTestNuoDBConnectionSpec() {
        final DriverManagerConnectionSpec connectionSpec = new DriverManagerConnectionSpec();
        connectionSpec.setUrl("jdbc:com.nuodb://localhost/test");
        connectionSpec.setUsername("dba");
        connectionSpec.setPassword("goalie");
        connectionSpec.setDriver("com.nuodb.jdbc.Driver");
        return connectionSpec;
    }

    public static OutputFormat getDefaultOutputFormat() throws IllegalAccessException, InstantiationException {
        return new OutputFormatLookupImpl().getDefaultFormatClass().newInstance();
    }

    public static SelectQuery createTestSelectQuery() {
        final SelectQuery query = new SelectQuery();
        final Schema testSchema = new Schema(new Catalog(new Database(), TEST_CATALOG_NAME), TEST_SCHEMA_NAME);
        final Table testTable = new Table(testSchema, TEST_TABLE_NAME);
        //testTable.createColumn(FIRST_COLUMN_NAME);
        //testTable.createColumn(SECOND_COLUMN_NAME);
        query.addTable(testTable);
        query.addColumn(new Column(testTable, FIRST_COLUMN_NAME));

        return query;
    }
}
