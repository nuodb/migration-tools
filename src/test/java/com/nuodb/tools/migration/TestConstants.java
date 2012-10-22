package com.nuodb.tools.migration;


public class TestConstants {

    public final static String[] ARGUMENTS = new String[]{
            "dump",
            "--source.driver=com.mysql.jdbc.Driver",
            "--source.url=jdbc:mysql://localhost:3306/test",
            "--source.username=root",
            "--output.type=cvs",
            "--output.path=/tmp/"
    };

    public static final String TEST_CATALOG_NAME = "Test_Catalog";
    public static final String TEST_SCHEMA_NAME = "Test_Schema";
    public static final String TEST_TABLE_NAME = "Test_table";
    public static final String FIRST_COLUMN_NAME = "FirstColumn";
    public static final String SECOND_COLUMN_NAME = "SecondColumn";
}
