package com.nuodb.tools.migration.jdbc.metamodel;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.Map;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class DatabaseIntrospectorTest {
    Database database;
    DatabaseMetaData mockMetaData;
    DatabaseIntrospector databaseIntrospector;

    final String TEST_CATALOG_NAME = "TEST_CATALOG";
    final String TEST_TABLE_NAME = "TEST_TABLE";
    final String TEST_SCHEMA_NAME = "TEST_SCHEMA";

    @Before
    public void setUp() throws Exception {
        databaseIntrospector = new DatabaseIntrospector();

        mockMetaData = mock(DatabaseMetaData.class);
        database = new Database(); // mock(Database.class);

        final ResultSet mockResultSet = mock(ResultSet.class);
        final ResultSetMetaData mockMetaDataResultSet = mock(ResultSetMetaData.class);
        when(mockResultSet.next()).thenReturn(true).thenReturn(false);

        when(mockResultSet.getString("TABLE_CAT")).thenReturn(TEST_CATALOG_NAME);
        when(mockResultSet.getString("TABLE_CATALOG")).thenReturn(TEST_CATALOG_NAME);
        when(mockResultSet.getString("TABLE_SCHEM")).thenReturn(TEST_CATALOG_NAME);
        when(mockResultSet.getString("TABLE_NAME")).thenReturn(TEST_CATALOG_NAME);
        when(mockResultSet.getMetaData()).thenReturn(mockMetaDataResultSet);


        when(mockMetaData.getCatalogs()).thenReturn(mockResultSet);
        when(mockMetaData.getSchemas()).thenReturn(mockResultSet);
        when(mockMetaData.getTables(null, null, null, null)).thenReturn(mockResultSet);
        when(mockMetaData.getColumns(null, null, null, null)).thenReturn(mockResultSet);
        when(mockMetaData.getColumns(null, null, TEST_CATALOG_NAME, null)).thenReturn(mockResultSet);
    }

    @Test
    public void testReadCatalogs() throws Exception {
        databaseIntrospector.readCatalogs(mockMetaData, database);
        final Map<Name, Catalog> catalogs = database.getCatalogs();
        Assert.assertNotNull(catalogs);
        Assert.assertFalse(catalogs.isEmpty());
        Assert.assertTrue(catalogs.containsKey(Name.valueOf(TEST_CATALOG_NAME)));
        Assert.assertEquals(catalogs.size(), 1);
    }

    @Test
    public void testReadSchemas() throws Exception {
        databaseIntrospector.readSchemas(mockMetaData, database);
        final Name schemaName = Name.valueOf(TEST_SCHEMA_NAME);
        final Schema schema =
                database.getSchema(Name.valueOf(TEST_CATALOG_NAME), schemaName);
        Assert.assertNotNull(schema);
        Assert.assertEquals(schema.getName(), schemaName);
    }

    @Test
    public void testReadTables() throws Exception {
        databaseIntrospector.readTables(mockMetaData, database);
        //TODO Add tables check
    }

    @Test
    public void testReadObjects() throws Exception {

        databaseIntrospector.readObjects(mockMetaData, database);

        final Map<Name, Catalog> catalogs = database.getCatalogs();
        Assert.assertNotNull(catalogs);
        Assert.assertFalse(catalogs.isEmpty());
        Assert.assertTrue(catalogs.containsKey(Name.valueOf(TEST_CATALOG_NAME)));
        Assert.assertEquals(catalogs.size(), 1);

        final Name schemaName = Name.valueOf(TEST_SCHEMA_NAME);
        final Schema schema = database.getSchema(Name.valueOf(TEST_CATALOG_NAME), schemaName);
        Assert.assertNotNull(schema);
        Assert.assertEquals(schema.getName(), schemaName);
    }
}