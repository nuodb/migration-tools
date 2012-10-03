package com.nuodb.tools.migration.jdbc.metamodel;

import com.nuodb.tools.migration.spec.JdbcConnectionSpec;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.util.Map;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class DatabaseIntrospectorTest extends DatabaseIntrospector {
    Database database;
    DatabaseMetaData mockMetaData;

    final String TEST_CATALOG_NAME = "TEST_CATALOG";
    final String TEST_SCHEMA_NAME = "TEST_SCHEMA";

    @Before
    public void setUp() throws Exception {

        mockMetaData = mock(DatabaseMetaData.class);
        database = new Database(); // mock(Database.class);

        final ResultSet mockResultSet = mock(ResultSet.class);
        when(mockResultSet.next()).thenReturn(true).thenReturn(false);

        when(mockResultSet.getString("TABLE_CAT")).thenReturn(TEST_CATALOG_NAME);
        when(mockResultSet.getString("TABLE_CATALOG")).thenReturn(TEST_CATALOG_NAME);
        when(mockResultSet.getString("TABLE_SCHEM")).thenReturn(TEST_CATALOG_NAME);

        when(mockMetaData.getCatalogs()).thenReturn(mockResultSet);
        when(mockMetaData.getSchemas()).thenReturn(mockResultSet);
    }


    @Test
    public void testWithConnection() throws Exception {
        final DatabaseIntrospector databaseIntrospector =
                withConnection(mock(JdbcConnectionSpec.class));
        Assert.assertNotNull(databaseIntrospector);
    }

    @Test
    public void testReadCatalogs() throws Exception {
        readCatalogs(mockMetaData, database);
        final Map<Name, Catalog> catalogs = database.getCatalogs();
        Assert.assertNotNull(catalogs);
        Assert.assertFalse(catalogs.isEmpty());
        Assert.assertTrue(catalogs.containsKey(Name.valueOf(TEST_CATALOG_NAME)));
        Assert.assertEquals(catalogs.size(), 1);

    }

    @Test
    public void testReadSchemas() throws Exception {
        readSchemas(mockMetaData, database);
        final Name schemaName = Name.valueOf(TEST_SCHEMA_NAME);
        final Schema schema =
                database.getSchema(Name.valueOf(TEST_CATALOG_NAME), schemaName);
        Assert.assertNotNull(schema);
        Assert.assertEquals(schema.getName(), schemaName);
    }


}