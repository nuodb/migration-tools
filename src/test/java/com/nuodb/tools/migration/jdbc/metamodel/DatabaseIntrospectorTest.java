package com.nuodb.tools.migration.jdbc.metamodel;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;

import static org.mockito.Mockito.*;

public class DatabaseIntrospectorTest {
    Database mockDatabase;
    DatabaseMetaData mockMetaData;
    DatabaseIntrospector databaseIntrospector;


    Schema mockSchema;
    Table mockTable;

    final String TEST_CATALOG_NAME = "TEST_CATALOG";
    final String TEST_TABLE_NAME = "TEST_TABLE";
    final String TEST_SCHEMA_NAME = "TEST_SCHEMA";
    final String TEST_DRIVER_NAME = "TEST_DRIVER";
    final String TEST_TABLE_TYPE = "TEST_TABLE_TYPE";

    @Before
    public void setUp() throws Exception {
        databaseIntrospector = new DatabaseIntrospector();

        mockMetaData = mock(DatabaseMetaData.class);
        mockDatabase = mock(Database.class);
        mockSchema = mock(Schema.class);
        mockTable = mock(Table.class);

        final ResultSet mockResultSet = mock(ResultSet.class);
        final ResultSetMetaData mockMetaDataResultSet = mock(ResultSetMetaData.class);

        when(mockResultSet.next()).thenReturn(true).thenReturn(false);

        when(mockResultSet.getString("TABLE_CAT")).thenReturn(TEST_CATALOG_NAME);
        when(mockResultSet.getString("TABLE_CATALOG")).thenReturn(TEST_CATALOG_NAME);
        when(mockResultSet.getString("TABLE_SCHEM")).thenReturn(TEST_SCHEMA_NAME);
        when(mockResultSet.getString("TABLE_NAME")).thenReturn(TEST_TABLE_NAME);
        when(mockResultSet.getString("TABLE_TYPE")).thenReturn(TEST_TABLE_TYPE);
        when(mockResultSet.getMetaData()).thenReturn(mockMetaDataResultSet);


        when(mockMetaData.getDriverName()).thenReturn(TEST_DRIVER_NAME);
        when(mockMetaData.getCatalogs()).thenReturn(mockResultSet);
        when(mockMetaData.getSchemas()).thenReturn(mockResultSet);
        when(mockMetaData.getTables(null, null, null, null)).thenReturn(mockResultSet);
        when(mockMetaData.getColumns(null, null, null, null)).thenReturn(mockResultSet);
        when(mockMetaData.getColumns(null, null, TEST_TABLE_NAME, null)).thenReturn(mockResultSet);

        when(mockDatabase.getSchema(TEST_CATALOG_NAME, TEST_SCHEMA_NAME)).thenReturn(mockSchema);
        when(mockSchema.createTable(TEST_TABLE_NAME, TEST_TABLE_TYPE)).thenReturn(mockTable);
        when(mockTable.getName()).thenReturn(Name.valueOf(TEST_TABLE_NAME));


    }

    @Test
    public void testReadCatalogs() throws Exception {
        databaseIntrospector.readCatalogs(mockMetaData, mockDatabase);
        verify(mockDatabase, times(1)).createCatalog(TEST_CATALOG_NAME);
    }

    @Test
    public void testReadSchemas() throws Exception {
        databaseIntrospector.readSchemas(mockMetaData, mockDatabase);
        verify(mockDatabase, times(1)).createSchema(TEST_CATALOG_NAME, TEST_SCHEMA_NAME);
    }

    @Test
    public void testReadTables() throws Exception {


        databaseIntrospector.readTables(mockMetaData, mockDatabase);
        verify(mockDatabase, times(1)).getSchema(TEST_CATALOG_NAME, TEST_SCHEMA_NAME);
        verify(mockSchema, times(1)).createTable(TEST_TABLE_NAME, TEST_TABLE_TYPE);

    }

    @Test
    public void testReadObjects() throws Exception {
        final DatabaseIntrospector spyIntrospector = spy(databaseIntrospector);
        spyIntrospector.readObjects(mockMetaData, mockDatabase);

        verify(spyIntrospector, times(1)).readCatalogs(mockMetaData, mockDatabase);
        verify(spyIntrospector, times(1)).readSchemas(mockMetaData, mockDatabase);
        verify(spyIntrospector, times(1)).readTables(mockMetaData, mockDatabase);

    }

    @Test
    public void testReadInfo() throws Exception {
        final DatabaseIntrospector spyIntrospector = spy(databaseIntrospector);
        spyIntrospector.readInfo(mockMetaData, mockDatabase);

        verify(mockDatabase, times(1)).setDatabaseInfo(any(DatabaseInfo.class));
        verify(mockMetaData, times(1)).getDriverName();
        verify(mockMetaData, times(1)).getDriverVersion();

    }

    @Test
    public void testIntrospect() throws Exception {
        final Connection mock = mock(Connection.class);
        when(mock.getMetaData()).thenReturn(mockMetaData);
        databaseIntrospector.withConnection(mock);
        final Database introspectResult = databaseIntrospector.introspect();
        //TODO Add result check

    }


}