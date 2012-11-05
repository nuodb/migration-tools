package com.nuodb.migration.jdbc.metamodel;

import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;

import static org.mockito.Mockito.*;

public class DatabaseInspectorTest {

    private Database database;
    private DatabaseMetaData metaData;
    private DatabaseInspector databaseInspector;

    private Schema schema;
    private Table table;

    private final String TEST_CATALOG_NAME = "TEST_CATALOG";
    private final String TEST_TABLE_NAME = "TEST_TABLE";
    private final String TEST_SCHEMA_NAME = "TEST_SCHEMA";
    private final String TEST_DRIVER_NAME = "TEST_DRIVER";
    private final String TEST_TABLE_TYPE = "TEST_TABLE_TYPE";
    private final String TEST_COLUMN_NAME = "TEST_COLUMN_NAME";

    @Before
    public void setUp() throws Exception {
        databaseInspector = new DatabaseInspector();

        metaData = mock(DatabaseMetaData.class);
        database = mock(Database.class);
        schema = mock(Schema.class);
        table = mock(Table.class);

        final ResultSet mockResultSet = mock(ResultSet.class);
        final ResultSetMetaData mockMetaDataResultSet = mock(ResultSetMetaData.class);

        when(mockResultSet.next()).thenReturn(true).thenReturn(false);

        when(mockResultSet.getString("TABLE_CAT")).thenReturn(TEST_CATALOG_NAME);
        when(mockResultSet.getString("TABLE_CATALOG")).thenReturn(TEST_CATALOG_NAME);
        when(mockResultSet.getString("TABLE_SCHEM")).thenReturn(TEST_SCHEMA_NAME);
        when(mockResultSet.getString("TABLE_NAME")).thenReturn(TEST_TABLE_NAME);
        when(mockResultSet.getString("TABLE_TYPE")).thenReturn(TEST_TABLE_TYPE);
        when(mockResultSet.getString("COLUMN_NAME")).thenReturn(TEST_COLUMN_NAME);
        when(mockResultSet.getMetaData()).thenReturn(mockMetaDataResultSet);


        when(metaData.getDriverName()).thenReturn(TEST_DRIVER_NAME);
        when(metaData.getCatalogs()).thenReturn(mockResultSet);
        when(metaData.getSchemas()).thenReturn(mockResultSet);
        when(metaData.getTables(null, null, null, null)).thenReturn(mockResultSet);
        when(metaData.getColumns(null, null, null, null)).thenReturn(mockResultSet);
        when(metaData.getColumns(null, null, TEST_TABLE_NAME, null)).thenReturn(mockResultSet);

        when(database.createSchema(TEST_CATALOG_NAME, TEST_SCHEMA_NAME)).thenReturn(schema);
        when(schema.createTable(TEST_TABLE_NAME, TEST_TABLE_TYPE)).thenReturn(table);
        when(table.getNameObject()).thenReturn(Name.valueOf(TEST_TABLE_NAME));
    }

    @Test
    public void testReadCatalogs() throws Exception {
        // databaseInspector.readCatalogs(metaData, database);
        // verify(database, times(1)).createCatalog(TEST_CATALOG_NAME);
    }

    @Test
    public void testReadSchemas() throws Exception {
        // databaseInspector.readSchemas(metaData, database);
        // verify(database, times(1)).createSchema(TEST_CATALOG_NAME, TEST_SCHEMA_NAME);
    }

    @Test
    public void testReadTables() throws Exception {
        // databaseInspector.readTables(metaData, database);
        // verify(database, times(1)).createSchema(TEST_CATALOG_NAME, TEST_SCHEMA_NAME);
        // verify(schema, times(1)).createTable(TEST_TABLE_NAME, TEST_TABLE_TYPE);
    }

    @Test
    public void testReadObjects() throws Exception {
        // final DatabaseInspector spyInspector = spy(databaseInspector);
        // spyInspector.readObjects(metaData, database);

        // verify(spyInspector, times(1)).readCatalogs(metaData, database);
        // verify(spyInspector, times(1)).readSchemas(metaData, database);
        // verify(spyInspector, times(1)).readTables(metaData, database);
    }

    @Test
    public void testReadTableColumns() throws Exception {
        final Column mockColumn = mock(Column.class);
        stub(table.createColumn(TEST_COLUMN_NAME)).toReturn(mockColumn);

        databaseInspector.readTableColumns(metaData, table);
        verify(table, times(1)).createColumn(TEST_COLUMN_NAME);
    }

    @Test
    public void testReadInfo() throws Exception {
        final DatabaseInspector spyInspector = spy(databaseInspector);
        spyInspector.readInfo(metaData, database);

        verify(database, times(1)).setDatabaseInfo(any(DatabaseInfo.class));
        verify(database, times(1)).setDriverInfo(any(DriverInfo.class));
        verify(metaData, times(1)).getDriverName();
        verify(metaData, times(1)).getDriverVersion();
        verify(metaData, times(1)).getDatabaseMajorVersion();
        verify(metaData, times(1)).getDriverMajorVersion();
        verify(metaData, times(1)).getDatabaseMinorVersion();
        verify(metaData, times(1)).getDriverMinorVersion();
        verify(metaData, times(1)).getDatabaseProductName();
        verify(metaData, times(1)).getDatabaseProductVersion();
    }

    @Test
    public void testIntrospect() throws Exception {
        final Connection mockConnection = mock(Connection.class);
        when(mockConnection.getMetaData()).thenReturn(metaData);
        databaseInspector.withConnection(mockConnection);

        final DatabaseInspector spy = spy(databaseInspector);

        spy.inspect();
        verify(mockConnection, times(1)).getMetaData();
        verify(spy, times(1)).readInfo(eq(metaData), any(Database.class));
        verify(spy, times(1)).readObjects(eq(metaData), any(Database.class));
    }
}