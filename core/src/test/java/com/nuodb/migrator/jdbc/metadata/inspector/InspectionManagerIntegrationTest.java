package com.nuodb.migrator.jdbc.metadata.inspector;


import com.nuodb.migrator.jdbc.connection.ConnectionProvider;
import com.nuodb.migrator.jdbc.connection.JdbcPoolingConnectionProvider;
import org.testng.annotations.*;

import java.sql.Connection;

import static com.nuodb.migrator.TestUtils.createConnectionSpec;
import static com.nuodb.migrator.jdbc.JdbcUtils.*;
import static com.nuodb.migrator.jdbc.metadata.MetaDataType.*;
import static org.testng.Assert.assertNotNull;

public class InspectionManagerIntegrationTest {

    private InspectionManager inspector;
    private Connection connection;
    private ConnectionProvider connectionProvider;

    @BeforeClass
    public void beforeClass() throws Exception {
        connectionProvider = new JdbcPoolingConnectionProvider(createConnectionSpec());
    }

    @BeforeMethod
    public void setUp() throws Exception {
        connection = connectionProvider.getConnection();
        inspector = new InspectionManager();
        inspector.setConnection(connection);
    }

    @Test
    public void testInspectCatalog() throws Exception {
        InspectionResults inspectionResults = inspector.inspect(CATALOG);
        assertNotNull(inspectionResults.getObjects(CATALOG));
    }

    @Test
    public void testInspectSchema() throws Exception {
        InspectionResults inspectionResults = inspector.inspect(SCHEMA);
        assertNotNull(inspectionResults.getObjects(SCHEMA));
    }

    @Test
    public void testInspectTable() throws Exception {
        InspectionResults inspectionResults = inspector.inspect(TABLE);
        assertNotNull(inspectionResults.getObjects(TABLE));
    }

    @AfterMethod
    public void afterMethod() throws Exception {
        close(connection);
    }
}
