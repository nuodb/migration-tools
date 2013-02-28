package com.nuodb.migrator.jdbc.metadata;


import com.nuodb.migrator.TestUtils;
import com.nuodb.migrator.jdbc.connection.JdbcConnectionProvider;
import com.nuodb.migrator.jdbc.connection.JdbcPoolingConnectionProvider;
import com.nuodb.migrator.jdbc.metadata.inspector.InspectionManager;
import com.nuodb.migrator.spec.JdbcConnectionSpec;
import junit.framework.Assert;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;

import static com.nuodb.migrator.jdbc.JdbcUtils.close;

public class DatabaseInspectorIntegrationTest {

    private Connection connection;
    private InspectionManager inspector;

    @Before
    public void setUp() throws Exception {
        JdbcConnectionSpec nuodb = TestUtils.createConnectionSpec();
        JdbcConnectionProvider connectionProvider = new JdbcPoolingConnectionProvider(nuodb);
        connection = connectionProvider.getConnection();

        Assert.assertNotNull(connection);
        Assert.assertNotNull(connection.getMetaData());
        Assert.assertFalse(connection.isClosed());

        inspector = new InspectionManager();
        inspector.setConnection(connection);
    }

    @Test
    public void testInspectCatalog() throws Exception {
        inspector.inspect(MetaDataType.CATALOG);
    }

    @Test
    public void testInspectSchema() throws Exception {
        inspector.inspect(MetaDataType.CATALOG, MetaDataType.SCHEMA);
    }

    @Test
    public void testInspectTable() throws Exception {
        inspector.inspect(MetaDataType.CATALOG, MetaDataType.SCHEMA, MetaDataType.TABLE);
    }

    @After
    public void tearDown() throws Exception {
        close(connection);
    }
}
