package com.nuodb.migration.jdbc.metadata;


import com.nuodb.migration.TestUtils;
import com.nuodb.migration.jdbc.connection.JdbcConnectionProvider;
import com.nuodb.migration.jdbc.connection.JdbcPoolingConnectionProvider;
import com.nuodb.migration.jdbc.metadata.inspector.DatabaseInspector;
import com.nuodb.migration.spec.JdbcConnectionSpec;
import junit.framework.Assert;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;

import static com.nuodb.migration.jdbc.JdbcUtils.close;

public class DatabaseInspectorIntegrationTest {

    private Connection connection;
    private DatabaseInspector inspector;

    @Before
    public void setUp() throws Exception {
        JdbcConnectionSpec nuodb = TestUtils.createConnectionSpec();
        JdbcConnectionProvider connectionProvider = new JdbcPoolingConnectionProvider(nuodb);
        connection = connectionProvider.getConnection();

        Assert.assertNotNull(connection);
        Assert.assertNotNull(connection.getMetaData());
        Assert.assertFalse(connection.isClosed());

        inspector = new DatabaseInspector();
        inspector.withConnection(connection);
    }

    @Test
    public void testInspectCatalog() throws Exception {
        inspector.withMetaDataTypes(MetaDataType.CATALOG);
        inspector.inspect();
    }

    @Test
    public void testInspectSchema() throws Exception {
        inspector.withMetaDataTypes(MetaDataType.CATALOG, MetaDataType.SCHEMA);
        inspector.inspect();
    }

    @Test
    public void testInspectTable() throws Exception {
        inspector.withMetaDataTypes(MetaDataType.CATALOG, MetaDataType.SCHEMA, MetaDataType.TABLE);
        inspector.inspect();
    }

    @After
    public void tearDown() throws Exception {
        close(connection);
    }
}
