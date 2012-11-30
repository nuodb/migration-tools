package com.nuodb.migration.jdbc.metadata;


import com.nuodb.migration.TestUtils;
import com.nuodb.migration.jdbc.connection.DriverConnectionProvider;
import com.nuodb.migration.jdbc.connection.DriverPoolingConnectionProvider;
import com.nuodb.migration.jdbc.metadata.inspector.DatabaseInspector;
import com.nuodb.migration.spec.DriverConnectionSpec;
import junit.framework.Assert;
import org.junit.After;
import org.junit.Before;

import java.sql.Connection;

public class DatabaseIntrospectorIntegrationTest {

    private Connection connection;
    private DatabaseInspector inspector;

    @Before
    public void setUp() throws Exception {
        DriverConnectionSpec mysql = new DriverConnectionSpec();
        mysql.setDriverClassName("com.mysql.jdbc.Driver");
        mysql.setUrl("jdbc:mysql://localhost:3306/test");
        mysql.setUsername("root");

        DriverConnectionSpec nuodb = TestUtils.createTestNuoDBConnectionSpec();

        final DriverConnectionProvider connectionProvider = new DriverPoolingConnectionProvider(nuodb);
        connection = connectionProvider.getConnection();

        Assert.assertNotNull(connection);
        Assert.assertNotNull(connection.getMetaData());
        Assert.assertFalse(connection.isClosed());

        inspector = new DatabaseInspector();
        inspector.withConnection(connection);
    }

    @After
    public void tearDown() throws Exception {
        if (connection != null) {
            connection.close();
        }
    }
}
