package com.nuodb.migration.jdbc.metadata;


import com.nuodb.migration.TestUtils;
import com.nuodb.migration.jdbc.connection.JdbcConnectionProvider;
import com.nuodb.migration.jdbc.metadata.inspector.DatabaseInspector;
import com.nuodb.migration.spec.JdbcConnectionSpec;
import junit.framework.Assert;
import org.junit.After;
import org.junit.Before;

import java.sql.Connection;

public class DatabaseIntrospectorIntegrationTest {

    private Connection connection;
    private DatabaseInspector inspector;

    @Before
    public void setUp() throws Exception {
        JdbcConnectionSpec mysql = new JdbcConnectionSpec();
        mysql.setDriverClassName("com.mysql.jdbc.Driver");
        mysql.setUrl("jdbc:mysql://localhost:3306/test");
        mysql.setUsername("root");

        JdbcConnectionSpec nuodb = TestUtils.createTestNuoDBConnectionSpec();

        final JdbcConnectionProvider connectionProvider = new JdbcConnectionProvider(nuodb);
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
