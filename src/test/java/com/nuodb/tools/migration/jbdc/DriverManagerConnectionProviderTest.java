package com.nuodb.tools.migration.jbdc;


import com.nuodb.tools.migration.jdbc.connection.DriverManagerConnectionProvider;
import com.nuodb.tools.migration.spec.DriverManagerConnectionSpec;
import junit.framework.Assert;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;

import static org.mockito.Mockito.*;

public class DriverManagerConnectionProviderTest {

    private DriverManagerConnectionSpec connectionSpec;
    private Connection connection;
    private Driver driver;

    @Before
    public void setUp() throws Exception {
        connectionSpec = mock(DriverManagerConnectionSpec.class);
        when(connectionSpec.getDriver()).thenReturn(JdbcDriverMock.class.getName());
        when(connectionSpec.getUrl()).thenReturn(JdbcDriverMock.URL);
        when(connectionSpec.getUsername()).thenReturn("user");
        when(connectionSpec.getPassword()).thenReturn("pass");

        connection = mock(Connection.class);
        driver = new JdbcDriverMock(connection);
        DriverManager.registerDriver(driver);
    }

    @Test
    public void testGetConnection() throws Exception {
        DriverManagerConnectionProvider connectionProvider =
                new DriverManagerConnectionProvider(connectionSpec);
        final Connection connection = connectionProvider.getConnection();
        Assert.assertNotNull(connection);
        Assert.assertTrue(this.connection == connection);
        verify(connectionSpec, times(1)).getDriver();
        verify(connectionSpec, times(1)).getUsername();
        verify(connectionSpec, times(1)).getUrl();
        verify(connectionSpec, times(1)).getPassword();
        verify(connection, times(0)).setTransactionIsolation(anyInt());
        verify(connection, times(1)).setAutoCommit(false);
    }

    @Test
    public void testCreation() throws Exception {
        final DriverManagerConnectionProvider connectionProvider =
                new DriverManagerConnectionProvider(connectionSpec, true, 1);
        final Connection connection = connectionProvider.getConnection();
        Assert.assertNotNull(connection);
        Assert.assertTrue(this.connection == connection);
        verify(connectionSpec, times(1)).getDriver();
        verify(connectionSpec, times(1)).getUsername();
        verify(connectionSpec, times(1)).getUrl();
        verify(connectionSpec, times(1)).getPassword();
        verify(connection, times(1)).setTransactionIsolation(1);
        verify(connection, times(1)).setAutoCommit(true);
    }

    @After
    public void tearDown() throws Exception {
        DriverManager.deregisterDriver(driver);
    }
}
