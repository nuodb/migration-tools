package com.nuodb.tools.migration.jbdc;


import com.nuodb.tools.migration.jdbc.connection.DriverManagerConnectionProvider;
import com.nuodb.tools.migration.spec.DriverManagerConnectionSpec;
import junit.framework.Assert;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;

import static org.mockito.Mockito.*;

public class DriverManagerConnectionProviderTest {

    private DriverManagerConnectionSpec specMock;
    private Connection testConnection;
    private MockJdbcDriver mockJdbcDriver;

    @Before
    public void setUp() throws Exception {
        final String testUrl = "test//url";

        specMock = mock(DriverManagerConnectionSpec.class);
        when(specMock.getDriver()).thenReturn("com.nuodb.tools.migration.jbdc.MockJdbcDriver");
        when(specMock.getUrl()).thenReturn(testUrl);
        when(specMock.getUsername()).thenReturn("user");
        when(specMock.getPassword()).thenReturn("pass");


        mockJdbcDriver = new MockJdbcDriver();
        testConnection = mock(Connection.class);
        mockJdbcDriver.setTestConnection(testConnection);
        DriverManager.registerDriver(mockJdbcDriver);
    }

    @After
    public void tearDown() throws Exception {
        DriverManager.deregisterDriver(mockJdbcDriver);
    }

    @Test
    public void testGetConnection() throws Exception {
        DriverManagerConnectionProvider driverManagerConnectionProvider = new DriverManagerConnectionProvider(specMock);

        final Connection connection = driverManagerConnectionProvider.getConnection();
        Assert.assertNotNull(connection);
        Assert.assertTrue(testConnection == connection);

        verify(specMock, times(1)).getDriver();
        verify(specMock, times(1)).getUsername();
        verify(specMock, times(1)).getUrl();
        verify(specMock, times(1)).getPassword();

        verify(connection, times(0)).setTransactionIsolation(anyInt());
        verify(connection, times(1)).setAutoCommit(false);
    }

    @Test
    public void testCreation() throws Exception {
        final DriverManagerConnectionProvider driverManagerConnectionProvider =
                new DriverManagerConnectionProvider(specMock, true, 1);

        final Connection connection = driverManagerConnectionProvider.getConnection();
        Assert.assertNotNull(connection);
        Assert.assertTrue(testConnection == connection);

        verify(specMock, times(1)).getDriver();
        verify(specMock, times(1)).getUsername();
        verify(specMock, times(1)).getUrl();
        verify(specMock, times(1)).getPassword();
        verify(connection, times(1)).setTransactionIsolation(1);
        verify(connection, times(1)).setAutoCommit(true);
    }


}
