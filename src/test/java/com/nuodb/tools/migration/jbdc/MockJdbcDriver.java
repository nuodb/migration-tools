package com.nuodb.tools.migration.jbdc;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.util.Properties;

public class MockJdbcDriver implements Driver {

    private Connection testConnection;

    @Override
    public Connection connect(String s, Properties properties) throws SQLException {
        return testConnection;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public boolean acceptsURL(String s) throws SQLException {
        return "test//url".equals(s);
    }

    @Override
    public DriverPropertyInfo[] getPropertyInfo(String s, Properties properties) throws SQLException {
        return new DriverPropertyInfo[0];  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public int getMajorVersion() {
        return 0;
    }

    @Override
    public int getMinorVersion() {
        return 0;
    }

    @Override
    public boolean jdbcCompliant() {
        return false;
    }

    public Connection getTestConnection() {
        return testConnection;
    }

    public void setTestConnection(Connection testConnection) {
        this.testConnection = testConnection;
    }
}
