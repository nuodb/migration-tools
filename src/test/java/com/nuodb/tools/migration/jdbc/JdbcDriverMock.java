package com.nuodb.tools.migration.jdbc;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.util.Properties;

public class JdbcDriverMock implements Driver {

    public static final String URL = "jdbc:test";

    private Connection connection;

    public JdbcDriverMock(Connection connection) {
        this.connection = connection;
    }

    @Override
    public Connection connect(String s, Properties properties) throws SQLException {
        return connection;
    }

    @Override
    public boolean acceptsURL(String s) throws SQLException {
        return URL.equals(s);
    }

    @Override
    public DriverPropertyInfo[] getPropertyInfo(String s, Properties properties) throws SQLException {
        return new DriverPropertyInfo[0];
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
}
