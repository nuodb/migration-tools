package com.nuodb.tools.migration.jdbc.connection;

import java.sql.Connection;
import java.sql.SQLException;

public interface JdbcConnectionProvider {

    public Connection getConnection() throws SQLException;

    public void closeConnection(Connection connection) throws SQLException;
}
