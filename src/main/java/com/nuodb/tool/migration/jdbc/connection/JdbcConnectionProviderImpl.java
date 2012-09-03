package com.nuodb.tool.migration.jdbc.connection;

import com.nuodb.tool.migration.definition.JdbcConnection;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

public class JdbcConnectionProviderImpl implements JdbcConnectionProvider {

    public static final String USER_PROPERTY = "user";
    public static final String PASSWORD_PROPERTY = "password";

    private transient final Log log = LogFactory.getLog(this.getClass());

    private JdbcConnection jdbcConnection;

    public JdbcConnectionProviderImpl(JdbcConnection jdbcConnection) {
        this.jdbcConnection = jdbcConnection;
    }

    public Connection getConnection() throws SQLException {
        loadDriver();
        return createConnection();
    }

    protected void loadDriver() {
        try {
            String driver = jdbcConnection.getDriver();
            if (log.isDebugEnabled()) {
                log.debug(String.format("Loading driver %s", driver));
            }
            Class.forName(driver);
        } catch (ClassNotFoundException e) {
            if (log.isWarnEnabled()) {
                log.warn("Driver can't be loaded", e);
            }
        }
    }

    protected Connection createConnection() throws SQLException {
        String url = jdbcConnection.getUrl();
        Properties properties = new Properties();
        if (jdbcConnection.getProperties() != null) {
            properties.putAll(jdbcConnection.getProperties());
        }
        String username = jdbcConnection.getUsername();
        String password = jdbcConnection.getPassword();
        if (username != null) {
            properties.setProperty(USER_PROPERTY, username);
        }
        if (password != null) {
            properties.setProperty(PASSWORD_PROPERTY, password);
        }
        if (log.isDebugEnabled()) {
            log.debug(String.format("Creating new connection at %s", url));
        }
        return DriverManager.getConnection(url, properties);
    }

    public void closeConnection(Connection connection) throws SQLException {
        if (log.isDebugEnabled()) {
            log.debug("Closing connection");
        }
        connection.close();
    }
}
