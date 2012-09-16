package com.nuodb.tools.migration.jdbc.connection;

import com.nuodb.tools.migration.definition.JdbcConnection;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

public class JdbcConnectionProviderImpl implements JdbcConnectionProvider {

    public static final String USER_PROPERTY = "user";
    public static final String PASSWORD_PROPERTY = "password";

    private transient final Log log = LogFactory.getLog(this.getClass());

    private JdbcConnection connection;

    public JdbcConnectionProviderImpl(JdbcConnection connection) {
        this.connection = connection;
    }

    public java.sql.Connection getConnection() throws SQLException {
        loadDriver();
        return createConnection();
    }

    protected void loadDriver() {
        try {
            String driver = connection.getDriver();
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

    protected java.sql.Connection createConnection() throws SQLException {
        String url = connection.getUrl();
        Properties properties = new Properties();
        if (connection.getProperties() != null) {
            properties.putAll(connection.getProperties());
        }
        String username = connection.getUsername();
        String password = connection.getPassword();
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

    public void closeConnection(java.sql.Connection connection) throws SQLException {
        if (log.isDebugEnabled()) {
            log.debug("Closing connection");
        }
        connection.close();
    }
}
