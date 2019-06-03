package com.nuodb.migrator.integration.nuodb;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLClassLoader;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.logging.Logger;

public class TestSetUpHelper {

    public static final String NUODB_JDBC_JAR = "nuodbjdbc.jar";

    protected Connection sourceConnection;

    public static void main(String[] args) throws Exception {
        TestSetUpHelper th = new TestSetUpHelper();
        th.setUpBlobs();
    }

    public TestSetUpHelper() throws Exception {
        String sourceJdbcJar = System.getProperty("source.jdbcjar");
        String nuodbJdbcJar = System.getProperty("nuodb.jdbcjar");
        String nuodbHome = System.getProperty("nuodb.home");
        String sourceDriver = System.getProperty("source.driver");
        String sourceUsername = System.getProperty("source.username");
        String sourcePassword = System.getProperty("source.password");
        String sourceSchema = System.getProperty("source.schema");
        String sourceUrl = System.getProperty("source.url");

        List<URL> urls = new ArrayList<URL>();

        if (sourceJdbcJar != null && sourceJdbcJar.trim().length() > 0) {
            File sourceJdbcJarFile = new File(sourceJdbcJar);
            sourceJdbcJarFile = new File(sourceJdbcJarFile.getCanonicalPath());
            urls.add(sourceJdbcJarFile.toURI().toURL());
        }

        String nuodbJdbcJarLoc = nuodbJdbcJar;
        if (nuodbJdbcJarLoc == null || nuodbJdbcJarLoc.trim().length() == 0) {
            // Take it from the nuodb.home
            nuodbJdbcJarLoc = nuodbHome + File.separator + "jar" + File.separator + NUODB_JDBC_JAR;
        }
        if (nuodbJdbcJarLoc != null || nuodbJdbcJarLoc.trim().length() > 0) {
            File nuodbJdbcJarFile = new File(nuodbJdbcJarLoc);
            nuodbJdbcJarFile = new File(nuodbJdbcJarFile.getCanonicalPath());
            urls.add(nuodbJdbcJarFile.toURI().toURL());
        }

        URLClassLoader ucl = new URLClassLoader(urls.toArray(new URL[urls.size()]),
                Thread.currentThread().getContextClassLoader());
        Thread.currentThread().setContextClassLoader(ucl);

        Driver d1 = (Driver) Class.forName(sourceDriver, true, ucl).newInstance();
        DriverManager.registerDriver(new DriverShim(d1));

        Properties sourceProp = new Properties();
        sourceProp.put("user", sourceUsername);
        sourceProp.put("password", sourcePassword);
        sourceProp.put("schema", sourceSchema);
        sourceConnection = DriverManager.getConnection(sourceUrl, sourceProp);
    }

    public void setUpBlobs() throws SQLException, URISyntaxException, FileNotFoundException {
        URL binUrl = getClass().getResource("/nuodb/nuo_logo.png");
        File binFile = new File(binUrl.toURI());
        long size = binFile.length();
        String sqlStr = "update datatypes3 set \"c2\"=? where \"fk1\"=?";
        PreparedStatement stmt1 = null;
        try {
            stmt1 = sourceConnection.prepareStatement(sqlStr);
            stmt1.setBinaryStream(1, new FileInputStream(binFile), size);
            stmt1.setInt(2, 1);
            stmt1.executeUpdate();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            stmt1.close();
        }
    }

    protected void finalize() throws Throwable {
        if (sourceConnection != null) {
            sourceConnection.close();
        }
    }

    private class DriverShim implements Driver {
        private Driver driver;

        DriverShim(Driver d) {
            this.driver = d;
        }

        public boolean acceptsURL(String u) throws SQLException {
            return this.driver.acceptsURL(u);
        }

        public Connection connect(String u, Properties p) throws SQLException {
            return this.driver.connect(u, p);
        }

        public int getMajorVersion() {
            return this.driver.getMajorVersion();
        }

        public int getMinorVersion() {
            return this.driver.getMinorVersion();
        }

        public DriverPropertyInfo[] getPropertyInfo(String u, Properties p) throws SQLException {
            return this.driver.getPropertyInfo(u, p);
        }

        public boolean jdbcCompliant() {
            return this.driver.jdbcCompliant();
        }

        public Logger getParentLogger() throws SQLFeatureNotSupportedException {
            throw new SQLFeatureNotSupportedException();
        }
    }
}
