/**
 * Copyright (c) 2015, NuoDB, Inc.
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *     * Redistributions of source code must retain the above copyright
 *       notice, this list of conditions and the following disclaimer.
 *     * Redistributions in binary form must reproduce the above copyright
 *       notice, this list of conditions and the following disclaimer in the
 *       documentation and/or other materials provided with the distribution.
 *     * Neither the name of NuoDB, Inc. nor the names of its contributors may
 *       be used to endorse or promote products derived from this software
 *       without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED. IN NO EVENT SHALL NUODB, INC. BE LIABLE FOR ANY DIRECT, INDIRECT,
 * INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA,
 * OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
 * LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE
 * OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF
 * ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */
package com.nuodb.migrator.integration;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.logging.Logger;

import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Optional;
import org.testng.annotations.Parameters;

/**
 * @author Krishnamoorthy Dhandapani
 */
public class MigrationTestBase {

    public static final String NUODB_JDBC_JAR = "nuodbjdbc.jar";

    protected Connection sourceConnection;
    protected Connection nuodbConnection;
    protected String nuodbSchemaUsed = null;
    protected String sourceSchemaUsed = null;

    protected ResultSetUtil rsUtil;

    @Parameters({ "source.driver", "source.url", "source.username", "source.password", "source.schema",
            "source.jdbcjar", "nuodb.home", "nuodb.driver", "nuodb.url", "nuodb.username", "nuodb.password",
            "nuodb.schema", "nuodb.jdbcjar" })
    @BeforeClass(alwaysRun = true)
    public void beforeTest(String sourceDriver, String sourceUrl, String sourceUsername,
            @Optional("") String sourcePassword, @Optional("") String sourceSchema, @Optional("") String sourceJdbcJar,
            @Optional("") String nuodbRoot, String nuodbDriver, String nuodbUrl, String nuodbUsername,
            String nuodbPassword, String nuodbSchema, @Optional("") String nuodbJdbcJar)
            throws ClassNotFoundException, SQLException, IOException, InstantiationException, IllegalAccessException {

        List<URL> urls = new ArrayList<URL>();

        if (sourceJdbcJar != null && sourceJdbcJar.trim().length() > 0) {
            String[] listOfJars = sourceJdbcJar.split(";");
            for (String sourceJar : listOfJars) {
                File sourceJdbcJarFile = new File(sourceJar);
                sourceJdbcJarFile = new File(sourceJdbcJarFile.getCanonicalPath());
                Assert.assertTrue(sourceJdbcJarFile.exists());
                Assert.assertTrue(sourceJdbcJarFile.canRead());
                urls.add(sourceJdbcJarFile.toURI().toURL());
            }
        }

        String nuodbJdbcJarLoc = nuodbJdbcJar;
        if (nuodbJdbcJarLoc == null || nuodbJdbcJarLoc.trim().length() == 0) {
            // Take it from the nuodb.home
            nuodbJdbcJarLoc = nuodbRoot + File.separator + "jar" + File.separator + NUODB_JDBC_JAR;
        }

        if (nuodbJdbcJarLoc != null || nuodbJdbcJarLoc.trim().length() > 0) {
            File nuodbJdbcJarFile = new File(nuodbJdbcJarLoc);
            nuodbJdbcJarFile = new File(nuodbJdbcJarFile.getCanonicalPath());
            Assert.assertTrue(nuodbJdbcJarFile.exists());
            Assert.assertTrue(nuodbJdbcJarFile.canRead());
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
        sourceConnection.setAutoCommit(false);

        sourceSchemaUsed = sourceSchema;

        Driver d2 = (Driver) Class.forName(nuodbDriver, true, ucl).newInstance();
        DriverManager.registerDriver(new DriverShim(d2));
        Properties nuodbProp = new Properties();
        nuodbProp.put("user", nuodbUsername);
        nuodbProp.put("password", nuodbPassword);
        nuodbProp.put("schema", nuodbSchema);
        nuodbConnection = DriverManager.getConnection(nuodbUrl, nuodbProp);
        nuodbConnection.setAutoCommit(false);

        // Save this to support java 1.6 where connection.getSchema method is
        // not there.
        nuodbSchemaUsed = nuodbSchema;

        rsUtil = new ResultSetUtil(sourceDriver);

    }

    @AfterClass
    public void tearDown() throws SQLException {
        if (sourceConnection != null) {
            sourceConnection.close();
        }
        if (nuodbConnection != null) {
            nuodbConnection.close();
        }
    }

    protected void closeAll(ResultSet rs1, Statement stmt1) throws SQLException {
        if (rs1 != null) {
            rs1.close();
        }
        if (stmt1 != null) {
            stmt1.close();
        }
    }

    protected void closeAll(ResultSet rs1, Statement stmt1, ResultSet rs2, Statement stmt2) throws SQLException {
        closeAll(rs1, stmt1);
        closeAll(rs2, stmt2);
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
