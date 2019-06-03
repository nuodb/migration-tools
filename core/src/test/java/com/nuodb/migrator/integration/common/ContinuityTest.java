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
package com.nuodb.migrator.integration.common;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.nuodb.migrator.integration.MigrationTestBase;

/**
 * @author Krishnamoorthy Dhandapani
 */
@Test(groups = { "integrationtest", "dataloadperformed" })
public class ContinuityTest extends MigrationTestBase {

    /*
     * Verify Primary Key constraints are working
     */
    public void testPrimaryKeyViolation() throws Exception {
        String sqlStr = "insert into \"datatypes1\" (\"c2\",\"c6\") values (7,5)";
        Statement stmt1 = null;
        try {
            stmt1 = nuodbConnection.createStatement();
            int rows = stmt1.executeUpdate(sqlStr);
            Assert.assertEquals(rows, 1);
            // second time should fail
            try {
                rows = stmt1.executeUpdate(sqlStr);
            } catch (SQLException e) {
                Assert.assertEquals(e.getSQLState(), "23000");
            }
        } finally {
            nuodbConnection.rollback();
            stmt1.close();
        }
    }

    /*
     * Verify Unique Key constraints are working
     */
    public void testUniqueKeyViolation() throws Exception {
        String sqlStr = "insert into \"datatypes1\" (\"c2\",\"c6\") values ('1',20)";
        Statement stmt1 = null;
        try {
            stmt1 = nuodbConnection.createStatement();
            int rows = stmt1.executeUpdate(sqlStr);
            Assert.assertEquals(rows, 1);
            // second time should fail
            try {
                rows = stmt1.executeUpdate(sqlStr);
            } catch (SQLException e) {
                Assert.assertEquals(e.getSQLState(), "23000");
            }
        } finally {
            nuodbConnection.rollback();
            stmt1.close();
        }
    }

    /*
     * Verify Foreign Key constraints are working - NOT WORKING YET
     */
    @Test(groups = { "disabled" })
    public void testForeignKeyViolation() throws Exception {
    }

    /*
     * Verify Auto increments are working. We don't know what value the auto inc
     * is going to start because it depend on the number of times and test is
     * execute. So we are going to execute it twice and make sure the id is
     * incremented by 1 between those runs.
     */
    @Test(groups = { "disabled" })
    public void testAutoIncrement() throws Exception {
        String sqlStr = "insert into \"datatypes2\" (\"c5\") values (?)";
        String sqlStr2 = "select \"k1\" from \"datatypes2\" where \"c5\"=?";
        PreparedStatement stmt1 = null, stmt2 = null;
        ResultSet rs2 = null;
        int firstId = -1;
        try {
            String testId = "testAI1";
            {
                stmt1 = nuodbConnection.prepareStatement(sqlStr);
                stmt1.setString(1, testId);
                int rows = stmt1.executeUpdate();
                Assert.assertEquals(rows, 1);

                stmt2 = nuodbConnection.prepareStatement(sqlStr2);
                stmt2.setString(1, testId);
                rs2 = stmt2.executeQuery();
                boolean found = false;
                while (rs2.next()) {
                    found = true;
                    firstId = rs2.getInt(1);
                    Assert.assertTrue(firstId > 0);
                }
                Assert.assertTrue(found);

                closeAll(null, stmt1, rs2, stmt2);
            }
            // Now insert second row
            testId = "testAI2";
            {
                stmt1 = nuodbConnection.prepareStatement(sqlStr);
                stmt1.setString(1, testId);
                int rows = stmt1.executeUpdate();
                Assert.assertEquals(rows, 1);

                stmt2 = nuodbConnection.prepareStatement(sqlStr2);
                stmt2.setString(1, testId);
                rs2 = stmt2.executeQuery();
                boolean found = false;
                while (rs2.next()) {
                    found = true;
                    Assert.assertEquals(rs2.getInt(1), firstId + 1);
                }
                Assert.assertTrue(found);
            }
        } finally {
            nuodbConnection.rollback();
            closeAll(null, stmt1, rs2, stmt2);
        }
    }

}
