/**
 * Copyright (c) 2014, NuoDB, Inc.
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

import com.nuodb.migrator.integration.MigrationTestBase;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.sql.ResultSet;
import java.sql.Statement;

/**
 * @author Krishnamoorthy Dhandapani
 */
@Test(groups = {"integrationtest"})
public class BasicMigrationTest extends MigrationTestBase {

    /*
     * Assert Load is execute and database is not empty
     */
    @Test(groups = {"dataloadperformed"})
    public void testDataLoaded() throws Exception {
        String sqlStr1 = "select count(*) from datatypes1";
        String sqlStr2 = "select count(*) from \"datatypes1\"";
        Statement stmt1 = null, stmt2 = null;
        ResultSet rs1 = null, rs2 = null;
        try {
            stmt1 = sourceConnection.createStatement();
            rs1 = stmt1.executeQuery(sqlStr1);

            Assert.assertNotNull(rs1);

            stmt2 = nuodbConnection.createStatement();
            rs2 = stmt2.executeQuery(sqlStr2);

            Assert.assertNotNull(rs2);

            Assert.assertTrue(rs1.next());
            Assert.assertTrue(rs2.next());
            Assert.assertEquals(rs2.getInt(1), rs1.getInt(1),
                    "Please check if data is loaded.");

        } finally {
            closeAll(rs1, stmt1, rs2, stmt2);
        }
    }

    /*
     * The data type tests compares the result set values against all datatypes
     * set into multiple tables. See the table definition for specific data
     * types.
     *
     * The test executes the same select all statement against the source
     * database and the target database and compares the result values.
     */
    @Test(dependsOnGroups = {"dataloadperformed"})
    public void testDataTypes1() throws Exception {
        String sqlStr1 = "select * from datatypes1";
        String sqlStr2 = "select * from \"datatypes1\"";
        Statement stmt1 = null, stmt2 = null;
        ResultSet rs1 = null, rs2 = null;
        try {
            stmt1 = sourceConnection.createStatement();
            rs1 = stmt1.executeQuery(sqlStr1);

            Assert.assertNotNull(rs1);

            stmt2 = nuodbConnection.createStatement();
            rs2 = stmt2.executeQuery(sqlStr2);

            Assert.assertNotNull(rs2);

            rsUtil.assertIsEqual(rs1, rs2, true, true);

        } finally {
            closeAll(rs1, stmt1, rs2, stmt2);
        }
    }

    /*
     * See documentation for testDataTypes1
     */
    @Test(dependsOnGroups = {"dataloadperformed"})
    public void testDataTypes2() throws Exception {
        String sqlStr1 = "select * from datatypes2";
        String sqlStr2 = "select * from \"datatypes2\"";
        Statement stmt1 = null, stmt2 = null;
        ResultSet rs1 = null, rs2 = null;
        try {
            stmt1 = sourceConnection.createStatement();
            rs1 = stmt1.executeQuery(sqlStr1);

            Assert.assertNotNull(rs1);

            stmt2 = nuodbConnection.createStatement();
            rs2 = stmt2.executeQuery(sqlStr2);

            Assert.assertNotNull(rs2);

            rsUtil.assertIsEqual(rs1, rs2, true, true);

        } finally {
            closeAll(rs1, stmt1, rs2, stmt2);
        }
    }

    /*
     * See documentation for testDataTypes1
     */
    @Test(dependsOnGroups = {"dataloadperformed"})
    public void testDataTypes3() throws Exception {
        String sqlStr1 = "select * from datatypes3";
        String sqlStr2 = "select * from \"datatypes3\"";
        Statement stmt1 = null, stmt2 = null;
        ResultSet rs1 = null, rs2 = null;
        try {
            stmt1 = sourceConnection.createStatement();
            rs1 = stmt1.executeQuery(sqlStr1);

            Assert.assertNotNull(rs1);

            stmt2 = nuodbConnection.createStatement();
            rs2 = stmt2.executeQuery(sqlStr2);

            Assert.assertNotNull(rs2);

            rsUtil.assertIsEqual(rs1, rs2, true, true);

        } finally {
            closeAll(rs1, stmt1, rs2, stmt2);
        }
    }
}
