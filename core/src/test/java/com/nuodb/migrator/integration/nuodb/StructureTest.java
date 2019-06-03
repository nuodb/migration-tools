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
package com.nuodb.migrator.integration.nuodb;

import java.sql.PreparedStatement;
import java.sql.ResultSet;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.nuodb.migrator.integration.MigrationTestBase;

/**
 * Test to make sure all the Tables, Constraints, Views, Triggers etc have been
 * migrated.
 * 
 * @author Krishnamoorthy Dhandapani
 */
@Test(groups = { "nuodbintegrationtest" }, dependsOnGroups = { "dataloadperformed" })
public class StructureTest extends MigrationTestBase {

    /*
     * test if all the Tables are migrated with the right columns
     */
    public void testTables() throws Exception {
        String sqlStr = "select * from system.TABLES T where T.TYPE = 'TABLE' and T.SCHEMA = ?";
        matchResultSetForSQL(sqlStr);
    }

    /*
     * TODO: Need to add check for complex data types with scale and precision
     */
    public void verifyTableColumns() throws Exception {
        String sqlStr = "select * from  system.FIELDS F where F.SCHEMA = ?";
        matchResultSetForSQL(sqlStr);
    }

    /*
     * test if all the Views are migrated
     */
    public void testViews() throws Exception {
        String sqlStr = "select * from system.VIEW_TABLES V where V.SCHEMA = ?";
        matchResultSetForSQL(sqlStr);
    }

    /*
     * test if all the Primary and Unique Key Constraints are migrated
     */
    public void testPrimaryAndUniqueKeyConstraints() throws Exception {
        String sqlStr = "SELECT * FROM SYSTEM.INDEXES SI INNER JOIN SYSTEM.INDEXFIELDS SIF ON "
                + "SI.SCHEMA=SIF.SCHEMA AND " + "SI.TABLENAME=SIF.TABLENAME AND "
                + "SI.INDEXNAME=SIF.INDEXNAME WHERE SI.SCHEMA=?";
        matchResultSetForSQL(sqlStr);
    }

    /*
     * test if all the Check Constraints are migrated
     */
    public void testCheckConstraints() throws Exception {
        String sqlStr = "SELECT * FROM SYSTEM.TABLECONSTRAINTS C WHERE C.SCHEMA=?";
        matchResultSetForSQL(sqlStr);
    }

    /*
     * test if all the Foreign Key Constraints are migrated
     */
    public void testForeignKeyConstraints() throws Exception {
        String sqlStr = "SELECT PRIMARYTABLE.SCHEMA AS PKTABLE_SCHEM, PRIMARYTABLE.TABLENAME AS PKTABLE_NAME, "
                + " PRIMARYFIELD.FIELD AS PKCOLUMN_NAME, FOREIGNTABLE.SCHEMA AS FKTABLE_SCHEM, "
                + " FOREIGNTABLE.TABLENAME AS FKTABLE_NAME, FOREIGNFIELD.FIELD AS FKCOLUMN_NAME, "
                + " FOREIGNKEYS.POSITION+1 AS KEY_SEQ, FOREIGNKEYS.UPDATERULE AS UPDATE_RULE, "
                + " FOREIGNKEYS.DELETERULE AS DELETE_RULE, FOREIGNKEYS.DEFERRABILITY AS DEFERRABILITY "
                + "FROM SYSTEM.FOREIGNKEYS "
                + "INNER JOIN SYSTEM.TABLES PRIMARYTABLE ON PRIMARYTABLEID=PRIMARYTABLE.TABLEID "
                + "INNER JOIN SYSTEM.FIELDS PRIMARYFIELD ON PRIMARYTABLE.SCHEMA=PRIMARYFIELD.SCHEMA "
                + "AND PRIMARYTABLE.TABLENAME=PRIMARYFIELD.TABLENAME "
                + "AND FOREIGNKEYS.PRIMARYFIELDID=PRIMARYFIELD.FIELDID "
                + "INNER JOIN SYSTEM.TABLES FOREIGNTABLE ON FOREIGNTABLEID=FOREIGNTABLE.TABLEID "
                + "INNER JOIN SYSTEM.FIELDS FOREIGNFIELD ON FOREIGNTABLE.SCHEMA=FOREIGNFIELD.SCHEMA "
                + "AND FOREIGNTABLE.TABLENAME=FOREIGNFIELD.TABLENAME "
                + "AND FOREIGNKEYS.FOREIGNFIELDID=FOREIGNFIELD.FIELDID "
                + "WHERE FOREIGNTABLE.SCHEMA=? ORDER BY PKTABLE_SCHEM, PKTABLE_NAME, KEY_SEQ ASC";
        matchResultSetForSQL(sqlStr);
    }

    /*
     * test if all the auto increment settings are migrated
     */
    @Test(groups = { "disabled" })
    public void testAutoIncrement() throws Exception {
        String sqlStr = "SELECT * FROM SYSTEM.SEQUENCES S " + "INNER JOIN SYSTEM.FIELDS F ON S.SCHEMA=F.SCHEMA "
                + "WHERE F.SCHEMA=?";
        matchResultSetForSQL(sqlStr);
    }

    /*
     * test if all the Indexes are migrated
     */
    public void testIndexes() throws Exception {
        String sqlStr = "SELECT * FROM SYSTEM.INDEXES I "
                + "INNER JOIN SYSTEM.INDEXFIELDS F ON I.SCHEMA=F.SCHEMA AND I.TABLENAME=F.TABLENAME AND I.INDEXNAME=F"
                + ".INDEXNAME " + "WHERE I.INDEXTYPE=2 AND F.SCHEMA=?";
        matchResultSetForSQL(sqlStr);
    }

    /*
     * test if all the Triggers are migrated
     */
    public void testTriggers() throws Exception {
        String sqlStr = "SELECT * FROM SYSTEM.TRIGGERS T WHERE T.SCHEMA=?";
        matchResultSetForSQL(sqlStr);
    }

    private void matchResultSetForSQL(String sqlStr) throws Exception {
        PreparedStatement stmt1 = null, stmt2 = null;
        ResultSet rs1 = null, rs2 = null;
        try {
            stmt1 = sourceConnection.prepareStatement(sqlStr);
            stmt1.setString(1, sourceConnection.getCatalog());
            rs1 = stmt1.executeQuery();

            Assert.assertNotNull(rs1);

            stmt2 = nuodbConnection.prepareStatement(sqlStr);
            stmt2.setString(1, nuodbSchemaUsed);
            rs2 = stmt2.executeQuery();

            Assert.assertNotNull(rs2);

            rsUtil.assertIsEqual(rs1, rs2, true, true);
        } finally {
            closeAll(rs1, stmt1, rs2, stmt2);
        }
    }
}
