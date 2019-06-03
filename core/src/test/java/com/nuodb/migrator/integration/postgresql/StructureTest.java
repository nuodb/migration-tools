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
package com.nuodb.migrator.integration.postgresql;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.HashMap;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.nuodb.migrator.integration.MigrationTestBase;
import com.nuodb.migrator.integration.types.PostgreSQLTypes;

/**
 * Test to make sure all the Tables, Constraints, Views, Triggers etc have been
 * migrated.
 * 
 * @author Krishnamoorthy Dhandapani
 */
@Test(groups = { "postgresqlintegrationtest" }, dependsOnGroups = { "dataloadperformed" })
public class StructureTest extends MigrationTestBase {

    /*
     * test if all the Tables are migrated with the right columns
     */
    public void testTables() throws Exception {
        String sqlStr1 = "SELECT table_name FROM information_schema.tables WHERE table_schema='public' order by "
                + "table_name";
        String sqlStr2 = "select tablename from system.TABLES where TYPE = 'TABLE' and schema = ?";
        PreparedStatement stmt1 = null, stmt2 = null;
        ResultSet rs1 = null, rs2 = null;
        ArrayList<String> list1 = new ArrayList<String>();
        ArrayList<String> list2 = new ArrayList<String>();
        try {
            stmt1 = sourceConnection.prepareStatement(sqlStr1);
            sourceConnection.setAutoCommit(true);
            rs1 = stmt1.executeQuery();

            Assert.assertNotNull(rs1);
            boolean sourceFound = false;

            while (rs1.next()) {
                sourceFound = true;
                list1.add(rs1.getString(1).toUpperCase());
            }
            Assert.assertTrue(sourceFound);
            Assert.assertFalse(list1.isEmpty());

            stmt2 = nuodbConnection.prepareStatement(sqlStr2);
            stmt2.setString(1, nuodbSchemaUsed);
            rs2 = stmt2.executeQuery();

            Assert.assertNotNull(rs2);
            boolean targetFound = false;
            while (rs2.next()) {
                targetFound = true;
                list2.add(rs2.getString(1).toUpperCase());
            }
            Assert.assertTrue(targetFound);
            Assert.assertFalse(list2.isEmpty());

            for (String tname : list1) {
                Assert.assertTrue(list2.contains(tname));
                verifyTableColumns(tname);
            }

        } finally {
            closeAll(rs1, stmt1, rs2, stmt2);
        }
    }

    /*
     * TODO: Need to add check for complex data types with scale and precision
     */
    private void verifyTableColumns(String tableName) throws Exception {
        String sqlStr1 = " select * from information_schema.columns where table_catalog = ? and TABLE_NAME = ? and "
                + "COLUMN_NAME is not null order by ordinal_position";
        String sqlStr2 = "select * from  system.FIELDS F inner join system.DATATYPES D on "
                + "F.DATATYPE = D.ID and F.SCHEMA = ? and F.TABLENAME = ? order by F.FIELDPOSITION";
        String[] colNames = new String[] { "COLUMN_NAME", "ORDINAL_POSITION", "COLUMN_DEFAULT", "IS_NULLABLE",
                "DATA_TYPE", "CHARACTER_MAXIMUM_LENGTH", "NUMERIC_PRECISION", "NUMERIC_SCALE", "CHARACTER_SET_NAME",
                "COLLATION_NAME" };
        PreparedStatement stmt1 = null, stmt2 = null;
        ResultSet rs1 = null, rs2 = null;
        HashMap<String, HashMap<String, String>> tabColMap = new HashMap<String, HashMap<String, String>>();
        try {
            stmt1 = sourceConnection.prepareStatement(sqlStr1);
            stmt1.setString(1, sourceConnection.getCatalog());
            stmt1.setString(2, tableName.toLowerCase());
            rs1 = stmt1.executeQuery();

            Assert.assertNotNull(rs1);
            boolean sourceFound = false;
            while (rs1.next()) {
                sourceFound = true;
                HashMap<String, String> tabColDetailsMap = new HashMap<String, String>();
                for (String colName : colNames) {
                    tabColDetailsMap.put(colName, rs1.getString(colName));
                }
                Assert.assertTrue(sourceFound);
                Assert.assertFalse(tabColDetailsMap.isEmpty(), tableName + " column details empty at source");

                tabColMap.put(tabColDetailsMap.get(colNames[0]), tabColDetailsMap);
            }
            Assert.assertTrue(sourceFound);
            Assert.assertFalse(tabColMap.isEmpty(), tableName + " column details map empty at source");

            stmt2 = nuodbConnection.prepareStatement(sqlStr2);
            stmt2.setString(1, nuodbSchemaUsed);
            stmt2.setString(2, tableName);
            rs2 = stmt2.executeQuery();

            Assert.assertNotNull(rs2);
            boolean targetFound = false;
            while (rs2.next()) {
                targetFound = true;
                String colName = rs2.getString("FIELD");
                HashMap<String, String> tabColDetailsMap = tabColMap.get(colName);
                Assert.assertNotNull(tabColDetailsMap);
                Assert.assertEquals(colName, tabColDetailsMap.get(colNames[0]),
                        "Column name " + colName + " of table " + tableName + " did not match");

                Assert.assertEquals(rs2.getInt("JDBCTYPE"),
                        PostgreSQLTypes.getMappedJDBCType(tabColDetailsMap.get(colNames[4])),
                        "JDBCTYPE of column " + colName + " of table " + tableName + " did not match");

                Assert.assertEquals(rs2.getString("LENGTH"),
                        PostgreSQLTypes.getMappedLength(tabColDetailsMap.get(colNames[4]),
                                tabColDetailsMap.get(colNames[5])),
                        "LENGTH of column " + colName + " of table " + tableName + " did not match");
            }
            Assert.assertTrue(targetFound);
        } finally {
            closeAll(rs1, stmt1, rs2, stmt2);
        }
    }

    /*
     * test if all the Views are migrated
     */
    @Test(groups = { "disabled" })
    public void testViews() throws Exception {
        // MYSQL Views are not migrated yet.
    }

    /*
     * test if all the Primary and Unique Key Constraints are migrated
     */
    public void testPrimaryAndUniqueKeyConstraints() throws Exception {
        String sqlStr1 = "SELECT tc.table_name, kcu.column_name, tc.constraint_type, tc.constraint_name,clm.DATA_TYPE"
                + " FROM information_schema.table_constraints AS tc"
                + " JOIN information_schema.key_column_usage AS kcu ON tc.constraint_name = kcu.constraint_name"
                + " JOIN information_schema.columns as clm ON kcu.column_name = clm.column_name"
                + " WHERE constraint_type in ('PRIMARY KEY', 'UNIQUE') and tc.table_catalog=?";
        String sqlStr2 = "SELECT FIELD FROM SYSTEM.INDEXES SI INNER JOIN SYSTEM.INDEXFIELDS SFI ON "
                + "SI.SCHEMA=SFI.SCHEMA AND " + "SI.TABLENAME=SFI.TABLENAME AND "
                + "SI.INDEXNAME=SFI.INDEXNAME WHERE SI.SCHEMA=? AND SI.TABLENAME=? AND SI.INDEXTYPE=?";
        PreparedStatement stmt1 = null, stmt2 = null;
        ResultSet rs1 = null, rs2 = null;
        try {
            stmt1 = sourceConnection.prepareStatement(sqlStr1);
            stmt1.setString(1, sourceConnection.getCatalog());
            rs1 = stmt1.executeQuery();

            Assert.assertNotNull(rs1);
            boolean sourceFound = false;
            while (rs1.next()) {
                sourceFound = true;
                String tName = rs1.getString("TABLE_NAME");
                String cName = rs1.getString("COLUMN_NAME");
                String cKey = rs1.getString("constraint_type");

                stmt2 = nuodbConnection.prepareStatement(sqlStr2);
                stmt2.setString(1, nuodbSchemaUsed);
                stmt2.setString(2, tName);
                stmt2.setInt(3, PostgreSQLTypes.getKeyType(cKey));
                rs2 = stmt2.executeQuery();
                boolean found = false;
                while (rs2.next()) {
                    found = true;
                    Assert.assertEquals(rs2.getString(1), cName, "Source column name " + cName + " of table " + tName
                            + " did not match with target column name ");
                }
                Assert.assertTrue(found);
                rs2.close();
                stmt2.close();
            }
            Assert.assertTrue(sourceFound);
        } finally {
            closeAll(rs1, stmt1, rs2, stmt2);
        }
    }

    /*
     * test if all the Check Constraints are migrated
     */
    @Test(groups = { "disabled" })
    public void testCheckConstraints() throws Exception {
        // MYSQL Does not have any implementations for CHECK constraints
    }

    /*
     * test if all the Foreign Key Constraints are migrated
     */
    public void testForeignKeyConstraints() throws Exception {
        String sqlStr1 = "SELECT tc.table_name, tc.constraint_name, kcu.column_name, ccu.table_name AS referenced_table_name, "
                + "ccu.column_name AS referenced_column_name" + " FROM information_schema.table_constraints AS tc "
                + " JOIN information_schema.key_column_usage AS kcu ON tc.constraint_name = kcu.constraint_name"
                + " JOIN information_schema.constraint_column_usage AS ccu ON ccu.constraint_name = tc"
                + ".constraint_name" + " WHERE constraint_type = 'FOREIGN KEY' and tc.table_catalog=?";

        String sqlStr2 = "SELECT PRIMARYTABLE.SCHEMA AS PKTABLE_SCHEM, PRIMARYTABLE.TABLENAME AS PKTABLE_NAME, "
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
                + "WHERE FOREIGNTABLE.SCHEMA=? AND FOREIGNTABLE.TABLENAME=? ORDER BY PKTABLE_SCHEM, PKTABLE_NAME, "
                + "KEY_SEQ ASC";

        PreparedStatement stmt1 = null, stmt2 = null;
        ResultSet rs1 = null, rs2 = null;
        try {
            stmt1 = sourceConnection.prepareStatement(sqlStr1);
            stmt1.setString(1, sourceConnection.getCatalog());
            sourceConnection.setAutoCommit(true);
            rs1 = stmt1.executeQuery();

            Assert.assertNotNull(rs1);
            boolean sourceFound = false;
            while (rs1.next()) {
                sourceFound = true;
                String tName = rs1.getString("TABLE_NAME");
                String cName = rs1.getString("COLUMN_NAME");
                String rtName = rs1.getString("REFERENCED_TABLE_NAME");
                String rcName = rs1.getString("REFERENCED_COLUMN_NAME");

                stmt2 = nuodbConnection.prepareStatement(sqlStr2);
                stmt2.setString(1, nuodbSchemaUsed);
                stmt2.setString(2, tName);
                rs2 = stmt2.executeQuery();
                boolean found = false;
                while (rs2.next()) {
                    found = true;
                    Assert.assertEquals(rs2.getString("FKTABLE_SCHEM"), rs2.getString("PKTABLE_SCHEM"),
                            "Foreign key and Primary key Schema did not match");
                    Assert.assertEquals(rs2.getString("FKTABLE_NAME"), tName, "Foreign key table name did not match");
                    Assert.assertEquals(rs2.getString("FKCOLUMN_NAME"), cName,
                            "Foreign key column name" + rs2.getString("FKCOLUMN_NAME") + "of table"
                                    + rs2.getString("FKTABLE_NAME") + "did not match");
                    Assert.assertEquals(rs2.getString("PKTABLE_NAME"), rtName, "Primary key table name did not match");
                    Assert.assertEquals(rs2.getString("PKCOLUMN_NAME"), rcName,
                            "Primary key column name" + rs2.getString("PKCOLUMN_NAME") + "of table"
                                    + rs2.getString("PKTABLE_NAME") + "did not match");
                }
                Assert.assertTrue(found);
                rs2.close();
                stmt2.close();
            }
            Assert.assertTrue(sourceFound);
        } finally {
            closeAll(rs1, stmt1, rs2, stmt2);
        }
    }

    /*
     * test if all the auto increment settings are migrated
     */
    @Test(groups = { "disabled" })
    public void testAutoIncrement() throws Exception {
        String sqlStr1 = "select T.TABLE_NAME, c.ordinal_position as AUTO_INCREMENT, C.COLUMN_NAME "
                + "from information_schema.TABLES T INNER JOIN information_schema.COLUMNS C "
                + "on T.TABLE_CATALOG=? and C.TABLE_CATALOG = T.TABLE_CATALOG and "
                + "T.TABLE_NAME = C.TABLE_NAME AND c.column_default like 'nextval%'";
        String sqlStr2 = "SELECT S.SEQUENCENAME FROM SYSTEM.SEQUENCES S "
                + "INNER JOIN SYSTEM.FIELDS F ON S.SCHEMA=F.SCHEMA "
                + "WHERE F.SCHEMA=? AND F.TABLENAME=? AND F.FIELD=?";
        PreparedStatement stmt1 = null, stmt2 = null;
        ResultSet rs1 = null, rs2 = null;
        try {
            stmt1 = sourceConnection.prepareStatement(sqlStr1);
            stmt1.setString(1, sourceConnection.getCatalog());
            sourceConnection.setAutoCommit(true);
            rs1 = stmt1.executeQuery();

            Assert.assertNotNull(rs1);

            boolean sourceFound = false;
            while (rs1.next()) {
                sourceFound = true;
                String tName = rs1.getString("TABLE_NAME");
                String cName = rs1.getString("COLUMN_NAME");
                stmt2 = nuodbConnection.prepareStatement(sqlStr2);
                stmt2.setString(1, nuodbSchemaUsed);
                stmt2.setString(2, tName);
                stmt2.setString(3, cName);
                rs2 = stmt2.executeQuery();
                boolean found = false;
                while (rs2.next()) {
                    found = true;
                    String seqName = rs2.getString("SEQUENCENAME");
                    Assert.assertNotNull(seqName);
                    if (seqName.equals(nuodbSchemaUsed + "$" + "IDENTITY_SEQUENCE")) {
                        continue;
                    }
                    Assert.assertEquals(seqName.substring(0, 4), "SEQ_");
                    // TODO: Need to check start value - Don't know how yet
                }
                Assert.assertTrue(found);
                rs2.close();
                stmt2.close();

            }
            Assert.assertTrue(sourceFound);
        } finally {
            closeAll(rs1, stmt1, rs2, stmt2);
        }
    }

    /*
     * test if all the Indexes are migrated
     */
    public void testIndexes() throws Exception {
        String sqlStr1 = "SELECT i.relname as indname," + " i.relowner as indowner,"
                + " idx.indrelid::regclass as TABLE_NAME," + " am.amname as indam," + " idx.indkey,"
                + " ( SELECT pg_get_indexdef(idx.indexrelid, k + 1, true) FROM generate_subscripts(idx.indkey, 1) as k ORDER BY k ) as indkey_names,"
                + " idx.indexprs IS NOT NULL as indexprs," + " idx.indpred IS NOT NULL as indpred"
                + " FROM   pg_index as idx" + " JOIN   pg_class as i ON i.oid = idx.indexrelid"
                + " JOIN   pg_am as am ON i.relam = am.oid" + " JOIN   pg_namespace as ns ON ns.oid = i.relnamespace"
                + " AND    ns.nspname = ANY(current_schemas(false))"
                + " WHERE pg_get_indexdef(indexrelid) like 'CREATE INDEX%'";

        String sqlStr2 = "SELECT I.INDEXNAME FROM SYSTEM.INDEXES I "
                + "INNER JOIN SYSTEM.INDEXFIELDS F ON I.SCHEMA=F.SCHEMA AND I.TABLENAME=F.TABLENAME AND I.INDEXNAME=F.INDEXNAME "
                + "WHERE I.INDEXTYPE=2 AND F.SCHEMA=? AND F.TABLENAME=? AND F.FIELD=?";
        PreparedStatement stmt1 = null, stmt2 = null;
        ResultSet rs1 = null, rs2 = null;
        try {
            stmt1 = sourceConnection.prepareStatement(sqlStr1);
            rs1 = stmt1.executeQuery();

            Assert.assertNotNull(rs1);
            boolean sourceFound = false;
            while (rs1.next()) {
                sourceFound = true;
                String tName = rs1.getString("TABLE_NAME");
                String cName = rs1.getString("indkey_names");

                stmt2 = nuodbConnection.prepareStatement(sqlStr2);
                stmt2.setString(1, nuodbSchemaUsed);
                stmt2.setString(2, tName);
                stmt2.setString(3, cName);
                rs2 = stmt2.executeQuery();
                boolean found = false;
                while (rs2.next()) {
                    found = true;
                    String idxName = rs2.getString("INDEXNAME");
                    Assert.assertNotNull(idxName);
                    Assert.assertEquals(idxName.substring(0, 4), "IDX_");
                }
                Assert.assertTrue(found);
                rs2.close();
                stmt2.close();

            }
            Assert.assertTrue(sourceFound);
        } finally {
            closeAll(rs1, stmt1, rs2, stmt2);
        }
    }

    /*
     * test if all the Triggers are migrated
     */
    @Test(groups = { "disabled" })
    public void testTriggers() throws Exception {
        // MYSQL Triggers are not migrated yet.
    }
}
