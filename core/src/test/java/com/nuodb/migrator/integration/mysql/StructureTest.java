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
package com.nuodb.migrator.integration.mysql;

import static com.nuodb.migrator.integration.precision.MySQLPrecisions.getMySQLPrecision1;
import static com.nuodb.migrator.integration.precision.MySQLPrecisions.getMySQLPrecision2;
import static java.lang.String.format;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;

import org.apache.commons.lang3.StringUtils;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.nuodb.migrator.integration.MigrationTestBase;
import com.nuodb.migrator.integration.precision.MySQLPrecision1;
import com.nuodb.migrator.integration.precision.MySQLPrecision2;
import com.nuodb.migrator.integration.types.MySQLTypes;

/**
 * Test to make sure all the Tables, Constraints, Views, Triggers etc have been
 * migrated.
 * 
 * @author Krishnamoorthy Dhandapani
 */
@Test(groups = { "mysqlintegrationtest" }, dependsOnGroups = { "dataloadperformed" })
public class StructureTest extends MigrationTestBase {

    /*
     * test if all the Tables are migrated with the right columns
     */
    public void testTables() throws Exception {
        String sqlStr1 = "select TABLE_NAME from information_schema.TABLES where TABLE_TYPE='BASE TABLE' AND "
                + "TABLE_SCHEMA = ?";
        String sqlStr2 = "select tablename from system.TABLES where TYPE = 'TABLE' and schema = ?";
        PreparedStatement stmt1 = null, stmt2 = null;
        ResultSet rs1 = null, rs2 = null;
        ArrayList<String> list1 = new ArrayList<String>();
        ArrayList<String> list2 = new ArrayList<String>();
        try {
            stmt1 = sourceConnection.prepareStatement(sqlStr1);
            stmt1.setString(1, sourceConnection.getCatalog());
            rs1 = stmt1.executeQuery();

            Assert.assertNotNull(rs1);
            boolean sourceFound = false;
            while (rs1.next()) {
                sourceFound = true;
                list1.add(rs1.getString(1).toUpperCase());
            }
            assertTrue(sourceFound);
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
            assertTrue(targetFound);
            Assert.assertFalse(list2.isEmpty());

            for (String tname : list1) {
                assertTrue(list2.contains(tname));
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
        String sqlStr1 = "select * from information_schema.COLUMNS where TABLE_SCHEMA = ? and TABLE_NAME = ? order by"
                + " ORDINAL_POSITION";
        String sqlStr2 = "select * from  system.FIELDS F inner join system.DATATYPES D on "
                + "F.DATATYPE = D.ID and F.SCHEMA = ? and F.TABLENAME = ? order by F.FIELDPOSITION";
        String[] colNames = new String[] { "COLUMN_NAME", "ORDINAL_POSITION", "COLUMN_DEFAULT", "IS_NULLABLE",
                "DATA_TYPE", "CHARACTER_MAXIMUM_LENGTH", "NUMERIC_PRECISION", "NUMERIC_SCALE", "CHARACTER_SET_NAME",
                "COLLATION_NAME", "COLUMN_TYPE" };
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

                Assert.assertFalse(tabColDetailsMap.isEmpty(), tableName + " column details empty at source");

                tabColMap.put(tabColDetailsMap.get(colNames[0]), tabColDetailsMap);
            }
            assertTrue(sourceFound);
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
                assertEquals(colName, tabColDetailsMap.get(colNames[0]),
                        "Column name " + colName + " of table " + tableName + " did not match");

                assertEquals(rs2.getInt("JDBCTYPE"),
                        MySQLTypes.getMappedJDBCType(tabColDetailsMap.get(colNames[4]),
                                tabColDetailsMap.get(colNames[10])),
                        "JDBCTYPE of column " + colName + " of table " + tableName + " did not match");

                assertEquals(rs2.getString("LENGTH"),
                        MySQLTypes.getMappedLength(tabColDetailsMap.get(colNames[4]),
                                tabColDetailsMap.get(colNames[10]), tabColDetailsMap.get(colNames[5])),
                        "LENGTH of column " + colName + " of table " + tableName + " did not match");
                // TBD
                String actualVal = rs2.getString("DEFAULTVALUE");
                String expectedVal = MySQLTypes.getMappedDefault(tabColDetailsMap.get(colNames[4]),
                        tabColDetailsMap.get(colNames[2]));

                // normalize null values
                if (actualVal == null || "'NULL'".equalsIgnoreCase(actualVal) || "NULL".equalsIgnoreCase(actualVal)
                        || StringUtils.isEmpty(actualVal)) {
                    actualVal = "'NULL'";
                }
                if (expectedVal == null || "'NULL'".equalsIgnoreCase(expectedVal)
                        || "NULL".equalsIgnoreCase(expectedVal)) {
                    expectedVal = "'NULL'";
                }
                assertEquals(actualVal, expectedVal,
                        "DEFAULTVALUE of column " + colName + " of table " + tableName + " did not match");
            }
            assertTrue(targetFound);
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
     * test if all the Primary Key Constraints are migrated
     */
    public void testPrimaryConstraints() throws Exception {
        String sqlStr1 = "select TC.TABLE_NAME, C.COLUMN_NAME, C.COLUMN_KEY from information_schema.TABLE_CONSTRAINTS TC "
                + "inner join information_schema.COLUMNS C on TC.CONSTRAINT_SCHEMA=? "
                + "and C.TABLE_SCHEMA = TC.CONSTRAINT_SCHEMA "
                + "and TC.TABLE_NAME=C.TABLE_NAME AND C.COLUMN_KEY=SUBSTRING(TC.CONSTRAINT_TYPE,1,3) "
                + "AND C.COLUMN_KEY IN ('PRI')";
        String sqlStr2 = "SELECT FIELD FROM SYSTEM.INDEXES AS I INNER JOIN SYSTEM.INDEXFIELDS AS F ON "
                + "I.SCHEMA=F.SCHEMA AND I.TABLENAME=F.TABLENAME AND "
                + "I.INDEXNAME=F.INDEXNAME WHERE I.SCHEMA=? AND I.TABLENAME=? AND FIELD=? AND "
                + "I.INDEXTYPE=? AND I.INDEXNAME LIKE '%PRIMARY_KEY%'";
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
                String cKey = rs1.getString("COLUMN_KEY");

                stmt2 = nuodbConnection.prepareStatement(sqlStr2);
                stmt2.setString(1, nuodbSchemaUsed);
                stmt2.setString(2, tName);
                stmt2.setString(3, cName);
                stmt2.setInt(4, MySQLTypes.getKeyType(cKey));
                rs2 = stmt2.executeQuery();
                assertTrue(rs2.next(), "Source column name " + cName + " of table " + tName
                        + " did not match with target column name ");
                rs2.close();
                stmt2.close();
            }
            assertTrue(sourceFound);
        } finally {
            closeAll(rs1, stmt1, rs2, stmt2);
        }
    }

    /*
     * test if all the Unique Key Constraints are migrated
     */
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public void testUniqueKeyConstraints() throws Exception {
        String sqlStr1 = "select distinct TC.TABLE_NAME, C.COLUMN_NAME, C.COLUMN_KEY from information_schema.TABLE_CONSTRAINTS"
                + " TC " + "inner join information_schema.COLUMNS C on TC.CONSTRAINT_SCHEMA=? "
                + "and C.TABLE_SCHEMA = TC.CONSTRAINT_SCHEMA "
                + "and TC.TABLE_NAME=C.TABLE_NAME AND C.COLUMN_KEY=SUBSTRING(TC.CONSTRAINT_TYPE,1,3) "
                + "AND C.COLUMN_KEY IN ('UNI')";
        String sqlStr2 = "SELECT F.FIELD, IFS.INDEXNAME, I.INDEXTYPE FROM SYSTEM.INDEXES AS I "
                + "INNER JOIN SYSTEM.INDEXFIELDS AS IFS ON "
                + "I.SCHEMA=IFS.SCHEMA AND I.TABLENAME=IFS.TABLENAME AND I.INDEXNAME=IFS.INDEXNAME "
                + "INNER JOIN SYSTEM.FIELDS F ON IFS.SCHEMA=F.SCHEMA AND IFS.TABLENAME=F.TABLENAME AND IFS.FIELD=F.FIELD "
                + "WHERE I.SCHEMA=? AND I.TABLENAME=? ORDER BY F.FIELDPOSITION";
        PreparedStatement stmt1 = null, stmt2 = null;
        ResultSet rs1 = null, rs2 = null;
        HashMap map = new HashMap();
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

                if (!map.containsKey(tName)) {
                    ArrayList list = new ArrayList();
                    list.add(cName);
                    map.put(tName, list);
                } else {
                    ArrayList list = (ArrayList) map.get(tName);
                    list.add(cName);
                    map.put(tName, list);
                }
            }
            assertTrue(sourceFound);
            Iterator<Entry<String, ArrayList>> uniKey = map.entrySet().iterator();
            while (uniKey.hasNext()) {
                Entry<String, ArrayList> pairs = uniKey.next();
                String srcTname = pairs.getKey();
                ArrayList<String> srcColList = pairs.getValue();
                ArrayList<String> tarColList = new ArrayList<String>();

                stmt2 = nuodbConnection.prepareStatement(sqlStr2);
                stmt2.setString(1, nuodbSchemaUsed);
                stmt2.setString(2, srcTname);
                rs2 = stmt2.executeQuery();
                boolean found = false;
                while (rs2.next()) {
                    found = true;
                    if (rs2.getInt(3) == 1) {
                        tarColList.add(rs2.getString(1));
                    }
                }
                assertTrue(found, format("Expecting unique keys for %s table to be found", srcTname));
                assertEquals(tarColList, srcColList,
                        format("Expecting unique keys for %s table to be equal", srcTname));
                rs2.close();
                stmt2.close();
            }

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
        String sqlStr1 = "select TC.TABLE_NAME, CU.COLUMN_NAME, CU.REFERENCED_TABLE_NAME, CU.REFERENCED_COLUMN_NAME "
                + "from information_schema.TABLE_CONSTRAINTS TC INNER JOIN information_schema.KEY_COLUMN_USAGE CU "
                + "on TC.CONSTRAINT_SCHEMA=? and CU.TABLE_SCHEMA = TC.CONSTRAINT_SCHEMA and "
                + "TC.TABLE_NAME = CU.TABLE_NAME AND TC.CONSTRAINT_TYPE = 'FOREIGN KEY' AND TC.CONSTRAINT_NAME = CU.CONSTRAINT_NAME;";
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
                + "WHERE FOREIGNTABLE.SCHEMA=? AND FOREIGNTABLE.TABLENAME=? AND PRIMARYTABLE.TABLENAME = ? ORDER BY PKTABLE_SCHEM, PKTABLE_NAME, "
                + "KEY_SEQ ASC";
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
                String rtName = rs1.getString("REFERENCED_TABLE_NAME");
                String rcName = rs1.getString("REFERENCED_COLUMN_NAME");

                stmt2 = nuodbConnection.prepareStatement(sqlStr2);
                stmt2.setString(1, nuodbSchemaUsed);
                stmt2.setString(2, tName);
                stmt2.setString(3, rtName);
                rs2 = stmt2.executeQuery();
                boolean found = false;
                while (rs2.next()) {
                    found = true;
                    assertEquals(rs2.getString("FKTABLE_SCHEM"), rs2.getString("PKTABLE_SCHEM"),
                            "Foreign key and Primary key Schema did not match");
                    assertEquals(rs2.getString("FKTABLE_NAME"), tName, "Foreign key table name did not match");
                    assertEquals(rs2.getString("FKCOLUMN_NAME"), cName,
                            "Foreign key column name" + rs2.getString("FKCOLUMN_NAME") + "of table"
                                    + rs2.getString("FKTABLE_NAME") + "did not match");
                    assertEquals(rs2.getString("PKTABLE_NAME"), rtName, "Primary key table name did not match");
                    assertEquals(rs2.getString("PKCOLUMN_NAME"), rcName,
                            "Primary key column name" + rs2.getString("PKCOLUMN_NAME") + "of table"
                                    + rs2.getString("PKTABLE_NAME") + "did not match");
                }
                assertTrue(found);
                rs2.close();
                stmt2.close();
            }
            assertTrue(sourceFound);
        } finally {
            closeAll(rs1, stmt1, rs2, stmt2);
        }
    }

    /*
     * test if all the auto increment settings are migrated
     */
    public void testAutoIncrement() throws Exception {
        String sqlStr1 = "select T.TABLE_NAME, T.AUTO_INCREMENT, C.COLUMN_NAME "
                + "from information_schema.TABLES T INNER JOIN information_schema.COLUMNS C "
                + "on T.TABLE_SCHEMA=? and C.TABLE_SCHEMA = T.TABLE_SCHEMA and "
                + "T.TABLE_NAME = C.TABLE_NAME AND C.EXTRA = 'auto_increment' AND " + "T.AUTO_INCREMENT IS NOT NULL";
        String sqlStr2 = "SELECT S.SEQUENCENAME FROM SYSTEM.SEQUENCES S "
                + "INNER JOIN SYSTEM.FIELDS F ON S.SCHEMA=F.SCHEMA "
                + "WHERE F.SCHEMA=? AND F.TABLENAME=? AND F.FIELD=?";
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
                // long ai = rs1.getLong("AUTO_INCREMENT");
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
                    if (seqName.endsWith("$IDENTITY_SEQUENCE")) {
                        continue;
                    }
                    assertEquals(seqName.substring(0, 4).toLowerCase(), "seq_");
                    // TODO: Need to check start value - Don't know how yet
                }
                assertTrue(found);
                rs2.close();
                stmt2.close();

            }
            assertTrue(sourceFound);
        } finally {
            closeAll(rs1, stmt1, rs2, stmt2);
        }
    }

    /*
     * test if all the Indexes are migrated
     */
    public void testIndexes() throws Exception {
        String sqlStr1 = "select C.TABLE_NAME, C.COLUMN_NAME " + "from information_schema.COLUMNS C "
                + "where C.TABLE_SCHEMA=? AND C.COLUMN_KEY = 'MUL'";
        String sqlStr2 = "SELECT I.INDEXNAME FROM SYSTEM.INDEXES I "
                + "INNER JOIN SYSTEM.INDEXFIELDS F ON I.SCHEMA=F.SCHEMA AND I.TABLENAME=F.TABLENAME AND I.INDEXNAME=F"
                + ".INDEXNAME " + "WHERE I.INDEXTYPE=2 AND F.SCHEMA=? AND F.TABLENAME=? AND F.FIELD=?";
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
                    assertEquals(idxName.substring(0, 4).toLowerCase(), "idx_");
                }
                assertTrue(found);
                rs2.close();
                stmt2.close();

            }
            assertTrue(sourceFound);
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

    /*
     * test the precisions and scale are migrated properly
     */

    public void testPrecisions() throws Exception {
        String sqlStr1 = "select * from precision1";
        String sqlStr2 = "select * from precision2";
        PreparedStatement stmt1 = null, stmt2 = null;
        ResultSet rs1 = null, rs2 = null;
        try {
            stmt1 = nuodbConnection.prepareStatement(sqlStr1);
            rs1 = stmt1.executeQuery();
            Collection<MySQLPrecision1> expected = getMySQLPrecision1();
            Collection<MySQLPrecision1> actual = new ArrayList<MySQLPrecision1>();
            while (rs1.next()) {
                MySQLPrecision1 obj = new MySQLPrecision1(rs1.getInt(1), rs1.getInt(2), rs1.getLong(3), rs1.getLong(4),
                        rs1.getLong(5));
                actual.add(obj);
            }
            assertEquals(actual, expected,
                    format("The actual values %s does not match the expected %s", actual, expected));

        } finally {
            closeAll(rs1, stmt1);
        }
        try {
            stmt2 = nuodbConnection.prepareStatement(sqlStr2);
            rs2 = stmt2.executeQuery();
            Collection<MySQLPrecision2> expected = getMySQLPrecision2();
            Collection<MySQLPrecision2> actual = new ArrayList<MySQLPrecision2>();
            while (rs2.next()) {
                MySQLPrecision2 obj = new MySQLPrecision2(rs2.getString(1), rs2.getString(2), rs2.getDouble(3),
                        rs2.getDouble(4), rs2.getDouble(5), rs2.getString(6), rs2.getString(7));
                actual.add(obj);
            }
            assertEquals(actual, expected,
                    format("The actual values %s does not match the expected %s", actual, expected));
            rs2.close();
            stmt2.close();
        } finally {
            closeAll(rs2, stmt2);
        }
    }
}
