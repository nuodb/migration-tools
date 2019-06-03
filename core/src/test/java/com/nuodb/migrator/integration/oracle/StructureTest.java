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
package com.nuodb.migrator.integration.oracle;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.nuodb.migrator.integration.MigrationTestBase;
import com.nuodb.migrator.integration.types.OracleTypes;

/**
 * Test to make sure all the Tables, Constraints, Views, Triggers etc have been
 * migrated.
 * 
 * @author Krishnamoorthy Dhandapani
 */
@Test(groups = { "oracleintegrationtest" }, dependsOnGroups = { "dataloadperformed" })
public class StructureTest extends MigrationTestBase {

    /*
     * test if all the Tables are migrated with the right columns
     */
    public void testTables() throws Exception {
        String sqlStr1 = "select table_name from user_tables";
        String sqlStr2 = "select tablename from system.TABLES where TYPE = 'TABLE' and schema = ?";
        PreparedStatement stmt1 = null, stmt2 = null;
        ResultSet rs1 = null, rs2 = null;
        ArrayList<String> list1 = new ArrayList<String>();
        ArrayList<String> list2 = new ArrayList<String>();
        try {
            stmt1 = sourceConnection.prepareStatement(sqlStr1);
            // stmt1.setString(1, sourceConnection.getCatalog());
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
        String sqlStr1 = "SELECT * FROM all_tab_cols where table_name = " + "? order by column_id";
        String sqlStr2 = "select * from  system.FIELDS F inner join system.DATATYPES D on "
                + "F.DATATYPE = D.ID and F.SCHEMA = ? and F.TABLENAME = ? order by F.FIELDPOSITION";
        String[] colNames = new String[] { "COLUMN_NAME", "DATA_TYPE", "DATA_LENGTH", "DATA_DEFAULT", "DATA_PRECISION",
                "DATA_SCALE" };
        PreparedStatement stmt1 = null, stmt2 = null;
        ResultSet rs1 = null, rs2 = null;
        HashMap<String, HashMap<String, String>> tabColMap = new HashMap<String, HashMap<String, String>>();
        try {
            stmt1 = sourceConnection.prepareStatement(sqlStr1);
            // stmt1.setString(1, sourceConnection.getCatalog());
            stmt1.setString(1, tableName);
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

                int srcJdbcType = OracleTypes.getMappedJDBCType(tabColDetailsMap.get(colNames[1]),
                        tabColDetailsMap.get(colNames[4]), tabColDetailsMap.get(colNames[5]));
                int tarJdbcType = rs2.getInt("JDBCTYPE");

                /*
                 * System.out.println("colName:" + colName + " ::" +
                 * " tableName:" + tableName + " ::" + " datatype:" +
                 * tabColDetailsMap.get(colNames[1]));
                 */

                Assert.assertEquals(tarJdbcType, srcJdbcType,
                        "JDBCTYPE of column " + colName + " of table " + tableName + " did not match");

                String srcLength = rs2.getString("LENGTH");

                String tarLength = OracleTypes.getMappedLength(tabColDetailsMap.get(colNames[1]),
                        tabColDetailsMap.get(colNames[2]), tabColDetailsMap.get(colNames[4]),
                        tabColDetailsMap.get(colNames[5]));

                Assert.assertEquals(srcLength, tarLength,
                        "LENGTH of column " + colName + " of table " + tableName + " did not match");
                // TBD
                // String val = tabColDetailsMap.get(colNames[7]);
                // Assert.assertEquals(rs2.getInt("SCALE"), val == null ? 0
                // : Integer.parseInt(val));

                // Assert.assertEquals(rs2.getString("PRECISION"),
                // tabColDetailsMap.get(colNames[6]));

                /*
                 * String val = tabColDetailsMap.get(colNames[3]);
                 * Assert.assertEquals(rs2.getString("DEFAULTVALUE"),
                 * OracleTypes.getMappedDefault(
                 * tabColDetailsMap.get(colNames[1]),
                 * tabColDetailsMap.get(colNames[3])));
                 */
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
     * test if all the Primary Constraints are migrated
     */
    public void testPrimaryKeyConstraints() throws Exception {
        String sqlStr1 = "SELECT cols.table_name, cols.column_name FROM all_constraints cons, all_cons_columns"
                + " cols WHERE cons.constraint_type = 'P' AND cons.constraint_name = cols.constraint_name AND cons"
                + ".owner = cols.owner "
                + " AND cons.owner= ? AND cols.TABLE_NAME not like 'BIN$%' ORDER BY cols.table_name, cols.position ";
        String sqlStr2 = "SELECT FIELD FROM SYSTEM.INDEXES SI INNER JOIN SYSTEM.INDEXFIELDS SIF ON "
                + "SI.SCHEMA=SIF.SCHEMA AND " + "SI.TABLENAME=SIF.TABLENAME AND "
                + "SI.INDEXNAME=SIF.INDEXNAME WHERE SI.SCHEMA=? AND SI.TABLENAME=? AND SI.INDEXNAME LIKE '%PRIMARY_KEY%'";
        PreparedStatement stmt1 = null, stmt2 = null;
        ResultSet rs1 = null, rs2 = null;
        try {
            stmt1 = sourceConnection.prepareStatement(sqlStr1);
            stmt1.setString(1, sourceSchemaUsed);
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
                rs2 = stmt2.executeQuery();
                boolean found = false;
                while (rs2.next()) {
                    found = true;
                    String tarCol = rs2.getString(1);
                    Assert.assertEquals(tarCol, cName, "Source column name " + cName + " of table " + tName
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
     * test if all the Unique Key Constraints are migrated
     */
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public void testUniqueKeyConstraints() throws Exception {
        String sqlStr1 = "SELECT cols.table_name, cols.column_name FROM all_constraints cons, all_cons_columns"
                + " cols WHERE cons.constraint_type = 'U' AND cons.constraint_name = cols.constraint_name AND cons"
                + ".owner = cols.owner "
                + " AND cons.owner= ? AND cols.TABLE_NAME not like 'BIN$%' ORDER BY cols.table_name, cols.position ";
        String sqlStr2 = "SELECT FIELD FROM SYSTEM.INDEXES SI INNER JOIN SYSTEM.INDEXFIELDS SIF ON "
                + "SI.SCHEMA=SIF.SCHEMA AND " + "SI.TABLENAME=SIF.TABLENAME AND "
                + "SI.INDEXNAME=SIF.INDEXNAME WHERE SI.SCHEMA=? AND SI.TABLENAME=? AND SI.INDEXNAME LIKE '%UNIQUE%'";
        PreparedStatement stmt1 = null, stmt2 = null;
        ResultSet rs1 = null, rs2 = null;
        HashMap map = new HashMap();
        try {
            stmt1 = sourceConnection.prepareStatement(sqlStr1);
            stmt1.setString(1, sourceSchemaUsed);
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
            Assert.assertTrue(sourceFound);
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
                    tarColList.add(rs2.getString(1));
                }
                Assert.assertTrue(found);
                Assert.assertEquals(srcColList, tarColList);
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
        String sqlStr1 = "SELECT a.table_name, a.column_name, uc.table_name, uc.column_name  "
                + "FROM all_cons_columns a JOIN all_constraints c ON a.owner = c.owner AND a.constraint_name = c"
                + ".constraint_name "
                + "JOIN all_constraints c_pk ON c.r_owner = c_pk.owner AND c.r_constraint_name = c_pk.constraint_name "
                + "join USER_CONS_COLUMNS uc on uc.constraint_name = c.r_constraint_name";
        // WHERE C.R_OWNER = ?
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
                + "WHERE PRIMARYTABLE.SCHEMA=? AND PRIMARYTABLE.TABLENAME=? ORDER BY PKTABLE_SCHEM, PKTABLE_NAME, "
                + "KEY_SEQ ASC";
        PreparedStatement stmt1 = null, stmt2 = null;
        ResultSet rs1 = null, rs2 = null;
        try {
            stmt1 = sourceConnection.prepareStatement(sqlStr1);
            // stmt1.setString(1, sourceConnection.getCatalog());
            rs1 = stmt1.executeQuery();

            Assert.assertNotNull(rs1);
            boolean sourceFound = false;
            while (rs1.next()) {
                sourceFound = true;
                String tName = rs1.getString(1);
                String cName = rs1.getString(2);
                String rtName = rs1.getString(3);
                String rcName = rs1.getString(4);

                stmt2 = nuodbConnection.prepareStatement(sqlStr2);
                stmt2.setString(1, nuodbSchemaUsed);
                stmt2.setString(2, rtName);
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
    public void testAutoIncrement() throws Exception {
        String sqlStr1 = "select ut.table_name , ud.referenced_name as sequence_name from   user_dependencies ud "
                + "join user_triggers ut on (ut.trigger_name = ud.name) where ud.type='TRIGGER'  and ud.referenced_type='SEQUENCE' "
                + "and ut.TABLE_NAME not like 'BIN$%' ";
        String sqlStr2 = "SELECT S.SEQUENCENAME FROM SYSTEM.SEQUENCES S "
                + "INNER JOIN SYSTEM.FIELDS F ON S.SCHEMA=F.SCHEMA " + "WHERE F.SCHEMA=? AND F.TABLENAME=? ";
        PreparedStatement stmt1 = null, stmt2 = null;
        ResultSet rs1 = null, rs2 = null;
        try {
            stmt1 = sourceConnection.prepareStatement(sqlStr1);
            // stmt1.setString(1, sourceConnection.getCatalog());
            rs1 = stmt1.executeQuery();

            Assert.assertNotNull(rs1);
            boolean sourceFound = false;
            while (rs1.next()) {
                sourceFound = true;
                String tName = rs1.getString("TABLE_NAME");
                stmt2 = nuodbConnection.prepareStatement(sqlStr2);
                stmt2.setString(1, nuodbSchemaUsed);
                stmt2.setString(2, tName);
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
        String sqlStr1 = "select c.index_name,c.column_name,c.TABLE_NAME FROM   user_indexes i, user_ind_columns c "
                + "WHERE  i.index_name = c.index_name  AND c.INDEX_NAME not like 'SYS_%' AND i.INDEX_NAME "
                + "not like 'SYS_%' ORDER  BY c.index_name, c.column_position";
        String sqlStr2 = "SELECT I.INDEXNAME FROM SYSTEM.INDEXES I "
                + "INNER JOIN SYSTEM.INDEXFIELDS F ON I.SCHEMA=F.SCHEMA AND I.TABLENAME=F.TABLENAME AND I.INDEXNAME=F.INDEXNAME "
                + "WHERE I.INDEXTYPE=2 AND F.SCHEMA=? AND F.TABLENAME=? AND F.FIELD=?";
        PreparedStatement stmt1 = null, stmt2 = null;
        ResultSet rs1 = null, rs2 = null;
        try {
            stmt1 = sourceConnection.prepareStatement(sqlStr1);
            // stmt1.setString(1, sourceConnection.getCatalog());
            rs1 = stmt1.executeQuery();

            Assert.assertNotNull(rs1);
            boolean sourceFound = false;
            while (rs1.next()) {
                sourceFound = true;
                // String iName = rs1.getString(1);
                String cName = rs1.getString(2);
                String tName = rs1.getString(3);
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
