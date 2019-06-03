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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Calendar;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.testng.Assert;

import com.nuodb.migrator.integration.types.DatabaseTypes;
import com.nuodb.migrator.integration.types.DatabaseTypesFactory;
import com.nuodb.migrator.integration.types.JDBCGetMethod;

public class ResultSetUtil {

    private DatabaseTypes dbTypes = null;

    public ResultSetUtil(String sourceDriver) {
        this.dbTypes = new DatabaseTypesFactory().getDatabaseTypes(sourceDriver);
    }

    public void assertIsEqual(ResultSet source, ResultSet target, boolean compareMetaData, boolean compareData)
            throws Exception {
        ResultSetMetaData metadataSrc = source.getMetaData();
        int countSrc = metadataSrc.getColumnCount();
        ResultSetMetaData metadataTar = target.getMetaData();
        int countTar = metadataTar.getColumnCount();
        String tabName = null;

        Assert.assertEquals(countTar, countSrc, "Column counts do not match");

        Map<String, String> mapSrc = new LinkedHashMap<String, String>();
        Map<String, String> mapTar = new LinkedHashMap<String, String>();
        for (int i = 1; i <= countSrc; i++) {
            String colNameSrc = metadataSrc.getColumnName(i);
            String srcType = metadataSrc.getColumnTypeName(i);
            String colNameTar = metadataTar.getColumnName(i);
            String tarType = metadataTar.getColumnTypeName(i);
            tabName = metadataTar.getTableName(i);

            if (!dbTypes.isCaseSensitive()) {
                colNameSrc = colNameSrc.toLowerCase();
                colNameTar = colNameTar.toLowerCase();
            }
            if (compareMetaData) {
                Assert.assertEquals(colNameTar, colNameSrc, "Column type do not match");
            }
            mapSrc.put(colNameSrc, srcType);
            mapTar.put(colNameTar, tarType);
        }

        if (compareData) {
            while (source.next()) {
                target.next();
                Iterator<Entry<String, String>> itSrc = mapSrc.entrySet().iterator();
                Iterator<Entry<String, String>> itTar = mapTar.entrySet().iterator();
                while (itSrc.hasNext() && itTar.hasNext()) {
                    StringBuffer resultString = new StringBuffer();
                    Entry<String, String> pairs = itSrc.next();
                    String colName = pairs.getKey();
                    String dTypeSrc = pairs.getValue();
                    Entry<String, String> pairsTar = itTar.next();
                    String colNameTar = pairsTar.getKey();
                    // String dTypeTar = pairsTar.getValue();
                    JDBCGetMethod[] matchTypes = dbTypes.getJDBCTypes(dTypeSrc);
                    Assert.assertNotNull(matchTypes, "No type match for " + dTypeSrc);
                    Assert.assertNotEquals(matchTypes.length, 0, "No type match for " + dTypeSrc);
                    boolean flag = false;
                    for (JDBCGetMethod jdbcType : matchTypes) {
                        Object o1 = null;
                        Object o2 = null;
                        resultString.append(jdbcType);
                        resultString.append(",");
                        try {
                            o1 = getValue(source, colName, jdbcType);
                            o2 = getValue(target, colNameTar, jdbcType);
                            flag = true;
                        } catch (Exception ex) {
                            continue;
                        }
                        Assert.assertTrue(flag);
                        // check for binary types first
                        if (o1 instanceof Clob || o1 instanceof Blob) {
                            InputStream srcStream = null;
                            InputStream tarStream = null;
                            try {
                                if (o1 instanceof Clob && source.toString().contains("ibm.db2")) {
                                    byte[] charDataBytes = source.getString(colName).getBytes();
                                    srcStream = (InputStream) new ByteArrayInputStream(charDataBytes);
                                } else {
                                    srcStream = source.getBinaryStream(colName);
                                }

                                tarStream = target.getBinaryStream(colName);
                                compareStream(srcStream, tarStream, colName);
                            } catch (Exception e) {
                                e.printStackTrace();
                                throw new Exception(dTypeSrc + "Failed to compare binary Stream for " + colName, e);
                            } finally {
                                if (srcStream != null) {
                                    srcStream.close();
                                }
                                if (tarStream != null) {
                                    tarStream.close();
                                }
                            }
                        } else if (o1 instanceof java.sql.Date || o1 instanceof java.sql.Date) {
                            // compare only the year, month and date values
                            Calendar cal1 = Calendar.getInstance();
                            cal1.setTimeInMillis(((java.sql.Date) o1).getTime());
                            Calendar cal2 = Calendar.getInstance();
                            cal2.setTimeInMillis(((java.sql.Date) o2).getTime());
                            Assert.assertEquals(cal1.get(Calendar.YEAR), cal2.get(Calendar.YEAR));
                            Assert.assertEquals(cal1.get(Calendar.MONTH), cal2.get(Calendar.MONTH));
                            // Disable till CDMT-86 is fixed - fails currently
                        } else {
                            // Disable timestamp comparison till CDMT-86 is
                            // fixed
                            if (o1 instanceof java.sql.Timestamp)
                                continue;
                            if (o1 instanceof java.sql.Time)
                                continue;
                            Assert.assertEquals(o2, o1, "Data not matched in table " + tabName + " for " + colName
                                    + " via " + jdbcType.name() + " comparison.");
                        }
                        if (flag) {
                            break;
                        }
                    }
                    Assert.assertTrue(flag, "Could'nt get values using " + resultString.toString() + "in table "
                            + tabName + " for " + dTypeSrc + " datatype.");
                }
            }
        }
    }

    private static void compareStream(InputStream srcStream, InputStream tarStream, String colName) throws IOException {
        int b1, b2;
        while ((b1 = srcStream.read()) != -1 && (b2 = tarStream.read()) != -1) {
            Assert.assertEquals(b2, b1, "Binary Stream is different for " + colName);
        }
        // make sure this is also the end of the target stream
        Assert.assertEquals(tarStream.read(), -1, "Target Stream is longer than expected");
    }

    private static Object getValue(ResultSet rs, String name, JDBCGetMethod dType) throws SQLException {
        if (JDBCGetMethod.BOOLEAN == dType)
            return rs.getBoolean(name);

        if (JDBCGetMethod.STRING == dType)
            return rs.getString(name);

        if (JDBCGetMethod.BIGDECIMAL == dType)
            return rs.getBigDecimal(name);

        if (JDBCGetMethod.BYTE == dType)
            return rs.getByte(name);

        if (JDBCGetMethod.SHORT == dType)
            return rs.getShort(name);

        if (JDBCGetMethod.INT == dType)
            return rs.getInt(name);

        if (JDBCGetMethod.LONG == dType)
            return rs.getLong(name);

        if (JDBCGetMethod.FLOAT == dType)
            return rs.getFloat(name);

        if (JDBCGetMethod.DOUBLE == dType)
            return rs.getDouble(name);

        if (JDBCGetMethod.BYTES == dType)
            return rs.getBytes(name);

        if (JDBCGetMethod.DATE == dType)
            return rs.getDate(name);

        if (JDBCGetMethod.TIME == dType)
            return rs.getTime(name);

        if (JDBCGetMethod.TIMESTAMP == dType)
            return rs.getTimestamp(name);

        if (JDBCGetMethod.CLOB == dType)
            return rs.getClob(name);

        if (JDBCGetMethod.BLOB == dType)
            return rs.getBlob(name);

        if (JDBCGetMethod.ARRAY == dType)
            return rs.getArray(name);

        if (JDBCGetMethod.REF == dType)
            return rs.getRef(name);

        if (JDBCGetMethod.BIGDECIMAL == dType)
            return rs.getBigDecimal(name);
        throw new IllegalArgumentException("Unknown Type:" + dType);
    }
}
