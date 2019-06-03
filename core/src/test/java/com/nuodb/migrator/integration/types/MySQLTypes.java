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
package com.nuodb.migrator.integration.types;

import java.sql.Types;
import java.util.HashMap;
import java.util.Map;

/**
 * @author Krishnamoorthy Dhandapani
 */
public class MySQLTypes implements DatabaseTypes {
    private static Map<String, JDBCGetMethod[]> map = new HashMap<String, JDBCGetMethod[]>();

    static {
        map.put("BOOL", new JDBCGetMethod[] { JDBCGetMethod.BOOLEAN });
        map.put("BOOLEAN", new JDBCGetMethod[] { JDBCGetMethod.BOOLEAN });

        map.put("LONG", new JDBCGetMethod[] { JDBCGetMethod.LONG });

        map.put("VARCHAR", new JDBCGetMethod[] { JDBCGetMethod.STRING });
        map.put("VARCHAR2", new JDBCGetMethod[] { JDBCGetMethod.STRING });
        map.put("TEXT", new JDBCGetMethod[] { JDBCGetMethod.STRING });
        map.put("TINYTEXT", new JDBCGetMethod[] { JDBCGetMethod.STRING });
        map.put("MEDIUMTEXT", new JDBCGetMethod[] { JDBCGetMethod.STRING });
        map.put("LONGTEXT", new JDBCGetMethod[] { JDBCGetMethod.STRING });
        map.put("CHAR", new JDBCGetMethod[] { JDBCGetMethod.STRING });

        map.put("INT", new JDBCGetMethod[] { JDBCGetMethod.SHORT, JDBCGetMethod.INT, JDBCGetMethod.LONG });
        map.put("INT UNSIGNED", new JDBCGetMethod[] { JDBCGetMethod.SHORT, JDBCGetMethod.INT, JDBCGetMethod.LONG }); // INT
                                                                                                                     // UNSIGNED
        map.put("BIT", new JDBCGetMethod[] { JDBCGetMethod.BOOLEAN });
        map.put("TINYINT", new JDBCGetMethod[] { JDBCGetMethod.SHORT, JDBCGetMethod.INT });
        map.put("TINYINT UNSIGNED", new JDBCGetMethod[] { JDBCGetMethod.SHORT, JDBCGetMethod.INT }); // TINYINT
                                                                                                     // UNSIGNED
        map.put("SMALLINT", new JDBCGetMethod[] { JDBCGetMethod.SHORT, JDBCGetMethod.INT });
        map.put("SMALLINT UNSIGNED", new JDBCGetMethod[] { JDBCGetMethod.SHORT, JDBCGetMethod.INT }); // SMALLINT
                                                                                                      // UNSIGNED
        map.put("MEDIUMINT", new JDBCGetMethod[] { JDBCGetMethod.INT, JDBCGetMethod.LONG });
        map.put("MEDIUMINT UNSIGNED", new JDBCGetMethod[] { JDBCGetMethod.INT, JDBCGetMethod.LONG }); // MEDIUMINT
                                                                                                      // UNSIGNED
        map.put("BIGINT", new JDBCGetMethod[] { JDBCGetMethod.INT, JDBCGetMethod.LONG });
        map.put("BIGINT UNSIGNED",
                new JDBCGetMethod[] { JDBCGetMethod.INT, JDBCGetMethod.LONG, JDBCGetMethod.BIGDECIMAL });
        map.put("FLOAT", new JDBCGetMethod[] { JDBCGetMethod.FLOAT });
        map.put("FLOAT UNSIGNED", new JDBCGetMethod[] { JDBCGetMethod.FLOAT }); // FLOAT
                                                                                // UNSIGNED
        map.put("DOUBLE UNSIGNED", new JDBCGetMethod[] { JDBCGetMethod.DOUBLE }); // DOUBLE
                                                                                  // UNSIGNED
        map.put("DOUBLE", new JDBCGetMethod[] { JDBCGetMethod.DOUBLE });
        // TODO: revert back to double after bug fix
        // JDBCType.DOUBLE });
        map.put("DECIMAL", new JDBCGetMethod[] { JDBCGetMethod.STRING });
        map.put("DECIMAL UNSIGNED", new JDBCGetMethod[] { JDBCGetMethod.STRING }); // DECIMAL
                                                                                   // UNSIGNED
        map.put("DATE", new JDBCGetMethod[] { JDBCGetMethod.DATE, JDBCGetMethod.TIMESTAMP });
        map.put("DATETIME", new JDBCGetMethod[] { JDBCGetMethod.DATE, JDBCGetMethod.TIMESTAMP });
        map.put("TIMESTAMP", new JDBCGetMethod[] { JDBCGetMethod.DATE, JDBCGetMethod.TIMESTAMP });
        map.put("TIME", new JDBCGetMethod[] { JDBCGetMethod.DATE, JDBCGetMethod.TIME, JDBCGetMethod.TIMESTAMP });
        map.put("YEAR", new JDBCGetMethod[] { JDBCGetMethod.SHORT });

        map.put("BLOB", new JDBCGetMethod[] { JDBCGetMethod.BLOB });
        map.put("TINYBLOB", new JDBCGetMethod[] { JDBCGetMethod.BLOB });
        map.put("MEDIUMBLOB", new JDBCGetMethod[] { JDBCGetMethod.BLOB });
        map.put("LONGBLOB", new JDBCGetMethod[] { JDBCGetMethod.BLOB });
        map.put("BINARY", new JDBCGetMethod[] { JDBCGetMethod.BLOB });
        map.put("VARBINARY", new JDBCGetMethod[] { JDBCGetMethod.BLOB });
    }

    public JDBCGetMethod[] getJDBCTypes(String type) {
        return map.get(type);
    }

    public static int getKeyType(String type) {
        if ("PRI".equals(type)) {
            return 0;
        } else if ("UNI".equals(type)) {
            return 1;
        }
        return -1;
    }

    public static int getMappedJDBCType(String type, String colType) {
        if ("VARCHAR".equalsIgnoreCase(type)) {
            return Types.VARCHAR;
        } else if ("VARCHAR2".equalsIgnoreCase(type)) {
            return Types.NVARCHAR;
        } else if ("TINYINT".equalsIgnoreCase(type)) {
            return Types.SMALLINT;
        } else if ("TEXT".equalsIgnoreCase(type)) {
            return Types.CLOB;
        } else if ("DATE".equalsIgnoreCase(type)) {
            return Types.DATE;
        } else if ("SMALLINT".equalsIgnoreCase(type)) {
            if (colType != null && colType.toLowerCase().contains("unsigned")) {
                return Types.INTEGER;
            } else {
                return Types.SMALLINT;
            }
        } else if ("MEDIUMINT".equalsIgnoreCase(type)) {
            return Types.INTEGER;
        } else if ("BIGINT".equalsIgnoreCase(type)) {
            if (colType != null && colType.toLowerCase().contains("unsigned")) {
                return Types.NUMERIC;
            } else {
                return Types.BIGINT;
            }
        } else if ("INT".equalsIgnoreCase(type)) {
            if (colType != null && colType.toLowerCase().contains("unsigned")) {
                return Types.BIGINT;
            } else {
                return Types.INTEGER;
            }
        } else if ("FLOAT".equalsIgnoreCase(type)) {
            return Types.DOUBLE; // in NuoDB, float is double internally
        } else if ("DOUBLE".equalsIgnoreCase(type)) {
            return Types.DOUBLE;
        } else if ("DECIMAL".equalsIgnoreCase(type)) {
            if (colType != null && colType.equalsIgnoreCase("decimal(6,2)")) {
                return Types.INTEGER;
            } else if (colType != null && colType.contains("decimal(65,30)")) {
                return Types.NUMERIC;
            } else {
                return Types.BIGINT;
            }
        } else if ("DATETIME".equalsIgnoreCase(type)) {
            return Types.TIMESTAMP;
        } else if ("TIMESTAMP".equalsIgnoreCase(type)) {
            return Types.TIMESTAMP;
        } else if ("YEAR".equalsIgnoreCase(type)) {
            return Types.SMALLINT;
        } else if ("CHAR".equalsIgnoreCase(type)) {
            return Types.CHAR;
        } else if ("TINYTEXT".equalsIgnoreCase(type)) {
            return Types.VARCHAR;
        } else if ("BLOB".equalsIgnoreCase(type)) {
            return Types.BLOB;
        } else if ("MEDIUMBLOB".equalsIgnoreCase(type)) {
            return Types.BLOB;
        } else if ("LONGBLOB".equalsIgnoreCase(type)) {
            return Types.BLOB;
        } else if ("VARBINARY".equalsIgnoreCase(type)) {
            return Types.BLOB;
        } else if ("BINARY".equalsIgnoreCase(type)) {
            return Types.BLOB;
        } else if ("MEDIUMTEXT".equalsIgnoreCase(type)) {
            return Types.CLOB;
        } else if ("LONGTEXT".equalsIgnoreCase(type)) {
            return Types.CLOB;
        } else if ("BIT".equalsIgnoreCase(type)) {
            return Types.BOOLEAN;
        } else if ("TINYBLOB".equalsIgnoreCase(type)) {
            return Types.BLOB;
        } else if ("ENUM".equalsIgnoreCase(type)) {
            return Types.SMALLINT;
        } else if ("SET".equalsIgnoreCase(type)) {
            return Types.CHAR;
        } else if ("TIME".equalsIgnoreCase(type)) {
            return Types.TIME;
        }

        return 0;
    }

    public static String getMappedLength(String type, String colType, String length) {
        if ("VARCHAR".equalsIgnoreCase(type)) {
            return length;
        } else if ("VARCHAR2".equalsIgnoreCase(type)) {
            return length;
        } else if ("TINYINT".equalsIgnoreCase(type)) {
            return "2";
        } else if ("TEXT".equalsIgnoreCase(type)) {
            return "8";
        } else if ("DATE".equalsIgnoreCase(type)) {
            return "8";
        } else if ("SMALLINT".equalsIgnoreCase(type)) {
            if (colType != null && colType.toLowerCase().contains("unsigned")) {
                return "4";
            } else {
                return "2";
            }
        } else if ("MEDIUMINT".equalsIgnoreCase(type)) {
            return "4";
        } else if ("BIGINT".equalsIgnoreCase(type)) {
            if (colType != null && colType.toLowerCase().contains("unsigned")) {
                return "32";
            } else {
                return "8";
            }
        } else if ("INT".equalsIgnoreCase(type)) {
            if (colType != null && colType.toLowerCase().contains("unsigned")) {
                return "8";
            } else {
                return "4";
            }
        } else if ("FLOAT".equalsIgnoreCase(type)) {
            return "8"; // in NuoDB, float is double internally
        } else if ("DOUBLE".equalsIgnoreCase(type)) {
            return "8";
        } else if ("DECIMAL".equalsIgnoreCase(type)) {
            if (colType != null && colType.equalsIgnoreCase("decimal(6,2)")) {
                return "4";
            } else if (colType != null && colType.contains("decimal(65,30)")) {
                return "32";
            } else {
                return "8";
            }
        } else if ("DATETIME".equalsIgnoreCase(type)) {
            return "12";
        } else if ("TIME".equalsIgnoreCase(type)) {
            return "8";
        } else if ("TIMESTAMP".equalsIgnoreCase(type)) {
            return "12";
        } else if ("YEAR".equalsIgnoreCase(type)) {
            return "2";
        } else if ("CHAR".equalsIgnoreCase(type)) {
            return length;
        } else if ("TINYTEXT".equalsIgnoreCase(type)) {
            return "255";
        } else if ("BLOB".equalsIgnoreCase(type)) {
            return "8";
        } else if ("MEDIUMBLOB".equalsIgnoreCase(type)) {
            return "8";
        } else if ("LONGBLOB".equalsIgnoreCase(type)) {
            return "8";
        } else if ("MEDIUMTEXT".equalsIgnoreCase(type)) {
            return "8";
        } else if ("LONGTEXT".equalsIgnoreCase(type)) {
            return "8";
        } else if ("BIT".equalsIgnoreCase(type)) {
            return "2";
        } else if ("VARBINARY".equalsIgnoreCase(type)) {
            return "90";
        } else if ("BINARY".equalsIgnoreCase(type)) {
            return "90";
        } else if ("TINYBLOB".equalsIgnoreCase(type)) {
            return "255";
        } else if ("ENUM".equalsIgnoreCase(type)) {
            // TODO: temporary fix NuoDB ENUM length issue
            // How to reproduce the issue:
            // CREATE TABLE TEST.T1 (F1 ENUM('123', '12345'));
            // SELECT LENGTH FROM SYSTEM.FIELDS WHERE SCHEMA='TEST' AND
            // TABLENAME='T1';
            // Actual result:
            // 2
            // Expected result:
            // 5=max('123'.length(), '12345'.length());
            return "2";
        } else if ("SET".equalsIgnoreCase(type)) {
            if (length != null && length.equalsIgnoreCase("22")) {
                return "22";
            } else if (length != null && length.equalsIgnoreCase("19")) {
                return "19";
            } else if (length != null && length.equalsIgnoreCase("24")) {
                return "26";
            } else if (length != null && length.equalsIgnoreCase("21")) {
                return "21";
            } else {
                return "14";
            }
        }
        return null;
    }

    public static String getMappedDefault(String type, String defaultValue) {
        if ("SMALLINT".equalsIgnoreCase(type) || "MEDIUMINT".equalsIgnoreCase(type) || "BIGINT".equalsIgnoreCase(type)
                || "INT".equalsIgnoreCase(type) || "TINYINT".equalsIgnoreCase(type)) {
            return defaultValue == null ? null : "'" + defaultValue + "'";
        } else if ("TIMESTAMP".equalsIgnoreCase(type)) {
            if ("0000-00-00 00:00:00".equals(defaultValue)) {
                return "'0000-00-00 00:00:00'";
            }
            return "CURRENT_TIMESTAMP".equals(defaultValue) ? "'NOW'" : "'" + defaultValue + "'";
        } else if ("char".equalsIgnoreCase(type)) {
            return defaultValue == null ? null : "'" + defaultValue + "'";
        } else if ("datetime".equalsIgnoreCase(type)) {
            return defaultValue == null ? null : "'" + defaultValue + "'";
        } else if ("timestamp".equalsIgnoreCase(type)) {
            return defaultValue == null ? null : "'" + defaultValue + "'";
        } else if ("varchar".equalsIgnoreCase(type)) {
            return defaultValue == null ? null : "'" + defaultValue + "'";
        } else if ("year".equalsIgnoreCase(type)) {
            return defaultValue == null ? null : "'" + defaultValue + "'";
        }
        return defaultValue;
    }

    public boolean isCaseSensitive() {
        return false;
    }
}
