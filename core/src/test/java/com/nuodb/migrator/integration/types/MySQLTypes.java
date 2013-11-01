/**
 * Copyright (c) 2012, NuoDB, Inc.
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

public class MySQLTypes implements DatabaseTypes {
    private static Map<String, JDBCGetMethod[]> map = new HashMap<String, JDBCGetMethod[]>();

    static {
        map.put("BOOL", new JDBCGetMethod[]{JDBCGetMethod.BOOLEAN});
        map.put("BOOLEAN", new JDBCGetMethod[]{JDBCGetMethod.BOOLEAN});

        map.put("LONG", new JDBCGetMethod[]{JDBCGetMethod.LONG});

        map.put("VARCHAR", new JDBCGetMethod[]{JDBCGetMethod.STRING});
        map.put("VARCHAR2", new JDBCGetMethod[]{JDBCGetMethod.STRING});
        map.put("TEXT", new JDBCGetMethod[]{JDBCGetMethod.STRING});
        map.put("TINYTEXT", new JDBCGetMethod[]{JDBCGetMethod.STRING});
        map.put("MEDIUMTEXT", new JDBCGetMethod[]{JDBCGetMethod.STRING});
        map.put("LONGTEXT", new JDBCGetMethod[]{JDBCGetMethod.STRING});
        map.put("CHAR", new JDBCGetMethod[]{JDBCGetMethod.STRING});

        map.put("INT", new JDBCGetMethod[]{JDBCGetMethod.SHORT,
                JDBCGetMethod.INT});
        map.put("BIT", new JDBCGetMethod[]{JDBCGetMethod.BOOLEAN});
        map.put("TINYINT", new JDBCGetMethod[]{JDBCGetMethod.SHORT,
                JDBCGetMethod.INT});
        map.put("SMALLINT", new JDBCGetMethod[]{JDBCGetMethod.SHORT,
                JDBCGetMethod.INT});
        map.put("MEDIUMINT", new JDBCGetMethod[]{JDBCGetMethod.INT,
                JDBCGetMethod.LONG});
        map.put("BIGINT", new JDBCGetMethod[]{JDBCGetMethod.INT,
                JDBCGetMethod.LONG});
        map.put("BIGINT UNSIGNED", new JDBCGetMethod[]{JDBCGetMethod.INT,
                JDBCGetMethod.LONG});
        map.put("FLOAT", new JDBCGetMethod[]{JDBCGetMethod.FLOAT});
        map.put("DOUBLE", new JDBCGetMethod[]{JDBCGetMethod.DOUBLE});
        // TODO: revert back to double after bug fix
        // JDBCType.DOUBLE });
        map.put("DECIMAL", new JDBCGetMethod[]{JDBCGetMethod.STRING});

        map.put("DATE", new JDBCGetMethod[]{JDBCGetMethod.DATE,
                JDBCGetMethod.TIMESTAMP});
        map.put("DATETIME", new JDBCGetMethod[]{JDBCGetMethod.DATE,
                JDBCGetMethod.TIMESTAMP});
        map.put("TIMESTAMP", new JDBCGetMethod[]{JDBCGetMethod.DATE,
                JDBCGetMethod.TIMESTAMP});
        map.put("TIME", new JDBCGetMethod[]{JDBCGetMethod.DATE,
                JDBCGetMethod.TIME, JDBCGetMethod.TIMESTAMP});
        map.put("YEAR", new JDBCGetMethod[]{JDBCGetMethod.DATE});

        map.put("BLOB", new JDBCGetMethod[]{JDBCGetMethod.BLOB});
        map.put("TINYBLOB", new JDBCGetMethod[]{JDBCGetMethod.BLOB});
        map.put("MEDIUMBLOB", new JDBCGetMethod[]{JDBCGetMethod.BLOB});
        map.put("LONGBLOB", new JDBCGetMethod[]{JDBCGetMethod.BLOB});
        map.put("BINARY", new JDBCGetMethod[]{JDBCGetMethod.BLOB});
        map.put("VARBINARY", new JDBCGetMethod[]{JDBCGetMethod.BLOB});
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
            return Types.SMALLINT;
        } else if ("MEDIUMINT".equalsIgnoreCase(type)) {
            return Types.INTEGER;
        } else if ("BIGINT".equalsIgnoreCase(type)) {
            if (colType != null && colType.toLowerCase().contains("unsigned")) {
                return Types.NUMERIC;
            } else {
                return Types.BIGINT;
            }
        } else if ("INT".equalsIgnoreCase(type)) {
            return Types.INTEGER;
        } else if ("FLOAT".equalsIgnoreCase(type)) {
            return Types.FLOAT;
        } else if ("DOUBLE".equalsIgnoreCase(type)) {
            return Types.DOUBLE;
        } else if ("DECIMAL".equalsIgnoreCase(type)) {
            return Types.BIGINT;
        } else if ("DATETIME".equalsIgnoreCase(type)) {
            return Types.TIMESTAMP;
        } else if ("TIMESTAMP".equalsIgnoreCase(type)) {
            return Types.TIMESTAMP;
        } else if ("YEAR".equalsIgnoreCase(type)) {
            return Types.DATE;
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
            return Types.CHAR;
        } else if ("SET".equalsIgnoreCase(type)) {
            return Types.CHAR;
        }

        return 0;
    }

    public static String getMappedLength(String type, String colType,
                                         String length) {
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
            return "2";
        } else if ("MEDIUMINT".equalsIgnoreCase(type)) {
            return "4";
        } else if ("BIGINT".equalsIgnoreCase(type)) {
            if (colType != null && colType.toLowerCase().contains("unsigned")) {
                return "32";
            } else {
                return "8";
            }
        } else if ("INT".equalsIgnoreCase(type)) {
            return "4";
        } else if ("FLOAT".equalsIgnoreCase(type)) {
            return "4";
        } else if ("DOUBLE".equalsIgnoreCase(type)) {
            return "8";
        } else if ("DECIMAL".equalsIgnoreCase(type)) {
            return "8";
        } else if ("DATETIME".equalsIgnoreCase(type)) {
            return "12";
        } else if ("TIMESTAMP".equalsIgnoreCase(type)) {
            return "12";
        } else if ("YEAR".equalsIgnoreCase(type)) {
            return "8";
        } else if ("CHAR".equalsIgnoreCase(type)) {
            return "20";
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
            return "11";
        } else if ("SET".equalsIgnoreCase(type)) {
            return "14";
        }
        return null;
    }

    public static String getMappedDefault(String type, String defaultValue) {
        if ("SMALLINT".equalsIgnoreCase(type)
                || "MEDIUMINT".equalsIgnoreCase(type)
                || "BIGINT".equalsIgnoreCase(type)
                || "INT".equalsIgnoreCase(type)) {
            return defaultValue == null ? null : "'" + defaultValue + "'";
        } else if ("TIMESTAMP".equalsIgnoreCase(type)) {
            return "CURRENT_TIMESTAMP".equals(defaultValue) ? "'CURRENT_TIMESTAMP'"
                    : defaultValue;
        }
        return defaultValue;
    }

    public boolean isCaseSensitive() {
        return false;
    }
}
