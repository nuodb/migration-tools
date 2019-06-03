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
public class OracleTypes implements DatabaseTypes {
    private static Map<String, JDBCGetMethod[]> map = new HashMap<String, JDBCGetMethod[]>();

    static {
        map.put("BOOL", new JDBCGetMethod[] { JDBCGetMethod.BOOLEAN });
        map.put("BOOLEAN", new JDBCGetMethod[] { JDBCGetMethod.BOOLEAN });
        map.put("LONG", new JDBCGetMethod[] { JDBCGetMethod.STRING });

        map.put("VARCHAR", new JDBCGetMethod[] { JDBCGetMethod.STRING });
        map.put("VARCHAR2", new JDBCGetMethod[] { JDBCGetMethod.STRING });
        map.put("NVARCHAR", new JDBCGetMethod[] { JDBCGetMethod.STRING });
        map.put("TEXT", new JDBCGetMethod[] { JDBCGetMethod.STRING });
        map.put("TINYTEXT", new JDBCGetMethod[] { JDBCGetMethod.STRING });
        map.put("MEDIUMTEXT", new JDBCGetMethod[] { JDBCGetMethod.STRING });
        map.put("LONGTEXT", new JDBCGetMethod[] { JDBCGetMethod.STRING });
        map.put("CHAR", new JDBCGetMethod[] { JDBCGetMethod.STRING });
        map.put("NCHAR", new JDBCGetMethod[] { JDBCGetMethod.STRING });
        map.put("NTEXT", new JDBCGetMethod[] { JDBCGetMethod.STRING });
        map.put("INT", new JDBCGetMethod[] { JDBCGetMethod.SHORT, JDBCGetMethod.INT });
        map.put("NUMERIC", new JDBCGetMethod[] { JDBCGetMethod.SHORT, JDBCGetMethod.INT });
        map.put("BINARY_FLOAT", new JDBCGetMethod[] { JDBCGetMethod.DOUBLE });
        map.put("BINARY_DOUBLE", new JDBCGetMethod[] { JDBCGetMethod.DOUBLE });
        map.put("NUMBER", new JDBCGetMethod[] { JDBCGetMethod.LONG, JDBCGetMethod.BIGDECIMAL, JDBCGetMethod.STRING });
        map.put("FLOAT", new JDBCGetMethod[] { JDBCGetMethod.FLOAT });
        map.put("REAL", new JDBCGetMethod[] { JDBCGetMethod.FLOAT });
        map.put("DOUBLE", new JDBCGetMethod[] { JDBCGetMethod.DOUBLE });

        map.put("TIMESTAMPLTZ",
                new JDBCGetMethod[] { JDBCGetMethod.DATE, JDBCGetMethod.TIMESTAMP, JDBCGetMethod.STRING });
        // TODO: revert back to double after bug fix
        // JDBCType.DOUBLE });
        map.put("DECIMAL", new JDBCGetMethod[] { JDBCGetMethod.STRING });

        map.put("DATE", new JDBCGetMethod[] { JDBCGetMethod.DATE, JDBCGetMethod.TIMESTAMP });
        map.put("DATETIME", new JDBCGetMethod[] { JDBCGetMethod.DATE, JDBCGetMethod.TIMESTAMP });
        map.put("TIMESTAMP", new JDBCGetMethod[] { JDBCGetMethod.DATE });
        map.put("TIMESTAMPTZ", new JDBCGetMethod[] { JDBCGetMethod.DATE, JDBCGetMethod.TIMESTAMP });
        map.put("TIME", new JDBCGetMethod[] { JDBCGetMethod.DATE, JDBCGetMethod.TIME, JDBCGetMethod.TIMESTAMP });
        map.put("BLOB", new JDBCGetMethod[] { JDBCGetMethod.BLOB });
        map.put("CLOB", new JDBCGetMethod[] { JDBCGetMethod.CLOB });
        map.put("RAW", new JDBCGetMethod[] { JDBCGetMethod.BYTES });
        map.put("TIMESTAMP WITH TIME ZONE", new JDBCGetMethod[] { JDBCGetMethod.TIMESTAMP, });
        map.put("TIMESTAMP WITH LOCAL TIME ZONE", new JDBCGetMethod[] { JDBCGetMethod.TIMESTAMP });
        map.put("NCLOB", new JDBCGetMethod[] { JDBCGetMethod.STRING, JDBCGetMethod.CLOB });
        map.put("NVARCHAR2", new JDBCGetMethod[] { JDBCGetMethod.BLOB, JDBCGetMethod.STRING });
        map.put("LONG RAW", new JDBCGetMethod[] { JDBCGetMethod.BYTES });
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

    public static int getMappedJDBCType(String type, String Dataprecision, String Datascale) {
        if ("VARCHAR".equalsIgnoreCase(type)) {
            return Types.VARCHAR;
        } else if ("NVARCHAR".equalsIgnoreCase(type)) {
            return Types.VARCHAR;
        } else if ("VARCHAR2".equalsIgnoreCase(type)) {
            return Types.VARCHAR;
        } else if ("TEXT".equalsIgnoreCase(type)) {
            return Types.CLOB;
        } else if ("NTEXT".equalsIgnoreCase(type)) {
            return Types.CLOB;
        } else if ("DATE".equalsIgnoreCase(type)) {
            // return Types.DATE;
            // testing using latest jdbc jar version returns this as TIMESTAMP
            return Types.TIMESTAMP; // changed to TIMESTAMP from DATE for data
                                    // types
            // migration
        } else if ("SMALLINT".equalsIgnoreCase(type)) {
            return Types.SMALLINT;
        } else if ("INT".equalsIgnoreCase(type)) {
            return Types.INTEGER;
        } else if ("NUMERIC".equalsIgnoreCase(type)) {
            return Types.BIGINT;
        } else if ("FLOAT".equalsIgnoreCase(type)) {
            return Types.FLOAT;
        } else if ("REAL".equalsIgnoreCase(type)) {
            return Types.FLOAT;
        } else if ("DOUBLE".equalsIgnoreCase(type)) {
            return Types.DOUBLE;
        } else if ("DECIMAL".equalsIgnoreCase(type)) {
            return Types.BIGINT;
        } else if ("RAW".equalsIgnoreCase(type)) {
            return Types.BLOB;
        } else if ("DATETIME".equalsIgnoreCase(type)) {
            return Types.TIMESTAMP;
        } else if ("DATETIME2".equalsIgnoreCase(type)) {
            return Types.VARCHAR;
        } else if ("TIMESTAMP".equalsIgnoreCase(type)) {
            return Types.BLOB;
        } else if ("TIME".equalsIgnoreCase(type)) {
            return Types.VARCHAR;
        } else if ("CHAR".equalsIgnoreCase(type)) {
            return Types.CHAR;
        } else if ("CLOB".equalsIgnoreCase(type)) {
            return Types.CLOB;
        } else if ("NCHAR".equalsIgnoreCase(type)) {
            return Types.CHAR;
        } else if ("TIMESTAMP(6)".equalsIgnoreCase(type)) {
            return Types.TIMESTAMP;
        } else if ("TIMESTAMPLTZ".equalsIgnoreCase(type)) {
            return Types.TIMESTAMP;
        } else if ("BLOB".equalsIgnoreCase(type)) {
            return Types.BLOB;
        } else if ("BINARY".equalsIgnoreCase(type)) {
            return Types.BLOB;
        } else if ("BINARY_FLOAT".equalsIgnoreCase(type)) {
            return Types.FLOAT;
        } else if ("NUMBER".equalsIgnoreCase(type)) {
            if ("0".equalsIgnoreCase(Datascale)) {
                return Types.NUMERIC;
            } else if ("2".equalsIgnoreCase(Datascale)) {
                return Types.INTEGER;
            }
            return Types.DECIMAL;
        } else if ("BINARY_DOUBLE".equalsIgnoreCase(type)) {
            return Types.DOUBLE;
        } else if ("TIMESTAMP(6) WITH TIME ZONE".equalsIgnoreCase(type)) {
            return Types.TIMESTAMP;
        } else if ("TIMESTAMP(6) WITH LOCAL TIME ZONE".equalsIgnoreCase(type)) {
            return Types.TIMESTAMP;
        } else if ("LONG".equalsIgnoreCase(type)) {
            return Types.CLOB;
        } else if ("NCLOB".equalsIgnoreCase(type)) {
            return Types.CLOB;
        } else if ("NVARCHAR2".equalsIgnoreCase(type)) {
            return Types.VARCHAR;
        } else if ("LONG RAW".equalsIgnoreCase(type)) {
            return Types.BLOB;
        } else if ("TIMESTAMP(3)".equalsIgnoreCase(type)) {
            return Types.TIMESTAMP;
        } else if ("TIMESTAMP(0)".equalsIgnoreCase(type)) {
            return Types.TIMESTAMP;
        } else if ("BFILE".equalsIgnoreCase(type)) {
            return Types.BLOB;
        }
        return 0;
    }

    public static String getMappedLength(String type, String length, String Dataprecision, String Datascale) {
        if ("VARCHAR".equalsIgnoreCase(type)) {
            return length;
        } else if ("NVARCHAR".equalsIgnoreCase(type)) {
            return length;
        } else if ("VARCHAR2".equalsIgnoreCase(type)) {
            if ("20".equalsIgnoreCase(length)) {
                return "20";
            }
            return "4000";
        } else if ("TEXT".equalsIgnoreCase(type)) {
            return "8";
        } else if ("NTEXT".equalsIgnoreCase(type)) {
            return "8";
        } else if ("DATE".equalsIgnoreCase(type)) {
            // return "8";
            // latest jdbc jar treats this as TIMESTAMP
            return "12"; // changed to 8 from 12 for data types migration
        } else if ("INT".equalsIgnoreCase(type)) {
            return "4";
        } else if ("FLOAT".equalsIgnoreCase(type)) {
            return "4";
        } else if ("DOUBLE".equalsIgnoreCase(type)) {
            return "8";
        } else if ("DECIMAL".equalsIgnoreCase(type)) {
            return "8";
        } else if ("RAW".equalsIgnoreCase(type)) {
            if ("2000".equalsIgnoreCase(length)) {
                return "2000";
            }
            return "1000";
        } else if ("DATETIME".equalsIgnoreCase(type)) {
            return "12";
        } else if ("TIMESTAMP".equalsIgnoreCase(type)) {
            return "8";
        } else if ("CHAR".equalsIgnoreCase(type)) {
            if ("20".equalsIgnoreCase(length)) {
                return "20";
            }
            return "2000";
        } else if ("TINYTEXT".equalsIgnoreCase(type)) {
            return "255";
        } else if ("BLOB".equalsIgnoreCase(type)) {
            return "8";
        } else if ("CLOB".equalsIgnoreCase(type)) {
            return "8";
        } else if ("BINARY".equalsIgnoreCase(type)) {
            return "4";
        } else if ("VARBINARY".equalsIgnoreCase(type)) {
            return "8";
        } else if ("TIMESTAMP(6)".equalsIgnoreCase(type)) {
            return "12";
        } else if ("NCHAR".equalsIgnoreCase(type)) {
            if ("40".equalsIgnoreCase(length)) {
                return "20";
            }
            return "1000";
        } else if ("REAL".equalsIgnoreCase(type)) {
            return "4";
        } else if ("NUMERIC".equalsIgnoreCase(type)) {
            return "8";
        } else if ("NUMBER".equalsIgnoreCase(type)) {
            if ("0".equalsIgnoreCase(Datascale)) {
                return "32";
            } else if ("2".equalsIgnoreCase(Datascale)) {
                return "4";
            }
            return "8";
        } else if ("TIME".equalsIgnoreCase(type)) {
            return "16";
        } else if ("BINARY_FLOAT".equalsIgnoreCase(type)) {
            return "4";
        } else if ("BINARY_DOUBLE".equalsIgnoreCase(type)) {
            return "8";
        } else if ("TIMESTAMP(6) WITH TIME ZONE".equalsIgnoreCase(type)) {
            return "12";
        } else if ("TIMESTAMP(6) WITH LOCAL TIME ZONE".equalsIgnoreCase(type)) {
            return "12";
        } else if ("TIMESTAMPLTZ".equalsIgnoreCase(type)) {
            return "12";
        } else if ("LONG".equalsIgnoreCase(type)) {
            return "8";
        } else if ("NCLOB".equalsIgnoreCase(type)) {
            return "8";
        } else if ("NVARCHAR2".equalsIgnoreCase(type)) {
            if ("40".equalsIgnoreCase(length)) {
                return "20";
            }
            return "2000";
        } else if ("LONG RAW".equalsIgnoreCase(type)) {
            return "8";
        } else if ("TIMESTAMP(3)".equalsIgnoreCase(type) || "TIMESTAMP(0)".equalsIgnoreCase(type)) {
            return "12";
        } else if ("BFILE".equalsIgnoreCase(type)) {
            return "8";
        }
        return null;
    }

    public static String getMappedDefault(String type, String defaultValue) {
        if ("SMALLINT".equalsIgnoreCase(type) || "MEDIUMINT".equalsIgnoreCase(type) || "BIGINT".equalsIgnoreCase(type)
                || "INT".equalsIgnoreCase(type)) {
            return defaultValue == null ? null : "'" + defaultValue + "'";
        } else if ("TIMESTAMP".equalsIgnoreCase(type)) {
            return "CURRENT_TIMESTAMP".equals(defaultValue) ? "'NOW'" : defaultValue;
        }
        return defaultValue;
    }

    public boolean isCaseSensitive() {
        return false;
    }
}
