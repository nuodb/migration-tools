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
public class Db2Types implements DatabaseTypes {
    private static Map<String, JDBCGetMethod[]> map = new HashMap<String, JDBCGetMethod[]>();

    static {
        map.put("BOOLEAN", new JDBCGetMethod[] { JDBCGetMethod.BOOLEAN });
        map.put("GRAPHIC", new JDBCGetMethod[] { JDBCGetMethod.STRING });
        map.put("VARCHAR", new JDBCGetMethod[] { JDBCGetMethod.STRING });
        map.put("LONG VARCHAR", new JDBCGetMethod[] { JDBCGetMethod.STRING });
        map.put("LONG VARGRAPHIC", new JDBCGetMethod[] { JDBCGetMethod.STRING });
        map.put("CHAR", new JDBCGetMethod[] { JDBCGetMethod.STRING });
        map.put("VARGRAPHIC", new JDBCGetMethod[] { JDBCGetMethod.STRING });
        map.put("INTEGER", new JDBCGetMethod[] { JDBCGetMethod.SHORT, JDBCGetMethod.INT });
        map.put("SMALLINT", new JDBCGetMethod[] { JDBCGetMethod.SHORT, JDBCGetMethod.INT });
        map.put("MEDIUMINT", new JDBCGetMethod[] { JDBCGetMethod.INT, JDBCGetMethod.LONG });
        map.put("BIGINT", new JDBCGetMethod[] { JDBCGetMethod.INT, JDBCGetMethod.LONG });
        map.put("REAL", new JDBCGetMethod[] { JDBCGetMethod.FLOAT });
        map.put("FLOAT", new JDBCGetMethod[] { JDBCGetMethod.FLOAT });
        map.put("DOUBLE", new JDBCGetMethod[] { JDBCGetMethod.DOUBLE });
        map.put("DECIMAL", new JDBCGetMethod[] { JDBCGetMethod.STRING });

        map.put("DATE", new JDBCGetMethod[] { JDBCGetMethod.DATE, JDBCGetMethod.TIMESTAMP });
        map.put("TIMESTAMP", new JDBCGetMethod[] { JDBCGetMethod.DATE, JDBCGetMethod.TIMESTAMP });
        map.put("CLOB", new JDBCGetMethod[] { JDBCGetMethod.CLOB });
        map.put("DBCLOB", new JDBCGetMethod[] { JDBCGetMethod.CLOB });

        map.put("TIME", new JDBCGetMethod[] { JDBCGetMethod.TIME, JDBCGetMethod.TIMESTAMP });
        map.put("BLOB", new JDBCGetMethod[] { JDBCGetMethod.BLOB });
        map.put("XML", new JDBCGetMethod[] { JDBCGetMethod.STRING });
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

    public static int getMappedJDBCType(String type, String length) {
        if ("VARCHAR".equalsIgnoreCase(type)) {
            return Types.VARCHAR;
        } else if ("LONG VARCHAR".equalsIgnoreCase(type)) {
            return Types.VARCHAR;
        } else if ("VARGRAPHIC".equalsIgnoreCase(type)) {
            return Types.VARCHAR;
        } else if ("LONG VARGRAPHIC".equalsIgnoreCase(type)) {
            return Types.VARCHAR;
        } else if ("DATE".equalsIgnoreCase(type)) {
            return Types.DATE;
        } else if ("SMALLINT".equalsIgnoreCase(type)) {
            return Types.SMALLINT;
        } else if ("INTEGER".equalsIgnoreCase(type)) {
            return Types.INTEGER;
        } else if ("FLOAT".equalsIgnoreCase(type)) {
            return Types.FLOAT;
        } else if ("REAL".equalsIgnoreCase(type)) {
            return Types.FLOAT;
        } else if ("BIGINT".equalsIgnoreCase(type)) {
            return Types.BIGINT;
        } else if ("DOUBLE".equalsIgnoreCase(type)) {
            return Types.DOUBLE;
        } else if ("DECIMAL".equalsIgnoreCase(type)) {
            if ("31".equalsIgnoreCase(length)) {
                return Types.NUMERIC;
            }
            return Types.INTEGER;
        } else if ("TIMESTAMP".equalsIgnoreCase(type)) {
            return Types.TIMESTAMP;
        } else if ("CLOB".equalsIgnoreCase(type)) {
            return Types.CLOB;
        } else if ("DBCLOB".equalsIgnoreCase(type)) {
            return Types.CLOB;
        } else if ("TIME".equalsIgnoreCase(type)) {
            return Types.TIME;
        } else if ("GRAPHIC".equalsIgnoreCase(type)) {
            return Types.CHAR;
        } else if ("CHARACTER".equalsIgnoreCase(type)) {
            return Types.CHAR;
        } else if ("BLOB".equalsIgnoreCase(type)) {
            return Types.BLOB;
        } else if ("XML".equalsIgnoreCase(type)) {
            return Types.VARCHAR;
        }
        return 0;
    }

    public static String getMappedLength(String type, String length) {
        if ("VARCHAR".equalsIgnoreCase(type)) {
            return length;
        } else if ("GRAPHIC".equalsIgnoreCase(type)) {
            if ("68".equalsIgnoreCase(length)) {
                return "136";
            }
            return "40";
        } else if ("VARGRAPHIC".equalsIgnoreCase(type)) {
            if ("16292".equalsIgnoreCase(length)) {
                return "32584";
            }
            return "40";
        } else if ("DATE".equalsIgnoreCase(type)) {
            return "8";
        } else if ("SMALLINT".equalsIgnoreCase(type)) {
            return "2";
        } else if ("CHARACTER".equalsIgnoreCase(type)) {
            if ("20".equalsIgnoreCase(length)) {
                return "20";
            }
            return "195";
        } else if ("INTEGER".equalsIgnoreCase(type)) {
            return "4";
        } else if ("FLOAT".equalsIgnoreCase(type)) {
            return "4";
        } else if ("REAL".equalsIgnoreCase(type)) {
            return "4";
        } else if ("DOUBLE".equalsIgnoreCase(type)) {
            return "8";
        } else if ("BIGINT".equalsIgnoreCase(type)) {
            return "8";
        } else if ("DECIMAL".equalsIgnoreCase(type)) {
            if ("31".equalsIgnoreCase(length)) {
                return "32";
            }
            return "4";
        } else if ("CLOB".equalsIgnoreCase(type)) {
            return "8";
        } else if ("DBCLOB".equalsIgnoreCase(type)) {
            return "8";
        } else if ("TIMESTAMP".equalsIgnoreCase(type)) {
            return "12";
        } else if ("CHAR".equalsIgnoreCase(type)) {
            return "20";
        } else if ("TIME".equalsIgnoreCase(type)) {
            return "8";
        } else if ("LONG VARCHAR".equalsIgnoreCase(type)) {
            return "32700";
        } else if ("LONG VARGRAPHIC".equalsIgnoreCase(type)) {
            return "32700";
        } else if ("BLOB".equalsIgnoreCase(type)) {
            return "8";
        } else if ("XML".equalsIgnoreCase(type)) {
            return "100";
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
