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
public class SQLServerTypes implements DatabaseTypes {
    private static Map<String, JDBCGetMethod[]> map = new HashMap<String, JDBCGetMethod[]>();

    static {
        map.put("bool", new JDBCGetMethod[] { JDBCGetMethod.BOOLEAN });
        map.put("boolean", new JDBCGetMethod[] { JDBCGetMethod.BOOLEAN });
        map.put("bit", new JDBCGetMethod[] { JDBCGetMethod.SHORT, JDBCGetMethod.INT });
        map.put("long", new JDBCGetMethod[] { JDBCGetMethod.LONG });

        map.put("varchar", new JDBCGetMethod[] { JDBCGetMethod.STRING });
        map.put("nvarchar", new JDBCGetMethod[] { JDBCGetMethod.STRING });
        map.put("text", new JDBCGetMethod[] { JDBCGetMethod.STRING });
        map.put("tinytext", new JDBCGetMethod[] { JDBCGetMethod.STRING });
        map.put("mediumtext", new JDBCGetMethod[] { JDBCGetMethod.STRING });
        map.put("longtext", new JDBCGetMethod[] { JDBCGetMethod.STRING });
        map.put("char", new JDBCGetMethod[] { JDBCGetMethod.STRING });
        map.put("uniqueidentifier", new JDBCGetMethod[] { JDBCGetMethod.STRING });
        map.put("nchar", new JDBCGetMethod[] { JDBCGetMethod.STRING });
        map.put("ntext", new JDBCGetMethod[] { JDBCGetMethod.STRING });
        map.put("int", new JDBCGetMethod[] { JDBCGetMethod.SHORT, JDBCGetMethod.INT });
        map.put("int identity", new JDBCGetMethod[] { JDBCGetMethod.SHORT, JDBCGetMethod.INT });
        map.put("numeric", new JDBCGetMethod[] { JDBCGetMethod.SHORT, JDBCGetMethod.LONG, JDBCGetMethod.INT });
        map.put("tinyint", new JDBCGetMethod[] { JDBCGetMethod.SHORT, JDBCGetMethod.INT });
        map.put("smallint", new JDBCGetMethod[] { JDBCGetMethod.SHORT, JDBCGetMethod.INT });
        map.put("mediumint", new JDBCGetMethod[] { JDBCGetMethod.INT, JDBCGetMethod.LONG });
        map.put("bigint", new JDBCGetMethod[] { JDBCGetMethod.INT, JDBCGetMethod.LONG });

        map.put("float", new JDBCGetMethod[] { JDBCGetMethod.FLOAT });
        map.put("real", new JDBCGetMethod[] { JDBCGetMethod.FLOAT });
        map.put("double", new JDBCGetMethod[] { JDBCGetMethod.DOUBLE });
        map.put("money", new JDBCGetMethod[] { JDBCGetMethod.INT, JDBCGetMethod.LONG });
        map.put("smallmoney", new JDBCGetMethod[] { JDBCGetMethod.INT, JDBCGetMethod.LONG });
        // TODO: revert back to double after bug fix
        // JDBCType.DOUBLE });
        map.put("decimal", new JDBCGetMethod[] { JDBCGetMethod.STRING });

        map.put("date", new JDBCGetMethod[] { JDBCGetMethod.DATE, JDBCGetMethod.TIMESTAMP });
        map.put("datetime", new JDBCGetMethod[] { JDBCGetMethod.DATE, JDBCGetMethod.TIMESTAMP });
        map.put("smalldatetime", new JDBCGetMethod[] { JDBCGetMethod.DATE, JDBCGetMethod.TIMESTAMP });
        map.put("datetime2", new JDBCGetMethod[] { JDBCGetMethod.DATE, JDBCGetMethod.TIMESTAMP });
        map.put("timestamp", new JDBCGetMethod[] { JDBCGetMethod.BLOB });
        map.put("time", new JDBCGetMethod[] { JDBCGetMethod.DATE, JDBCGetMethod.TIME, JDBCGetMethod.TIMESTAMP });
        map.put("image", new JDBCGetMethod[] { JDBCGetMethod.BLOB });
        map.put("binary", new JDBCGetMethod[] { JDBCGetMethod.BLOB });
        map.put("varbinary", new JDBCGetMethod[] { JDBCGetMethod.BLOB });
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

    public static int getMappedJDBCType(String type, String precision) {
        if ("varchar".equalsIgnoreCase(type)) {
            return Types.VARCHAR;
        } else if ("nvarchar".equalsIgnoreCase(type)) {
            return Types.VARCHAR;
        } else if ("tinyint".equalsIgnoreCase(type)) {
            return Types.SMALLINT;
        } else if ("text".equalsIgnoreCase(type)) {
            return Types.CLOB;
        } else if ("ntext".equalsIgnoreCase(type)) {
            return Types.CLOB;
        } else if ("date".equalsIgnoreCase(type)) {
            return Types.VARCHAR;
        } else if ("smallint".equalsIgnoreCase(type)) {
            return Types.SMALLINT;
        } else if ("mediumint".equalsIgnoreCase(type)) {
            return Types.INTEGER;
        } else if ("bigint".equalsIgnoreCase(type)) {
            return Types.BIGINT;
        } else if ("int".equalsIgnoreCase(type)) {
            return Types.INTEGER;
        } else if ("numeric".equalsIgnoreCase(type)) {
            if ("38".equalsIgnoreCase(precision)) {
                return Types.NUMERIC;
            }
            if ("18".equalsIgnoreCase(precision)) {
                return Types.BIGINT;
            }
            return Types.INTEGER;
        } else if ("float".equalsIgnoreCase(type)) {
            return Types.DOUBLE;
        } else if ("real".equalsIgnoreCase(type)) {
            return Types.FLOAT;
        } else if ("double".equalsIgnoreCase(type)) {
            return Types.DOUBLE;
        } else if ("decimal".equalsIgnoreCase(type)) {
            if ("18".equalsIgnoreCase(precision)) {
                return Types.BIGINT;
            }
            return Types.NUMERIC;
        } else if ("money".equalsIgnoreCase(type)) {
            return Types.BIGINT;
        } else if ("smallmoney".equalsIgnoreCase(type)) {
            return Types.BIGINT;
        } else if ("datetime".equalsIgnoreCase(type)) {
            return Types.TIMESTAMP;
        } else if ("smalldatetime".equalsIgnoreCase(type)) {
            return Types.TIMESTAMP;
        } else if ("datetime2".equalsIgnoreCase(type)) {
            return Types.VARCHAR;
        } else if ("timestamp".equalsIgnoreCase(type)) {
            return Types.BLOB;
        } else if ("time".equalsIgnoreCase(type)) {
            return Types.VARCHAR;
        } else if ("char".equalsIgnoreCase(type)) {
            return Types.CHAR;
        } else if ("uniqueidentifier".equalsIgnoreCase(type)) {
            return Types.CHAR;
        } else if ("datetimeoffset".equalsIgnoreCase(type)) {
            return Types.VARCHAR;
        } else if ("nchar".equalsIgnoreCase(type)) {
            return Types.CHAR;
        } else if ("bit".equalsIgnoreCase(type)) {
            return Types.BOOLEAN;
        } else if ("image".equalsIgnoreCase(type)) {
            return Types.BLOB;
        } else if ("binary".equalsIgnoreCase(type)) {
            return Types.BLOB;
        } else if ("varbinary".equalsIgnoreCase(type)) {
            return Types.BLOB;
        } else if ("hierarchyid".equalsIgnoreCase(type)) {
            return Types.BLOB;
        }
        return 0;
    }

    public static String getMappedLength(String type, String length, String dataPrecision, String dataScale,
            String numericPrecision) {
        if ("varchar".equalsIgnoreCase(type)) {
            return length;
        } else if ("nvarchar".equalsIgnoreCase(type)) {
            return length;
        } else if ("tinyint".equalsIgnoreCase(type)) {
            return "2";
        } else if ("text".equalsIgnoreCase(type)) {
            return "8";
        } else if ("ntext".equalsIgnoreCase(type)) {
            return "8";
        } else if ("date".equalsIgnoreCase(type)) {
            return "10";
        } else if ("bigint".equalsIgnoreCase(type)) {
            return "8";
        } else if ("int".equalsIgnoreCase(type)) {
            return "4";
        } else if ("float".equalsIgnoreCase(type)) {
            return "8";
        } else if ("double".equalsIgnoreCase(type)) {
            return "8";
        } else if ("decimal".equalsIgnoreCase(type)) {
            if ("18".equalsIgnoreCase(dataPrecision) || (null == dataScale || "0".equalsIgnoreCase(dataScale))) {
                return "8";
            }
            return "32";
        } else if ("money".equalsIgnoreCase(type)) {
            return "8";
        } else if ("smallmoney".equalsIgnoreCase(type)) {
            return "8";
        } else if ("datetime".equalsIgnoreCase(type)) {
            return "12";
        } else if ("datetime2".equalsIgnoreCase(type)) {
            return "27";
        } else if ("timestamp".equalsIgnoreCase(type)) {
            return "8";
        } else if ("char".equalsIgnoreCase(type)) {
            if ("8000".equalsIgnoreCase(length)) {
                return "8000";
            }
            if ("20".equalsIgnoreCase(length)) {
                return "20";
            }
            return "1";
        } else if ("tinytext".equalsIgnoreCase(type)) {
            return "255";
        } else if ("image".equalsIgnoreCase(type)) {
            return "8";
        } else if ("datetimeoffset".equalsIgnoreCase(type)) {
            if ("4".equalsIgnoreCase(numericPrecision)) {
                return "31";
            }
            return "34";
        } else if ("binary".equalsIgnoreCase(type)) {
            if ("8000".equalsIgnoreCase(length)) {
                return "8000";
            }
            if ("4".equalsIgnoreCase(length)) {
                return "4";
            }
            return "1";
        } else if ("varbinary".equalsIgnoreCase(type)) {
            if ("8000".equalsIgnoreCase(length)) {
                return "8000";
            }
            if ("1".equalsIgnoreCase(length)) {
                return "1";
            }
            return "8";
        } else if ("bit".equalsIgnoreCase(type)) {
            return "2";
        } else if ("nchar".equalsIgnoreCase(type)) {
            if ("4000".equalsIgnoreCase(length)) {
                return "4000";
            }
            if ("10".equalsIgnoreCase(length)) {
                return "10";
            }
            return "1";
        } else if ("real".equalsIgnoreCase(type)) {
            return "4";
        } else if ("numeric".equalsIgnoreCase(type)) {
            if ("18".equalsIgnoreCase(dataPrecision) || (null == dataScale || "0".equalsIgnoreCase(dataScale))) {
                return "8";
            }
            return "32";
        } else if ("smalldatetime".equalsIgnoreCase(type)) {
            return "12";
        } else if ("smallint".equalsIgnoreCase(type)) {
            return "2";
        } else if ("time".equalsIgnoreCase(type)) {
            return "16";
        } else if ("uniqueidentifier".equalsIgnoreCase(type)) {
            return "36";
        } else if ("hierarchyid".equalsIgnoreCase(type)) {
            return "8";
        }
        return null;
    }

    public static String getMappedDefault(String type, String defaultValue) {
        if (defaultValue != null) {
            if (!"binary".equalsIgnoreCase(type) && !"varbinary".equalsIgnoreCase(type))
                defaultValue = defaultValue.replaceAll("[()]", "");
        }
        if ("smallint".equalsIgnoreCase(type) || "mediumint".equalsIgnoreCase(type) || "bigint".equalsIgnoreCase(type)
                || "int".equalsIgnoreCase(type) || "tinyint".equalsIgnoreCase(type) || "numeric".equalsIgnoreCase(type)
                || "decimal".equalsIgnoreCase(type) || "money".equalsIgnoreCase(type)
                || "smallmoney".equalsIgnoreCase(type) || "real".equalsIgnoreCase(type) || "bit".equalsIgnoreCase(type)
                || "float".equalsIgnoreCase(type)) {
            return defaultValue == null ? null : "'" + defaultValue + "'";
        } else if ("timestamp".equalsIgnoreCase(type)) {
            return "CURRENT_TIMESTAMP".equals(defaultValue) ? "'NOW'" : defaultValue;
        } else if ("binary".equalsIgnoreCase(type) || "varbinary".equalsIgnoreCase(type)) {
            return defaultValue == null ? null
                    : "'" + defaultValue.substring(defaultValue.indexOf('(') + 1, defaultValue.lastIndexOf(')')) + "'";
        }
        return defaultValue;
    }

    public boolean isCaseSensitive() {
        return false;
    }
}
