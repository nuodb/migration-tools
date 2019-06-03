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
public class PostgreSQLTypes implements DatabaseTypes {
    private static Map<String, JDBCGetMethod[]> map = new HashMap<String, JDBCGetMethod[]>();

    static {
        map.put("BOOLEAN", new JDBCGetMethod[] { JDBCGetMethod.BOOLEAN });
        map.put("BOOL", new JDBCGetMethod[] { JDBCGetMethod.BOOLEAN });

        map.put("TEXT", new JDBCGetMethod[] { JDBCGetMethod.STRING });

        map.put("BIGINT", new JDBCGetMethod[] { JDBCGetMethod.LONG });

        map.put("CHAR", new JDBCGetMethod[] { JDBCGetMethod.STRING });
        map.put("BPCHAR", new JDBCGetMethod[] { JDBCGetMethod.STRING });
        map.put("CHARACTER", new JDBCGetMethod[] { JDBCGetMethod.STRING });
        map.put("BIT", new JDBCGetMethod[] { JDBCGetMethod.SHORT, JDBCGetMethod.INT });
        map.put("VARBIT", new JDBCGetMethod[] { JDBCGetMethod.SHORT, JDBCGetMethod.STRING });
        map.put("BIGSERIAL", new JDBCGetMethod[] { JDBCGetMethod.LONG });
        map.put("SERIAL8", new JDBCGetMethod[] { JDBCGetMethod.LONG });
        map.put("SMALLINT", new JDBCGetMethod[] { JDBCGetMethod.SHORT });
        map.put("INT8", new JDBCGetMethod[] { JDBCGetMethod.SHORT, JDBCGetMethod.INT, JDBCGetMethod.LONG });
        map.put("INT4", new JDBCGetMethod[] { JDBCGetMethod.SHORT, JDBCGetMethod.INT });
        map.put("INT2", new JDBCGetMethod[] { JDBCGetMethod.SHORT });
        map.put("REAL", new JDBCGetMethod[] { JDBCGetMethod.FLOAT });
        map.put("NUMERIC", new JDBCGetMethod[] { JDBCGetMethod.INT });
        map.put("INT", new JDBCGetMethod[] { JDBCGetMethod.INT });
        map.put("MONEY", new JDBCGetMethod[] { JDBCGetMethod.DOUBLE });

        map.put("INTERVAL", new JDBCGetMethod[] { JDBCGetMethod.STRING });
        map.put("INET", new JDBCGetMethod[] { JDBCGetMethod.STRING });
        map.put("CIDR", new JDBCGetMethod[] { JDBCGetMethod.STRING });
        map.put("BYTEA", new JDBCGetMethod[] { JDBCGetMethod.BLOB });

        map.put("INTEGER", new JDBCGetMethod[] { JDBCGetMethod.SHORT, JDBCGetMethod.INT });
        map.put("SMALLINT", new JDBCGetMethod[] { JDBCGetMethod.SHORT, JDBCGetMethod.INT });
        map.put("MEDIUMINT", new JDBCGetMethod[] { JDBCGetMethod.INT, JDBCGetMethod.LONG });
        map.put("BIGINT", new JDBCGetMethod[] { JDBCGetMethod.INT, JDBCGetMethod.LONG });

        map.put("FLOAT", new JDBCGetMethod[] { JDBCGetMethod.FLOAT });
        map.put("FLOAT4", new JDBCGetMethod[] { JDBCGetMethod.FLOAT });
        map.put("DOUBLE", new JDBCGetMethod[] { JDBCGetMethod.DOUBLE });
        // TODO: revert back to double after bug fix
        // JDBCType.DOUBLE });
        map.put("DATE", new JDBCGetMethod[] { JDBCGetMethod.DATE });
        map.put("DATETIME", new JDBCGetMethod[] { JDBCGetMethod.DATE, JDBCGetMethod.TIMESTAMP });
        map.put("TIMESTAMP", new JDBCGetMethod[] { JDBCGetMethod.DATE, JDBCGetMethod.TIMESTAMP });
        map.put("TIMESTAMPTZ", new JDBCGetMethod[] { JDBCGetMethod.DATE, JDBCGetMethod.TIMESTAMP });
        map.put("TIME", new JDBCGetMethod[] { JDBCGetMethod.TIME });
        map.put("TIMETZ", new JDBCGetMethod[] { JDBCGetMethod.TIME });
        map.put("BIT", new JDBCGetMethod[] { JDBCGetMethod.STRING });
        map.put("BIT VARYING", new JDBCGetMethod[] { JDBCGetMethod.STRING });
        map.put("BYTEA", new JDBCGetMethod[] { JDBCGetMethod.BYTES });
        map.put("FLOAT8", new JDBCGetMethod[] { JDBCGetMethod.DOUBLE });
        map.put("CHARACTER VARYING", new JDBCGetMethod[] { JDBCGetMethod.STRING });
        map.put("SERIAL", new JDBCGetMethod[] { JDBCGetMethod.INT });
        map.put("SERIAL4", new JDBCGetMethod[] { JDBCGetMethod.INT });
        map.put("VARCHAR", new JDBCGetMethod[] { JDBCGetMethod.STRING });

        map.put("TIME WITH TIME ZONE", new JDBCGetMethod[] { JDBCGetMethod.TIMESTAMP });
        map.put("TIM WITHOUT TIME ZONE", new JDBCGetMethod[] { JDBCGetMethod.TIMESTAMP });
        map.put("TIMESTAMP WITH TIME ZONE", new JDBCGetMethod[] { JDBCGetMethod.TIMESTAMP });
        map.put("TIMESTAMP WITHOUT TIME ZONE", new JDBCGetMethod[] { JDBCGetMethod.TIMESTAMP });

    }

    public JDBCGetMethod[] getJDBCTypes(String type) {
        if (isCaseSensitive()) {
            return map.get(type);
        } else {
            return map.get(type.toUpperCase());
        }
    }

    public static int getKeyType(String type) {
        if ("PRIMARY KEY".equals(type)) {
            return 0;
        } else if ("UNIQUE".equals(type)) {
            return 1;
        }
        return -1;
    }

    public static int getMappedJDBCType(String type) {
        if ("CHAR".equalsIgnoreCase(type)) {
            return Types.VARCHAR;
        } else if ("BPCHAR".equalsIgnoreCase(type)) {
            return Types.VARCHAR;
        } else if ("TEXT".equalsIgnoreCase(type)) {
            return Types.VARCHAR;
        } else if ("SMALLINT".equalsIgnoreCase(type)) {
            return Types.SMALLINT;
        } else if ("DATE".equalsIgnoreCase(type)) {
            return Types.DATE;
        } else if ("SMALLINT".equalsIgnoreCase(type)) {
            return Types.SMALLINT;
        } else if ("MEDIUMINT".equalsIgnoreCase(type)) {
            return Types.INTEGER;
        } else if ("BIGINT".equalsIgnoreCase(type)) {
            return Types.BIGINT;
        } else if ("INTEGER".equalsIgnoreCase(type)) {
            return Types.INTEGER;
        } else if ("INT".equalsIgnoreCase(type)) {
            return Types.INTEGER;
        } else if ("INT2".equalsIgnoreCase(type)) {
            return Types.SMALLINT;
        } else if ("INT4".equalsIgnoreCase(type)) {
            return Types.SMALLINT;
        } else if ("INT8".equalsIgnoreCase(type)) {
            return Types.INTEGER;
        } else if ("NUMERIC".equalsIgnoreCase(type)) {
            return Types.INTEGER;
        } else if ("REAL".equalsIgnoreCase(type)) {
            return Types.FLOAT;
        } else if ("FLOAT".equalsIgnoreCase(type)) {
            return Types.FLOAT;
        } else if ("FLOAT4".equalsIgnoreCase(type)) {
            return Types.FLOAT;
        } else if ("DOUBLE".equalsIgnoreCase(type)) {
            return Types.DOUBLE;
        } else if ("DECIMAL".equalsIgnoreCase(type)) {
            return Types.BIGINT;
        } else if ("TIME".equalsIgnoreCase(type)) {
            return Types.TIMESTAMP;
        } else if ("TIMETZ".equalsIgnoreCase(type)) {
            return Types.TIMESTAMP;
        } else if ("DATETIME".equalsIgnoreCase(type)) {
            return Types.TIMESTAMP;
        } else if ("TIMESTAMP".equalsIgnoreCase(type)) {
            return Types.TIMESTAMP;
        } else if ("TIMESTAMPTZ".equalsIgnoreCase(type)) {
            return Types.TIMESTAMP;
        } else if ("YEAR".equalsIgnoreCase(type)) {
            return Types.DATE;
        } else if ("CHAR".equalsIgnoreCase(type)) {
            return Types.CHAR;
        } else if ("CHARACTER".equalsIgnoreCase(type)) {
            return Types.CHAR;
        } else if ("BLOB".equalsIgnoreCase(type)) {
            return Types.BLOB;
        } else if ("MONEY".equalsIgnoreCase(type)) {
            return Types.DOUBLE;
        } else if ("INET".equalsIgnoreCase(type)) {
            return Types.VARCHAR;
        } else if ("BIT".equalsIgnoreCase(type)) {
            return Types.CHAR;
        } else if ("VARBIT".equalsIgnoreCase(type)) {
            return Types.VARCHAR;
        } else if ("BYTEA".equalsIgnoreCase(type)) {
            return Types.BLOB;
        } else if ("FLOAT8".equalsIgnoreCase(type)) {
            return Types.DOUBLE;
        } else if ("DOUBLE PRECISION".equalsIgnoreCase(type)) {
            return Types.DOUBLE;
        } else if ("CHARACTER VARYING".equalsIgnoreCase(type)) {
            return Types.VARCHAR;
        } else if ("BOOLEAN".equalsIgnoreCase(type)) {
            return Types.BOOLEAN;
        } else if ("VARCHAR".equalsIgnoreCase(type)) {
            return Types.VARCHAR;
        } else if ("TIME WITH TIME ZONE".equalsIgnoreCase(type)) {
            return Types.TIME;
        } else if ("TIME WITHOUT TIME ZONE".equalsIgnoreCase(type)) {
            return Types.TIME;
        } else if ("TIMESTAMP WITHOUT TIME ZONE".equalsIgnoreCase(type)) {
            return Types.TIMESTAMP;
        } else if ("TIMESTAMP WITH TIME ZONE".equalsIgnoreCase(type)) {
            return Types.TIMESTAMP;
        }
        return 0;
    }

    public static String getMappedLength(String type, String length) {
        if ("CHAR".equalsIgnoreCase(type)) {
            return length;
        } else if ("BOOL".equalsIgnoreCase(type)) {
            return "1";
        } else if ("TEXT".equalsIgnoreCase(type)) {
            return "2147483647";
        } else if ("DATE".equalsIgnoreCase(type)) {
            return "8";
        } else if ("SMALLINT".equalsIgnoreCase(type)) {
            return "2";
        } else if ("MEDIUMINT".equalsIgnoreCase(type)) {
            return "4";
        } else if ("BIGINT".equalsIgnoreCase(type)) {
            return "8";
        } else if ("INT".equalsIgnoreCase(type)) {
            return "4";
        } else if ("INT4".equalsIgnoreCase(type)) {
            return "4";
        } else if ("INT2".equalsIgnoreCase(type)) {
            return "4";
        } else if ("INT8".equalsIgnoreCase(type)) {
            return "8";
        } else if ("FLOAT".equalsIgnoreCase(type)) {
            return "4";
        } else if ("FLOAT4".equalsIgnoreCase(type)) {
            return "4";
        } else if ("DOUBLE".equalsIgnoreCase(type)) {
            return "8";
        } else if ("MONEY".equalsIgnoreCase(type)) {
            return "8";
        } else if ("DATETIME".equalsIgnoreCase(type)) {
            return "12";
        } else if ("TIMESTAMP".equalsIgnoreCase(type)) {
            return "12";
        } else if ("TIMESTAMPTZ".equalsIgnoreCase(type)) {
            return "12";
        } else if ("TIMETZ".equalsIgnoreCase(type)) {
            return "8";
        } else if ("TIME WITH TIME ZONE".equalsIgnoreCase(type)) {
            return "8";
        } else if ("TIME WITHOUT TIME ZONE".equalsIgnoreCase(type)) {
            return "8";
        } else if ("TIMESTAMP WITH TIME ZONE".equalsIgnoreCase(type)) {
            return "12";
        } else if ("TIMESTAMP WITHOUT TIME ZONE".equalsIgnoreCase(type)) {
            return "12";
        } else if ("BIT".equalsIgnoreCase(type)) {
            return length;
        } else if ("REAL".equalsIgnoreCase(type)) {
            return "4";
        } else if ("INET".equalsIgnoreCase(type)) {
            return "15";
        } else if ("BIT VARYING".equalsIgnoreCase(type)) {
            return length;
        } else if ("BYTEA".equalsIgnoreCase(type)) {
            return "2147483647";
        } else if ("DOUBLE PRECISION".equalsIgnoreCase(type)) {
            return "8";
        } else if ("CHARACTER VARYING".equalsIgnoreCase(type)) {
            return length;
        } else if ("CHARACTER".equalsIgnoreCase(type)) {
            return length;
        } else if ("NUMERIC".equalsIgnoreCase(type)) {
            return "4";
        } else if ("SERIAL8".equalsIgnoreCase(type)) {
            return "8";
        } else if ("SERIAL4".equalsIgnoreCase(type)) {
            return "4";
        } else if ("SERIAL".equalsIgnoreCase(type)) {
            return "4";
        } else if ("DECIMAL".equalsIgnoreCase(type)) {
            return "8";
        } else if ("INTEGER".equalsIgnoreCase(type)) {
            return "4";
        } else if ("VARCHAR".equalsIgnoreCase(type)) {
            return "2147483647";
        } else if ("float8".equalsIgnoreCase(type)) {
            return "8";
        } else if ("BOOLEAN".equalsIgnoreCase(type)) {
            return "2";
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
