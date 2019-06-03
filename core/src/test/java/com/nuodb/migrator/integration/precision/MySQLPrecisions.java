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
package com.nuodb.migrator.integration.precision;

import java.util.ArrayList;
import java.util.Collection;

public class MySQLPrecisions {

    public static Collection<MySQLPrecision1> getMySQLPrecision1() {

        long l1 = -9223372036854775807L;
        long l2 = -2147483648L;
        long l3 = -8388608L;
        /*
         * long l1 = 0L; long l2 = 0L; long l3 = 0L;
         */
        Collection<MySQLPrecision1> t1List = new ArrayList<MySQLPrecision1>();
        t1List.add(new MySQLPrecision1(66, 2687, 678246, 49, 3720368547758L));
        t1List.add(new MySQLPrecision1(127, 32767, 8388607, 2147483647, 9223372036854775807L));
        t1List.add(new MySQLPrecision1(-128, -32768, l3, l2, l1));
        return t1List;
    }

    public static Collection<MySQLPrecision2> getMySQLPrecision2() {
        /* Original values are changed to avoid float data type issue */
        /* DB-23789 */
        Collection<MySQLPrecision2> values = new ArrayList<MySQLPrecision2>();
        values.add(new MySQLPrecision2("sample text", "sample data", 23.22, 4.6, 416.7, "true", "1234567890"));
        values.add(new MySQLPrecision2("sample text length20", "total word lenght 20", 1.234567822E7, 9.8765432E7,
                34567891.17, "false", "12345678900123456789"));
        values.add(new MySQLPrecision2("", "sample data", 23.22, 4.6, 416.7, "true", "5291"));
        return values;
    }
}
