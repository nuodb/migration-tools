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

public class MSSQLServerPrecisions {

    public static Collection<MSSQLServerPrecision1> getMSSQLServerPrecision1() {
        Collection<MSSQLServerPrecision1> values = new ArrayList<MSSQLServerPrecision1>();
        values.add(new MSSQLServerPrecision1(2147483647, 9223372036854775807L, 32767, 255));
        values.add(new MSSQLServerPrecision1(-2147483648, -9223372036854775807L, -32768, 0));
        return values;
    }

    public static Collection<MSSQLServerPrecision2> getMSSQLServerPrecision2() {
        /*
         * Original values are changed to avoid numeric(7,2), decimal(7,2) data
         * type issue
         */
        Collection<MSSQLServerPrecision2> values = new ArrayList<MSSQLServerPrecision2>();
        values.add(new MSSQLServerPrecision2("total word", "text length", 25.0, "F", 0, 76.0, 15.25, "M"));
        values.add(new MSSQLServerPrecision2("total word lenght 20", "sample text length20", 54325.0, "F", 0, 98765.0,
                56.65, "M"));
        values.add(new MSSQLServerPrecision2("lenght", "sample", 525.0, "F", 0, 145.0, 96.65, "M"));
        return values;
    }
}
