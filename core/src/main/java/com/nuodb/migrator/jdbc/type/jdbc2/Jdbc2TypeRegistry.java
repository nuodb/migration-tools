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
package com.nuodb.migrator.jdbc.type.jdbc2;

import com.nuodb.migrator.jdbc.type.SimpleJdbcTypeRegistry;
import com.nuodb.migrator.jdbc.type.JdbcTypeRegistry;
import com.nuodb.migrator.jdbc.type.adapter.*;

/**
 * @author Sergey Bushik
 */
public class Jdbc2TypeRegistry extends SimpleJdbcTypeRegistry {

    public static final JdbcTypeRegistry INSTANCE = new Jdbc2TypeRegistry();

    public Jdbc2TypeRegistry() {
        addJdbcType(JdbcArrayValue.INSTANCE);
        addJdbcType(JdbcBigIntValue.INSTANCE);
        addJdbcType(JdbcBinaryValue.INSTANCE);
        addJdbcType(JdbcVarBinaryValue.INSTANCE);
        addJdbcType(JdbcLongVarBinaryValue.INSTANCE);
        addJdbcType(JdbcBitValue.INSTANCE);
        addJdbcType(JdbcBlobValue.INSTANCE);
        addJdbcType(JdbcCharValue.INSTANCE);
        addJdbcType(JdbcVarCharValue.INSTANCE);
        addJdbcType(JdbcLongVarCharValue.INSTANCE);
        addJdbcType(JdbcClobValue.INSTANCE);
        addJdbcType(JdbcDateValue.INSTANCE);
        addJdbcType(JdbcNumericValue.INSTANCE);
        addJdbcType(JdbcDecimalValue.INSTANCE);
        addJdbcType(JdbcDoubleValue.INSTANCE);
        addJdbcType(JdbcFloatValue.INSTANCE);
        addJdbcType(JdbcRealValue.INSTANCE);
        addJdbcType(JdbcIntegerValue.INSTANCE);
        addJdbcType(JdbcNullValue.INSTANCE);
        addJdbcType(JdbcObjectValue.INSTANCE);
        addJdbcType(JdbcOtherValue.INSTANCE);
        addJdbcType(JdbcStructValue.INSTANCE);
        addJdbcType(JdbcRefValue.INSTANCE);
        addJdbcType(JdbcSmallIntValue.INSTANCE);
        addJdbcType(JdbcTinyIntValue.INSTANCE);
        addJdbcType(JdbcTimestampValue.INSTANCE);
        addJdbcType(JdbcTimeValue.INSTANCE);

        addJdbcTypeAdapter(JdbcDateTypeAdapter.INSTANCE);
        addJdbcTypeAdapter(JdbcTimeTypeAdapter.INSTANCE);
        addJdbcTypeAdapter(JdbcTimestampTypeAdapter.INSTANCE);
        addJdbcTypeAdapter(JdbcBlobTypeAdapter.INSTANCE);
        addJdbcTypeAdapter(JdbcClobTypeAdapter.INSTANCE);
        addJdbcTypeAdapter(JdbcBigDecimalTypeAdapter.INSTANCE);
        addJdbcTypeAdapter(JdbcBigIntegerTypeAdapter.INSTANCE);
    }
}
