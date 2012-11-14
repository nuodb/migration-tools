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
package com.nuodb.migration.jdbc.type.jdbc2;

import com.nuodb.migration.jdbc.type.JdbcTypeRegistry;
import com.nuodb.migration.jdbc.type.JdbcTypeRegistryBase;

/**
 * @author Sergey Bushik
 */
public class Jdbc2TypeRegistry extends JdbcTypeRegistryBase {

    public static final JdbcTypeRegistry INSTANCE = new Jdbc2TypeRegistry();

    public Jdbc2TypeRegistry() {
        addJdbcType(JdbcArrayType.INSTANCE);
        addJdbcType(JdbcBigIntType.INSTANCE);
        addJdbcType(JdbcBinaryType.INSTANCE);
        addJdbcType(JdbcVarBinaryType.INSTANCE);
        addJdbcType(JdbcLongVarBinaryType.INSTANCE);
        addJdbcType(JdbcBitType.INSTANCE);
        addJdbcType(JdbcBlobType.INSTANCE);
        addJdbcType(JdbcCharType.INSTANCE);
        addJdbcType(JdbcVarCharType.INSTANCE);
        addJdbcType(JdbcLongVarCharType.INSTANCE);
        addJdbcType(JdbcClobType.INSTANCE);
        addJdbcType(JdbcDateType.INSTANCE);
        addJdbcType(JdbcNumericType.INSTANCE);
        addJdbcType(JdbcDecimalType.INSTANCE);
        addJdbcType(JdbcDoubleType.INSTANCE);
        addJdbcType(JdbcFloatType.INSTANCE);
        addJdbcType(JdbcRealType.INSTANCE);
        addJdbcType(JdbcIntegerType.INSTANCE);
        addJdbcType(JdbcNullType.INSTANCE);
        addJdbcType(JdbcObjectType.INSTANCE);
        addJdbcType(JdbcOtherType.INSTANCE);
        addJdbcType(JdbcStructType.INSTANCE);
        addJdbcType(JdbcRefType.INSTANCE);
        addJdbcType(JdbcSmallIntType.INSTANCE);
        addJdbcType(JdbcTinyIntType.INSTANCE);
        addJdbcType(JdbcTimestampType.INSTANCE);
        addJdbcType(JdbcTimeType.INSTANCE);

        addJdbcTypeAdapter(JdbcDateTypeAdapter.INSTANCE);
        addJdbcTypeAdapter(JdbcTimeTypeAdapter.INSTANCE);
        addJdbcTypeAdapter(JdbcTimestampTypeAdapter.INSTANCE);
        addJdbcTypeAdapter(JdbcBlobTypeAdapter.INSTANCE);
        addJdbcTypeAdapter(JdbcClobTypeAdapter.INSTANCE);
    }
}
