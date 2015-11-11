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
package com.nuodb.migrator.jdbc.type.adapter;

import com.nuodb.migrator.jdbc.type.JdbcTypeAdapter;
import com.nuodb.migrator.jdbc.type.JdbcTypeAdapterBase;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Connection;
import java.sql.SQLException;

/**
 * @author Sergey Bushik
 */
@SuppressWarnings("unchecked")
public class JdbcBigIntegerTypeAdapter extends JdbcTypeAdapterBase<BigInteger> {

    public static final JdbcTypeAdapter INSTANCE = new JdbcBigIntegerTypeAdapter();

    public JdbcBigIntegerTypeAdapter() {
        super(BigInteger.class);
    }

    @Override
    public <X> BigInteger wrap(X value, Connection connection) throws SQLException {
        if (value == null) {
            return null;
        } else if (BigInteger.class.isInstance(value)) {
            return (BigInteger) value;
        } else if (BigDecimal.class.isInstance(value)) {
            return ((BigDecimal) value).toBigInteger();
        } else if (Number.class.isInstance(value)) {
            return BigInteger.valueOf(((Number) value).longValue());
        } else if (String.class.isInstance(value)) {
            return new BigInteger((String) value);
        } else {
            throw newWrapFailure(value);
        }
    }

    @Override
    public <X> X unwrap(BigInteger value, Class<X> valueClass, Connection connection) throws SQLException {
        if (value == null) {
            return null;
        } else if (valueClass.isAssignableFrom(BigInteger.class)) {
            return (X) value;
        } else if (valueClass.isAssignableFrom(BigDecimal.class)) {
            return (X) new BigDecimal(value);
        } else if (valueClass.isAssignableFrom(Double.class)) {
            return (X) Double.valueOf(value.doubleValue());
        } else if (valueClass.isAssignableFrom(Float.class)) {
            return (X) Float.valueOf(value.floatValue());
        } else if (valueClass.isAssignableFrom(Long.class)) {
            return (X) Long.valueOf(value.longValue());
        } else if (valueClass.isAssignableFrom(Integer.class)) {
            return (X) Integer.valueOf(value.intValue());
        } else if (valueClass.isAssignableFrom(Short.class)) {
            return (X) Short.valueOf(value.shortValue());
        } else if (valueClass.isAssignableFrom(Byte.class)) {
            return (X) Byte.valueOf(value.byteValue());
        } else if (valueClass.isAssignableFrom(String.class)) {
            return (X) value.toString();
        } else {
            throw newUnwrapFailure(valueClass);
        }
    }
}
