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
package com.nuodb.tools.migration.jdbc.type.jdbc2;

import com.google.common.io.ByteStreams;
import com.nuodb.tools.migration.jdbc.type.JdbcTypeAdapterBase;
import com.nuodb.tools.migration.jdbc.type.JdbcTypeException;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Blob;
import java.sql.Connection;
import java.sql.SQLException;

/**
 * @author Sergey Bushik
 */
@SuppressWarnings("unchecked")
public class JdbcBlobTypeAdapter extends JdbcTypeAdapterBase<Blob> {

    public static final JdbcBlobTypeAdapter INSTANCE = new JdbcBlobTypeAdapter();

    public JdbcBlobTypeAdapter() {
        super(Blob.class);
    }

    @Override
    public <X> Blob wrap(X value, Connection connection) throws SQLException {
        if (value == null) {
            return null;
        }
        Blob blob;
        if (byte[].class.isInstance(value)) {
            blob = connection.createBlob();
            blob.setBytes(0, (byte[]) value);
        } else if (InputStream.class.isInstance(value)) {
            blob = connection.createBlob();
            try {
                ByteStreams.copy((InputStream) value, blob.setBinaryStream(0));
            } catch (IOException exception) {
                throw new JdbcTypeException(exception);
            }
        } else {
            throw newWrapFailure(value);
        }
        return blob;
    }

    @Override
    public <X> X unwrap(Blob value, Class<X> valueClass, Connection connection) throws SQLException {
        if (value == null) {
            return null;
        } else if (valueClass.isAssignableFrom(Blob.class)) {
            return (X) value;
        } else if (valueClass.isAssignableFrom(byte[].class)) {
            try {
                return (X) ByteStreams.toByteArray(value.getBinaryStream());
            } catch (IOException exception) {
                throw new JdbcTypeException(exception);
            }
        } else if (valueClass.isAssignableFrom(InputStream.class)) {
            return (X) value.getBinaryStream();
        } else {
            throw newUnwrapFailure(valueClass);
        }
    }
}
