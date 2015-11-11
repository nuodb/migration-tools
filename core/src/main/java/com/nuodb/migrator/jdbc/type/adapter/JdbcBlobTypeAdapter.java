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

import com.google.common.io.ByteStreams;
import com.nuodb.migrator.jdbc.type.JdbcTypeAdapter;
import com.nuodb.migrator.jdbc.type.JdbcTypeAdapterBase;
import com.nuodb.migrator.jdbc.type.JdbcTypeException;

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

    public static final JdbcTypeAdapter INSTANCE = new JdbcBlobTypeAdapter();
    private JdbcLobTypeSupport jdbcLobTypeSupport;

    public JdbcBlobTypeAdapter() {
        this(new SimpleJdbcLobTypeSupport());
    }

    public JdbcBlobTypeAdapter(JdbcLobTypeSupport jdbcLobTypeSupport) {
        super(Blob.class);
        this.jdbcLobTypeSupport = jdbcLobTypeSupport;
    }

    @Override
    public <X> Blob wrap(X value, Connection connection) throws SQLException {
        if (value == null) {
            return null;
        }
        Blob blob;
        if (byte[].class.isInstance(value)) {
            blob = createBlob(connection);
            blob.setBytes(1, (byte[]) value);
            closeBlob(connection, blob);
        } else if (InputStream.class.isInstance(value)) {
            blob = createBlob(connection);
            try {
                ByteStreams.copy((InputStream) value, blob.setBinaryStream(1));
            } catch (IOException exception) {
                throw new JdbcTypeException(exception);
            }
            closeBlob(connection, blob);
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
                initBlobBeforeAccess(connection, value);
                X x = (X) ByteStreams.toByteArray(value.getBinaryStream());
                releaseBlobAfterAccess(connection, value);
                return x;
            } catch (IOException exception) {
                throw new JdbcTypeException(exception);
            }
        } else if (valueClass.isAssignableFrom(InputStream.class)) {
            initBlobBeforeAccess(connection, value);
            X x = (X) value.getBinaryStream();
            releaseBlobAfterAccess(connection, value);
            return x;
        } else {
            throw newUnwrapFailure(valueClass);
        }
    }

    protected Blob createBlob(Connection connection) throws SQLException {
        return jdbcLobTypeSupport.createBlob(connection);
    }

    private void closeBlob(Connection connection, Blob blob) throws SQLException {
        jdbcLobTypeSupport.closeBlob(connection, blob);
    }

    protected void initBlobBeforeAccess(Connection connection, Blob blob) throws SQLException {
        jdbcLobTypeSupport.initBlobBeforeAccess(connection, blob);
    }

    protected void releaseBlobAfterAccess(Connection connection, Blob blob) throws SQLException {
        jdbcLobTypeSupport.releaseBlobAfterAccess(connection, blob);
    }
}
