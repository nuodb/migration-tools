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
import com.google.common.io.CharStreams;
import com.nuodb.migrator.jdbc.type.JdbcTypeAdapter;
import com.nuodb.migrator.jdbc.type.JdbcTypeAdapterBase;
import com.nuodb.migrator.jdbc.type.JdbcTypeException;

import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.SQLException;

/**
 * @author Sergey Bushik
 */
@SuppressWarnings("unchecked")
public class JdbcClobTypeAdapter extends JdbcTypeAdapterBase<Clob> {

    public static final JdbcTypeAdapter INSTANCE = new JdbcClobTypeAdapter();

    private JdbcLobTypeSupport jdbcLobTypeSupport;

    public JdbcClobTypeAdapter() {
        this(new SimpleJdbcLobTypeSupport());
    }

    public JdbcClobTypeAdapter(JdbcLobTypeSupport jdbcLobTypeSupport) {
        super(Clob.class);
        this.jdbcLobTypeSupport = jdbcLobTypeSupport;
    }

    @Override
    public <X> Clob wrap(X value, Connection connection) throws SQLException {
        if (value == null) {
            return null;
        }
        Clob clob;
        if (String.class.isInstance(value)) {
            clob = createClob(connection);
            clob.setString(1, (String) value);
            closeClob(connection, clob);
        } else if (char[].class.isInstance(value)) {
            clob = createClob(connection);
            clob.setString(1, new String((char[]) value));
            closeClob(connection, clob);
        } else if (Reader.class.isInstance(value)) {
            clob = createClob(connection);
            try {
                CharStreams.copy((Reader) value, clob.setCharacterStream(1));
            } catch (IOException exception) {
                throw new JdbcTypeException(exception);
            }
            closeClob(connection, clob);
        } else if (InputStream.class.isInstance(value)) {
            clob = createClob(connection);
            try {
                ByteStreams.copy((InputStream) value, clob.setAsciiStream(1));
            } catch (IOException exception) {
                throw new JdbcTypeException(exception);
            }
            closeClob(connection, clob);
        } else {
            throw newWrapFailure(value);
        }
        return clob;
    }

    @Override
    public <X> X unwrap(Clob value, Class<X> valueClass, Connection connection) throws SQLException {
        if (value == null) {
            return null;
        }
        if (valueClass.isAssignableFrom(Clob.class)) {
            return (X) value;
        }
        if (valueClass.isAssignableFrom(char[].class)) {
            try {
                initClobBeforeAccess(connection, value);
                X x = (X) CharStreams.toString(value.getCharacterStream()).toCharArray();
                releaseClobAfterAccess(connection, value);
                return x;
            } catch (IOException exception) {
                throw new JdbcTypeException(exception);
            }
        } else if (valueClass.isAssignableFrom(String.class)) {
            try {
                initClobBeforeAccess(connection, value);
                X x = (X) CharStreams.toString(value.getCharacterStream());
                releaseClobAfterAccess(connection, value);
                return x;
            } catch (IOException exception) {
                throw new JdbcTypeException(exception);
            }
        } else if (valueClass.isAssignableFrom(Reader.class)) {
            initClobBeforeAccess(connection, value);
            X x = (X) value.getCharacterStream();
            releaseClobAfterAccess(connection, value);
            return x;
        } else if (valueClass.isAssignableFrom(InputStream.class)) {
            initClobBeforeAccess(connection, value);
            X x = (X) value.getAsciiStream();
            releaseClobAfterAccess(connection, value);
            return x;
        } else {
            throw newUnwrapFailure(valueClass);
        }
    }

    protected Clob createClob(Connection connection) throws SQLException {
        return jdbcLobTypeSupport.createClob(connection);
    }

    protected void closeClob(Connection connection, Clob clob) throws SQLException {
        jdbcLobTypeSupport.closeClob(connection, clob);
    }

    protected void initClobBeforeAccess(Connection connection, Clob clob) throws SQLException {
        jdbcLobTypeSupport.initClobBeforeAccess(connection, clob);
    }

    protected void releaseClobAfterAccess(Connection connection, Clob clob) throws SQLException {
        jdbcLobTypeSupport.releaseClobAfterAccess(connection, clob);
    }
}
