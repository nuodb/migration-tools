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
import com.nuodb.migrator.jdbc.type.JdbcTypeException;
import org.apache.commons.io.IOUtils;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Reader;
import java.sql.Connection;
import java.sql.NClob;
import java.sql.SQLException;

/**
 * @author Sergey Bushik
 */
public class JdbcNClobTypeAdapter extends JdbcTypeAdapterBase<NClob> {

    public static final JdbcTypeAdapter INSTANCE = new JdbcNClobTypeAdapter();

    public JdbcNClobTypeAdapter() {
        super(NClob.class);
    }

    @Override
    public <X> NClob wrap(X value, Connection connection) throws SQLException {
        if (value == null) {
            return null;
        }
        NClob clob;
        if (String.class.isInstance(value)) {
            clob = connection.createNClob();
            clob.setString(1, (String) value);
        } else if (char[].class.isInstance(value)) {
            clob = connection.createNClob();
            clob.setString(1, new String((char[]) value));
        } else if (Reader.class.isInstance(value)) {
            clob = connection.createNClob();
            try {
                IOUtils.copy((Reader) value, clob.setCharacterStream(1));
            } catch (IOException exception) {
                throw new JdbcTypeException(exception);
            }
        } else if (InputStream.class.isInstance(value)) {
            clob = connection.createNClob();
            try {
                IOUtils.copy((InputStream) value, clob.setAsciiStream(1));
            } catch (IOException exception) {
                throw new JdbcTypeException(exception);
            }
        } else {
            throw newWrapFailure(value);
        }
        return clob;
    }

    @Override
    public <X> X unwrap(NClob value, Class<X> valueClass, Connection connection) throws SQLException {
        if (value == null) {
            return null;
        } else if (valueClass.isAssignableFrom(NClob.class)) {
            return (X) value;
        } else if (valueClass.isAssignableFrom(char[].class)) {
            try {
                return (X) IOUtils.toString(value.getCharacterStream()).toCharArray();
            } catch (IOException exception) {
                throw new JdbcTypeException(exception);
            }
        } else if (valueClass.isAssignableFrom(String.class)) {
            try {
                return (X) IOUtils.toString(value.getCharacterStream());
            } catch (IOException exception) {
                throw new JdbcTypeException(exception);
            }
        } else if (valueClass.isAssignableFrom(Reader.class)) {
            return (X) value.getCharacterStream();
        } else if (valueClass.isAssignableFrom(OutputStream.class)) {
            return (X) value.getAsciiStream();
        } else {
            throw newUnwrapFailure(valueClass);
        }
    }
}
