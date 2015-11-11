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
import java.io.Reader;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLXML;

/**
 * @author Sergey Bushik
 */
@SuppressWarnings("unchecked")
public class JdbcSqlXmlTypeAdapter extends JdbcTypeAdapterBase<SQLXML> {

    public static final JdbcTypeAdapter INSTANCE = new JdbcSqlXmlTypeAdapter();

    public JdbcSqlXmlTypeAdapter() {
        super(SQLXML.class);
    }

    @Override
    public <X> SQLXML wrap(X value, Connection connection) throws SQLException {
        if (value == null) {
            return null;
        }
        SQLXML sqlXml;
        if (String.class.isInstance(value)) {
            sqlXml = connection.createSQLXML();
            sqlXml.setString((String) value);
        } else if (InputStream.class.isInstance(value)) {
            sqlXml = connection.createSQLXML();
            try {
                IOUtils.copy((InputStream) value, sqlXml.setBinaryStream());
            } catch (IOException exception) {
                throw new JdbcTypeException(exception);
            }
        } else if (Reader.class.isInstance(value)) {
            sqlXml = connection.createSQLXML();
            try {
                IOUtils.copy((Reader) value, sqlXml.setCharacterStream());
            } catch (IOException exception) {
                throw new JdbcTypeException(exception);
            }
        } else {
            throw newWrapFailure(value);
        }
        return sqlXml;
    }

    @Override
    public <X> X unwrap(SQLXML value, Class<X> valueClass, Connection connection) throws SQLException {
        if (value == null) {
            return null;
        } else if (valueClass.isAssignableFrom(SQLXML.class)) {
            return (X) value;
        } else if (valueClass.isAssignableFrom(String.class)) {
            try {
                return (X) IOUtils.toString(value.getCharacterStream());
            } catch (IOException exception) {
                throw new JdbcTypeException(exception);
            }
        } else if (valueClass.isAssignableFrom(InputStream.class)) {
            try {
                return (X) IOUtils.toString(value.getBinaryStream());
            } catch (IOException exception) {
                throw new JdbcTypeException(exception);
            }
        } else if (valueClass.isAssignableFrom(Reader.class)) {
            return (X) value.getCharacterStream();
        } else {
            throw newUnwrapFailure(valueClass);
        }
    }
}
