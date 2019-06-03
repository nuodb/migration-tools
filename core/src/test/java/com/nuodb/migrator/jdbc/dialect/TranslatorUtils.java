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
package com.nuodb.migrator.jdbc.dialect;

import com.nuodb.migrator.jdbc.metadata.Column;
import com.nuodb.migrator.jdbc.metadata.Table;
import com.nuodb.migrator.jdbc.session.Session;

import java.sql.SQLException;

import static com.nuodb.migrator.jdbc.metadata.DefaultValue.valueOf;
import static com.nuodb.migrator.jdbc.metadata.Identifier.EMPTY;

/**
 * Utilities for use by test classes
 *
 * @author Sergey Bushik
 */
public class TranslatorUtils {

    /**
     * Creates simple script from provided string script object & database
     * dialect
     *
     * @param script
     *            source for the translation
     * @return simple script object initialized with source script, database
     *         dialect & connection url
     */
    public static Script createScript(String script) throws SQLException {
        return new SimpleScript(script);
    }

    /**
     * Creates column script initialized with a given default value, type code,
     * type name & a session
     *
     * @param defaultValue
     *            column default value
     * @param typeCode
     *            database type code
     * @param typeName
     *            database type name
     * @return fully initialized column script
     */
    public static Script createScript(String defaultValue, int typeCode, String typeName) {
        Column column = new Column(EMPTY);
        column.setTypeCode(typeCode);
        column.setTypeName(typeName);
        column.setDefaultValue(valueOf(defaultValue));
        Table table = new Table(EMPTY);
        table.addColumn(column);
        return new ColumnScript(column);
    }
}
