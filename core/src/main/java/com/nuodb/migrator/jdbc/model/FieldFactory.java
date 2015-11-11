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
package com.nuodb.migrator.jdbc.model;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

/**
 * @author Sergey Bushik
 */
public class FieldFactory {

    public static Field newField() {
        return new SimpleField();
    }

    public static Field newField(String name) {
        final SimpleField field = new SimpleField();
        field.setName(name);
        return field;
    }

    public static Field newField(ResultSet resultSet, int column) throws SQLException {
        return newField(resultSet.getMetaData(), column);
    }

    public static Field newField(ResultSetMetaData metaData, int column) throws SQLException {
        final SimpleField field = new SimpleField();
        field.setName(metaData.getColumnLabel(column));
        field.setTypeCode(metaData.getColumnType(column));
        field.setTypeName(metaData.getColumnTypeName(column));
        field.setPrecision(metaData.getPrecision(column));
        field.setScale(metaData.getScale(column));
        return field;
    }

    public static FieldList<Field> newFieldList(ResultSet resultSet) throws SQLException {
        return newFieldList(resultSet.getMetaData());
    }

    public static FieldList<Field> newFieldList(ResultSetMetaData metaData) throws SQLException {
        final int size = metaData.getColumnCount();
        final Field[] fields = new Field[size];
        for (int i = 0; i < size; i++) {
            fields[i] = newField(metaData, i + 1);
        }
        return newFieldList(fields);
    }

    public static <T extends Field> FieldList<T> newFieldList(T... columns) {
        return new SimpleFieldList<T>(columns);
    }

    public static <T extends Field> FieldList<T> newFieldList(Iterable<T> fields) {
        return new SimpleFieldList<T>(fields);
    }
}
