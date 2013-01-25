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
package com.nuodb.migration.jdbc.model;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

/**
 * @author Sergey Bushik
 */
public class ValueModelFactory {

    public static ValueModel createValueModel() {
        return new SimpleValueModel();
    }

    public static ValueModel createValueModel(String name) {
        return new SimpleValueModel(name);
    }

    public static ValueModel createValueModel(ResultSet resultSet, int column) throws SQLException {
        return createValueModel(resultSet.getMetaData(), column);
    }

    public static ValueModel createValueModel(ResultSetMetaData metaData, int column) throws SQLException {
        return new SimpleValueModel(
                metaData.getColumnLabel(column), metaData.getColumnType(column), metaData.getColumnTypeName(column),
                metaData.getPrecision(column), metaData.getScale(column));
    }

    public static ValueModelList<ValueModel> createValueModelList(
            ResultSet resultSet) throws SQLException {
        return createValueModelList(resultSet.getMetaData());
    }

    public static ValueModelList<ValueModel> createValueModelList(
            ResultSetMetaData metaData) throws SQLException {
        final int size = metaData.getColumnCount();
        final ValueModel[] values = new ValueModel[size];
        for (int i = 0; i < size; i++) {
            values[i] = createValueModel(metaData, i + 1);
        }
        return new SimpleValueModelList<ValueModel>(values);
    }

    public static <T extends ValueModel> ValueModelList<T> createValueModelList() {
        return new SimpleValueModelList<T>();
    }

    public static <T extends ValueModel> ValueModelList<T> createValueModelList(T... values) {
        return new SimpleValueModelList<T>(values);
    }

    public static <T extends ValueModel> ValueModelList<T> createValueModelList(Iterable<T> values) {
        return new SimpleValueModelList<T>(values);
    }
}
