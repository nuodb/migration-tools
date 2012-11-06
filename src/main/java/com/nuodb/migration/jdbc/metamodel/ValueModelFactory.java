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
package com.nuodb.migration.jdbc.metamodel;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

/**
 * @author Sergey Bushik
 */
public class ValueModelFactory {

    public static ValueModel createValueModel(ResultSet resultSet, int column) throws SQLException {
        return createValueModel(resultSet.getMetaData(), column);
    }

    public static ValueModel createValueModel(ResultSetMetaData metaData, int column) throws SQLException {
        return new ValueModelImpl(
                metaData.getColumnLabel(column), metaData.getColumnType(column),
                metaData.getPrecision(column), metaData.getScale(column));
    }

    public static ValueSetModel createValueSetModel(ResultSet resultSet) throws SQLException {
        return createValueSetModel(resultSet.getMetaData());
    }

    public static ValueSetModel createValueSetModel(ResultSetMetaData metaData) throws SQLException {
        final int columnCount = metaData.getColumnCount();
        final ValueModel[] values = new ValueModel[columnCount];
        for (int i = 0; i < columnCount; i++) {
            values[i] = createValueModel(metaData, i + 1);
        }
        return new ValueSetModelImpl(values);
    }

    public static ValueSetModel createValueSetModel(String[] names, int[] typeCodes) {
        final int length = names.length;
        return createValueSetModel(names, typeCodes, new int[length], new int[length]);
    }

    public static ValueSetModel createValueSetModel(String[] names, int[] typeCodes, int[] precisions, int[] scales) {
        final int columnCount = names.length;
        final ValueModel[] values = new ValueModel[columnCount];
        for (int i = 0; i < columnCount; i++) {
            values[i] = new ValueModelImpl(names[i], typeCodes[i], precisions[i], scales[i]);
        }
        return new ValueSetModelImpl(values);
    }
}
