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
package com.nuodb.tools.migration.jdbc.metamodel;

import com.google.common.collect.Lists;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Collection;
import java.util.List;

public class ResultSetModel implements ColumnSetModel {

    private List<Integer> columnTypes;
    private List<String> columns;

    public ResultSetModel(ResultSet resultSet) throws SQLException {
        this(resultSet.getMetaData());
    }

    public ResultSetModel(ResultSetMetaData metaData) throws SQLException {
        int columnCount = metaData.getColumnCount();

        List<String> columns = Lists.newArrayList();
        List<Integer> columnTypes = Lists.newArrayList();
        for (int i = 0; i < columnCount; i++) {
            int column = i + 1;
            columns.add(metaData.getColumnLabel(column));
            columnTypes.add(metaData.getColumnType(column));
        }
        this.columns = columns;
        this.columnTypes = columnTypes;
    }

    public boolean hasColumn(String column) {
        return columns.contains(column);
    }

    public int getColumnType(int column) {
        return columnTypes.get(column);
    }

    public Collection<Integer> getColumnTypes() {
        return columnTypes;
    }

    public String getColumn(int index) {
        return columns.get(index);
    }

    public Collection<String> getColumns() {
        return columns;
    }

    public int getColumnCount() {
        return columns.size();
    }
}
