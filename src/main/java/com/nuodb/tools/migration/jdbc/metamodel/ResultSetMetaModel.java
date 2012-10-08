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

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

public class ResultSetMetaModel {

    private int columnCount;
    private int[] columnTypes;
    private String[] columns;
    private List<String> columnsList;

    public ResultSetMetaModel(ResultSet resultSet) throws SQLException {
        this(resultSet.getMetaData());
    }

    public ResultSetMetaModel(ResultSetMetaData metaData) throws SQLException {
        int columnCount = metaData.getColumnCount();

        String[] columns = new String[columnCount];
        int[] columnTypes = new int[columnCount];
        for (int i = 0; i < columnCount; i++) {
            int column = i + 1;
            columns[i] = metaData.getColumnLabel(column);
            columnTypes[i] = metaData.getColumnType(column);
        }
        this.columns = columns;
        this.columnsList = Arrays.asList(columns);
        this.columnTypes = columnTypes;
        this.columnCount = columnCount;
    }

    public int getColumnCount() {
        return columnCount;
    }

    public String[] getColumns() {
        return columns;
    }

    public boolean hasColumn(String column) {
        return columnsList.contains(column);
    }

    public int getColumnType(int column) {
        return columnTypes[column];
    }

    public int[] getColumnTypes() {
        return columnTypes;
    }

    public String getColumn(int column) {
        return columns[column];
    }
}
