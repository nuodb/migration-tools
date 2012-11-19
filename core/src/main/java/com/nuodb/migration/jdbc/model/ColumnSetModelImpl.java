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

import org.apache.commons.lang.StringUtils;

import java.util.Arrays;

/**
 * @author Sergey Bushik
 */
public class ColumnSetModelImpl implements ColumnSetModel {

    private final ColumnModel[] columns;

    public ColumnSetModelImpl(ColumnModel[] columns) {
        this.columns = columns;
    }

    public ColumnSetModelImpl(ColumnSetModel columnSetModel) {
        int columnCount = columnSetModel.getLength();
        ColumnModel[] columnModels = new ColumnModel[columnCount];
        for (int index = 0; index < columnCount; index++) {
            columnModels[index] = columnSetModel.item(index);
        }
        this.columns = columnModels;
    }

    @Override
    public String getName(int column) {
        return columns[column].getName();
    }

    @Override
    public int getTypeCode(int column) {
        return columns[column].getTypeCode();
    }

    @Override
    public void setTypeCode(int column, int typeCode) {
        columns[column].setTypeCode(typeCode);
    }

    @Override
    public String getTypeName(int column) {
        return columns[column].getTypeName();
    }

    @Override
    public void setTypeName(int column, String typeName) {
        columns[column].setTypeName(typeName);
    }

    @Override
    public int getPrecision(int column) {
        return columns[column].getPrecision();
    }

    @Override
    public void setPrecision(int column, int precision) {
        columns[column].setPrecision(precision);
    }

    @Override
    public int getScale(int column) {
        return columns[column].getScale();
    }

    @Override
    public void setScale(int column, int scale) {
        columns[column].setScale(scale);
    }

    @Override
    public int getLength() {
        return columns.length;
    }

    @Override
    public ColumnModel item(int column) {
        return columns[column];
    }

    @Override
    public ColumnModel item(String name) {
        for (ColumnModel column : columns) {
            if (StringUtils.equals(column.getName(), name)) {
                return column;
            }
        }
        return null;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ColumnSetModelImpl that = (ColumnSetModelImpl) o;
        if (!Arrays.equals(columns, that.columns)) return false;
        return true;
    }

    @Override
    public int hashCode() {
        return columns != null ? Arrays.hashCode(columns) : 0;
    }

    @Override
    public String toString() {
        return Arrays.asList(columns).toString();
    }
}
