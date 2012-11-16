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
public class ColumnModelSetImpl implements ColumnModelSet {

    private final ColumnModel[] columns;

    public ColumnModelSetImpl(ColumnModel[] columns) {
        this.columns = columns;
    }

    public ColumnModelSetImpl(ColumnModelSet columnModelSet) {
        int columnCount = columnModelSet.getLength();
        ColumnModel[] columnModels = new ColumnModel[columnCount];
        for (int index = 0; index < columnCount; index++) {
            columnModels[index] = columnModelSet.item(index);
        }
        this.columns = columnModels;
    }

    @Override
    public String getName(int index) {
        return columns[index].getName();
    }

    @Override
    public String[] getNames() {
        int columnCount = getLength();
        String[] names = new String[columnCount];
        for (int index = 0; index < columnCount; index++) {
            names[index] = getName(index);
        }
        return names;
    }

    @Override
    public int getTypeCode(int index) {
        return columns[index].getTypeCode();
    }

    @Override
    public void setTypeCode(int index, int typeCode) {
        columns[index].setTypeCode(typeCode);
    }

    @Override
    public int[] getTypeCodes() {
        int columnCount = getLength();
        int[] typeCodes = new int[columnCount];
        for (int index = 0; index < columnCount; index++) {
            typeCodes[index] = getTypeCode(index);
        }
        return typeCodes;
    }

    @Override
    public int getPrecision(int index) {
        return columns[index].getPrecision();
    }

    @Override
    public void setPrecision(int index, int precision) {
        columns[index].setPrecision(precision);
    }

    @Override
    public int[] getPrecisions() {
        int columnCount = getLength();
        int[] precisions = new int[columnCount];
        for (int index = 0; index < columnCount; index++) {
            precisions[index] = getPrecision(index);
        }
        return precisions;
    }

    @Override
    public int getScale(int index) {
        return columns[index].getScale();
    }

    @Override
    public void setScale(int index, int scale) {
        columns[index].setScale(scale);
    }

    @Override
    public int[] getScales() {
        int columnCount = getLength();
        int[] scales = new int[columnCount];
        for (int index = 0; index < columnCount; index++) {
            scales[index] = getScale(index);
        }
        return scales;
    }

    @Override
    public int getLength() {
        return columns.length;
    }

    @Override
    public ColumnModel item(int index) {
        return columns[index];
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
        ColumnModelSetImpl that = (ColumnModelSetImpl) o;
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
