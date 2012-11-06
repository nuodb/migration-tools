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

import com.nuodb.util.StringUtils;

import java.util.Arrays;

/**
 * @author Sergey Bushik
 */
public class ValueSetModelImpl implements ValueSetModel {

    private final ValueModel[] values;

    public ValueSetModelImpl(ValueModel[] values) {
        this.values = values;
    }

    public ValueSetModelImpl(ValueSetModel valueSetModel) {
        int columnCount = valueSetModel.getLength();
        ValueModel[] valueModels = new ValueModel[columnCount];
        for (int index = 0; index < columnCount; index++) {
            valueModels[index] = valueSetModel.item(index);
        }
        this.values = valueModels;
    }

    @Override
    public String getName(int index) {
        return values[index].getName();
    }

    @Override
    public void setName(int index, String name) {
        values[index].setName(name);
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
        return values[index].getTypeCode();
    }

    @Override
    public void setTypeCode(int index, int typeCode) {
        values[index].setTypeCode(typeCode);
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
        return values[index].getPrecision();
    }

    @Override
    public void setPrecision(int index, int precision) {
        values[index].setPrecision(precision);
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
        return values[index].getScale();
    }

    @Override
    public void setScale(int index, int scale) {
        values[index].setScale(scale);
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
        return values.length;
    }

    @Override
    public ValueModel item(int index) {
        return values[index];
    }

    @Override
    public ValueModel item(String name) {
        for (ValueModel value : values) {
            if (StringUtils.equals(value.getName(), name)) {
                return value;
            }
        }
        return null;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ValueSetModelImpl that = (ValueSetModelImpl) o;
        if (!Arrays.equals(values, that.values)) return false;
        return true;
    }

    @Override
    public int hashCode() {
        return values != null ? Arrays.hashCode(values) : 0;
    }

    @Override
    public String toString() {
        return Arrays.asList(values).toString();
    }
}
