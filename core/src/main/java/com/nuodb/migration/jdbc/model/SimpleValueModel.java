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

/**
 * @author Sergey Bushik
 */
public class SimpleValueModel implements ValueModel {

    private String name;
    private int typeCode;
    private String typeName;
    private int precision;
    private int scale;
    private ValueModel valueModel;

    public SimpleValueModel() {
    }

    public SimpleValueModel(String name) {
        this.name = name;
    }

    public SimpleValueModel(String name, int typeCode) {
        this.name = name;
        this.typeCode = typeCode;
    }

    public SimpleValueModel(String name, int typeCode, String typeName) {
        this.name = name;
        this.typeCode = typeCode;
        this.typeName = typeName;
    }

    public SimpleValueModel(String name, int typeCode, String typeName, int precision) {
        this.name = name;
        this.typeCode = typeCode;
        this.typeName = typeName;
        this.precision = precision;
    }

    public SimpleValueModel(String name, int typeCode, String typeName, int precision, int scale) {
        this.name = name;
        this.typeCode = typeCode;
        this.typeName = typeName;
        this.precision = precision;
        this.scale = scale;
    }

    public SimpleValueModel(ValueModel valueModel) {
        this.name = valueModel.getName();
        this.typeCode = valueModel.getTypeCode();
        this.typeName = valueModel.getTypeName();
        this.precision = valueModel.getPrecision();
        this.scale = valueModel.getScale();
        this.valueModel = valueModel;
    }

    @Override
    public ValueModel asValueModel() {
        return valueModel != null ? valueModel : this;
    }

    @Override
    public void fromValueModel(ValueModel valueModel) {
        setName(valueModel.getName());
        setTypeName(valueModel.getTypeName());
        setTypeCode(valueModel.getTypeCode());
        setPrecision(valueModel.getPrecision());
        setScale(valueModel.getScale());
        this.valueModel = valueModel;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public void setName(String name) {
        this.name = name;
    }

    @Override
    public int getTypeCode() {
        return typeCode;
    }

    @Override
    public void setTypeCode(int typeCode) {
        this.typeCode = typeCode;
    }

    public String getTypeName() {
        return typeName;
    }

    public void setTypeName(String typeName) {
        this.typeName = typeName;
    }

    @Override
    public int getPrecision() {
        return precision;
    }

    @Override
    public void setPrecision(int precision) {
        this.precision = precision;
    }

    @Override
    public int getScale() {
        return scale;
    }

    @Override
    public void setScale(int scale) {
        this.scale = scale;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        SimpleValueModel that = (SimpleValueModel) o;

        if (precision != that.precision) return false;
        if (scale != that.scale) return false;
        if (typeCode != that.typeCode) return false;
        if (name != null ? !name.equalsIgnoreCase(that.name) : that.name != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = name != null ? name.hashCode() : 0;
        result = 31 * result + typeCode;
        result = 31 * result + precision;
        result = 31 * result + scale;
        return result;
    }

    @Override
    public String toString() {
        return "ColumnModel{" +
                "name='" + name + '\'' +
                ", typeCode=" + typeCode +
                ", typeName=" + typeName +
                ", precision=" + precision +
                ", scale=" + scale +
                '}';
    }
}
