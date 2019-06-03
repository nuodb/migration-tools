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

import com.nuodb.migrator.jdbc.type.JdbcType;
import com.nuodb.migrator.jdbc.type.JdbcTypeDesc;
import com.nuodb.migrator.jdbc.type.JdbcTypeOptions;
import com.nuodb.migrator.utils.ObjectUtils;

/**
 * @author Sergey Bushik
 */
public class SimpleField implements Field {

    private String name;
    private int position;
    private JdbcType jdbcType;
    private Field field;

    public SimpleField() {
        this.jdbcType = new JdbcType();
    }

    public SimpleField(Field field) {
        this.name = field.getName();
        this.jdbcType = field.getJdbcType();
        this.position = field.getPosition();
        this.field = field;
    }

    @Override
    public Field toField() {
        return field != null ? field : this;
    }

    @Override
    public void fromField(Field field) {
        setName(field.getName());
        setPosition(field.getPosition());
        setJdbcType(field.getJdbcType());
        this.field = field;
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
        return jdbcType.getTypeCode();
    }

    @Override
    public void setTypeCode(int typeCode) {
        jdbcType.setTypeCode(typeCode);
    }

    @Override
    public String getTypeName() {
        return jdbcType.getTypeName();
    }

    @Override
    public void setTypeName(String typeName) {
        jdbcType.setTypeName(typeName);
    }

    @Override
    public Long getSize() {
        return jdbcType.getSize();
    }

    public void setSize(Long size) {
        jdbcType.setSize(size);
    }

    @Override
    public Integer getPrecision() {
        return jdbcType.getPrecision();
    }

    public void setPrecision(Integer precision) {
        jdbcType.setPrecision(precision);
    }

    @Override
    public Integer getScale() {
        return jdbcType.getScale();
    }

    public void setScale(Integer scale) {
        jdbcType.setScale(scale);
    }

    @Override
    public JdbcType getJdbcType() {
        return jdbcType;
    }

    @Override
    public void setJdbcType(JdbcType jdbcType) {
        this.jdbcType = jdbcType;
    }

    @Override
    public JdbcTypeDesc getJdbcTypeDesc() {
        return jdbcType.getJdbcTypeDesc();
    }

    @Override
    public void setJdbcTypeDesc(JdbcTypeDesc jdbcTypeDesc) {
        jdbcType.setJdbcTypeDesc(jdbcTypeDesc);
    }

    @Override
    public JdbcTypeOptions getJdbcTypeOptions() {
        return jdbcType.getJdbcTypeOptions();
    }

    @Override
    public void setJdbcTypeOptions(JdbcTypeOptions jdbcTypeOptions) {
        jdbcType.setJdbcTypeOptions(jdbcTypeOptions);
    }

    @Override
    public int getPosition() {
        return position;
    }

    @Override
    public void setPosition(int position) {
        this.position = position;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        SimpleField that = (SimpleField) o;

        if (name != null ? !name.equals(that.name) : that.name != null)
            return false;
        if (jdbcType != null ? !jdbcType.equals(that.jdbcType) : that.jdbcType != null)
            return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = name != null ? name.hashCode() : 0;
        result = 31 * result + (jdbcType != null ? jdbcType.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return ObjectUtils.toString(this);
    }
}
