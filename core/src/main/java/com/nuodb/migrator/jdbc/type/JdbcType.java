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
package com.nuodb.migrator.jdbc.type;

import com.nuodb.migrator.utils.ObjectUtils;

import static com.nuodb.migrator.jdbc.type.JdbcTypeOptions.newOptions;

/**
 * @author Sergey Bushik
 */
public class JdbcType implements Cloneable {

    private JdbcTypeDesc jdbcTypeDesc;
    private JdbcTypeOptions jdbcTypeOptions;

    public JdbcType() {
        this(new JdbcTypeDesc());
    }

    public JdbcType(JdbcTypeDesc jdbcTypeDesc) {
        this(jdbcTypeDesc, new JdbcTypeOptions());
    }

    public JdbcType(JdbcTypeOptions jdbcTypeOptions) {
        this(new JdbcTypeDesc(), jdbcTypeOptions);
    }

    public JdbcType(JdbcTypeDesc jdbcTypeDesc, JdbcTypeOptions jdbcTypeOptions) {
        this.jdbcTypeDesc = jdbcTypeDesc;
        this.jdbcTypeOptions = jdbcTypeOptions;
    }

    public JdbcType(JdbcType jdbcType) {
        JdbcTypeDesc jdbcTypeDesc = jdbcType.getJdbcTypeDesc();
        this.jdbcTypeDesc = new JdbcTypeDesc(jdbcTypeDesc.getTypeCode(), jdbcTypeDesc.getTypeName());
        JdbcTypeOptions jdbcTypeOptions = jdbcType.getJdbcTypeOptions();
        this.jdbcTypeOptions = jdbcTypeOptions != null
                ? newOptions(jdbcTypeOptions.getSize(), jdbcTypeOptions.getPrecision(), jdbcTypeOptions.getScale())
                : new JdbcTypeOptions();
    }

    public int getTypeCode() {
        return jdbcTypeDesc.getTypeCode();
    }

    public void setTypeCode(int typeCode) {
        jdbcTypeDesc.setTypeCode(typeCode);
    }

    public JdbcType withTypeCode(int typeCode) {
        JdbcType jdbcType = clone();
        jdbcType.setTypeCode(typeCode);
        return jdbcType;
    }

    public String getTypeName() {
        return jdbcTypeDesc.getTypeName();
    }

    public void setTypeName(String typeName) {
        jdbcTypeDesc.setTypeName(typeName);
    }

    public JdbcType withTypeName(String typeName) {
        JdbcType jdbcType = clone();
        jdbcType.setTypeName(typeName);
        return jdbcType;
    }

    public Long getSize() {
        return jdbcTypeOptions.getSize();
    }

    public void setSize(Long size) {
        jdbcTypeOptions.setSize(size);
    }

    public JdbcType withSize(Long size) {
        JdbcType jdbcType = clone();
        jdbcType.setSize(size);
        return jdbcType;
    }

    public Integer getPrecision() {
        return jdbcTypeOptions.getPrecision();
    }

    public void setPrecision(Integer precision) {
        jdbcTypeOptions.setPrecision(precision);
    }

    public JdbcType withPrecision(Integer precision) {
        JdbcType jdbcType = clone();
        jdbcType.setPrecision(precision);
        return jdbcType;
    }

    public Integer getScale() {
        return jdbcTypeOptions.getScale();
    }

    public void setScale(Integer scale) {
        jdbcTypeOptions.setScale(scale);
    }

    public JdbcType withScale(Integer scale) {
        JdbcType jdbcType = clone();
        jdbcType.setScale(scale);
        return jdbcType;
    }

    public JdbcTypeDesc getJdbcTypeDesc() {
        return jdbcTypeDesc;
    }

    public void setJdbcTypeDesc(JdbcTypeDesc jdbcTypeDesc) {
        this.jdbcTypeDesc = jdbcTypeDesc;
    }

    public JdbcType withJdbcTypeDesc(JdbcTypeDesc jdbcTypeDesc) {
        JdbcType jdbcType = clone();
        jdbcType.setJdbcTypeDesc(jdbcTypeDesc);
        return jdbcType;
    }

    public JdbcTypeOptions getJdbcTypeOptions() {
        return jdbcTypeOptions;
    }

    public void setJdbcTypeOptions(JdbcTypeOptions jdbcTypeOptions) {
        this.jdbcTypeOptions = jdbcTypeOptions;
    }

    public JdbcType withJdbcTypeOptions(JdbcTypeOptions jdbcTypeOptions) {
        JdbcType jdbcType = clone();
        jdbcType.setJdbcTypeOptions(jdbcTypeOptions);
        return jdbcType;
    }

    @Override
    protected JdbcType clone() {
        try {
            JdbcType jdbcType = (JdbcType) super.clone();
            jdbcType.setJdbcTypeDesc(jdbcTypeDesc != null ? new JdbcTypeDesc(jdbcTypeDesc) : null);
            jdbcType.setJdbcTypeOptions(jdbcTypeOptions != null ? new JdbcTypeOptions(jdbcTypeOptions) : null);
            return jdbcType;
        } catch (CloneNotSupportedException exception) {
            throw new JdbcTypeException(exception);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        JdbcType that = (JdbcType) o;

        if (jdbcTypeDesc != null ? !jdbcTypeDesc.equals(that.jdbcTypeDesc) : that.jdbcTypeDesc != null)
            return false;
        if (jdbcTypeOptions != null ? !jdbcTypeOptions.equals(that.jdbcTypeOptions) : that.jdbcTypeOptions != null) {
            return false;
        }
        return true;
    }

    @Override
    public int hashCode() {
        int result = jdbcTypeDesc != null ? jdbcTypeDesc.hashCode() : 0;
        result = 31 * result + (jdbcTypeOptions != null ? jdbcTypeOptions.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return ObjectUtils.toString(this);
    }
}
