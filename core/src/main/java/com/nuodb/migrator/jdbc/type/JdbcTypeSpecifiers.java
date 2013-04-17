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
package com.nuodb.migrator.jdbc.type;

import com.nuodb.migrator.utils.ObjectUtils;

/**
 * @author Sergey Bushik
 */
public class JdbcTypeSpecifiers {

    private Integer size;
    private Integer precision;
    private Integer scale;

    public Integer getSize() {
        return size;
    }

    public void setSize(Integer size) {
        this.size = size;
    }

    public Integer getPrecision() {
        return precision;
    }

    public void setPrecision(Integer precision) {
        this.precision = precision;
    }

    public Integer getScale() {
        return scale;
    }

    public void setScale(Integer scale) {
        this.scale = scale;
    }

    public static JdbcTypeSpecifiers newSize(Integer size) {
        JdbcTypeSpecifiers typeSpecifiers = new JdbcTypeSpecifiers();
        typeSpecifiers.setSize(size);
        return typeSpecifiers;
    }

    public static JdbcTypeSpecifiers newPrecision(Integer precision) {
        JdbcTypeSpecifiers typeSpecifiers = new JdbcTypeSpecifiers();
        typeSpecifiers.setPrecision(precision);
        return typeSpecifiers;
    }

    public static JdbcTypeSpecifiers newScale(Integer scale) {
        JdbcTypeSpecifiers typeSpecifiers = new JdbcTypeSpecifiers();
        typeSpecifiers.setScale(scale);
        return typeSpecifiers;
    }

    public static JdbcTypeSpecifiers newSpecifiers(Integer size, Integer precision, Integer scale) {
        JdbcTypeSpecifiers typeSpecifiers = new JdbcTypeSpecifiers();
        typeSpecifiers.setSize(size);
        typeSpecifiers.setPrecision(precision);
        typeSpecifiers.setScale(scale);
        return typeSpecifiers;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof JdbcTypeSpecifiers)) return false;

        JdbcTypeSpecifiers that = (JdbcTypeSpecifiers) o;

        if (precision != null ? !precision.equals(that.precision) : that.precision != null) return false;
        if (scale != null ? !scale.equals(that.scale) : that.scale != null) return false;
        if (size != null ? !size.equals(that.size) : that.size != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = size != null ? size.hashCode() : 0;
        result = 31 * result + (precision != null ? precision.hashCode() : 0);
        result = 31 * result + (scale != null ? scale.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return ObjectUtils.toString(this);
    }
}