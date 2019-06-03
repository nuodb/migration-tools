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

import static org.apache.commons.lang3.StringUtils.equalsIgnoreCase;

/**
 * @author Sergey Bushik
 */
public class JdbcTypeDesc {

    private int typeCode;
    private String typeName;

    public JdbcTypeDesc() {
    }

    public JdbcTypeDesc(int typeCode) {
        this.typeCode = typeCode;
    }

    public JdbcTypeDesc(int typeCode, String typeName) {
        this.typeCode = typeCode;
        this.typeName = typeName;
    }

    public JdbcTypeDesc(JdbcTypeDesc typeDesc) {
        this.typeCode = typeDesc.getTypeCode();
        this.typeName = typeDesc.getTypeName();
    }

    public int getTypeCode() {
        return typeCode;
    }

    public void setTypeCode(int typeCode) {
        this.typeCode = typeCode;
    }

    public String getTypeName() {
        return typeName;
    }

    public void setTypeName(String typeName) {
        this.typeName = typeName;
    }

    public static boolean equals(String typeName1, String typeName2) {
        return equalsIgnoreCase(typeName1, typeName2);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (!(o instanceof JdbcTypeDesc))
            return false;
        JdbcTypeDesc that = (JdbcTypeDesc) o;
        return typeCode == that.getTypeCode() && equalsIgnoreCase(typeName, that.getTypeName());
    }

    @Override
    public int hashCode() {
        int result = typeCode;
        result = 31 * result + (typeName != null ? typeName.toUpperCase().hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return ObjectUtils.toString(this);
    }
}
