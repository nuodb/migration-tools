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

/**
 * @author Sergey Bushik
 */
public class HasJdbcTypeHandlerBase implements HasJdbcTypeHandler {

    private JdbcType jdbcType;

    public HasJdbcTypeHandlerBase() {
    }

    public HasJdbcTypeHandlerBase(JdbcType jdbcType) {
        this.jdbcType = jdbcType;
    }

    @Override
    public int getScore(JdbcType jdbcType) {
        int score = getScore(jdbcType.getJdbcTypeDesc());
        if (score == 0) {
            score = getScore(jdbcType.getJdbcTypeOptions());
        }
        return score;
    }

    protected int getScore(JdbcTypeDesc jdbcTypeDesc) {
        return getScore(getJdbcType().getJdbcTypeDesc(), jdbcTypeDesc);
    }

    protected int getScore(JdbcTypeOptions jdbcTypeOptions) {
        return getScore(getJdbcType().getJdbcTypeOptions(), jdbcTypeOptions);
    }

    private static int getScore(JdbcTypeDesc jdbcTypeDesc1, JdbcTypeDesc jdbcTypeDesc2) {
        return ObjectUtils.equals(jdbcTypeDesc1, jdbcTypeDesc2) ? 0 : -1;
    }

    private static int getScore(JdbcTypeOptions jdbcTypeOptions1, JdbcTypeOptions jdbcTypeOptions2) {
        if (jdbcTypeOptions1 == jdbcTypeOptions2) {
            return 0;
        }
        if (jdbcTypeOptions1 == null) {
            return 1;
        }
        if (jdbcTypeOptions2 == null) {
            return -1;
        }
        int score = compare(jdbcTypeOptions1.getSize(), jdbcTypeOptions2.getSize());
        if (score == 0) {
            score = compare(jdbcTypeOptions1.getPrecision(), jdbcTypeOptions2.getPrecision());
        }
        if (score == 0) {
            score = compare(jdbcTypeOptions1.getScale(), jdbcTypeOptions2.getScale());
        }
        return score;
    }

    private static int compare(Integer i1, Integer i2) {
        return i1 != null && i2 != null ? i1.compareTo(i2) : 0;
    }

    private static int compare(Long i1, Long i2) {
        return i1 != null && i2 != null ? i1.compareTo(i2) : 0;
    }

    @Override
    public JdbcType getJdbcType() {
        return jdbcType;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        HasJdbcTypeHandlerBase that = (HasJdbcTypeHandlerBase) o;

        if (jdbcType != null ? !jdbcType.equals(that.jdbcType) : that.jdbcType != null)
            return false;

        return true;
    }

    @Override
    public int hashCode() {
        return jdbcType != null ? jdbcType.hashCode() : 0;
    }
}
