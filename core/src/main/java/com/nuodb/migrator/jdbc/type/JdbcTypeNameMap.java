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

import com.google.common.base.Predicate;
import com.nuodb.migrator.utils.PrioritySet;

import java.util.Collection;

import static com.google.common.collect.Iterables.removeIf;
import static com.nuodb.migrator.jdbc.type.JdbcTypeNames.createTypeNameTemplate;
import static com.nuodb.migrator.utils.Collections.newPrioritySet;
import static com.nuodb.migrator.utils.Priority.LOW;
import static com.nuodb.migrator.utils.Priority.NORMAL;

/**
 * @author Sergey Bushik
 */
public class JdbcTypeNameMap {

    private Collection<JdbcTypeName> jdbcTypeNames = newPrioritySet();

    public void addJdbcTypeName(int typeCode, String typeName) {
        addJdbcTypeName(new JdbcTypeDesc(typeCode), typeName);
    }

    public void addJdbcTypeName(JdbcTypeDesc jdbcTypeDesc, String typeName) {
        addJdbcTypeName(jdbcTypeDesc, null, typeName);
    }

    public void addJdbcTypeName(JdbcTypeDesc jdbcTypeDesc, String typeName, int priority) {
        JdbcTypeName jdbcTypeName = createTypeNameTemplate(jdbcTypeDesc, typeName);
        addJdbcTypeName(jdbcTypeName, priority);
    }

    public void addJdbcTypeName(int typeCode, JdbcTypeOptions jdbcTypeOptions, String typeName) {
        addJdbcTypeName(new JdbcTypeDesc(typeCode), jdbcTypeOptions, typeName);
    }

    public void addJdbcTypeName(JdbcTypeDesc jdbcTypeDesc, JdbcTypeOptions jdbcTypeOptions, String typeName) {
        addJdbcTypeName(new JdbcType(jdbcTypeDesc, jdbcTypeOptions), typeName);
    }

    public void addJdbcTypeName(JdbcType jdbcType, String typeName) {
        JdbcTypeName jdbcTypeName = createTypeNameTemplate(jdbcType, typeName);
        addJdbcTypeName(jdbcTypeName, jdbcType.getJdbcTypeOptions() != null ? NORMAL : LOW);
    }

    public void addJdbcTypeName(JdbcTypeName jdbcTypeName) {
        addJdbcTypeName(jdbcTypeName, NORMAL);
    }

    public void addJdbcTypeName(JdbcTypeName jdbcTypeName, int priority) {
        ((PrioritySet<JdbcTypeName>) getJdbcTypeNames()).add(jdbcTypeName, priority);
    }

    public String getTypeName(JdbcType jdbcType) {
        Integer targetScore = null;
        JdbcTypeName targetJdbcTypeName = null;
        for (JdbcTypeName jdbcTypeName : getJdbcTypeNames()) {
            int score = jdbcTypeName.getScore(jdbcType);
            if (score >= 0 && (targetScore == null || score <= targetScore)) {
                targetScore = score;
                targetJdbcTypeName = jdbcTypeName;
            }
            if (score == 0) {
                break;
            }
        }
        return targetJdbcTypeName != null ? targetJdbcTypeName.getTypeName(jdbcType) : null;
    }

    public String getTypeName(JdbcTypeDesc jdbcTypeDesc, JdbcTypeOptions jdbcTypeOptions) {
        return getTypeName(new JdbcType(jdbcTypeDesc, jdbcTypeOptions));
    }

    public void removeJdbcTypeName(int typeCode) {
        removeJdbcTypeName(new JdbcTypeDesc(typeCode));
    }

    public void removeJdbcTypeName(JdbcTypeDesc jdbcTypeDesc) {
        removeJdbcTypeName(jdbcTypeDesc, null);
    }

    public void removeJdbcTypeName(JdbcTypeDesc jdbcTypeDesc, JdbcTypeOptions jdbcTypeOptions) {
        removeJdbcTypeName(new JdbcType(jdbcTypeDesc, jdbcTypeOptions));
    }

    public void removeJdbcTypeName(final JdbcType jdbcType) {
        removeIf(getJdbcTypeNames(), new Predicate<JdbcTypeName>() {
            @Override
            public boolean apply(JdbcTypeName jdbcTypeName) {
                return jdbcTypeName instanceof HasJdbcTypeHandler
                        && ((HasJdbcTypeHandler) jdbcTypeName).getJdbcType().equals(jdbcType);
            }
        });
    }

    public void removeJdbcTypeName(JdbcTypeName jdbcTypeName) {
        getJdbcTypeNames().remove(jdbcTypeName);
    }

    public Collection<JdbcTypeName> getJdbcTypeNames() {
        return jdbcTypeNames;
    }

    public void setJdbcTypeNames(Collection<JdbcTypeName> jdbcTypeNames) {
        this.jdbcTypeNames = jdbcTypeNames;
    }
}