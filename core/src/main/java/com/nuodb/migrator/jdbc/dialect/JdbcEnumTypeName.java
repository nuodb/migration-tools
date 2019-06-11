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
package com.nuodb.migrator.jdbc.dialect;

import com.nuodb.migrator.jdbc.type.JdbcEnumType;
import com.nuodb.migrator.jdbc.type.JdbcType;
import com.nuodb.migrator.jdbc.type.JdbcTypeName;

import java.util.Collection;
import java.util.Iterator;

/**
 * Generates enum type
 *
 * @author Sergey Bushik
 */
public class JdbcEnumTypeName implements JdbcTypeName {

    private static final String NAME = "ENUM";
    private final String name;

    public JdbcEnumTypeName() {
        this(NAME);
    }

    public JdbcEnumTypeName(String name) {
        this.name = name;
    }

    @Override
    public String getTypeName(JdbcType jdbcType) {
        StringBuilder buffer = new StringBuilder();
        buffer.append(getName());
        buffer.append("(");
        Collection<String> values = ((JdbcEnumType) jdbcType).getValues();
        for (Iterator<String> iterator = values.iterator(); iterator.hasNext();) {
            buffer.append("'");
            getValue(buffer, iterator.next());
            buffer.append("'");
            if (iterator.hasNext()) {
                buffer.append(",");
            }
        }
        buffer.append(")");
        return buffer.toString();
    }

    protected void getValue(StringBuilder buffer, String value) {
        char[] symbols = value.toCharArray();
        for (char symbol : symbols) {
            switch (symbol) {
            case '\'':
                buffer.append('\'');
                break;
            }
            buffer.append(symbol);
        }
    }

    @Override
    public int getScore(JdbcType jdbcType) {
        return jdbcType.getClass().equals(JdbcEnumType.class) ? 0 : -1;
    }

    public String getName() {
        return name;
    }
}
