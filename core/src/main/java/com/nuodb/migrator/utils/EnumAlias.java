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
package com.nuodb.migrator.utils;

import java.util.Map;
import java.util.TreeMap;

import static com.nuodb.migrator.utils.ReflectionUtils.getMethod;
import static com.nuodb.migrator.utils.ReflectionUtils.invokeMethod;
import static java.lang.String.CASE_INSENSITIVE_ORDER;

/**
 * @author Sergey Bushik
 */
public class EnumAlias<T extends Enum<T>> {

    protected Map<String, T> aliases = new TreeMap<String, T>(CASE_INSENSITIVE_ORDER);

    public EnumAlias() {
    }

    public EnumAlias(Class<? extends Enum<T>> type) {
        T[] values = invokeMethod(null, getMethod(type, "values"));
        for (T value : values) {
            addAlias(value);
        }
    }

    public void addAlias(T value) {
        addAlias(value.name().toLowerCase(), value);
    }

    public void addAlias(String alias, T value) {
        aliases.put(alias, value);
    }

    public String toAlias(T type) {
        for (Map.Entry<String, T> entry : aliases.entrySet()) {
            if (entry.getValue() == type) {
                return entry.getKey();
            }
        }
        return type.name().toLowerCase();
    }

    public T fromAlias(String alias) {
        return alias != null ? aliases.get(alias) : null;
    }
}
