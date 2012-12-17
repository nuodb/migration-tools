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
package com.nuodb.migration.jdbc.metadata;

import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.List;

import static com.nuodb.migration.utils.ValidationUtils.isNotNull;
import static java.lang.String.format;

public class MetaDataType implements Comparable<MetaDataType> {

    private static final Logger logger = LoggerFactory.getLogger(MetaDataType.class);

    public static final MetaDataType CATALOG = new MetaDataType("catalog");
    public static final MetaDataType SCHEMA = new MetaDataType("schema");
    public static final MetaDataType TABLE = new MetaDataType("table");
    public static final MetaDataType COLUMN = new MetaDataType("column");
    public static final MetaDataType PRIMARY_KEY = new MetaDataType("primary key");
    public static final MetaDataType FOREIGN_KEY = new MetaDataType("foreign key");
    public static final MetaDataType INDEX = new MetaDataType("index");
    public static final MetaDataType SEQUENCE = new MetaDataType("sequence");
    public static final MetaDataType TABLE_CHECK = new MetaDataType("table check");
    public static final MetaDataType COLUMN_CHECK = new MetaDataType("column check");

    public static final MetaDataType[] ALL_TYPES = getAllTypes();

    private static MetaDataType[] getAllTypes() {
        List<MetaDataType> types = Lists.newArrayList();
        Field[] fields = MetaDataType.class.getFields();
        for (Field field : fields) {
            if (Modifier.isStatic(field.getModifiers()) && field.getType() == MetaDataType.class) {
                try {
                    types.add((MetaDataType) field.get(null));
                } catch (IllegalAccessException exception) {
                    if (logger.isDebugEnabled()) {
                        logger.debug(format("Failed accessing %s field", MetaDataType.class), exception);
                    }
                }
            }
        }
        return types.toArray(new MetaDataType[types.size()]);
    }

    private final String type;

    public MetaDataType(String type) {
        isNotNull(type, "Meta data type is required");
        this.type = type;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof MetaDataType)) return false;
        MetaDataType that = (MetaDataType) o;
        if (type != null ? !type.equals(that.type) : that.type != null) return false;
        return true;
    }

    @Override
    public int hashCode() {
        return type != null ? type.hashCode() : 0;
    }

    @Override
    public String toString() {
        return type;
    }

    @Override
    public int compareTo(MetaDataType metaDataType) {
        return type.compareTo(metaDataType.type);
    }
}
