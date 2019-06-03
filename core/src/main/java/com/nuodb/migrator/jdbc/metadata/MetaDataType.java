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
package com.nuodb.migrator.jdbc.metadata;

import com.google.common.collect.Maps;
import org.slf4j.Logger;

import java.io.Serializable;
import java.lang.reflect.Modifier;
import java.util.Map;

import static com.nuodb.migrator.utils.ValidationUtils.isNotNull;
import static java.lang.String.format;
import static org.slf4j.LoggerFactory.getLogger;

public class MetaDataType implements Comparable<MetaDataType>, Serializable {

    private static final transient Logger logger = getLogger(MetaDataType.class);

    public static final MetaDataType DATABASE = new MetaDataType(Database.class);
    public static final MetaDataType CATALOG = new MetaDataType(Catalog.class);
    public static final MetaDataType USER_DEFINED_TYPE = new MetaDataType(UserDefinedType.class);
    public static final MetaDataType SCHEMA = new MetaDataType(Schema.class);
    public static final MetaDataType TABLE = new MetaDataType(Table.class);
    public static final MetaDataType COLUMN = new MetaDataType(Column.class);
    public static final MetaDataType PRIMARY_KEY = new MetaDataType(PrimaryKey.class);
    public static final MetaDataType FOREIGN_KEY = new MetaDataType(ForeignKey.class);
    public static final MetaDataType INDEX = new MetaDataType(Index.class);
    public static final MetaDataType SEQUENCE = new MetaDataType(Sequence.class);
    public static final MetaDataType CHECK = new MetaDataType(Check.class);
    public static final MetaDataType TRIGGER = new MetaDataType(Trigger.class);
    public static final MetaDataType COLUMN_TRIGGER = new MetaDataType(ColumnTrigger.class);

    private Class<? extends MetaData> objectType;

    public static final Map<String, MetaDataType> NAME_TYPE_MAP = getNameTypeMap();

    public static final MetaDataType[] TYPES = NAME_TYPE_MAP.values().toArray(new MetaDataType[] {});

    private static Map<String, MetaDataType> getNameTypeMap() {
        Map<String, MetaDataType> nameTypeMap = Maps.newLinkedHashMap();
        java.lang.reflect.Field[] fields = MetaDataType.class.getFields();
        for (java.lang.reflect.Field field : fields) {
            if (Modifier.isStatic(field.getModifiers()) && field.getType() == MetaDataType.class) {
                try {
                    nameTypeMap.put(field.getName(), (MetaDataType) field.get(null));
                } catch (IllegalAccessException exception) {
                    if (logger.isDebugEnabled()) {
                        logger.debug(format("Failed accessing %s field", MetaDataType.class), exception);
                    }
                }
            }
        }
        return nameTypeMap;
    }

    public MetaDataType(Class<? extends MetaData> objectType) {
        isNotNull(objectType, "Type class is required");
        this.objectType = objectType;
    }

    public Class<? extends MetaData> getObjectType() {
        return objectType;
    }

    public String getName() {
        return objectType.getName();
    }

    public boolean isAssignableFrom(MetaDataType metaDataType) {
        return objectType.isAssignableFrom(metaDataType.getObjectType());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        MetaDataType that = (MetaDataType) o;
        if (!objectType.equals(that.objectType))
            return false;
        return true;
    }

    @Override
    public int hashCode() {
        return objectType.hashCode();
    }

    @Override
    public int compareTo(MetaDataType that) {
        if (objectType.equals(that.getObjectType())) {
            return 0;
        } else {
            return objectType.isAssignableFrom(that.getObjectType()) ? -1 : 1;
        }
    }

    @Override
    public String toString() {
        return getName();
    }
}
