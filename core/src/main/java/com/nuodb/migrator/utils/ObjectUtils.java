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

import org.apache.commons.lang3.builder.ReflectionToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

import java.lang.reflect.Field;
import java.util.Collection;

import static com.google.common.collect.Lists.newArrayList;
import static com.nuodb.migrator.utils.Collections.isEmpty;
import static org.apache.commons.lang3.builder.ToStringStyle.MULTI_LINE_STYLE;

/**
 * @author Sergey Bushik
 */
public class ObjectUtils {

    public static final ToStringStyle TO_STRING_STYLE = MULTI_LINE_STYLE;

    public static boolean equals(Object object1, Object object2) {
        return object1 == object2 || !(object1 == null || object2 == null) && object1.equals(object2);
    }

    public static String toString(Object object) {
        return toString(object, null, null, null);
    }

    public static String toString(Object object, ToStringStyle toStringStyle) {
        return toString(object, null, null, toStringStyle);
    }

    public static String toString(Object object, Collection<String> includedFields) {
        return toString(object, includedFields, null);
    }

    public static String toString(Object object, Collection<String> includedFields, Collection<String> excludedFields) {
        return toString(object, includedFields, excludedFields, null);
    }

    public static String toString(Object object, Collection<String> includedFields, Collection<String> excludedFields,
            ToStringStyle toStringStyle) {
        FilterFieldsToStringBuilder filterFieldsToStringBuilder = new FilterFieldsToStringBuilder(object,
                toStringStyle != null ? toStringStyle : TO_STRING_STYLE);
        if (!isEmpty(includedFields)) {
            filterFieldsToStringBuilder.addIncludeFields(includedFields);
        }
        if (!isEmpty(excludedFields)) {
            filterFieldsToStringBuilder.addExcludedFields(excludedFields);
        }
        return filterFieldsToStringBuilder.build();
    }

    static class FilterFieldsToStringBuilder extends ReflectionToStringBuilder {

        private Collection<String> includedFields = newArrayList();
        private Collection<String> excludedFields = newArrayList();

        public FilterFieldsToStringBuilder(Object object) {
            super(object);
        }

        public FilterFieldsToStringBuilder(Object object, ToStringStyle style) {
            super(object, style);
        }

        public FilterFieldsToStringBuilder(Object object, ToStringStyle style, StringBuffer buffer) {
            super(object, style, buffer);
        }

        public <T> FilterFieldsToStringBuilder(T object, ToStringStyle style, StringBuffer buffer,
                Class<? super T> reflectUpToClass, boolean outputTransients, boolean outputStatics) {
            super(object, style, buffer, reflectUpToClass, outputTransients, outputStatics);
        }

        public void addIncludeFields(Collection<String> fields) {
            includedFields.addAll(fields);
        }

        public void addExcludedFields(Collection<String> fields) {
            excludedFields.addAll(fields);
        }

        @Override
        protected boolean accept(Field field) {
            boolean accept = super.accept(field);
            if (accept) {
                if (includedFields.isEmpty()) {
                    accept = !excludedFields.contains(field.getName());
                } else {
                    accept = includedFields.contains(field.getName());
                }
            }
            return accept;
        }
    }
}
