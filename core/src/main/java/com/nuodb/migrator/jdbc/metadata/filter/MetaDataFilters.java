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
package com.nuodb.migrator.jdbc.metadata.filter;

import com.nuodb.migrator.jdbc.metadata.Identifiable;
import com.nuodb.migrator.jdbc.metadata.MetaData;
import com.nuodb.migrator.jdbc.metadata.MetaDataType;

import java.util.Collection;

import static com.google.common.collect.Lists.newArrayList;

/**
 * Virtual constructors for various meta data filters.
 *
 * @author Sergey Bushik
 */
public class MetaDataFilters {

    private MetaDataFilters() {
    }

    public static <T extends Identifiable> MetaDataFilter<T> newInvertAcceptFilter(MetaDataType objectType,
            MetaDataFilter<T> filter) {
        return new MetaDataInvertAcceptFilter<T>(objectType, filter);
    }

    public static <T extends Identifiable> MetaDataFilter<T> newNameEqualsFilter(MetaDataType objectType,
            boolean qualifyName, String name) {
        return new MetaDataNameEqualsFilter<T>(objectType, qualifyName, name);
    }

    public static <T extends Identifiable> MetaDataFilter<T> newNameMatchesFilter(MetaDataType objectType,
            boolean qualifyName, String regex) {
        return new MetaDataNameMatchesFilter<T>(objectType, qualifyName, regex);
    }

    public static <T extends MetaData> MetaDataFilter<T> newEitherOfFilters(MetaDataType objectType,
            MetaDataFilter<T>... filters) {
        return newEitherOfFilters(objectType, newArrayList(filters));
    }

    public static <T extends MetaData> MetaDataFilter<T> newEitherOfFilters(MetaDataType objectType,
            Collection<MetaDataFilter<T>> filters) {
        return new MetaDataEitherOfFilters<T>(objectType, filters);
    }

    public static <T extends MetaData> MetaDataFilter<T> newAllOfFilters(MetaDataType objectType,
            MetaDataFilter<T>... filters) {
        return newAllOfFilters(objectType, newArrayList(filters));
    }

    public static <T extends MetaData> MetaDataFilter<T> newAllOfFilters(MetaDataType objectType,
            Collection<MetaDataFilter<T>> filters) {
        return new MetaDataAllOfFilters<T>(objectType, filters);
    }
}
