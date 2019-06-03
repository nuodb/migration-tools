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

import com.nuodb.migrator.jdbc.metadata.MetaData;
import com.nuodb.migrator.jdbc.metadata.MetaDataType;

import java.util.Collection;

import static com.google.common.collect.Lists.newArrayList;

/**
 * @author Sergey Bushik
 */
public abstract class MetaDataFiltersBase<T extends MetaData> extends MetaDataFilterBase<T> {

    private Collection<MetaDataFilter<T>> filters = newArrayList();

    public MetaDataFiltersBase(Class<? extends MetaData> objectClass) {
        super(objectClass);
    }

    public MetaDataFiltersBase(MetaDataType objectType) {
        super(objectType);
    }

    public MetaDataFiltersBase(MetaDataType objectType, Collection<MetaDataFilter<T>> filters) {
        super(objectType);
        if (filters != null) {
            for (MetaDataFilter<T> filter : filters) {
                addFilter(filter);
            }
        }
    }

    public void addFilter(MetaDataFilter<T> filter) {
        filters.add(filter);
    }

    public boolean removeFilter(MetaDataFilter filter) {
        return filters.remove(filter);
    }

    public Collection<MetaDataFilter<T>> getFilters() {
        return filters;
    }

    public void setFilters(Collection<MetaDataFilter<T>> filters) {
        this.filters = filters;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        if (!super.equals(o))
            return false;

        MetaDataFiltersBase that = (MetaDataFiltersBase) o;

        if (filters != null ? !filters.equals(that.filters) : that.filters != null)
            return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + (filters != null ? filters.hashCode() : 0);
        return result;
    }
}
