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

import com.nuodb.migrator.jdbc.metadata.MetaDataType;
import com.nuodb.migrator.utils.ObjectUtils;

import java.util.Collection;

import static com.nuodb.migrator.jdbc.metadata.MetaDataHandlerUtils.getHandler;
import static com.nuodb.migrator.utils.Collections.newPrioritySet;

/**
 * @author Sergey Bushik
 */
public class MetaDataFilterManager {

    private Collection<MetaDataFilter> metaDataFilters = newPrioritySet();

    /**
     * Returns meta data filter for a requested object type or null of there is
     * no registered filter.
     *
     * @param objectType
     *            requested object type to be filter.
     * @return meta data filter or null if not found.
     */
    public MetaDataFilter getMetaDataFilter(MetaDataType objectType) {
        return getHandler(metaDataFilters, objectType, false);
    }

    public void addMetaDataFilter(MetaDataFilter objectFilter) {
        metaDataFilters.add(objectFilter);
    }

    public void removeMetaDataFilter(MetaDataFilter objectFilter) {
        metaDataFilters.remove(objectFilter);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        MetaDataFilterManager that = (MetaDataFilterManager) o;

        if (metaDataFilters != null ? !metaDataFilters.equals(that.metaDataFilters) : that.metaDataFilters != null)
            return false;

        return true;
    }

    @Override
    public int hashCode() {
        return metaDataFilters != null ? metaDataFilters.hashCode() : 0;
    }

    @Override
    public String toString() {
        return ObjectUtils.toString(this);
    }
}
