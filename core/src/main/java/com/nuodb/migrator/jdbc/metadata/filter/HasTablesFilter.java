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

import com.google.common.base.Predicate;
import com.nuodb.migrator.jdbc.metadata.HasTables;
import com.nuodb.migrator.jdbc.metadata.MetaDataType;
import com.nuodb.migrator.jdbc.metadata.Sequence;
import com.nuodb.migrator.jdbc.metadata.Table;
import com.nuodb.migrator.jdbc.metadata.UserDefinedType;

import java.util.Collection;

import static com.google.common.collect.Iterables.filter;
import static com.google.common.collect.Lists.newArrayList;

/**
 * @author Sergey Bushik
 */
@SuppressWarnings("all")
public class HasTablesFilter implements HasTables {

    private HasTables hasTables;
    private MetaDataFilterManager metaDataFilterManager;

    public HasTablesFilter(HasTables hasTables, MetaDataFilterManager metaDataFilterManager) {
        this.hasTables = hasTables;
        this.metaDataFilterManager = metaDataFilterManager;
    }

    @Override
    public Collection<Table> getTables() {
        final MetaDataFilter filter = metaDataFilterManager != null
                ? metaDataFilterManager.getMetaDataFilter(MetaDataType.TABLE)
                : null;
        return newArrayList(filter(hasTables.getTables(), new Predicate<Table>() {
            @Override
            public boolean apply(Table table) {
                return filter == null || filter.accepts(table);
            }
        }));
    }

    @Override
    public Collection<Sequence> getSequences() {
        final MetaDataFilter filter = metaDataFilterManager != null
                ? metaDataFilterManager.getMetaDataFilter(MetaDataType.TABLE)
                : null;
        return newArrayList(filter(hasTables.getSequences(), new Predicate<Sequence>() {
            @Override
            public boolean apply(Sequence sequence) {
                return filter == null || filter.accepts(sequence);
            }
        }));
    }

    @Override
    public Collection<UserDefinedType> getUserDefinedTypes() {
        final MetaDataFilter filter = metaDataFilterManager != null
                ? metaDataFilterManager.getMetaDataFilter(MetaDataType.TABLE)
                : null;
        return newArrayList(filter(hasTables.getUserDefinedTypes(), new Predicate<UserDefinedType>() {
            @Override
            public boolean apply(UserDefinedType userDefinedType) {
                return filter == null || filter.accepts(userDefinedType);
            }
        }));
    }

    @Override
    public MetaDataType getObjectType() {
        return hasTables.getObjectType();
    }
}
