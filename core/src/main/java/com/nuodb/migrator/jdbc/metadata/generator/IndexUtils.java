/**
 * Copyright (c) 2014, NuoDB, Inc.
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
package com.nuodb.migrator.jdbc.metadata.generator;

import com.google.common.base.Predicate;
import com.nuodb.migrator.jdbc.metadata.Column;
import com.nuodb.migrator.jdbc.metadata.Index;
import com.nuodb.migrator.jdbc.metadata.Table;

import java.util.Collection;
import java.util.Map;

import static com.google.common.collect.Maps.newLinkedHashMap;
import static com.google.common.collect.Sets.newHashSet;

/**
 * @author Sergey Bushik
 */
public class IndexUtils {

    public static Collection<Index> getNonRepeatingIndexes(Table table) {
        return getNonRepeatingIndexes(table, null);
    }

    /**
     * Bypasses MIG-88 "an index on these columns already exists" and executes predicate for duplicated indexes
     *
     * @param table to get non repeating indexes for
     * @param predicate receives each duplicate index
     * @return non repeating indexes
     */
    public static Collection<Index> getNonRepeatingIndexes(Table table, Predicate<Index> predicate) {
        Map<Collection<Column>, Index> nonRepeatingIndexes = newLinkedHashMap();
        for (Index index : table.getIndexes()) {
            Collection<Column> columns = newHashSet(index.getColumns());
            Index nonRepeatingIndex = nonRepeatingIndexes.get(columns);
            boolean replaceWithUniqueIndex =
                    index.isUnique() && nonRepeatingIndex != null && !nonRepeatingIndex.isUnique();
            if (index.isPrimary() || replaceWithUniqueIndex || nonRepeatingIndex == null) {
                nonRepeatingIndexes.put(columns, index);
            } else if (predicate != null) {
                predicate.apply(index);
            }
        }
        return nonRepeatingIndexes.values();
    }
}
