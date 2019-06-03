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
package com.nuodb.migrator.jdbc.metadata.inspector;

import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.google.common.base.Supplier;
import com.google.common.collect.*;
import com.nuodb.migrator.jdbc.metadata.Identifiable;
import com.nuodb.migrator.jdbc.metadata.Identifier;
import com.nuodb.migrator.jdbc.metadata.MetaData;
import com.nuodb.migrator.jdbc.metadata.MetaDataType;
import org.apache.commons.lang3.ObjectUtils;

import java.util.Collection;
import java.util.Set;

import static com.google.common.collect.Multimaps.newSetMultimap;
import static com.google.common.collect.Sets.newLinkedHashSet;

/**
 * @author Sergey Bushik
 */
@SuppressWarnings("unchecked")
public class SimpleInspectionResults implements InspectionResults {

    private SetMultimap<MetaDataType, MetaData> objects = newSetMultimap(
            Maps.<MetaDataType, Collection<MetaData>>newHashMap(), new Supplier<Set<MetaData>>() {
                public Set<MetaData> get() {
                    return newLinkedHashSet();
                }
            });

    @Override
    public void addObject(MetaData object) {
        objects.put(object.getObjectType(), object);
    }

    @Override
    public void addObjects(Collection<? extends MetaData> objects) {
        for (MetaData object : objects) {
            addObject(object);
        }
    }

    @Override
    public <M extends MetaData> M getObject(MetaDataType objectType) {
        Set<MetaData> objectsByType = objects.get(objectType);
        return !objectsByType.isEmpty() ? (M) objectsByType.iterator().next() : null;
    }

    @Override
    public <M extends Identifiable> M getObject(MetaDataType objectType, String name) {
        return getObject(objectType, Identifier.valueOf(name));
    }

    @Override
    public <M extends Identifiable> M getObject(MetaDataType objectType, final Identifier identifier) {
        Optional<MetaData> identifiable = Iterables.tryFind(objects.get(objectType), new Predicate<MetaData>() {
            @Override
            public boolean apply(MetaData object) {
                return object instanceof Identifiable
                        && ObjectUtils.equals(((Identifiable) object).getIdentifier(), identifier);
            }
        });
        return identifiable.isPresent() ? (M) identifiable.get() : null;
    }

    @Override
    public <M extends MetaData> Collection<M> getObjects(MetaDataType objectType) {
        return (Collection<M>) objects.get(objectType);
    }

    @Override
    public Collection<? extends MetaData> getObjects() {
        return objects.values();
    }

    @Override
    public void removeObject(MetaData object) {
        objects.remove(object.getObjectType(), object);
    }
}
