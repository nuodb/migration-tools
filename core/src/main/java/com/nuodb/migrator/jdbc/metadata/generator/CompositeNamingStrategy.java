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
package com.nuodb.migrator.jdbc.metadata.generator;

import com.nuodb.migrator.jdbc.metadata.MetaData;
import com.nuodb.migrator.jdbc.metadata.MetaDataHandlerBase;
import com.nuodb.migrator.jdbc.metadata.MetaDataType;

import java.util.Collection;

import static com.google.common.collect.Lists.newArrayList;

/**
 * @author Sergey Bushik
 */
public class CompositeNamingStrategy<M extends MetaData> extends MetaDataHandlerBase implements NamingStrategy<M> {

    private Collection<NamingStrategy<M>> namingStrategies = newArrayList();

    public CompositeNamingStrategy(Class<? extends MetaData> objectClass) {
        super(objectClass);
    }

    public CompositeNamingStrategy(MetaDataType objectType) {
        super(objectType);
    }

    @Override
    public String getName(M object, ScriptGeneratorManager scriptGeneratorManager, boolean normalize) {
        for (NamingStrategy<M> namingStrategy : namingStrategies) {
            String name = getName(namingStrategy, object, scriptGeneratorManager, normalize);
            if (name != null) {
                return name;
            }
        }
        return null;
    }

    protected String getName(NamingStrategy<M> namingStrategy, M object, ScriptGeneratorManager scriptGeneratorManager,
            boolean normalize) {
        return namingStrategy.getName(object, scriptGeneratorManager, normalize);
    }

    @Override
    public String getQualifiedName(M object, ScriptGeneratorManager scriptGeneratorManager, String catalog,
            String schema, boolean normalize) {
        for (NamingStrategy<M> namingStrategy : namingStrategies) {
            String qualifiedName = getQualifiedName(namingStrategy, object, scriptGeneratorManager, catalog, schema,
                    normalize);
            if (qualifiedName != null) {
                return qualifiedName;
            }
        }
        return null;
    }

    protected String getQualifiedName(NamingStrategy<M> namingStrategy, M object,
            ScriptGeneratorManager scriptGeneratorManager, String catalog, String schema, boolean normalize) {
        return namingStrategy.getQualifiedName(object, scriptGeneratorManager, catalog, schema, normalize);
    }

    public void addNamingStrategy(NamingStrategy<M> namingStrategy) {
        namingStrategies.add(namingStrategy);
    }

    public void removeNamingStrategy(NamingStrategy<M> namingStrategy) {
        namingStrategies.remove(namingStrategy);
    }

    public Collection<NamingStrategy<M>> getNamingStrategies() {
        return namingStrategies;
    }

    public void setNamingStrategies(Collection<NamingStrategy<M>> namingStrategies) {
        this.namingStrategies = namingStrategies;
    }
}
