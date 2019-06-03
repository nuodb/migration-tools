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
import com.nuodb.migrator.jdbc.metadata.Table;

import java.util.Collection;

import static com.google.common.collect.Lists.newArrayList;
import static com.nuodb.migrator.jdbc.metadata.generator.ScriptType.CREATE;
import static com.nuodb.migrator.jdbc.metadata.generator.ScriptType.DROP;
import static java.util.Collections.emptySet;

/**
 * @author Sergey Bushik
 */
public abstract class ScriptGeneratorBase<T extends MetaData> extends MetaDataHandlerBase
        implements ScriptGenerator<T> {

    protected ScriptGeneratorBase(MetaDataType objectType) {
        super(objectType);
    }

    protected ScriptGeneratorBase(Class<? extends MetaData> objectClass) {
        super(objectClass);
    }

    @Override
    public Collection<Script> getScripts(T object, ScriptGeneratorManager scriptGeneratorManager) {
        Collection<Script> scripts;
        if (isGenerateScript(DROP, scriptGeneratorManager) && isGenerateScript(CREATE, scriptGeneratorManager)) {
            scripts = getDropCreateScripts(object, scriptGeneratorManager);
        } else if (isGenerateScript(DROP, scriptGeneratorManager)) {
            scripts = getDropScripts(object, scriptGeneratorManager);
        } else if (isGenerateScript(CREATE, scriptGeneratorManager)) {
            scripts = getCreateScripts(object, scriptGeneratorManager);
        } else {
            scripts = emptySet();
        }
        return scripts;
    }

    protected abstract Collection<Script> getDropScripts(T object, ScriptGeneratorManager scriptGeneratorManager);

    protected abstract Collection<Script> getCreateScripts(T object, ScriptGeneratorManager scriptGeneratorManager);

    protected Collection<Script> getDropCreateScripts(T object, ScriptGeneratorManager scriptGeneratorManager) {
        Collection<Script> scripts = newArrayList();
        scripts.addAll(getDropScripts(object, scriptGeneratorManager));
        scripts.addAll(getCreateScripts(object, scriptGeneratorManager));
        return scripts;
    }

    protected boolean isGenerateScript(ScriptType scriptType, ScriptGeneratorManager scriptGeneratorManager) {
        Collection<ScriptType> scriptTypes = scriptGeneratorManager.getScriptTypes();
        return scriptTypes != null && scriptTypes.contains(scriptType);
    }
}
