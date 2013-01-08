/**
 * Copyright (c) 2012, NuoDB, Inc.
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
package com.nuodb.migration.jdbc.metadata.generator;

import com.nuodb.migration.jdbc.metadata.Relational;

import java.util.Collection;

import static com.google.common.collect.Lists.newArrayList;
import static com.nuodb.migration.jdbc.metadata.generator.ScriptType.CREATE;
import static com.nuodb.migration.jdbc.metadata.generator.ScriptType.DROP;
import static java.util.Collections.emptySet;

/**
 * @author Sergey Bushik
 */
public abstract class ScriptGeneratorBase<R extends Relational> extends GeneratorServiceBase<R> implements ScriptGenerator<R> {

    protected ScriptGeneratorBase(Class<R> relationalType) {
        super(relationalType);
    }

    @Override
    public Collection<String> getScripts(R relational, ScriptGeneratorContext context) {
        Collection<String> scripts;
        if (hasScriptType(context, DROP) && hasScriptType(context, CREATE)) {
            scripts = getDropCreateScripts(relational, context);
        } else if (hasScriptType(context, DROP)) {
            scripts = getDropScripts(relational, context);
        } else if (hasScriptType(context, CREATE)) {
            scripts = getCreateScripts(relational, context);
        } else {
            scripts = emptySet();
        }
        return scripts;
    }

    protected Collection<String> getDropCreateScripts(R relational, ScriptGeneratorContext context) {
        Collection<String> scripts = newArrayList();
        scripts.addAll(getDropScripts(relational, context));
        scripts.addAll(getCreateScripts(relational, context));
        return scripts;
    }

    protected boolean hasScriptType(ScriptGeneratorContext context, ScriptType scriptType) {
        Collection<ScriptType> scriptTypes = context.getScriptTypes();
        return scriptTypes != null && scriptTypes.contains(scriptType);
    }

    protected abstract Collection<String> getDropScripts(R relational, ScriptGeneratorContext context);

    protected abstract Collection<String> getCreateScripts(R relational, ScriptGeneratorContext context);
}
