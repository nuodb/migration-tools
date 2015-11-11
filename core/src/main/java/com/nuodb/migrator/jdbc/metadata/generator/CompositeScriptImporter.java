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

import java.util.Collection;

import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Sets.newLinkedHashSet;

/**
 * @author Sergey Bushik
 */
public class CompositeScriptImporter implements ScriptImporter {

    private Collection<ScriptImporter> scriptImporters = newLinkedHashSet();

    public CompositeScriptImporter() {
    }

    public CompositeScriptImporter(ScriptImporter... scriptImporters) {
        this(newArrayList(scriptImporters));
    }

    public CompositeScriptImporter(Collection<ScriptImporter> scriptImporters) {
        this.scriptImporters = scriptImporters;
    }

    @Override
    public void open() throws Exception {
        for (ScriptImporter scriptImporter : getScriptImporters()) {
            scriptImporter.open();
        }
    }

    @Override
    public Collection<String> importScripts() throws Exception {
        Collection<String> scripts = newArrayList();
        for (ScriptImporter scriptImporter : getScriptImporters()) {
            scripts.addAll(scriptImporter.importScripts());
        }
        return scripts;
    }

    @Override
    public void close() throws Exception {
        for (ScriptImporter scriptImporter : getScriptImporters()) {
            scriptImporter.close();
        }
    }

    public Collection<ScriptImporter> getScriptImporters() {
        return scriptImporters;
    }

    public void setScriptImporters(Collection<ScriptImporter> scriptImporters) {
        this.scriptImporters = scriptImporters;
    }
}
