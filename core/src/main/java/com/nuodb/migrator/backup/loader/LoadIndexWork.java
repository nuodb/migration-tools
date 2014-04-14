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
package com.nuodb.migrator.backup.loader;

import com.nuodb.migrator.jdbc.metadata.MetaData;
import com.nuodb.migrator.jdbc.metadata.generator.CompositeScriptExporter;
import com.nuodb.migrator.jdbc.metadata.generator.ProxyScriptExporter;
import com.nuodb.migrator.jdbc.metadata.generator.ScriptExporter;
import com.nuodb.migrator.jdbc.metadata.generator.ScriptGeneratorManager;
import com.nuodb.migrator.jdbc.metadata.generator.SessionScriptExporter;
import com.nuodb.migrator.jdbc.session.WorkBase;

import java.util.Collection;

import static com.google.common.collect.Lists.newArrayList;
import static com.nuodb.migrator.jdbc.JdbcUtils.closeQuietly;
import static com.nuodb.migrator.jdbc.metadata.generator.SchemaScriptGeneratorUtils.getUseSchema;

/**
 * Loads schema scripts for a desired database object
 *
 * @author Sergey Bushik
 */
public class LoadIndexWork extends WorkBase {
    /**
     * Object to generate & load schema for
     */
    private MetaData index;
    /**
     * Backup load manager
     */
    private BackupLoaderManager backupLoaderManager;
    private BackupLoaderContext backupLoaderContext;
    private ScriptExporter scriptExporter;
    private ScriptGeneratorManager scriptGeneratorManager;

    public LoadIndexWork(MetaData index, BackupLoaderManager backupLoaderManager) {
        this.index = index;
        this.backupLoaderManager = backupLoaderManager;
    }

    @Override
    protected void init() throws Exception {
        backupLoaderContext = backupLoaderManager.getBackupLoaderContext();
        scriptGeneratorManager = backupLoaderContext.getScriptGeneratorManager();

        scriptExporter = createScriptExporter();
        scriptExporter.open();
    }

    protected ScriptExporter createScriptExporter() throws Exception {
        Collection<ScriptExporter> scriptExporters = newArrayList();

        ScriptExporter scriptExporter = backupLoaderContext.getScriptExporter();
        if (scriptExporter != null) {
            // will close underlying script exporter later manually
            scriptExporters.add(new ProxyScriptExporter(scriptExporter, false));
        }
        scriptExporters.add(new SessionScriptExporter(getSession()));
        return new CompositeScriptExporter(scriptExporters);
    }

    @Override
    public void execute() throws Exception {
        scriptExporter.exportScript(getUseSchema(scriptGeneratorManager));
        scriptExporter.exportScripts(scriptGeneratorManager.getScripts(index));
    }

    @Override
    public void close() throws Exception {
        super.close();
        closeQuietly(scriptExporter);
    }
}
