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
package com.nuodb.migrator.job;

import com.nuodb.migrator.jdbc.dialect.IdentifierNormalizer;
import com.nuodb.migrator.jdbc.dialect.IdentifierQuoting;
import com.nuodb.migrator.jdbc.metadata.MetaDataType;
import com.nuodb.migrator.jdbc.metadata.filter.MetaDataFilterManager;
import com.nuodb.migrator.jdbc.metadata.generator.GroupScriptsBy;
import com.nuodb.migrator.jdbc.metadata.generator.NamingStrategy;
import com.nuodb.migrator.jdbc.metadata.generator.ScriptType;
import com.nuodb.migrator.spec.*;
import com.nuodb.migrator.utils.PrioritySet;

import java.util.Collection;

/**
 * @author Sergey Bushik
 */
public abstract class ScriptGeneratorJobBase<T extends ScriptGeneratorJobSpecBase> extends HasServicesJobBase<T> {

    protected ScriptGeneratorJobBase() {
    }

    protected ScriptGeneratorJobBase(T jobSpec) {
        super(jobSpec);
    }

    protected MetaDataSpec getMetaDataSpec() {
        return getJobSpec().getMetaDataSpec();
    }

    protected MetaDataFilterManager getMetaDataFilterManager() {
        return getJobSpec().getMetaDataFilterManager();
    }

    protected String[] getTableTypes() {
        return getJobSpec().getTableTypes();
    }

    protected Collection<MetaDataType> getObjectTypes() {
        return getJobSpec().getObjectTypes();
    }

    protected ConnectionSpec getTargetSpec() {
        return getJobSpec().getTargetSpec();
    }

    protected Collection<ScriptType> getScriptTypes() {
        return getJobSpec().getScriptTypes();
    }

    protected GroupScriptsBy getGroupScriptsBy() {
        return getJobSpec().getGroupScriptsBy();
    }

    protected Collection<JdbcTypeSpec> getJdbcTypeSpecs() {
        return getJobSpec().getJdbcTypeSpecs();
    }

    protected PrioritySet<NamingStrategy> getNamingStrategies() {
        return getJobSpec().getNamingStrategies();
    }

    protected IdentifierQuoting getIdentifierQuoting() {
        return getJobSpec().getIdentifierQuoting();
    }

    protected IdentifierNormalizer getIdentifierNormalizer() {
        return getJobSpec().getIdentifierNormalizer();
    }
}
