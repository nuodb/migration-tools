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
package com.nuodb.migration.spec;

import com.nuodb.migration.jdbc.dialect.IdentifierNormalizer;
import com.nuodb.migration.jdbc.dialect.IdentifierQuoting;
import com.nuodb.migration.jdbc.metadata.MetaDataType;
import com.nuodb.migration.jdbc.metadata.generator.GroupScriptsBy;
import com.nuodb.migration.jdbc.metadata.generator.ScriptType;

import java.util.Collection;

import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Sets.newHashSet;

/**
 * @author Sergey Bushik
 */
public class SchemaSpec extends TaskSpecBase {

    private ConnectionSpec sourceConnectionSpec;
    private ConnectionSpec targetConnectionSpec;
    private ResourceSpec outputSpec;
    private Collection<MetaDataType> metaDataTypes = newArrayList(MetaDataType.ALL_TYPES);
    private Collection<ScriptType> scriptTypes = newHashSet(ScriptType.values());
    private GroupScriptsBy groupScriptsBy = GroupScriptsBy.TABLE;
    private Collection<JdbcTypeSpec> jdbcTypeSpecs = newHashSet();
    private IdentifierQuoting identifierQuoting;
    private IdentifierNormalizer identifierNormalizer;

    public ConnectionSpec getSourceConnectionSpec() {
        return sourceConnectionSpec;
    }

    public void setSourceConnectionSpec(ConnectionSpec sourceConnectionSpec) {
        this.sourceConnectionSpec = sourceConnectionSpec;
    }

    public ConnectionSpec getTargetConnectionSpec() {
        return targetConnectionSpec;
    }

    public void setTargetConnectionSpec(ConnectionSpec targetConnectionSpec) {
        this.targetConnectionSpec = targetConnectionSpec;
    }

    public ResourceSpec getOutputSpec() {
        return outputSpec;
    }

    public void setOutputSpec(ResourceSpec outputSpec) {
        this.outputSpec = outputSpec;
    }

    public Collection<MetaDataType> getMetaDataTypes() {
        return metaDataTypes;
    }

    public void setMetaDataTypes(Collection<MetaDataType> metaDataTypes) {
        this.metaDataTypes = metaDataTypes;
    }

    public Collection<ScriptType> getScriptTypes() {
        return scriptTypes;
    }

    public void setScriptTypes(Collection<ScriptType> scriptTypes) {
        this.scriptTypes = scriptTypes;
    }

    public GroupScriptsBy getGroupScriptsBy() {
        return groupScriptsBy;
    }

    public void setGroupScriptsBy(GroupScriptsBy groupScriptsBy) {
        this.groupScriptsBy = groupScriptsBy;
    }

    public Collection<JdbcTypeSpec> getJdbcTypeSpecs() {
        return jdbcTypeSpecs;
    }

    public void setJdbcTypeSpecs(Collection<JdbcTypeSpec> jdbcTypeSpecs) {
        this.jdbcTypeSpecs = jdbcTypeSpecs;
    }

    public IdentifierQuoting getIdentifierQuoting() {
        return identifierQuoting;
    }

    public void setIdentifierQuoting(IdentifierQuoting identifierQuoting) {
        this.identifierQuoting = identifierQuoting;
    }

    public IdentifierNormalizer getIdentifierNormalizer() {
        return identifierNormalizer;
    }

    public void setIdentifierNormalizer(IdentifierNormalizer identifierNormalizer) {
        this.identifierNormalizer = identifierNormalizer;
    }
}
