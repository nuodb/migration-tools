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
package com.nuodb.migrator.spec;

import com.nuodb.migrator.jdbc.dialect.IdentifierNormalizer;
import com.nuodb.migrator.jdbc.dialect.IdentifierQuoting;
import com.nuodb.migrator.jdbc.metadata.MetaDataType;
import com.nuodb.migrator.jdbc.metadata.generator.GroupScriptsBy;
import com.nuodb.migrator.jdbc.metadata.generator.ScriptType;

import java.util.Collection;

import static com.google.common.collect.Sets.newHashSet;

/**
 * @author Sergey Bushik
 */
public class SchemaSpec extends TaskSpecBase {

    private ConnectionSpec sourceConnectionSpec;
    private ConnectionSpec targetConnectionSpec;
    private ResourceSpec outputSpec;
    private Collection<MetaDataType> metaDataTypes = newHashSet(MetaDataType.TYPES);
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
        this.metaDataTypes = newHashSet(metaDataTypes);
    }

    public Collection<ScriptType> getScriptTypes() {
        return scriptTypes;
    }

    public void setScriptTypes(Collection<ScriptType> scriptTypes) {
        this.scriptTypes = newHashSet(scriptTypes);
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
        this.jdbcTypeSpecs = newHashSet(jdbcTypeSpecs);
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

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;

        SchemaSpec that = (SchemaSpec) o;

        if (groupScriptsBy != that.groupScriptsBy) return false;
        if (identifierNormalizer != null ? !identifierNormalizer.equals(
                that.identifierNormalizer) : that.identifierNormalizer != null) return false;
        if (identifierQuoting != null ? !identifierQuoting.equals(
                that.identifierQuoting) : that.identifierQuoting != null)
            return false;
        if (jdbcTypeSpecs != null ? !jdbcTypeSpecs.equals(that.jdbcTypeSpecs) : that.jdbcTypeSpecs != null)
            return false;
        if (metaDataTypes != null ? !metaDataTypes.equals(that.metaDataTypes) : that.metaDataTypes != null)
            return false;
        if (outputSpec != null ? !outputSpec.equals(that.outputSpec) : that.outputSpec != null) return false;
        if (scriptTypes != null ? !scriptTypes.equals(that.scriptTypes) : that.scriptTypes != null) return false;
        if (sourceConnectionSpec != null ? !sourceConnectionSpec.equals(
                that.sourceConnectionSpec) : that.sourceConnectionSpec != null) return false;
        if (targetConnectionSpec != null ? !targetConnectionSpec.equals(
                that.targetConnectionSpec) : that.targetConnectionSpec != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + (sourceConnectionSpec != null ? sourceConnectionSpec.hashCode() : 0);
        result = 31 * result + (targetConnectionSpec != null ? targetConnectionSpec.hashCode() : 0);
        result = 31 * result + (outputSpec != null ? outputSpec.hashCode() : 0);
        result = 31 * result + (metaDataTypes != null ? metaDataTypes.hashCode() : 0);
        result = 31 * result + (scriptTypes != null ? scriptTypes.hashCode() : 0);
        result = 31 * result + (groupScriptsBy != null ? groupScriptsBy.hashCode() : 0);
        result = 31 * result + (jdbcTypeSpecs != null ? jdbcTypeSpecs.hashCode() : 0);
        result = 31 * result + (identifierQuoting != null ? identifierQuoting.hashCode() : 0);
        result = 31 * result + (identifierNormalizer != null ? identifierNormalizer.hashCode() : 0);
        return result;
    }
}
