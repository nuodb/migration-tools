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

import com.google.common.collect.Lists;
import com.nuodb.migrator.jdbc.dialect.IdentifierNormalizer;
import com.nuodb.migrator.jdbc.dialect.IdentifierQuoting;
import com.nuodb.migrator.jdbc.metadata.MetaDataType;
import com.nuodb.migrator.jdbc.metadata.generator.GroupScriptsBy;
import com.nuodb.migrator.jdbc.metadata.generator.ScriptType;

import java.util.Arrays;
import java.util.Collection;

import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Sets.newHashSet;
import static com.google.common.collect.Sets.newLinkedHashSet;
import static com.nuodb.migrator.jdbc.dialect.IdentifierNormalizers.NOOP;
import static com.nuodb.migrator.jdbc.dialect.IdentifierQuotings.ALWAYS;
import static com.nuodb.migrator.jdbc.dialect.ImplicitDefaultsTranslator.USE_EXPLICIT_DEFAULTS;
import static com.nuodb.migrator.jdbc.metadata.Table.ALIAS;
import static com.nuodb.migrator.jdbc.metadata.Table.TABLE;

/**
 * @author Sergey Bushik
 */
public class SchemaGeneratorJobSpecBase extends JobSpecBase {

    private boolean useExplicitDefaults = USE_EXPLICIT_DEFAULTS;
    private String[] tableTypes = new String[]{TABLE, ALIAS};
    private ConnectionSpec sourceSpec;
    private ConnectionSpec targetSpec;
    private ResourceSpec outputSpec;
    private Collection<MetaDataType> objectTypes = newArrayList(MetaDataType.TYPES);
    private Collection<ScriptType> scriptTypes = newHashSet(ScriptType.values());
    private GroupScriptsBy groupScriptsBy = GroupScriptsBy.TABLE;
    private Collection<JdbcTypeSpec> jdbcTypeSpecs = newHashSet();
    private IdentifierQuoting identifierQuoting = ALWAYS;
    private IdentifierNormalizer identifierNormalizer = NOOP;

    public boolean isUseExplicitDefaults() {
        return useExplicitDefaults;
    }

    public void setUseExplicitDefaults(boolean useExplicitDefaults) {
        this.useExplicitDefaults = useExplicitDefaults;
    }

    public String[] getTableTypes() {
        return tableTypes;
    }

    public void setTableTypes(String[] tableTypes) {
        this.tableTypes = tableTypes;
    }

    public ConnectionSpec getSourceSpec() {
        return sourceSpec;
    }

    public void setSourceSpec(ConnectionSpec sourceSpec) {
        this.sourceSpec = sourceSpec;
    }

    public ConnectionSpec getTargetSpec() {
        return targetSpec;
    }

    public void setTargetSpec(ConnectionSpec targetSpec) {
        this.targetSpec = targetSpec;
    }

    public ResourceSpec getOutputSpec() {
        return outputSpec;
    }

    public void setOutputSpec(ResourceSpec outputSpec) {
        this.outputSpec = outputSpec;
    }

    public Collection<MetaDataType> getObjectTypes() {
        return objectTypes;
    }

    public void setObjectTypes(Collection<MetaDataType> objectTypes) {
        this.objectTypes = newHashSet(objectTypes);
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

        SchemaGeneratorJobSpecBase that = (SchemaGeneratorJobSpecBase) o;

        if (useExplicitDefaults != that.useExplicitDefaults) return false;
        if (groupScriptsBy != that.groupScriptsBy) return false;
        if (identifierNormalizer != null ? !identifierNormalizer.equals(that.identifierNormalizer) :
                that.identifierNormalizer != null) return false;
        if (identifierQuoting != null ? !identifierQuoting.equals(that.identifierQuoting) :
                that.identifierQuoting != null)
            return false;
        if (jdbcTypeSpecs != null ? !jdbcTypeSpecs.equals(that.jdbcTypeSpecs) : that.jdbcTypeSpecs != null)
            return false;
        if (objectTypes != null ? !objectTypes.equals(that.objectTypes) : that.objectTypes != null) return false;
        if (outputSpec != null ? !outputSpec.equals(that.outputSpec) : that.outputSpec != null) return false;
        if (scriptTypes != null ? !scriptTypes.equals(that.scriptTypes) : that.scriptTypes != null) return false;
        if (sourceSpec != null ? !sourceSpec.equals(that.sourceSpec) : that.sourceSpec != null) return false;
        if (!Arrays.equals(tableTypes, that.tableTypes)) return false;
        if (targetSpec != null ? !targetSpec.equals(that.targetSpec) : that.targetSpec != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + (useExplicitDefaults ? 1 : 0);
        result = 31 * result + (tableTypes != null ? Arrays.hashCode(tableTypes) : 0);
        result = 31 * result + (sourceSpec != null ? sourceSpec.hashCode() : 0);
        result = 31 * result + (targetSpec != null ? targetSpec.hashCode() : 0);
        result = 31 * result + (outputSpec != null ? outputSpec.hashCode() : 0);
        result = 31 * result + (objectTypes != null ? objectTypes.hashCode() : 0);
        result = 31 * result + (scriptTypes != null ? scriptTypes.hashCode() : 0);
        result = 31 * result + (groupScriptsBy != null ? groupScriptsBy.hashCode() : 0);
        result = 31 * result + (jdbcTypeSpecs != null ? jdbcTypeSpecs.hashCode() : 0);
        result = 31 * result + (identifierQuoting != null ? identifierQuoting.hashCode() : 0);
        result = 31 * result + (identifierNormalizer != null ? identifierNormalizer.hashCode() : 0);
        return result;
    }
}
