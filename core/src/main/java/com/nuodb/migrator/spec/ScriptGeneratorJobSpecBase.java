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
package com.nuodb.migrator.spec;

import com.nuodb.migrator.jdbc.dialect.IdentifierNormalizer;
import com.nuodb.migrator.jdbc.dialect.IdentifierQuoting;
import com.nuodb.migrator.jdbc.dialect.TranslationConfig;
import com.nuodb.migrator.jdbc.metadata.MetaDataType;
import com.nuodb.migrator.jdbc.metadata.filter.MetaDataFilterManager;
import com.nuodb.migrator.jdbc.metadata.generator.GroupScriptsBy;
import com.nuodb.migrator.jdbc.metadata.generator.NamingStrategy;
import com.nuodb.migrator.jdbc.metadata.generator.ScriptType;
import com.nuodb.migrator.utils.PrioritySet;

import java.util.Collection;

import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Sets.newHashSet;
import static com.nuodb.migrator.jdbc.dialect.IdentifierNormalizers.NOOP;
import static com.nuodb.migrator.jdbc.dialect.IdentifierQuotings.ALWAYS;
import static com.nuodb.migrator.utils.Collections.newPrioritySet;

/**
 * @author Sergey Bushik
 */
public class ScriptGeneratorJobSpecBase extends JobSpecBase {

    private MetaDataFilterManager metaDataFilterManager = new MetaDataFilterManager();
    private GroupScriptsBy groupScriptsBy = GroupScriptsBy.TABLE;
    private Collection<JdbcTypeSpec> jdbcTypeSpecs = newArrayList();
    private MetaDataSpec metaDataSpec = new MetaDataSpec();
    private PrioritySet<NamingStrategy> namingStrategies = newPrioritySet();
    private IdentifierQuoting identifierQuoting = ALWAYS;
    private IdentifierNormalizer identifierNormalizer = NOOP;
    private Collection<ScriptType> scriptTypes = newHashSet(ScriptType.values());
    private ConnectionSpec targetSpec;
    private TranslationConfig translationConfig = new TranslationConfig();

    public MetaDataFilterManager getMetaDataFilterManager() {
        return metaDataFilterManager;
    }

    public void setMetaDataFilterManager(MetaDataFilterManager metaDataFilterManager) {
        this.metaDataFilterManager = metaDataFilterManager;
    }

    public MetaDataSpec getMetaDataSpec() {
        return metaDataSpec;
    }

    public void setMetaDataSpec(MetaDataSpec metaDataSpec) {
        this.metaDataSpec = metaDataSpec;
    }

    public Collection<MetaDataType> getObjectTypes() {
        return metaDataSpec.getObjectTypes();
    }

    public void setTableTypes(String[] tableTypes) {
        metaDataSpec.setTableTypes(tableTypes);
    }

    public String[] getTableTypes() {
        return metaDataSpec.getTableTypes();
    }

    public void setObjectTypes(Collection<MetaDataType> objectTypes) {
        metaDataSpec.setObjectTypes(objectTypes);
    }

    public TranslationConfig getTranslationConfig() {
        return translationConfig;
    }

    public void setTranslationConfig(TranslationConfig translationConfig) {
        this.translationConfig = translationConfig;
    }

    public ConnectionSpec getTargetSpec() {
        return targetSpec;
    }

    public void setTargetSpec(ConnectionSpec targetSpec) {
        this.targetSpec = targetSpec;
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

    public PrioritySet<NamingStrategy> getNamingStrategies() {
        return namingStrategies;
    }

    public void setNamingStrategies(PrioritySet<NamingStrategy> namingStrategies) {
        this.namingStrategies = namingStrategies;
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
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        if (!super.equals(o))
            return false;

        ScriptGeneratorJobSpecBase that = (ScriptGeneratorJobSpecBase) o;

        if (metaDataFilterManager != null ? !metaDataFilterManager.equals(that.metaDataFilterManager)
                : that.metaDataFilterManager != null)
            return false;
        if (groupScriptsBy != that.groupScriptsBy)
            return false;
        if (identifierNormalizer != null ? !identifierNormalizer.equals(that.identifierNormalizer)
                : that.identifierNormalizer != null)
            return false;
        if (identifierQuoting != null ? !identifierQuoting.equals(that.identifierQuoting)
                : that.identifierQuoting != null)
            return false;
        if (jdbcTypeSpecs != null ? !jdbcTypeSpecs.equals(that.jdbcTypeSpecs) : that.jdbcTypeSpecs != null)
            return false;
        if (metaDataSpec != null ? !metaDataSpec.equals(that.metaDataSpec) : that.metaDataSpec != null)
            return false;
        if (namingStrategies != null ? !namingStrategies.equals(that.namingStrategies) : that.namingStrategies != null)
            return false;
        if (scriptTypes != null ? !scriptTypes.equals(that.scriptTypes) : that.scriptTypes != null)
            return false;
        if (targetSpec != null ? !targetSpec.equals(that.targetSpec) : that.targetSpec != null)
            return false;
        if (translationConfig != null ? !translationConfig.equals(that.translationConfig)
                : that.translationConfig != null)
            return false;
        return true;
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + (metaDataFilterManager != null ? metaDataFilterManager.hashCode() : 0);
        result = 31 * result + (scriptTypes != null ? scriptTypes.hashCode() : 0);
        result = 31 * result + (groupScriptsBy != null ? groupScriptsBy.hashCode() : 0);
        result = 31 * result + (jdbcTypeSpecs != null ? jdbcTypeSpecs.hashCode() : 0);
        result = 31 * result + (identifierQuoting != null ? identifierQuoting.hashCode() : 0);
        result = 31 * result + (identifierNormalizer != null ? identifierNormalizer.hashCode() : 0);
        result = 31 * result + (metaDataSpec != null ? metaDataSpec.hashCode() : 0);
        result = 31 * result + (namingStrategies != null ? namingStrategies.hashCode() : 0);
        result = 31 * result + (targetSpec != null ? targetSpec.hashCode() : 0);
        result = 31 * result + (translationConfig != null ? translationConfig.hashCode() : 0);
        return result;
    }
}
