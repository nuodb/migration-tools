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
package com.nuodb.migrator.job;

import com.nuodb.migrator.jdbc.dialect.Dialect;
import com.nuodb.migrator.jdbc.dialect.DialectResolver;
import com.nuodb.migrator.jdbc.dialect.IdentifierNormalizer;
import com.nuodb.migrator.jdbc.dialect.IdentifierQuoting;
import com.nuodb.migrator.jdbc.dialect.ImplicitDefaultsTranslator;
import com.nuodb.migrator.jdbc.dialect.TranslationManager;
import com.nuodb.migrator.jdbc.dialect.Translator;
import com.nuodb.migrator.jdbc.metadata.MetaDataType;
import com.nuodb.migrator.jdbc.metadata.generator.GroupScriptsBy;
import com.nuodb.migrator.jdbc.metadata.generator.NamingStrategy;
import com.nuodb.migrator.jdbc.metadata.generator.ScriptGeneratorManager;
import com.nuodb.migrator.jdbc.metadata.generator.ScriptType;
import com.nuodb.migrator.jdbc.session.Session;
import com.nuodb.migrator.jdbc.type.JdbcTypeNameMap;
import com.nuodb.migrator.spec.*;
import com.nuodb.migrator.utils.PrioritySet;

import java.sql.SQLException;
import java.util.Collection;
import java.util.Iterator;

import static com.nuodb.migrator.jdbc.JdbcUtils.close;
import static com.nuodb.migrator.jdbc.metadata.DatabaseInfos.NUODB;
import static com.nuodb.migrator.jdbc.metadata.generator.HasTablesScriptGenerator.GROUP_SCRIPTS_BY;
import static com.nuodb.migrator.jdbc.type.JdbcTypeOptions.newOptions;

/**
 * @author Sergey Bushik
 */
@SuppressWarnings("unchecked")
public abstract class ScriptGeneratorJobBase<S extends ScriptGeneratorJobSpecBase> extends HasServicesJobBase<S> {

    private ConnectionSpec sourceSpec;
    private Session sourceSession;
    private Session targetSession;

    protected ScriptGeneratorJobBase() {
    }

    protected ScriptGeneratorJobBase(S jobSpec) {
        super(jobSpec);
    }

    protected ScriptGeneratorManager createScriptGeneratorManager() throws SQLException {
        ScriptGeneratorManager scriptGeneratorManager = new ScriptGeneratorManager();
        scriptGeneratorManager.getAttributes().put(GROUP_SCRIPTS_BY, getGroupScriptsBy());
        scriptGeneratorManager.setObjectTypes(getObjectTypes());
        scriptGeneratorManager.setScriptTypes(getScriptTypes());
        ConnectionSpec sourceSpec = getSourceSpec();
        scriptGeneratorManager.setSourceCatalog(sourceSpec != null ? sourceSpec.getCatalog() : null);
        scriptGeneratorManager.setSourceSchema(sourceSpec != null ? sourceSpec.getSchema() : null);
        scriptGeneratorManager.setSourceSession(getSourceSession());

        ConnectionSpec targetSpec = getTargetSpec();
        if (targetSpec != null) {
            scriptGeneratorManager.setTargetCatalog(targetSpec.getCatalog());
            scriptGeneratorManager.setTargetSchema(targetSpec.getSchema());
        }

        DialectResolver dialectResolver = createDialectResolver();
        Dialect dialect = getTargetSession() != null ? dialectResolver.resolve(
                getTargetSession().getConnection()) : dialectResolver.resolve(NUODB);
        TranslationManager translationManager = dialect.getTranslationManager();
        PrioritySet<Translator> translators = translationManager.getTranslators();
        for (Translator translator : translators) {
            if (translator instanceof ImplicitDefaultsTranslator) {
                ((ImplicitDefaultsTranslator)translator).setUseExplicitDefaults(isUseExplicitDefaults());
            }
        }
        JdbcTypeNameMap jdbcTypeNameMap = dialect.getJdbcTypeNameMap();
        for (JdbcTypeSpec jdbcTypeSpec : getJdbcTypeSpecs()) {
            jdbcTypeNameMap.addJdbcTypeName(
                    jdbcTypeSpec.getTypeCode(), newOptions(
                    jdbcTypeSpec.getSize(), jdbcTypeSpec.getPrecision(), jdbcTypeSpec.getScale()),
                    jdbcTypeSpec.getTypeName()
            );
        }
        dialect.setIdentifierQuoting(getIdentifierQuoting());
        dialect.setIdentifierNormalizer(getIdentifierNormalizer());
        for (PrioritySet.Item<NamingStrategy> item : getNamingStrategies().items()) {
            scriptGeneratorManager.addNamingStrategy(item.getValue(), item.getPriority());
        }
        scriptGeneratorManager.setTargetDialect(dialect);
        return scriptGeneratorManager;
    }

    @Override
    public void release() throws Exception {
        close(getSourceSession());
        close(getTargetSession());
    }

    public ConnectionSpec getSourceSpec() {
        return sourceSpec;
    }

    public void setSourceSpec(ConnectionSpec sourceSpec) {
        this.sourceSpec = sourceSpec;
    }

    public Session getSourceSession() {
        return sourceSession;
    }

    public void setSourceSession(Session sourceSession) {
        this.sourceSession = sourceSession;
    }

    public Session getTargetSession() {
        return targetSession;
    }

    public void setTargetSession(Session targetSession) {
        this.targetSession = targetSession;
    }

    protected boolean isUseExplicitDefaults() {
        return getJobSpec().isUseExplicitDefaults();
    }

    protected PrioritySet<NamingStrategy> getNamingStrategies() {
        return getJobSpec().getNamingStrategies();
    }

    protected GroupScriptsBy getGroupScriptsBy() {
        return getJobSpec().getGroupScriptsBy();
    }

    protected Collection<JdbcTypeSpec> getJdbcTypeSpecs() {
        return getJobSpec().getJdbcTypeSpecs();
    }

    protected IdentifierQuoting getIdentifierQuoting() {
        return getJobSpec().getIdentifierQuoting();
    }

    protected IdentifierNormalizer getIdentifierNormalizer() {
        return getJobSpec().getIdentifierNormalizer();
    }

    protected MetaDataSpec getMetaDataSpec() {
        return getJobSpec().getMetaDataSpec();
    }

    protected Collection<MetaDataType> getObjectTypes() {
        return getJobSpec().getObjectTypes();
    }

    protected Collection<TableSpec> getTableSpecs() {
        return getJobSpec().getTableSpecs();
    }

    protected String[] getTableTypes() {
        return getJobSpec().getTableTypes();
    }

    protected Collection<ScriptType> getScriptTypes() {
        return getJobSpec().getScriptTypes();
    }

    protected ConnectionSpec getTargetSpec() {
        return getJobSpec().getTargetSpec();
    }
}
