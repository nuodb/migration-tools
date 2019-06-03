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
package com.nuodb.migrator.jdbc.dialect;

import com.nuodb.migrator.jdbc.metadata.DatabaseInfo;
import com.nuodb.migrator.jdbc.url.JdbcUrl;
import com.nuodb.migrator.spec.ConnectionSpec;
import com.nuodb.migrator.spec.DriverConnectionSpec;

import java.sql.Connection;

/**
 * @author Sergey Bushik
 */
@SuppressWarnings("unchecked")
public abstract class TranslatorBase<S extends Script> implements Translator<S> {

    private DatabaseInfo sourceDatabaseInfo;
    private DatabaseInfo targetDatabaseInfo;
    private Class<? extends Script> scriptClass;

    protected TranslatorBase() {
    }

    protected TranslatorBase(DatabaseInfo sourceDatabaseInfo) {
        this.sourceDatabaseInfo = sourceDatabaseInfo;
    }

    protected TranslatorBase(DatabaseInfo sourceDatabaseInfo, DatabaseInfo targetDatabaseInfo) {
        this.sourceDatabaseInfo = sourceDatabaseInfo;
        this.targetDatabaseInfo = targetDatabaseInfo;
    }

    protected TranslatorBase(DatabaseInfo sourceDatabaseInfo, Class<? extends Script> scriptClass) {
        this.sourceDatabaseInfo = sourceDatabaseInfo;
        this.scriptClass = scriptClass;
    }

    protected TranslatorBase(DatabaseInfo sourceDatabaseInfo, DatabaseInfo targetDatabaseInfo,
            Class<? extends Script> scriptClass) {
        this.sourceDatabaseInfo = sourceDatabaseInfo;
        this.targetDatabaseInfo = targetDatabaseInfo;
        this.scriptClass = scriptClass;
    }

    public static Connection getConnection(TranslationContext context) {
        return context.getSession().getConnection();
    }

    public static ConnectionSpec getConnectionSpec(TranslationContext context) {
        return context.getSession().getConnectionSpec();
    }

    public static JdbcUrl getJdbcUrl(TranslationContext context) {
        return ((DriverConnectionSpec) getConnectionSpec(context)).getJdbcUrl();
    }

    @Override
    public boolean supports(Script script, TranslationContext context) {
        return supportsDatabase(context) && supportsScriptClass(script) && supportsScript((S) script, context);
    }

    protected boolean supportsDatabase(TranslationContext context) {
        return (sourceDatabaseInfo == null || sourceDatabaseInfo.isAssignable(context.getSession().getDatabaseInfo()))
                && (targetDatabaseInfo == null
                        || targetDatabaseInfo.isAssignable(context.getDialect().getDatabaseInfo()));
    }

    protected boolean supportsScriptClass(Script script) {
        return script != null && (scriptClass == null || scriptClass.isAssignableFrom(script.getClass()));
    }

    protected abstract boolean supportsScript(S script, TranslationContext context);

    public DatabaseInfo getSourceDatabaseInfo() {
        return sourceDatabaseInfo;
    }

    public void setSourceDatabaseInfo(DatabaseInfo sourceDatabaseInfo) {
        this.sourceDatabaseInfo = sourceDatabaseInfo;
    }

    public DatabaseInfo getTargetDatabaseInfo() {
        return targetDatabaseInfo;
    }

    public void setTargetDatabaseInfo(DatabaseInfo targetDatabaseInfo) {
        this.targetDatabaseInfo = targetDatabaseInfo;
    }

    public Class<? extends Script> getScriptClass() {
        return scriptClass;
    }

    public void setScriptClass(Class<? extends Script> scriptClass) {
        this.scriptClass = scriptClass;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        TranslatorBase that = (TranslatorBase) o;

        if (sourceDatabaseInfo != null ? !sourceDatabaseInfo.equals(that.sourceDatabaseInfo)
                : that.sourceDatabaseInfo != null)
            return false;
        if (targetDatabaseInfo != null ? !targetDatabaseInfo.equals(that.targetDatabaseInfo)
                : that.targetDatabaseInfo != null)
            return false;
        if (scriptClass != null ? !scriptClass.equals(that.scriptClass) : that.scriptClass != null)
            return false;
        return true;
    }

    @Override
    public int hashCode() {
        int result = sourceDatabaseInfo != null ? sourceDatabaseInfo.hashCode() : 0;
        result = 31 * result + (targetDatabaseInfo != null ? targetDatabaseInfo.hashCode() : 0);
        result = 31 * result + (scriptClass != null ? scriptClass.hashCode() : 0);
        return result;
    }
}
