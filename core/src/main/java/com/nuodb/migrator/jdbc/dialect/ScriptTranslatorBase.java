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
package com.nuodb.migrator.jdbc.dialect;

import com.nuodb.migrator.jdbc.resolve.DatabaseInfo;

/**
 * @author Sergey Bushik
 */
public abstract class ScriptTranslatorBase implements ScriptTranslator {

    private DatabaseInfo sourceDatabaseInfo;
    private DatabaseInfo targetDatabaseInfo;

    protected ScriptTranslatorBase() {
    }

    protected ScriptTranslatorBase(DatabaseInfo sourceDatabaseInfo) {
        this.sourceDatabaseInfo = sourceDatabaseInfo;
    }

    protected ScriptTranslatorBase(DatabaseInfo sourceDatabaseInfo, DatabaseInfo targetDatabaseInfo) {
        this.sourceDatabaseInfo = sourceDatabaseInfo;
        this.targetDatabaseInfo = targetDatabaseInfo;
    }

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

    @Override
    public boolean canTranslateScript(Script sourceScript, DatabaseInfo targetDatabaseInfo) {
        return (this.sourceDatabaseInfo == null || this.sourceDatabaseInfo.isInherited(sourceScript.getDatabaseInfo())
                && (this.targetDatabaseInfo == null || this.targetDatabaseInfo.isInherited(targetDatabaseInfo)));
    }

    @Override
    public Script translateScript(Script sourceScript, DatabaseInfo targetDatabaseInfo) {
        if (canTranslateScript(sourceScript, targetDatabaseInfo)) {
            return getScriptTranslation(sourceScript, targetDatabaseInfo);
        } else {
            return null;
        }
    }

    protected Script getScriptTranslation(Script sourceScript, DatabaseInfo targetDatabaseInfo) {
        String translation = getScriptTranslation(sourceScript.getScript(), sourceScript.getDatabaseInfo(),
                targetDatabaseInfo);
        if (translation != null) {
            return new SimpleScript(translation, targetDatabaseInfo);
        } else {
            return null;
        }
    }

    protected abstract String getScriptTranslation(String sourceScript, DatabaseInfo sourceDatabaseInfo,
                                                   DatabaseInfo targetDatabaseInfo);

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ScriptTranslatorBase that = (ScriptTranslatorBase) o;

        if (sourceDatabaseInfo != null ? !sourceDatabaseInfo.equals(
                that.sourceDatabaseInfo) : that.sourceDatabaseInfo != null) {
            return false;
        }
        if (targetDatabaseInfo != null ? !targetDatabaseInfo.equals(
                that.targetDatabaseInfo) : that.targetDatabaseInfo != null) {
            return false;
        }
        return true;
    }


    @Override
    public int hashCode() {
        int result = sourceDatabaseInfo != null ? sourceDatabaseInfo.hashCode() : 0;
        result = 31 * result + (targetDatabaseInfo != null ? targetDatabaseInfo.hashCode() : 0);
        return result;
    }
}
