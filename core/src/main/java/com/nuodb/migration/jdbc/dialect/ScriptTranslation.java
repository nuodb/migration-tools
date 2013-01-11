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
package com.nuodb.migration.jdbc.dialect;

import com.nuodb.migration.jdbc.resolve.DatabaseInfo;

/**
 * @author Sergey Bushik
 */
public class ScriptTranslation {

    private Dialect sourceDialect;
    private Dialect targetDialect;
    private DatabaseInfo sourceDatabaseInfo;
    private DatabaseInfo targetDatabaseInfo;
    private String sourceScript;
    private String targetScript;

    public ScriptTranslation(Dialect sourceDialect, DatabaseInfo targetDatabaseInfo, String sourceScript,
                             String targetScript) {
        this.sourceDialect = sourceDialect;
        this.targetDatabaseInfo = targetDatabaseInfo;
        this.sourceScript = sourceScript;
        this.targetScript = targetScript;
        this.sourceDatabaseInfo = sourceDialect.getDatabaseInfo();
    }

    public ScriptTranslation(DatabaseInfo sourceDatabaseInfo, Dialect targetDialect, String sourceScript,
                             String targetScript) {
        this.sourceDatabaseInfo = sourceDatabaseInfo;
        this.targetDialect = targetDialect;
        this.sourceScript = sourceScript;
        this.targetScript = targetScript;
        this.targetDatabaseInfo = targetDialect.getDatabaseInfo();
    }

    public ScriptTranslation(DatabaseInfo sourceDatabaseInfo, DatabaseInfo targetDatabaseInfo,
                             String sourceScript, String targetScript) {
        this.sourceDatabaseInfo = sourceDatabaseInfo;
        this.targetDatabaseInfo = targetDatabaseInfo;
        this.sourceScript = sourceScript;
        this.targetScript = targetScript;
    }

    public ScriptTranslation(Dialect sourceDialect, Dialect targetDialect,
                             String sourceScript, String targetScript) {
        this.sourceDialect = sourceDialect;
        this.targetDialect = targetDialect;
        this.sourceScript = sourceScript;
        this.targetScript = targetScript;
        this.sourceDatabaseInfo = sourceDialect.getDatabaseInfo();
        this.targetDatabaseInfo = targetDialect.getDatabaseInfo();
    }

    public Dialect getSourceDialect() {
        return sourceDialect;
    }

    public Dialect getTargetDialect() {
        return targetDialect;
    }

    public DatabaseInfo getSourceDatabaseInfo() {
        return sourceDatabaseInfo;
    }

    public DatabaseInfo getTargetDatabaseInfo() {
        return targetDatabaseInfo;
    }

    public String getSourceScript() {
        return sourceScript;
    }

    public String getTargetScript() {
        return targetScript;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ScriptTranslation that = (ScriptTranslation) o;

        if (sourceDatabaseInfo != null ? !sourceDatabaseInfo.equals(
                that.sourceDatabaseInfo) : that.sourceDatabaseInfo != null) return false;
        if (sourceDialect != null ? !sourceDialect.equals(that.sourceDialect) : that.sourceDialect != null)
            return false;
        if (sourceScript != null ? !sourceScript.equals(that.sourceScript) : that.sourceScript != null) return false;
        if (targetDatabaseInfo != null ? !targetDatabaseInfo.equals(
                that.targetDatabaseInfo) : that.targetDatabaseInfo != null) return false;
        if (targetDialect != null ? !targetDialect.equals(that.targetDialect) : that.targetDialect != null)
            return false;
        if (targetScript != null ? !targetScript.equals(that.targetScript) : that.targetScript != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = sourceDatabaseInfo != null ? sourceDatabaseInfo.hashCode() : 0;
        result = 31 * result + (targetDatabaseInfo != null ? targetDatabaseInfo.hashCode() : 0);
        result = 31 * result + (sourceScript != null ? sourceScript.hashCode() : 0);
        result = 31 * result + (targetScript != null ? targetScript.hashCode() : 0);
        return result;
    }
}