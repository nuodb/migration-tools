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

import com.nuodb.migrator.backup.BackupOps;

import java.util.Collection;

import static com.google.common.collect.Sets.newHashSet;

/**
 * @author Sergey Bushik
 */
public class SchemaJobSpec extends ScriptGeneratorJobSpecBase {

    public static final boolean FAIL_ON_EMPTY_DATABASE_DEFAULT = true;

    private boolean failOnEmptyDatabase = FAIL_ON_EMPTY_DATABASE_DEFAULT;
    private BackupOps backupOps;
    private ConnectionSpec sourceSpec;
    private ResourceSpec outputSpec;
    private Collection<MigrationMode> migrationModes = newHashSet(MigrationMode.DATA);

    public boolean isFailOnEmptyDatabase() {
        return failOnEmptyDatabase;
    }

    public void setFailOnEmptyDatabase(boolean failOnEmptyDatabase) {
        this.failOnEmptyDatabase = failOnEmptyDatabase;
    }

    public BackupOps getBackupOps() {
        return backupOps;
    }

    public void setBackupOps(BackupOps backupOps) {
        this.backupOps = backupOps;
    }

    public ConnectionSpec getSourceSpec() {
        return sourceSpec;
    }

    public void setSourceSpec(ConnectionSpec sourceSpec) {
        this.sourceSpec = sourceSpec;
    }

    public ResourceSpec getOutputSpec() {
        return outputSpec;
    }

    public void setOutputSpec(ResourceSpec outputSpec) {
        this.outputSpec = outputSpec;
    }

    public Collection<MigrationMode> getMigrationModes() {
        return migrationModes;
    }

    public void setMigrationModes(Collection<MigrationMode> migrationModes) {
        this.migrationModes = migrationModes;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        if (!super.equals(o))
            return false;

        SchemaJobSpec that = (SchemaJobSpec) o;

        if (failOnEmptyDatabase != that.failOnEmptyDatabase)
            return false;
        if (backupOps != null ? !backupOps.equals(that.backupOps) : that.backupOps != null)
            return false;
        if (migrationModes != null ? !migrationModes.equals(that.migrationModes) : that.migrationModes != null)
            return false;
        if (outputSpec != null ? !outputSpec.equals(that.outputSpec) : that.outputSpec != null)
            return false;
        if (sourceSpec != null ? !sourceSpec.equals(that.sourceSpec) : that.sourceSpec != null)
            return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + (failOnEmptyDatabase ? 1 : 0);
        result = 31 * result + (backupOps != null ? backupOps.hashCode() : 0);
        result = 31 * result + (sourceSpec != null ? sourceSpec.hashCode() : 0);
        result = 31 * result + (outputSpec != null ? outputSpec.hashCode() : 0);
        result = 31 * result + (migrationModes != null ? migrationModes.hashCode() : 0);
        return result;
    }
}
