/**
 * Copyright (c) 2015, NuoDB, Inc.
 * All rights reserved.
 *
 * Redistribution and use in connectionSpec and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *     * Redistributions of connectionSpec code must retain the above copyright
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

import com.nuodb.migrator.backup.writer.BackupWriterListener;
import com.nuodb.migrator.jdbc.metadata.MetaDataType;
import com.nuodb.migrator.jdbc.metadata.filter.MetaDataFilterManager;
import com.nuodb.migrator.jdbc.query.QueryLimit;

import java.util.Collection;
import java.util.TimeZone;

import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Sets.newHashSet;
import static com.nuodb.migrator.spec.MigrationMode.DATA;
import static com.nuodb.migrator.spec.MigrationMode.SCHEMA;

/**
 * @author Sergey Bushik
 */
public class DumpJobSpec extends JobSpecBase {

    private Collection<BackupWriterListener> listeners = newArrayList();
    private Collection<MigrationMode> migrationModes = newHashSet(DATA, SCHEMA);
    private Integer threads;
    private TimeZone timeZone;
    private ConnectionSpec sourceSpec;
    private ResourceSpec outputSpec;
    private MetaDataSpec metaDataSpec = new MetaDataSpec();
    private Collection<QuerySpec> querySpecs = newArrayList();
    private QueryLimit queryLimit;

    public void addListener(BackupWriterListener listener) {
        listeners.add(listener);
    }

    public void removeListener(BackupWriterListener listener) {
        listeners.remove(listener);
    }

    public Collection<BackupWriterListener> getListeners() {
        return listeners;
    }

    public Collection<MigrationMode> getMigrationModes() {
        return migrationModes;
    }

    public void setMigrationModes(Collection<MigrationMode> migrationModes) {
        this.migrationModes = migrationModes;
    }

    public Integer getThreads() {
        return threads;
    }

    public void setThreads(Integer threads) {
        this.threads = threads;
    }

    public TimeZone getTimeZone() {
        return timeZone;
    }

    public void setTimeZone(TimeZone timeZone) {
        this.timeZone = timeZone;
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

    public MetaDataFilterManager getMetaDataFilterManager() {
        return metaDataSpec.getMetaDataFilterManager();
    }

    public String[] getTableTypes() {
        return metaDataSpec.getTableTypes();
    }

    public void setMetaDataFilterManager(MetaDataFilterManager metaDataFilterManager) {
        metaDataSpec.setMetaDataFilterManager(metaDataFilterManager);
    }

    public void setObjectTypes(Collection<MetaDataType> objectTypes) {
        metaDataSpec.setObjectTypes(objectTypes);
    }

    public Collection<QuerySpec> getQuerySpecs() {
        return querySpecs;
    }

    public void setQuerySpecs(Collection<QuerySpec> querySpecs) {
        this.querySpecs = querySpecs;
    }

    public QueryLimit getQueryLimit() {
        return queryLimit;
    }

    public void setQueryLimit(QueryLimit queryLimit) {
        this.queryLimit = queryLimit;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        if (!super.equals(o))
            return false;

        DumpJobSpec that = (DumpJobSpec) o;

        if (metaDataSpec != null ? !metaDataSpec.equals(that.metaDataSpec) : that.metaDataSpec != null)
            return false;
        if (migrationModes != null ? !migrationModes.equals(that.migrationModes) : that.migrationModes != null)
            return false;
        if (outputSpec != null ? !outputSpec.equals(that.outputSpec) : that.outputSpec != null)
            return false;
        if (queryLimit != null ? !queryLimit.equals(that.queryLimit) : that.queryLimit != null)
            return false;
        if (querySpecs != null ? !querySpecs.equals(that.querySpecs) : that.querySpecs != null)
            return false;
        if (sourceSpec != null ? !sourceSpec.equals(that.sourceSpec) : that.sourceSpec != null)
            return false;
        if (threads != null ? !threads.equals(that.threads) : that.threads != null)
            return false;
        if (timeZone != null ? !timeZone.equals(that.timeZone) : that.timeZone != null)
            return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + (migrationModes != null ? migrationModes.hashCode() : 0);
        result = 31 * result + (threads != null ? threads.hashCode() : 0);
        result = 31 * result + (timeZone != null ? timeZone.hashCode() : 0);
        result = 31 * result + (sourceSpec != null ? sourceSpec.hashCode() : 0);
        result = 31 * result + (outputSpec != null ? outputSpec.hashCode() : 0);
        result = 31 * result + (metaDataSpec != null ? metaDataSpec.hashCode() : 0);
        result = 31 * result + (querySpecs != null ? querySpecs.hashCode() : 0);
        result = 31 * result + (queryLimit != null ? queryLimit.hashCode() : 0);
        return result;
    }
}
