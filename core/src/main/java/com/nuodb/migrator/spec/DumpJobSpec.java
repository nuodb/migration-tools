/**
 * Copyright (c) 2012, NuoDB, Inc.
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

import com.nuodb.migrator.jdbc.dialect.QueryLimit;

import java.util.Collection;
import java.util.TimeZone;

import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Sets.newHashSet;
import static java.util.Collections.unmodifiableSet;

/**
 * @author Sergey Bushik
 */
public class DumpJobSpec extends SchemaGeneratorJobSpecBase {

    public static final Collection<MigrationMode> MIGRATION_MODES = unmodifiableSet(newHashSet(MigrationMode.DATA));
    private Integer threads;
    private TimeZone timeZone;
    private Collection<TableSpec> tableSpecs = newArrayList();
    private Collection<QuerySpec> querySpecs = newArrayList();
    private QueryLimit queryLimit;
    private Collection<MigrationMode> migrationModes = MIGRATION_MODES;

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

    public Collection<TableSpec> getTableSpecs() {
        return tableSpecs;
    }

    public void setTableSpecs(Collection<TableSpec> tableSpecs) {
        this.tableSpecs = newArrayList(tableSpecs);
    }

    public Collection<QuerySpec> getQuerySpecs() {
        return querySpecs;
    }

    public void setQuerySpecs(Collection<QuerySpec> querySpecs) {
        this.querySpecs = newArrayList(querySpecs);
    }

    public QueryLimit getQueryLimit() {
        return queryLimit;
    }

    public void setQueryLimit(QueryLimit queryLimit) {
        this.queryLimit = queryLimit;
    }

    public Collection<MigrationMode> getMigrationModes() {
        return migrationModes;
    }

    public void setMigrationModes(Collection<MigrationMode> migrationModes) {
        this.migrationModes = migrationModes;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;

        DumpJobSpec that = (DumpJobSpec) o;

        if (migrationModes != null ? !migrationModes.equals(that.migrationModes) : that.migrationModes != null)
            return false;
        if (queryLimit != null ? !queryLimit.equals(that.queryLimit) : that.queryLimit != null) return false;
        if (querySpecs != null ? !querySpecs.equals(that.querySpecs) : that.querySpecs != null) return false;
        if (tableSpecs != null ? !tableSpecs.equals(that.tableSpecs) : that.tableSpecs != null) return false;
        if (threads != null ? !threads.equals(that.threads) : that.threads != null) return false;
        if (timeZone != null ? !timeZone.equals(that.timeZone) : that.timeZone != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + (threads != null ? threads.hashCode() : 0);
        result = 31 * result + (timeZone != null ? timeZone.hashCode() : 0);
        result = 31 * result + (tableSpecs != null ? tableSpecs.hashCode() : 0);
        result = 31 * result + (querySpecs != null ? querySpecs.hashCode() : 0);
        result = 31 * result + (queryLimit != null ? queryLimit.hashCode() : 0);
        result = 31 * result + (migrationModes != null ? migrationModes.hashCode() : 0);
        return result;
    }
}
