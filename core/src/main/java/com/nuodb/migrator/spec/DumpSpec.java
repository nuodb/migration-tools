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

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import java.util.Collection;
import java.util.Collections;
import java.util.TimeZone;

import static com.nuodb.migrator.jdbc.metadata.Table.TABLE;

/**
 * @author Sergey Bushik
 */
public class DumpSpec extends TaskSpecBase {

    private ConnectionSpec connectionSpec;
    private TimeZone timeZone;
    private Collection<String> tableTypes = Collections.singleton(TABLE);
    private Collection<SelectQuerySpec> selectQuerySpecs = Lists.newArrayList();
    private Collection<NativeQuerySpec> nativeQuerySpecs = Lists.newArrayList();
    private ResourceSpec outputSpec;

    public ConnectionSpec getConnectionSpec() {
        return connectionSpec;
    }

    public void setConnectionSpec(ConnectionSpec connectionSpec) {
        this.connectionSpec = connectionSpec;
    }

    public TimeZone getTimeZone() {
        return timeZone;
    }

    public void setTimeZone(TimeZone timeZone) {
        this.timeZone = timeZone;
    }

    public Collection<String> getTableTypes() {
        return tableTypes;
    }

    public void setTableTypes(Collection<String> tableTypes) {
        this.tableTypes = Sets.newLinkedHashSet(tableTypes);
    }

    public Collection<SelectQuerySpec> getSelectQuerySpecs() {
        return selectQuerySpecs;
    }

    public void setSelectQuerySpecs(Collection<SelectQuerySpec> selectQuerySpecs) {
        this.selectQuerySpecs = Lists.newArrayList(selectQuerySpecs);
    }

    public Collection<NativeQuerySpec> getNativeQuerySpecs() {
        return nativeQuerySpecs;
    }

    public void setNativeQuerySpecs(Collection<NativeQuerySpec> nativeQuerySpecs) {
        this.nativeQuerySpecs = Lists.newArrayList(nativeQuerySpecs);
    }

    public ResourceSpec getOutputSpec() {
        return outputSpec;
    }

    public void setOutputSpec(ResourceSpec outputSpec) {
        this.outputSpec = outputSpec;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;

        DumpSpec dumpSpec = (DumpSpec) o;

        if (connectionSpec != null ? !connectionSpec.equals(dumpSpec.connectionSpec) : dumpSpec.connectionSpec != null)
            return false;
        if (nativeQuerySpecs != null ? !nativeQuerySpecs.equals(
                dumpSpec.nativeQuerySpecs) : dumpSpec.nativeQuerySpecs != null) return false;
        if (outputSpec != null ? !outputSpec.equals(dumpSpec.outputSpec) : dumpSpec.outputSpec != null) return false;
        if (selectQuerySpecs != null ? !selectQuerySpecs.equals(
                dumpSpec.selectQuerySpecs) : dumpSpec.selectQuerySpecs != null) return false;
        if (tableTypes != null ? !tableTypes.equals(dumpSpec.tableTypes) : dumpSpec.tableTypes != null) return false;
        if (timeZone != null ? !timeZone.equals(dumpSpec.timeZone) : dumpSpec.timeZone != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + (connectionSpec != null ? connectionSpec.hashCode() : 0);
        result = 31 * result + (timeZone != null ? timeZone.hashCode() : 0);
        result = 31 * result + (tableTypes != null ? tableTypes.hashCode() : 0);
        result = 31 * result + (selectQuerySpecs != null ? selectQuerySpecs.hashCode() : 0);
        result = 31 * result + (nativeQuerySpecs != null ? nativeQuerySpecs.hashCode() : 0);
        result = 31 * result + (outputSpec != null ? outputSpec.hashCode() : 0);
        return result;
    }
}
